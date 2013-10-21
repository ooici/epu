# Copyright 2013 University of Chicago

import logging
import threading
import heapq
from collections import namedtuple
from datetime import timedelta

from epu.util import now_datetime, ensure_timedelta
from epu.states import ProcessState, ProcessDispatcherState, ExecutionResourceState
from epu import tevent

log = logging.getLogger(__name__)


class PDDoctor(object):
    """The doctor is a singleton process (enforced by ZK leader election)
    that monitors and manages the health of the Process Dispatcher and its
    execution resources.

    Responsibilities:
    - Coordinates Process Dispatcher bootstrap and recovery. On startup, the
      doctor establishes the current state of the system before allowing any
      requests to be processed.
    - Tracks heartbeats from Execution Engine Agents (EEAgents). When an agent
      hasn't been heard from in too long, the agent is marked as disabled.
    """

    _PROCESS_STATES_TO_REQUEUE = (ProcessState.REQUESTED,
        ProcessState.DIED_REQUESTED, ProcessState.WAITING)

    CONFIG_MONITOR_HEARTBEATS = "monitor_resource_heartbeats"

    def __init__(self, core, store, config=None):
        """
        @type core: ProcessDispatcherCore
        @type store: ProcessDispatcherStore
        """
        self.core = core
        self.store = store
        self.config = config or {}

        self.monitor = None
        self.monitor_thread = None

        self.condition = threading.Condition()

        self.is_leader = False
        self.watching_system_boot = False

    def start_election(self):
        """Initiates participation in the leader election"""
        self.store.contend_doctor(self)

    def inaugurate(self):
        """Callback from the election fired when this leader is elected

        This routine is expected to never return as long as we want to
        remain the leader.
        """
        with self.condition:
            if self.is_leader:
                raise Exception("Elected as Doctor but already initialized. This worker is in an inconsistent state!")

        try:
            self.is_leader = True

            self._initialize()
        finally:
            # ensure flag is cleared in case of error
            self.is_leader = False

    def _initialize(self):

        pd_state = self.store.get_pd_state()
        log.info("Elected as Process Dispatcher Doctor. State is %s", pd_state)
        if pd_state == ProcessDispatcherState.UNINITIALIZED:
            self.initialize_pd()
            pd_state = self.store.get_pd_state()

        if pd_state == ProcessDispatcherState.SYSTEM_BOOTING:
            self.watching_system_boot = True
            self._watch_system_boot()

        with self.condition:
            if self.is_leader and self.config.get(self.CONFIG_MONITOR_HEARTBEATS, True):
                self.monitor = ExecutionResourceMonitor(self.core, self.store)
                self.monitor_thread = tevent.spawn(self.monitor.monitor)

        while self.is_leader:
            with self.condition:
                self.condition.wait()

        log.debug("Waiting on monitor thread to exit")
        if self.monitor_thread is not None:
            self.monitor_thread.join()
        self.monitor = None
        self.monitor_thread = None

    def cancel(self):
        with self.condition:
            self.is_leader = False
            if self.monitor:
                self.monitor.cancel()
            self.condition.notify_all()

    def _watch_system_boot(self, *args):
        if not (self.is_leader and self.watching_system_boot):
            return

        system_boot = self.store.is_system_boot(watcher=self._watch_system_boot)
        pd_state = self.store.get_pd_state()
        if not system_boot and pd_state == ProcessDispatcherState.SYSTEM_BOOTING:
            self.schedule_pending_processes()
            self.store.set_pd_state(ProcessDispatcherState.OK)
            self.watching_system_boot = False

    def initialize_pd(self):

        system_boot = self.store.is_system_boot()

        # for system boot we clear out all nodes and roll relevant processes
        # back to UNSCHEDULED_PENDING state. Then, after system boot is done,
        # we requeue all of these UNSCHEDULED_PENDING processes
        if system_boot:
            # clear out all nodes
            for node_id in self.store.get_node_ids():
                node = self.store.get_node(node_id)
                if node is None:
                    continue

                # evacuate the node. Move restartable processes to
                # UNSCHEDULED_PENDING. They will be restarted after system
                # boot completes. Move dead processes to TERMINATED.
                self.core.evacuate_node(node, is_system_restart=True,
                    dead_process_state=ProcessState.TERMINATED,
                    rescheduled_process_state=ProcessState.UNSCHEDULED_PENDING)

            # look for any other processes that should be queued after boot
            for owner, upid in self.store.get_process_ids():
                process = self.store.get_process(owner, upid)
                if process is None:
                    continue

                # these processes were stuck in a transitional state at shutdown
                if process.state in self._PROCESS_STATES_TO_REQUEUE:

                    if self.core.process_should_restart(process,
                            ProcessState.TERMINATED, is_system_restart=True):
                        self.core.process_next_round(process,
                            newstate=ProcessState.UNSCHEDULED_PENDING,
                            enqueue=False)
                    else:
                        self.core.process_change_state(process, ProcessState.TERMINATED)

            self.store.clear_queued_processes()

            self.store.set_pd_state(ProcessDispatcherState.SYSTEM_BOOTING)

        else:
            # system boot might have ended while we were dead.
            # schedule pending procs just in case
            self.schedule_pending_processes()
            self.store.set_pd_state(ProcessDispatcherState.OK)

            # TODO this is a good place to do general consistency checking on init,
            # before other actors start working. For example we could actually query
            # EPUM to determine the current state of resources. We might be recovering
            # from a major outage.

        self.store.set_initialized()

    def schedule_pending_processes(self):
        log.debug("Checking for UNSCHEDULED_PENDING processes to reschedule")
        process_ids = self.store.get_process_ids()

        for owner, upid in process_ids:
            process = self.store.get_process(owner, upid)
            if process is None or process.state != ProcessState.UNSCHEDULED_PENDING:
                continue

            process, updated = self.core.process_change_state(process,
                ProcessState.REQUESTED)
            if updated:
                self.store.enqueue_process(process.owner, process.upid,
                    process.round)


class ExecutionResourceMonitor(object):
    """Implements missing heartbeat detection for execution resources

    Execution resources emit heartbeats to the Process Dispatcher (PD).
    These heartbeats serve two purposes:
        1. To notify the PD of state changes of managed processes
        2. To allow the PD to detect when resources have stalled or otherwise
           disappeared.

    Heartbeats are sent on a regular interval, and are also sent immediately
    on a process state change. If the PD is aware of this interval, it can
    detect when something is wrong by comparing the last heartbeat time to
    the current time. However there may be clock sync or message queue backup
    delays that interfere with this detection. So we exercise restraint in
    declaring resources dead. The following algorithm is used:

    - Each resource has a timestamp indicating the time the last heartbeat
      was *sent*. Each resource also has warning_time and missing_time
      thresholds

    - When a resource's last heartbeat time is older than the warning_time
      threshold, the resource is declared to be in the WARNING state. This is
      based purely on the time delta and does not take into account potential
      clock differences or messaging delays. The result of this state change
      is some noisy logging but no countermeasures are used.

    - If the resource stays in the WARNING state for missing_time-warning_time
      *without any heartbeats received* as observed by the Doctor, the
      resource is declared MISSING. This will not happen simply because the
      last heartbeat time is older than missing_time. If a heartbeat is
      received from a resource in the WARNING state, this event "resets" the
      doctor's clock, regardless of the timestamp on the heartbeat. This means
      that resources may stay in the WARNING state indefinitely if their clocks
      are significantly off from the Doctor's clock.

    - If at any point in this process the resource heartbeats resume or
      "catch up" with warning_time, the resource is moved back to the OK
      state. Likewise, if a resource is DISABLED it is ignored by the doctor.
    """

    _MONITOR_ERROR_DELAY_SECONDS = 60

    _now_func = staticmethod(now_datetime)

    def __init__(self, core, store):
        self.core = core
        self.store = store

        self.condition = threading.Condition()
        self.cancelled = False

        self.resources = {}
        self.resource_set_changed = True
        self.changed_resources = set()

        self.resource_checks = _ResourceChecks()

        # local times at which resources were moved to the WARNING state.
        self.resource_warnings = {}

    def cancel(self):
        with self.condition:
            self.cancelled = True
            self.condition.notify_all()

    def monitor(self):
        """Monitor execution resources until cancelled
        """

        self.resource_set_changed = True

        while not self.cancelled:
            try:
                delay = self.monitor_cycle()

            except Exception:
                log.exception("Unexpected error monitoring heartbeats")
                delay = self._MONITOR_ERROR_DELAY_SECONDS

            # wait until the next check time, unless a resource update or cancel request
            # happens first
            if delay is None or delay > 0:
                with self.condition:
                    if not any((self.cancelled, self.changed_resources,
                               self.resource_set_changed)):
                        self.condition.wait(delay)

    def monitor_cycle(self):
        """Run a single cycle of the monitor

        returns the number of seconds until the next needed check, or None if no checks are needed
        """
        changed = self._update()
        now = self._now_func()

        for resource_id in changed:
            resource = self.resources.get(resource_id)

            # skip resource and remove any checks if it is removed or disabled
            if resource is None or resource.state == ExecutionResourceState.DISABLED:
                self.resource_checks.discard_resource_check(resource_id)
                try:
                    del self.resource_warnings[resource_id]
                except KeyError:
                    pass
                continue

            # otherwise, queue up the resource to be looked at in this cycle
            self.resource_checks.set_resource_check(resource_id, now)

        # walk the resource checks, up to the current time. This data structure
        # ensures that we only check resources that have either been updated, or
        # are due to timeout
        for resource_id in self.resource_checks.walk_through_time(now):

            if self.cancelled:  # back out early if cancelled
                return

            try:
                resource = self.resources[resource_id]
                self._check_one_resource(resource, now)
            except Exception:
                log.exception("Problem checking execution resource %s. Will retry.")
                if resource_id not in self.resource_checks:
                    next_check_time = now + timedelta(seconds=self._MONITOR_ERROR_DELAY_SECONDS)
                    self.resource_checks.set_resource_check(resource_id, next_check_time)

        # return the number of seconds until the next expected check, or None
        next_check_time = self.resource_checks.next_check_time
        if next_check_time is not None:
            return max((next_check_time - self._now_func()).total_seconds(), 0)
        return None

    def _check_one_resource(self, resource, now):

        last_heartbeat = resource.last_heartbeat_datetime
        warning_threshold, missing_threshold = self._get_resource_thresholds(resource)
        if warning_threshold is None or missing_threshold is None:
            return

        log.debug("Examining heartbeat for resource %s (in state %s)",
            resource.resource_id, resource.state)

        resource_id = resource.resource_id
        next_check_time = None

        # this could end up negative in case of unsynced or shifting clocks
        heartbeat_age = now - last_heartbeat

        if resource.state == ExecutionResourceState.OK:

            # go to WARNING state if heartbeat is older than threshold
            if heartbeat_age >= warning_threshold:
                self._mark_resource_warning(resource, now, last_heartbeat)
                next_check_time = now + (missing_threshold - warning_threshold)
            else:
                next_check_time = last_heartbeat + warning_threshold

        elif resource.state == ExecutionResourceState.WARNING:

            # our real threshold is the gap between missing  and warning
            threshold = missing_threshold - warning_threshold

            warning = self.resource_warnings.get(resource_id)

            if heartbeat_age < warning_threshold:
                self._mark_resource_ok(resource, now, last_heartbeat)

            elif not warning or warning.last_heartbeat != last_heartbeat:
                self.resource_warnings[resource_id] = _ResourceWarning(resource_id, last_heartbeat, now)
                next_check_time = now + threshold

            else:
                delta = now - warning.warning_time
                if delta >= threshold:
                    self._mark_resource_missing(resource, now, last_heartbeat)
                else:
                    next_check_time = warning.warning_time + threshold

        elif resource.state == ExecutionResourceState.MISSING:
            # go OK if a heartbeat has been receieved in warning_time threshold
            # go WARNING if a heartbeat has been receieved in missing_time threashold
            # set check time
            if heartbeat_age < warning_threshold:
                self._mark_resource_ok(resource, now, last_heartbeat)

            elif heartbeat_age < missing_threshold:
                self._mark_resource_warning(resource, now, last_heartbeat)

            elif resource.assigned:
                # if resource has assignments, evacuate them.
                # likely we would only get in this situation if previously
                # marked the resource MISSING but failed before we evacuated
                # everything
                self.core.evacuate_resource(resource)

        if next_check_time is not None:
            self.resource_checks.set_resource_check(resource_id, next_check_time)

    def _mark_resource_warning(self, resource, now, last_heartbeat):
        resource, updated = self.core.resource_change_state(resource,
            ExecutionResourceState.WARNING)
        if updated:
            resource_id = resource.resource_id
            heartbeat_age = (now - last_heartbeat).total_seconds()
            self.resource_warnings[resource_id] = _ResourceWarning(
                resource_id, last_heartbeat, now)
            log.warn("Execution resource %s is in WARNING state: Last known "
                "heartbeat was sent %s seconds ago. This may be a legitimately "
                "missing resource, OR it may be an issue of clock sync or "
                "messaging delays.", resource.resource_id, heartbeat_age)

    def _mark_resource_missing(self, resource, now, last_heartbeat):
        resource, updated = self.core.resource_change_state(resource,
            ExecutionResourceState.MISSING)
        if updated:
            resource_id = resource.resource_id

            try:
                del self.resource_warnings[resource_id]
            except KeyError:
                pass

            heartbeat_age = (now - last_heartbeat).total_seconds()
            log.warn("Execution resource %s is MISSING: Last known "
                "heartbeat was sent %s seconds ago. ", resource.resource_id, heartbeat_age)

            self.core.evacuate_resource(resource)

    def _mark_resource_ok(self, resource, now, last_heartbeat):
        resource, updated = self.core.resource_change_state(resource,
            ExecutionResourceState.OK)
        if updated:
            heartbeat_age = (now - last_heartbeat).total_seconds()
            log.info("Execution resource %s is OK again: Last known "
                "heartbeat was sent %s seconds ago. ", resource.resource_id, heartbeat_age)

    def _get_resource_thresholds(self, resource):
        engine = self.core.get_resource_engine(resource)
        warning = engine.heartbeat_warning
        if warning is not None:
            warning = ensure_timedelta(warning)

        missing = engine.heartbeat_missing
        if missing is not None:
            missing = ensure_timedelta(missing)

        return warning, missing

    def _get_resource_missing_threshold(self, resource):
        engine = self.core.get_resource_engine(resource)
        return ensure_timedelta(engine.heartbeat_missing)

    def _notify_resource_set_changed(self, *args):
        with self.condition:
            self.resource_set_changed = True
            self.condition.notify_all()

    def _notify_resource_changed(self, resource_id, *args):
        with self.condition:
            self.changed_resources.add(resource_id)
            self.condition.notify_all()

    def _update(self):
        changed = set()
        if self.resource_set_changed:
            changed.update(self._update_resource_set())
        if self.changed_resources:
            changed.update(self._update_resources())
        return changed

    def _update_resource_set(self):
        self.resource_set_changed = False
        resource_ids = set(self.store.get_resource_ids(
            watcher=self._notify_resource_set_changed))

        previous = set(self.resources.keys())

        added = resource_ids - previous
        removed = previous - resource_ids

        for resource_id in removed:
            del self.resources[resource_id]

        for resource_id in added:
            self.resources[resource_id] = self.store.get_resource(
                resource_id, watcher=self._notify_resource_changed)

        return added | removed

    def _update_resources(self):
        with self.condition:
            changed = self.changed_resources.copy()
            self.changed_resources.clear()

        for resource_id in changed:
            resource = self.store.get_resource(resource_id,
                                               watcher=self._notify_resource_changed)
            if resource:
                self.resources[resource_id] = resource

        return changed


_ResourceWarning = namedtuple("_ResourceWarning",
    ["resource_id", "last_heartbeat", "warning_time"])


class _ResourceChecks(object):

    def __init__(self):
        self.checks = {}
        self.check_heap = []

    def __contains__(self, item):
        return item in self.checks

    def set_resource_check(self, resource_id, check_time):

        # do nothing if check already exists and is correct
        if self.checks.get(resource_id) == check_time:
            return
        self.checks[resource_id] = check_time
        heapq.heappush(self.check_heap, (check_time, resource_id))

    def discard_resource_check(self, resource_id):
        try:
            del self.checks[resource_id]
        except KeyError:
            pass

    def walk_through_time(self, now):
        while self.check_heap:

            # heap guarantees smallest element is at index 0, so we can peek before we pop
            check_time, resource_id = self.check_heap[0]

            # skip and drop entries that are no longer represented in our check set
            if check_time != self.checks.get(resource_id):
                heapq.heappop(self.check_heap)

            elif check_time <= now:
                heapq.heappop(self.check_heap)
                del self.checks[resource_id]
                yield resource_id

            else:
                break

    @property
    def next_check_time(self):
        if self.check_heap:
            return self.check_heap[0][0]
        return None
