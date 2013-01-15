import logging
import threading

from epu.states import ProcessState, ProcessDispatcherState

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
      hasn't been heard from in too long, the doctor first attempts to provoke
      a response from the agent. If that fails, the agent is marked as disabled
      and ultimately its VM is killed. (TODO)
    """

    _PROCESS_STATES_TO_REQUEUE = (ProcessState.REQUESTED,
        ProcessState.DIED_REQUESTED, ProcessState.WAITING)

    def __init__(self, core, store):
        """
        @type core: ProcessDispatcherCore
        @type store: ProcessDispatcherStore
        """
        self.core = core
        self.store = store

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
                raise Exception("already the leader???")
            self.is_leader = True

        pd_state = self.store.get_pd_state()
        log.info("Elected as Process Dispatcher Doctor. State is %s", pd_state)
        if pd_state == ProcessDispatcherState.UNINITIALIZED:
            self.initialize_pd()
            pd_state = self.store.get_pd_state()

        if pd_state == ProcessDispatcherState.SYSTEM_BOOTING:
            self.watching_system_boot = True
            self._watch_system_boot()

        # TODO this is where we can set up the heartbeat monitor

        while self.is_leader:
            with self.condition:
                self.condition.wait()

    def cancel(self):
        with self.condition:
            self.is_leader = False
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
