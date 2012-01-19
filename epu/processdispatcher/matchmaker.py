import logging
import threading

from epu.processdispatcher.store import WriteConflictError, NotFoundError
from epu.states import ProcessState

log = logging.getLogger(__name__)

class PDMatchmaker(object):
    """The matchmaker is a singleton process (enforced by ZK leader election)
    that matches process requests to available execution engine slots.

    Responsibilities:
    - Tracks available resources in memory, backed by persistence. Maintains
      watches on the persistence for updates from other workers.
    - Pulls process requests from a queue and matches them to available slots.

    While there are any engine updates, process them. Otherwise, if there are
    any slots available
    """

    def __init__(self, store, resource_client):
        """
        @type store: ProcessDispatcherStore
        @type resource_client: EEAgentClient
        """
        self.store = store
        self.resource_client = resource_client

        self.resources = None
        self.queued_processes = None

        self.condition = threading.Condition()

        self.cancelled = False

        self.needs_matchmaking = False

    def initialize(self):
        #TODO leader election ?

        self.resources = {}
        self.queued_processes = []

        self.resource_set_changed = True
        self.changed_resources = set()
        self.process_set_changed = True

        self.needs_matchmaking = True

    def _find_assigned_resource(self, owner, upid, round):
        # could speedup with cache
        t = (owner, upid, round)
        for resource in self.resources.itervalues():
            if t in resource.assigned:
                return resource
        return None

    def _notify_resource_set_changed(self):
        with self.condition:
            self.resource_set_changed = True
            self.condition.notifyAll()

    def _notify_process_set_changed(self):
        with self.condition:
            self.process_set_changed = True
            self.condition.notifyAll()

    def _notify_resource_changed(self, resource_id):
        with self.condition:
            self.changed_resources.add(resource_id)
            self.condition.notifyAll()

    def _get_queued_processes(self):
        self.process_set_changed = False
        processes = self.store.get_queued_processes(
            watcher=self._notify_process_set_changed)

        #TODO not really caring about priority or queue order
        # at this point

        for process_handle in processes:
            if process_handle not in self.queued_processes:
                log.debug("Found new queued process: %s", process_handle)
                self.queued_processes.append(process_handle)

                self.needs_matchmaking = True

    def _get_resource_set(self):
        self.resource_set_changed = False
        resource_ids = set(self.store.get_resource_ids(
            watcher=self._notify_resource_set_changed))

        previous = set(self.resources.keys())

        added = resource_ids - previous
        removed = previous - resource_ids

        # resource removal doesn't need to trigger matchmaking
        if added:
            self.needs_matchmaking = True

        for resource_id in removed:
            del self.resources[resource_id]

        for resource_id in added:
            resource = self.store.get_resource(resource_id,
                                               watcher=self._notify_resource_changed)
            self.resources[resource_id] = resource

    def _get_resources(self):
        with self.condition:
            changed = self.changed_resources.copy()
            self.changed_resources.clear()

        if changed:
            self.needs_matchmaking = True

        for resource_id in changed:
            resource = self.store.get_resource(resource_id,
                                               watcher=self._notify_resource_changed)
            #TODO fold in assignment vector in some fancy way?
            if resource:
                self.resources[resource_id] = resource

    def cancel(self):
        with self.condition:
            self.cancelled = True
            self.condition.notifyAll()

    def run(self):
        while not self.cancelled:
            # first fold in any changes to queued processes and available resources

            if self.process_set_changed:
                self._get_queued_processes()

            if self.resource_set_changed:
                self._get_resource_set()

            if self.changed_resources:
                self._get_resources()

            # check again if we lost leadership
            if self.cancelled:
                return

            # now do a matchmaking cycle if anything changed enough to warrant
            if self.needs_matchmaking:
                self.matchmake()

            with self.condition:
                if not (self.cancelled or self.resource_set_changed or
                        self.changed_resources or self.process_set_changed):
                    self.condition.wait()

    def matchmake(self):
        # this is inefficient but that is ok for now

        resources = self.get_available_resources()
        log.debug("Matchmaking. Processes: %d  Available resources: %d",
                  len(self.queued_processes), len(resources))

        for owner, upid, round in list(self.queued_processes):
            log.debug("Matching process %s", upid)

            process = self.store.get_process(owner, upid)
            if not (process and process.round == round and
                    process.state < ProcessState.PENDING):
                self.store.remove_queued_process(owner, upid, round)
                self.queued_processes.remove((owner, upid, round))
                continue

            # ensure process is not already assigned a slot
            matched_resource = self._find_assigned_resource(owner, upid, round)
            if not matched_resource and resources:
                matched_resource = matchmake_process(process, resources)

            if matched_resource:
                # update the resource record

                matched_resource.assigned.append((owner, upid, round))
                try:
                    self.store.update_resource(matched_resource)
                except WriteConflictError:

                    # in case of write conflict, bail out of the matchmaker
                    # run and the outer loop will take care of updating data
                    # and trying again
                    return

                # attempt to also update the process record and mark it as pending.
                # If the process has since been terminated, this update will fail.
                # In that case we must back out the resource record update we just
                # made.
                process, assigned = self._maybe_update_assigned_process(
                    process, matched_resource)

                # backout resource record update if the process update failed due to
                # 3rd party changes to the process.
                if not assigned:
                    matched_resource, removed = self._backout_resource_assignment(
                        matched_resource, process)

                # remove resource from consideration if we took the last slot
                if not matched_resource.available_slots:
                    resources.remove(matched_resource)

                #TODO move this to a separate operation that MM submits to queue?
                self.resource_client.launch_process(
                    matched_resource.resource_id, process.upid, process.round,
                    process.spec['run_type'], process.spec['parameters'])

                self.store.remove_queued_process(owner, upid, round)
                self.queued_processes.remove((owner, upid, round))


            elif process.state < ProcessState.WAITING:
                self._update_process_state(process)

                # remove rejected processes from the queue
                if process.state == ProcessState.REJECTED:
                    self.store.remove_queued_process(owner, upid, round)

        # if we made it through all processes, we don't need to matchmake
        # again until new information arrives
        self.needs_matchmaking = False

    def _maybe_update_assigned_process(self, process, resource):
        updated = False
        while process.state < ProcessState.PENDING:
            process.assigned = resource.resource_id
            process.state = ProcessState.PENDING
            try:
                self.store.update_process(process)
                updated = True

            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)

        return process, updated

    def _backout_resource_assignment(self, resource, process):
        removed = False
        pkey = process.key
        while resource and pkey in resource.assigned:
            resource.assigned.remove(pkey)
            try:
                self.store.update_resource(resource)
                removed = True
            except WriteConflictError:
                resource = self.store.get_resource(resource.resource_id)
            except NotFoundError:
                resource = None

        return resource, removed

    def _update_process_state(self, process):

        # update process record to indicate queuing state. if writes conflict
        # retry until success or until process has moved to a state where it
        # doesn't matter anymore

        while process.state < ProcessState.WAITING:
            if process.immediate:
                process.state = ProcessState.REJECTED
                log.info("Process %s: no available slots. REJECTED due to immediate flag",
                         process.upid)
            else:
                log.info("Process %s: no available slots. WAITING in queue",
                         process.upid)
                process.state = ProcessState.WAITING

            try:
                self.store.update_process(process)
            except WriteConflictError:
                process = self.store.get_process(process.caller, process.upid)
                continue

            #TODO send notification to process subscribers


    def get_available_resources(self):
        available = []
        for resource in self.resources.itervalues():
            log.debug("Examining %s", resource)
            if resource.enabled and resource.available_slots:
                available.append(resource)

        # sort by 1) whether any processes are already assigned to resource
        # and 2) number of available slots
        available.sort(key=lambda r: (0 if r.assigned else 1,
                                      r.slot_count - len(r.assigned)))
        return available

def matchmake_process(process, resources):
    matched = None
    for resource in resources:
        log.debug("checking %s", resource.resource_id)
        if match_constraints(process.constraints, resource.properties):
            matched = resource
            break
    return matched

def match_constraints(constraints, properties):
    """Match process constraints against resource properties

    Simple equality matches for now.
    """
    if constraints is None:
        return True

    for key,value in constraints.iteritems():
        if value is None:
            continue

        if properties is None:
            return False

        advertised = properties.get(key)
        if advertised is None:
            return False

        if isinstance(value,(list,tuple)):
            if not advertised in value:
                return False
        else:
            if advertised != value:
                return False

    return True