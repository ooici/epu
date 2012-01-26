import logging

from epu.states import InstanceState, ProcessState
from epu.processdispatcher.util import node_id_from_eeagent_name, \
    node_id_to_eeagent_name
from epu.processdispatcher.store import ProcessRecord, WriteConflictError, \
    NotFoundError, NodeRecord, ResourceRecord

log = logging.getLogger(__name__)


class ProcessDispatcherCore(object):
    """Service that fields requests from application engines and operators
    for process launches and termination.

    The PD has several responsibilities:

        - Receive and process requests from clients. These requests dictate
          which processes should be running. There may also be information
          queries about the state of the system.

        - Track available execution engine resources. It subscribes to a feed
          of DT deployment information from EPUM and uses this along with
          direct EEAgent heartbeats to determine available and healthy
          resources.

        - Maintain a priority queue of runnable WAITING processes. Matchmake
          processes with available resources and send dispatch requests to
          EEAgents. When resources are not available, escalate to EPUM for
          more DTs of a compatible type.

        - Track state of all processes in the system. When a process dies or
          is killed, attempt to replace it (and perhaps give it a higher
          launch priority than other WAITING processes). If a process
          repeatedly fails on its own (not due to VMs dying wholesale), mark
          it as FAILED and report to client.

    """

    def __init__(self, name, store, ee_registry, eeagent_client, epum_client, notifier):
        """

        @param name:
        @param store:
        @type store: ProcessDispatcherStore
        @param ee_registry:
        @param eeagent_client:
        @param epum_client:
        @param notifier:
        @return:
        """
        self.name = name
        self.store = store
        self.ee_registry = ee_registry
        self.eeagent_client = eeagent_client
        self.epum_client = epum_client
        self.notifier = notifier

    def initialize(self):
        #TODO not registering needs on-demand yet, just registering
        # base needs on initialize
        for engine_spec in self.ee_registry:
            base_need = engine_spec.base_need

            log.debug("Registering need for %d instances of DT %s", base_need,
                      engine_spec.deployable_type)
            self.epum_client.register_need(engine_spec.deployable_type, {},
                                           base_need, self.name, "dt_state")

    def dispatch_process(self, owner, upid, spec, subscribers, constraints=None, immediate=False):
        """Dispatch a new process into the system

        @param upid: unique process identifier
        @param spec: description of what is started
        @param subscribers: where to send status updates of this process
        @param constraints: optional scheduling constraints (IaaS site? other stuff?)
        @param immediate: don't provision new resources if no slots are available
        @rtype: ProcessRecord
        @return: description of process launch status


        Retry
        =====
        If a call to this operation times out without a reply, it can safely
        be retried. The upid and other parameters will be used to ensure that
        nothing is repeated. If the service fields an operation request that
        it thinks has already been acknowledged, it will return the current
        state of the process.
        """

        #TODO subscribers

        #TODO validate inputs
        process = ProcessRecord.new(owner, upid, spec, ProcessState.REQUESTED,
                                    constraints, immediate=immediate)

        existed = False
        try:
            self.store.add_process(process)
        except WriteConflictError:
            process = self.store.get_process(owner, upid)
            existed = True

        if not existed:
            log.debug("Enqueing process %s", upid)
            self.store.enqueue_process(owner, upid, process.round)

        return process

    def describe_process(self, owner, upid):
        """
        Get the state of a process in the system
        @param owner: owner of the process
        @param upid: ID of process
        @return: process description, or None
        """
        return self.store.get_process(owner, upid)

    def describe_processes(self):
        """
        Get a list of processes in the system
        @return: list of process descriptions
        """
        return [self.store.get_process(owner, upid)
                for owner, upid in self.store.get_process_ids()]

    def terminate_process(self, owner, upid):
        """
        Kill a running process
        @param owner: owner of the process
        @param upid: ID of process
        @return: description of process termination status

        This is an RPC-style call that returns quickly, as soon as termination
        of the process has begun (TERMINATING state).

        Retry
        =====
        If a call to this operation times out without a reply, it can safely
        be retried. Termination of processes should be an idempotent operation
        here and at the EEAgent. It is important that eeids not be repeated to
        faciliate this.
        """

        #TODO process might not exist
        process = self.store.get_process(owner, upid)

        if process.state >= ProcessState.TERMINATED:
            return process

        if process.assigned is None:

            # there could be a race where the process is assigned just
            # after we pulled the record. In this case our write will
            # fail. we keep trying until we either see an assignment
            # or we mark the process as terminated.
            updated = False
            while process.assigned is None:
                process.state = ProcessState.TERMINATED
                try:
                    self.store.update_process(process)
                    updated = True
                except WriteConflictError:
                    process = self.store.get_process(process.owner,
                                                     process.upid)
            if updated:
                # EARLY RETURN: the process was never assigned to a resource
                return process

        self.eeagent_client.terminate_process(process.assigned, upid,
                                              process.round)

        # same as above: we want to mark the process as terminating but
        # other players may also be updating this record. we keep trying
        # in the face of conflict until the process is >= TERMINATING --
        # but note that it may be another worker that actually makes the
        # write. For example the heartbeat could be received and processed
        # remarkably quickly and the process could go right to TERMINATED.
        while process.state < ProcessState.TERMINATING:
            process.state = ProcessState.TERMINATING
            try:
                self.store.update_process(process)
            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)

        return process

    def dt_state(self, node_id, deployable_type, state, properties=None):
        """
        Handle updates about available instances of deployable types.

        @param node_id: unique instance identifier
        @param deployable_type: type of instance
        @param state: EPU state of instance
        @param properties: Optional properties about this instance
        @return:

        This operation is the recipient of a "subscription" the PD makes to
        DT state updates. Calls to this operation are NOT RPC-style.

        This information is used for two purposes:

            1. To correlate EE agent heartbeats with a DT and various deploy
               information (site, allocation, security groups, etc).

            2. To detect EEs which have been killed due to underlying death
               of a resource (VM).
        """

        if state == InstanceState.RUNNING:
            node = self.store.get_node(node_id)
            if not node:
                node = NodeRecord.new(node_id, deployable_type, properties)

                try:
                    self.store.add_node(node)
                except WriteConflictError:
                    # if the node record was written by someone else,
                    # no big deal.
                    return

                log.info("DT resource %s is %s", node_id, state)

        elif state in (InstanceState.TERMINATING, InstanceState.TERMINATED):
            # reschedule processes running on node

            node = self.store.get_node(node_id)
            if node is None:
                log.warn("Got dt_state for unknown node %s in state %s",
                         node_id, state)
                return

            resource_id = node_id_to_eeagent_name(node_id)
            resource = self.store.get_resource(resource_id)

            if not resource:
                log.warn("Got dt_state for node without a resource")
            else:

                # mark resource ineligible for scheduling
                self._disable_resource(resource)

                # go through and reschedule processes as needed
                for owner, upid, round in resource.assigned:
                    self._evacuate_process(owner, upid, resource)

            try:
                self.store.remove_node(node_id)
            except NotFoundError:
                pass
            try:
                self.store.remove_resource(resource_id)
            except NotFoundError:
                pass

    def _disable_resource(self, resource):
        while resource.enabled:
            resource.enabled = False
            try:
                self.store.update_resource(resource)
            except WriteConflictError:
                resource = self.store.get_resource(resource_id)

    def _evacuate_process(self, owner, upid, resource):
        """Deal with a process on a terminating/terminated node
        """
        process = self.store.get_process(owner, upid)
        if process is None:
            return

        # send a last ditch terminate just in case
        if process.state < ProcessState.TERMINATED:
            self.eeagent_client.terminate_process(resource.resource_id,
                upid,
                process.round)

        if process.state == ProcessState.TERMINATING:
            #what luck. the process already wants to die.
            process, updated = self._change_process_state(
                process, ProcessState.TERMINATED)
            if updated:
                self.notifier.notify_process(process)

        elif process.state < ProcessState.TERMINATING:
            log.debug("Rescheduling process %s from terminating node %s",
                upid, resource.node_id)

            process, updated = self._process_next_round(process)
            if updated:
                self.notifier.notify_process(process)
                self.store.enqueue_process(process.owner, process.upid,
                    process.round)

    def ee_heartbeart(self, sender, beat):
        """Incoming heartbeat from an EEAgent

        @param sender: ION name of sender
        @param beat: information about running processes
        @return:

        When an EEAgent starts, it immediately begins sending heartbeats to
        the PD. The first received heartbeat will trigger the PD to mark the
        EE as available in its slot tables, and potentially start deploying
        some WAITING process requests.

        The heartbeat message will consist of at least these fields:
            - node id - unique ID for the provisioned resource (VM) the EE runs on
            - timestamp - time heartbeat was generated
            - processes - list of running process IDs
        """

        resource = self.store.get_resource(sender)
        if resource is None:
            # first heartbeat from this EE
            self._first_heartbeat(sender)
            return #  *** EARLY RETURN **

        assigned_procs = set()
        processes = beat['processes']
        for procstate in processes:
            upid = procstate['upid']
            round = procstate['round']
            state = procstate['state']

            #TODO hack to handle how states are formatted in EEAgent heartbeat
            if isinstance(state, (list,tuple)):
                state = "-".join(str(s) for s in state)

            #TODO owner?
            process = self.store.get_process(None, upid)
            if not process:
                log.warn("EE reports process %s that is unknown!", upid)
                continue

            assigned_procs.add(process.key)

            if round < process.round:
                # skip heartbeat info for processes that are already redeploying
                # but send a cleanup request first
                self.eeagent_client.cleanup_process(sender, upid, round)
                continue

            if state == process.state:
                continue

            if process.state == ProcessState.PENDING and \
               state == ProcessState.RUNNING:

                log.info("Process %s is %s", upid, state)

                # mark as running and notify subscriber
                process, changed = self._change_process_state(
                    process, ProcessState.RUNNING)

                if changed:
                    self.notifier.notify_process(process)

            elif state in (ProcessState.TERMINATED, ProcessState.FAILED):

                # process has died in resource. Obvious culprit is that it was
                # killed on request.
                log.info("Process %s is %s", upid, state)

                if process.state == ProcessState.TERMINATING:
                    # mark as terminated and notify subscriber
                    process, updated = self._change_process_state(
                        process, ProcessState.TERMINATED, assigned=None)
                    if updated:
                        self.notifier.notify_process(process)

                # otherwise it needs to be rescheduled
                elif process.state in (ProcessState.PENDING,
                                    ProcessState.RUNNING):

                    process, updated = self._process_next_round(process)
                    if updated:
                        self.notifier.notify_process(process)
                        self.store.enqueue_process(process.owner, process.upid,
                                                   process.round)

                # send cleanup request to EEAgent now that we have dealt
                # with the dead process
                self.eeagent_client.cleanup_process(sender, upid, round)

        new_assigned = []
        for owner, upid, round in resource.assigned:
            key = (owner, upid, round)
            if key in assigned_procs:
                new_assigned.append(key)
                continue

            process = self.store.get_process(owner, upid)

            # prune process assignments once the process has terminated or
            # moved onto the next round
            if (process and process.round == round
                and process.state < ProcessState.TERMINATED):
                new_assigned.append(key)

        if len(new_assigned) != len(resource.assigned):
            resource.assigned = new_assigned
            try:
                self.store.update_resource(resource)
            except (WriteConflictError, NotFoundError):
                #TODO? right now this will just wait for the next heartbeat
                pass
        
    def _first_heartbeat(self, sender):

        node_id = node_id_from_eeagent_name(sender)

        node = self.store.get_node(node_id)
        if node is None:
            log.warn("EE heartbeat from unknown node. Still booting? "+
                     "node_id=%s sender=%s", node_id, sender)

            # TODO I'm thinking the best thing to do here is query EPUM
            # for the state of this node in case the initial dt_state
            # update got lost. Note that we shouldn't go ahead and
            # schedule processes onto this EE until we get the RUNNING
            # dt_state update -- there could be a failure later on in
            # the contextualization process that triggers the node to be
            # terminated.

            return

        log.info("Got first heartbeat from EEAgent %s on node %s",
            sender, node_id)

        if node.properties:
            properties = node.properties.copy()
        else:
            properties = {}

        engine_spec = self.ee_registry.get_engine_by_dt(node.deployable_type)
        slots = engine_spec.slots

        # just making engine type a generic property/constraint for now,
        # until it is clear something more formal is needed.
        properties['engine_type'] = engine_spec.engine_id

        resource = ResourceRecord.new(sender, node_id, slots, properties)
        try:
            self.store.add_resource(resource)
        except WriteConflictError:
            # no problem if this resource was just created by another worker
            log.info("Conflict writing new resource record %s. Ignoring.", sender)

    def _process_next_round(self, process):
        cur_round = process.round
        updated = False
        while (process.state < ProcessState.TERMINATING and
               cur_round == process.round):
            process.state = ProcessState.DIED_REQUESTED
            process.assigned = None
            process.round = cur_round+1
            try:
                self.store.update_process(process)
                updated = True
            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)
        return process, updated

    def _change_process_state(self, process, newstate, **updates):
        """
        Tentatively update a process record

        Because some other worker may update the process record before this one,
        this method retries writes in the face of conflict, as long as the
        current record of the process start is less than the new state, and the
        round remains the same.
        @param process: process to update
        @param newstate: the new state. update will only happen if current
                         state is less than the new state
        @param updates: keyword arguments of additional fields to update in process
        @return:
        """
        cur_round = process.round
        updated = False
        while process.state < newstate and cur_round == process.round:
            process.state = newstate
            process.update(updates)
            try:
                self.store.update_process(process)
                updated = True
            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)

        return process, updated

    def dump(self):
        resources = {}
        processes = {}
        state = dict(resources=resources, processes=processes)

        for resource_id in self.store.get_resource_ids():
            resource = self.store.get_resource(resource_id)
            if not resource:
                continue
            resources[resource_id] = dict(resource)

        for owner, upid in self.store.get_process_ids():
            process = self.store.get_process(owner, upid)
            if not process:
                continue
            processes[process.upid] = dict(process)

        return state