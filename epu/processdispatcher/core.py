import logging

from epu.states import InstanceState, ProcessState, ExecutionResourceState
from epu.exceptions import NotFoundError, WriteConflictError, BadRequestError
from epu.processdispatcher.engines import engine_id_from_domain
from epu.util import is_valid_identifier, parse_datetime, ceiling_datetime
from epu.processdispatcher.store import ProcessRecord, NodeRecord, \
    ResourceRecord, ProcessDefinitionRecord
from epu.processdispatcher.modes import RestartMode
from epu.processdispatcher.util import get_process_state_message

log = logging.getLogger(__name__)

_WRITE_CONFLICT_RETRIES = 5


def validate_owner_upid(owner, upid):
    # so far we don't enforce owner since it isn't used for anything
    if not is_valid_identifier(upid):
        raise BadRequestError("invalid upid")


def validate_definition_id(definition_id):
    if not is_valid_identifier(definition_id):
        raise BadRequestError("invalid definition_id")


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

    def __init__(self, store, ee_registry, eeagent_client, notifier):
        """

        @param store:
        @type store: ProcessDispatcherStore
        @param ee_registry:
        @param eeagent_client:
        @param notifier:
        @return:
        """
        self.store = store
        self.ee_registry = ee_registry
        self.eeagent_client = eeagent_client
        self.notifier = notifier

    def set_system_boot(self, system_boot):
        """Operation used at the end of a launch to disable system boot mode

        Currently it is only ever really set to False via this operation. It is set
        to True by directly writing to ZooKeeper before any PD services are started.
        """
        if system_boot is not False and system_boot is not True:
            raise BadRequestError("expected a boolean value for system boot")
        self.store.set_system_boot(system_boot)

    def create_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):
        validate_definition_id(definition_id)
        definition = ProcessDefinitionRecord.new(definition_id,
            definition_type, executable, name=name, description=description)
        log.debug("creating definition %s" % definition)
        self.store.add_definition(definition)

    def describe_definition(self, definition_id):
        validate_definition_id(definition_id)
        return self.store.get_definition(definition_id)

    def update_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):
        validate_definition_id(definition_id)
        definition = ProcessDefinitionRecord.new(definition_id,
            definition_type, executable, name=name, description=description)
        self.store.update_definition(definition)

    def remove_definition(self, definition_id):
        validate_definition_id(definition_id)
        self.store.remove_definition(definition_id)

    def list_definitions(self):
        return self.store.list_definition_ids()

    def create_process(self, owner, upid, definition_id, name=None):
        """Create a new process in the system

        @param upid: unique process identifier
        @param definition_id: process definition to start
        @param name: a (hopefully) human recognizable name for the process


        Retry
        =====
        If a call to this operation times out without a reply, it can safely
        be retried. The upid and other parameters will be used to ensure that
        nothing is repeated. If the service fields an operation request that
        it thinks has already been acknowledged, it will return the current
        state of the process.
        """
        validate_owner_upid(owner, upid)
        validate_definition_id(definition_id)

        # if not a real def, a NotFoundError will bubble up to caller
        definition = self.store.get_definition(definition_id)
        if definition is None:
            raise NotFoundError("Couldn't find process definition %s in store" % definition_id)

        process = ProcessRecord.new(owner, upid, definition,
            ProcessState.UNSCHEDULED, name=name)

        try:
            self.store.add_process(process)
        except WriteConflictError:
            process = self.store.get_process(owner, upid)

            if not process:
                raise BadRequestError("process exists but cannot be found")

            # check parameters from call against process record in store.
            # if they don't match, the caller probably has a coding error
            # and is reusing upids or something.
            if name is not None and process.name != name:
                raise BadRequestError("process %s exists with different name '%s'" %
                    (upid, process.name))
            if definition_id != process.definition['definition_id']:
                raise BadRequestError("process %s exists with different definition_id '%s'" %
                    (upid, process.definition['definition_id']))
        return process

    def schedule_process(self, owner, upid, definition_id=None,
                         configuration=None, subscribers=None,
                         constraints=None, queueing_mode=None,
                         restart_mode=None, execution_engine_id=None,
                         node_exclusive=None, name=None):
        """Schedule a process for execution

        @param upid: unique process identifier
        @param definition_id: process definition to start
        @param configuration: process configuration
        @param subscribers: where to send status updates of this process
        @param constraints: optional scheduling constraints
        @param queueing_mode: when a process can be queued
        @param restart_mode: when and if failed/terminated procs should be restarted
        @param execution_engine_id: dispatch a process to a specific eea
        @param node_exclusive: property that will only be permitted once on a node
        @param name: a (hopefully) human recognizable name for the process
        @rtype: ProcessRecord
        @return: description of process launch status

        If an unknown process is specified and a definition_id is included,
        this operation will create and schedule the process. If the process
        already exists, it will be scheduled.

        Retry
        =====
        If a call to this operation times out without a reply, it can safely
        be retried. The upid and other parameters will be used to ensure that
        nothing is repeated. If the service fields an operation request that
        it thinks has already been acknowledged, it will return the current
        state of the process.
        """

        validate_owner_upid(owner, upid)

        if constraints is None:
            constraints = {}
        if execution_engine_id:
            constraints['engine'] = execution_engine_id

        process_updates = dict(configuration=configuration,
            subscribers=subscribers, constraints=constraints,
            queueing_mode=queueing_mode, restart_mode=restart_mode,
            node_exclusive=node_exclusive, name=name)

        process = self.store.get_process(owner, upid)

        for _ in range(_WRITE_CONFLICT_RETRIES):

            if process is None:
                # if process is new but definition is specified, we create and
                # schedule the process in one call. Without a definition it is
                # an error.
                if not definition_id:
                    raise NotFoundError("process %s does not exist" % upid)
                process = self.create_process(owner, upid,
                    definition_id=definition_id, name=name)
                process.update(process_updates)
                process.state = ProcessState.REQUESTED

            # if the process existed, but is being scheduled for the first time
            elif process.state == ProcessState.UNSCHEDULED:
                if definition_id:
                    validate_definition_id(definition_id)

                    definition = process.definition
                    if not definition or not definition.get('definition_id'):
                        raise BadRequestError("process %s exists but has no definition")

                    if definition_id != definition['definition_id']:
                        raise BadRequestError(
                            "process %s definition_id %s doesn't match request"
                            % (upid, definition_id))

                process.update(process_updates)
                process.state = ProcessState.REQUESTED

            # if the process is dead in a terminal state and is to be started.
            # we need to increment the process round number.
            elif process.state in ProcessState.TERMINAL_STATES:
                process.update(process_updates)
                process.state = ProcessState.REQUESTED
                process.round = process.round + 1

            # if the process is already in play, it is ok for this operation
            # to be repeated. however it must have the same parameters or else
            # that is likely a programming error on the caller's part.
            else:
                _check_process_schedule_idempotency(process, process_updates)
                # process record matches the request. no update is needed.
                break

            try:
                self.store.update_process(process)
                # once we successfully update the process, get out of the loop
                break

            except WriteConflictError:
                process = self.store.get_process(owner, upid)

        else:
            raise BadRequestError("process %s could not be updated" % (upid,))

        # always enqueue the process, even if it is probably already in the
        # queue. this is harmless and provides an easy way to "nudge" matchmaking
        # along in the face of bugs.
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

    def restart_process(self, owner, upid):
        """
        Restart a running process
        @param owner: owner of the process
        @param upid: ID of process
        @return: description of process termination status

        This is an RPC-style call that returns quickly, as soon as restarting
        of the process has begun (TERMINATING state).

        Retry
        =====
        If a call to this operation times out without a reply, it can safely
        be retried. Restarting of processes should be an idempotent operation
        here and at the EEAgent.
        """
        validate_owner_upid(owner, upid)

        process = self.store.get_process(owner, upid)
        if process is None:
            raise NotFoundError("process %s does not exist" % upid)

        if process.state != ProcessState.RUNNING:
            log.warning("Tried to restart a process that isn't RUNNING")
            return process

        while process.state != ProcessState.PENDING:
            process.state = ProcessState.PENDING
            process.round = process.round + 1
            try:
                self.store.update_process(process)
            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)

                if process.state != ProcessState.RUNNING:
                    log.warning("Tried to restart a process that isn't RUNNING")
                    return process

        self.eeagent_client.restart_process(process.assigned, upid,
                                              process.round)

        return process

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
        validate_owner_upid(owner, upid)

        process = self.store.get_process(owner, upid)
        if process is None:
            raise NotFoundError("process %s does not exist" % upid)

        # if the process is already at rest -- UNSCHEDULED, or REJECTED
        # for example -- we do nothing and leave the process state as is.
        if process.state in ProcessState.TERMINAL_STATES:
            return process

        # unassigned processes can just be marked as terminated, but note
        # that we may be racing with the matchmaker.
        if process.assigned is None:

            # there could be a race where the process is assigned just
            # after we pulled the record. In this case our write will
            # fail. we keep trying until we either see an assignment
            # or we mark the process as terminated.
            updated = False
            while process.assigned is None and not updated:
                process.state = ProcessState.TERMINATED
                try:
                    self.store.update_process(process)
                    updated = True
                except WriteConflictError:
                    process = self.store.get_process(process.owner,
                                                     process.upid)
            if updated:
                log.info(get_process_state_message(process))
                self.notifier.notify_process(process)

                # also try to remove process from queue
                try:
                    self.store.remove_queued_process(process.owner,
                        process.upid, process.round)
                except NotFoundError:
                    pass

                # EARLY RETURN: the process was never assigned to a resource
                return process

        # same as above: we want to mark the process as terminating but
        # other players may also be updating this record. we keep trying
        # in the face of conflict until the process is >= TERMINATING --
        # but note that it may be another worker that actually makes the
        # write.
        self.process_change_state(process, ProcessState.TERMINATING)

        self.eeagent_client.terminate_process(process.assigned, upid,
                                              process.round)

        return process

    def node_state(self, node_id, domain_id, state, properties=None):
        """
        Handle updates about available domain nodes.

        @param node_id: unique instance identifier
        @param domain_id: domain of instance
        @param state: EPU state of instance
        @param properties: Optional properties about this instance
        @return:

        This operation is the recipient of a "subscription" the PD makes to
        domain state updates. Calls to this operation are NOT RPC-style.

        This information is used for two purposes:

            1. To correlate EE agent heartbeats with a node and various deploy
               information (site, allocation, security groups, etc).

            2. To detect EEs which have been killed due to underlying death
               of a resource (VM).
        """

        if state == InstanceState.RUNNING:
            node = self.store.get_node(node_id)
            if node is None:
                node = NodeRecord.new(node_id, domain_id, properties)

                try:
                    self.store.add_node(node)
                except WriteConflictError:
                    # if the node record was written by someone else,
                    # no big deal.
                    return

                log.info("Domain %s node %s is %s", domain_id, node_id, state)

        elif state in (InstanceState.TERMINATING, InstanceState.TERMINATED):
            # reschedule processes running on node

            node = self.store.get_node(node_id)
            if node is None:
                log.warn("got state for unknown node %s in state %s",
                         node_id, state)
                return
            self.evacuate_node(node)

    def evacuate_node(self, node, is_system_restart=False,
                      dead_process_state=None, rescheduled_process_state=None):
        """Remove a node and reschedule its processes as needed

        dead_process_state: the state non-restartable processes are moved to.
            defaults to FAILED
        rescheduled_process_state: the state restartable processes are moved to.
            defaults to REQUESTED
        """
        node_id = node.node_id

        for resource_id in node.resources:
            resource = self.store.get_resource(resource_id)

            if not resource:
                log.warn("Node has unknown resource %s", node_id, resource_id)
                continue

            else:
                # mark resource ineligible for scheduling
                self.resource_change_state(resource, ExecutionResourceState.DISABLED)

                self.evacuate_resource(
                    resource,
                    is_system_restart=is_system_restart,
                    dead_process_state=dead_process_state,
                    rescheduled_process_state=rescheduled_process_state,
                    update_node_exclusive=False)
                # node is going away, so we don't bother updating node exclusive

            try:
                self.store.remove_resource(resource_id)
            except NotFoundError:
                pass

        try:
            self.store.remove_node(node_id)
        except NotFoundError:
            pass

    def evacuate_resource(self, resource, is_system_restart=False,
                      dead_process_state=None, rescheduled_process_state=None,
                      update_node_exclusive=True):
        """Remove and reschedule processes from a resource

        This assumes the resource is already disabled or is otherwise prevented
        from being assigned any new processes
        """

        processes = []
        for owner, upid, round in resource.assigned:
            process = self.store.get_process(owner, upid)
            if process:
                processes.append(process)

        # update node exclusive tags first, in case we die partway through this
        # operation. on recovery it should be retried and we don't want to leave
        # node exclusives orphaned.
        if update_node_exclusive:
            to_remove = [p.node_exclusive for p in processes if p.node_exclusive]
            if to_remove:
                node = self.store.get_node(resource.node_id)
                if node:
                    self.node_remove_exclusive_tags(node, to_remove)

        # now go through and evacuate each process
        for process in processes:
            self._evacuate_process(process, resource,
                is_system_restart=is_system_restart,
                dead_process_state=dead_process_state,
                rescheduled_process_state=rescheduled_process_state)

    def _evacuate_process(self, process, resource, is_system_restart=False,
            dead_process_state=None, rescheduled_process_state=None):
        """Deal with a process on a terminating/terminated node
        """
        if not dead_process_state:
            dead_process_state = ProcessState.FAILED

        if process.state == ProcessState.TERMINATING:
            # what luck. the process already wants to die.
            process, updated = self.process_change_state(
                process, ProcessState.TERMINATED)

        elif process.state < ProcessState.TERMINATING:
            if self.process_should_restart(process, dead_process_state,
                    is_system_restart=is_system_restart):
                log.debug("Rescheduling process %s from evacuated resource %s on node %s",
                    process.upid, resource.resource_id, resource.node_id)
                if rescheduled_process_state:
                    self.process_next_round(process,
                        newstate=rescheduled_process_state)
                else:
                    self.process_next_round(process)
            else:
                self.process_change_state(process, dead_process_state, assigned=None)

    def ee_heartbeat(self, sender, beat):
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

        # sender can be in the format $sysname.$eename when CFG.dashi.sysname
        # is set, or it will be just $eename, if there is no sysname set.
        # We need to make sure that we remove the sysname when it is enabled to
        # get the correct eeagent name.
        if '.' in sender:
            sender = sender.split('.')[-1]
        resource = self.store.get_resource(sender)
        if resource is None:
            # first heartbeat from this EE
            self._first_heartbeat(sender, beat)
            return  # *** EARLY RETURN **

        resource_updated = False

        timestamp_str = beat['timestamp']
        timestamp = ceiling_datetime(parse_datetime(timestamp_str))

        resource_timestamp = resource.last_heartbeat_datetime
        if resource_timestamp is None or timestamp > resource_timestamp:
            resource.new_last_heartbeat_datetime(timestamp)
            resource_updated = True

        assigned_procs = set()
        processes = beat['processes']
        node_exclusives_to_remove = []
        for procstate in processes:
            upid = procstate['upid']
            round = int(procstate['round'])
            state = procstate['state']

            # TODO hack to handle how states are formatted in EEAgent heartbeat
            if isinstance(state, (list, tuple)):
                state = "-".join(str(s) for s in state)

            # TODO owner?
            process = self.store.get_process(None, upid)
            if not process:
                log.warn("EE reports process %s that is unknown!", upid)

                if state < ProcessState.TERMINATED:
                    assigned_procs.add((None, upid, round))
                else:
                    self.eeagent_client.cleanup_process(sender, upid, round)

                continue

            if round < process.round:
                # skip heartbeat info for processes that are already redeploying
                # but send a cleanup request first
                if state < ProcessState.TERMINATED:
                    self.eeagent_client.terminate_process(sender, upid, round)
                else:
                    self.eeagent_client.cleanup_process(sender, upid, round)
                continue

            if state == process.state:

                # if we managed to update the process record already for a
                # terminated process but didn't update the resource record,
                # clean up the process
                if state >= ProcessState.TERMINATED:
                    self.eeagent_client.cleanup_process(sender, upid, round)

                continue

            if process.state == ProcessState.PENDING and \
               state == ProcessState.RUNNING:

                assigned_procs.add(process.key)

                # mark as running and notify subscriber
                process, changed = self.process_change_state(
                    process, ProcessState.RUNNING)

            elif state in (ProcessState.TERMINATED, ProcessState.FAILED,
                           ProcessState.EXITED):

                # process has died in resource. Obvious culprit is that it was
                # killed on request.

                if process.node_exclusive:
                    node_exclusives_to_remove.append(process.node_exclusive)

                if process.state == ProcessState.TERMINATING:
                    # mark as terminated and notify subscriber
                    process, updated = self.process_change_state(
                        process, ProcessState.TERMINATED, assigned=None)

                # otherwise it may need to be rescheduled
                elif process.state in (ProcessState.PENDING,
                                    ProcessState.RUNNING):

                    if self.process_should_restart(process, state):
                        self.process_next_round(process)
                    else:
                        self.process_change_state(process, state, assigned=None)

                # send cleanup request to EEAgent now that we have dealt
                # with the dead process
                self.eeagent_client.cleanup_process(sender, upid, round)

        new_assigned = []
        for owner, upid, round in resource.assigned:
            key = (owner, upid, round)
            process = self.store.get_process(owner, upid)

            if key in assigned_procs:
                new_assigned.append(key)
            # prune process assignments once the process has terminated or
            # moved onto the next round
            elif (process and process.round == round
                 and process.state < ProcessState.TERMINATED):
                new_assigned.append(key)

        if len(new_assigned) != len(resource.assigned):
            # first update node exclusive tags
            if node_exclusives_to_remove:
                node = self.store.get_node(resource.node_id)
                if node:
                    self.node_remove_exclusive_tags(node, node_exclusives_to_remove)
                else:
                    log.warning("Node %s not found while attempting to update node_exclusive",
                        resource.node_id)

            if log.isEnabledFor(logging.DEBUG):
                old_assigned_set = set(tuple(item) for item in resource.assigned)
                new_assigned_set = set(tuple(item) for item in new_assigned)
                difference_message = get_set_difference_debug_message(
                    old_assigned_set, new_assigned_set)
                log.debug("updating resource %s assignments: %s",
                    resource.resource_id, difference_message)

            resource.assigned = new_assigned
            resource_updated = True

        if resource_updated:
            try:
                self.store.update_resource(resource)
            except (WriteConflictError, NotFoundError):
                # TODO? right now this will just wait for the next heartbeat
                pass

    def process_should_restart(self, process, exit_state, is_system_restart=False):

        config = process.configuration
        if is_system_restart and config:
            try:
                process_config = config.get('process')
                if process_config and process_config.get('omit_from_system_restart'):
                    return False
            except Exception:
                # don't want a weird process config structure to blow up PD
                log.exception("Error inspecting process config")
        should_restart = False
        if process.restart_mode is None or process.restart_mode == RestartMode.ABNORMAL:
            if exit_state != ProcessState.EXITED:
                should_restart = True

        elif process.restart_mode == RestartMode.ALWAYS:
            should_restart = True

        return should_restart

    def resource_change_state(self, resource, newstate):
        updated = False
        while resource and resource.state not in (ExecutionResourceState.DISABLED, newstate):
            resource.state = newstate
            try:
                self.store.update_resource(resource)
                updated = True
            except NotFoundError:
                resource = None
            except WriteConflictError:
                try:
                    resource = self.store.get_resource(resource.resource_id)
                except NotFoundError:
                    resource = None
        return resource, updated

    def _first_heartbeat(self, sender, beat):

        node_id = beat.get('node_id')
        if not node_id:
            log.error("EE heartbeat from %s without a node_id!: %s", sender, beat)
            return

        node = self.store.get_node(node_id)
        if node is None:
            log.warn("EE heartbeat from unknown node. Still booting? " +
                     "node_id=%s sender=%s.", node_id, sender)

            # TODO I'm thinking the best thing to do here is query EPUM
            # for the state of this node in case the initial node_state
            # update got lost. Note that we shouldn't go ahead and
            # schedule processes onto this EE until we get the RUNNING
            # node_state update -- there could be a failure later on in
            # the contextualization process that triggers the node to be
            # terminated.

            return

        if node.properties:
            properties = node.properties.copy()
        else:
            properties = {}

        log.info("First heartbeat from EEAgent %s on node %s (%s)",
            sender, node_id, properties.get("hostname", "unknown hostname"))

        try:
            engine_id = engine_id_from_domain(node.domain_id)
        except ValueError:
            log.exception("Node for EEagent %s has invalid domain_id!", sender)
            return

        engine_spec = self.get_engine(engine_id)
        slots = engine_spec.slots

        # just making engine type a generic property/constraint for now,
        # until it is clear something more formal is needed.
        properties['engine'] = engine_id

        try:
            self.node_add_resource(node, sender)
        except NotFoundError:
            log.warn("Node removed while processing heartbeat. ignoring. "
                     "node_id=%s sender=%s.", node_id, sender)
            return

        timestamp_str = beat['timestamp']
        timestamp = ceiling_datetime(parse_datetime(timestamp_str))

        resource = ResourceRecord.new(sender, node_id, slots, properties)
        resource.new_last_heartbeat_datetime(timestamp)
        try:
            self.store.add_resource(resource)
        except WriteConflictError:
            # no problem if this resource was just created by another worker
            log.info("Conflict writing new resource record %s. Ignoring.", sender)

    def get_resource_engine(self, resource):
        """Return an execution engine spec for a resource
        """
        engine_id = resource.properties.get('engine')
        if not engine_id:
            raise ValueError("Resource has no engine property")
        return self.get_engine(engine_id)

    def get_engine(self, engine_id):
        """Return an execution engine spec object
        """
        return self.ee_registry.get_engine_by_id(engine_id)

    def node_add_resource(self, node, resource_id):
        """Tentatively adds a resource to a node, retrying if conflict

        Note that this may raise NotFoundError if the node is removed before
        completion
        """
        updated = False
        while resource_id not in node.resources:
            node.resources.append(resource_id)
            try:
                self.store.update_node(node)
                updated = True
            except WriteConflictError:
                node = self.store.get_node(node.node_id)

        return node, updated

    def node_add_exclusive_tags(self, node, tags):
        """Add node exclusive tags to a node

        Will retry as long as node exists and tags are not present.
        Raises a NotFoundError if node record is removed
        """
        log.debug("Adding node %s node_exclusive tags: %s", node.node_id, tags)
        while True:
            update_needed = False
            for tag in tags:
                tag = str(tag)
                if tag not in node.node_exclusive:
                    node.node_exclusive.append(tag)
                    update_needed = True

            if not update_needed:
                return node, False  # nothing to update

            try:
                self.store.update_node(node)
                return node, True

            except WriteConflictError:
                node = self.store.get_node(node.node_id)

    def node_remove_exclusive_tags(self, node, tags):
        """Remove node exclusive tags from a node record

        Will retry as long as node exists and tags are present.
        """
        log.debug("Removing node %s node_exclusive tags: %s", node.node_id, tags)
        while True:
            new_node_exclusive = [tag for tag in node.node_exclusive if tag not in tags]
            if new_node_exclusive == node.node_exclusive:
                return node, False  # nothing to update, tags are already gone

            node.node_exclusive = new_node_exclusive
            try:
                self.store.update_node(node)
                return node, True

            except WriteConflictError:
                node = self.store.get_node(node.node_id)
            except NotFoundError:
                # we don't care if the node was removed
                return None, False

    def process_next_round(self, process, newstate=ProcessState.DIED_REQUESTED, enqueue=True):
        """Tentatively advance a process to the next round
        """
        assert newstate and newstate < ProcessState.TERMINATING

        cur_round = process.round
        updated = False
        while (process.state < ProcessState.TERMINATING and
               cur_round == process.round):
            process.state = newstate
            process.assigned = None
            process.round = cur_round + 1
            try:
                self.store.update_process(process)
                updated = True

                log.info(get_process_state_message(process))

            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)

        if updated:
            if enqueue:
                self.store.enqueue_process(process.owner, process.upid,
                    process.round)
            self.notifier.notify_process(process)
        return process, updated

    def process_change_state(self, process, newstate, **updates):
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
            if newstate == ProcessState.RUNNING:
                process.increment_starts()
            process.state = newstate
            process.update(updates)
            try:
                self.store.update_process(process)
                updated = True

                # log as error when processes fail
                if newstate == ProcessState.FAILED:
                    log.error(get_process_state_message(process))
                else:
                    log.info(get_process_state_message(process))

            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)

        if updated:
            self.notifier.notify_process(process)
        return process, updated

    def get_process_constraints(self, process):
        """Returns a dict of process constraints

        Includes constraints from the process itself as well as from the engine registry
        """
        constraints = {}
        engine_id = self.ee_registry.get_process_definition_engine_id(process.definition)
        if engine_id is None and process.constraints.get('engine') is None:
            engine_id = self.ee_registry.default

        if engine_id is not None:
            constraints['engine'] = engine_id

        if process.constraints:
            process.constraints.update(constraints)
            constraints = process.constraints
        return constraints

    def dump(self):
        resources = {}
        nodes = {}
        processes = {}
        state = dict(resources=resources, processes=processes, nodes=nodes)

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

        for node_id in self.store.get_node_ids():
            node = self.store.get_node(node_id)
            if not node:
                continue
            nodes[node_id] = dict(node)

        return state


def get_set_difference_debug_message(set1, set2):
    """Utility function for building log messages about set content changes
    """
    try:
        difference1 = list(set1.difference(set2))
        difference2 = list(set2.difference(set1))
    except Exception, e:
        return "can't calculate set difference. are these really sets?: %s" % str(e)

    if difference1 and difference2:
        return "removed=%s added=%s" % (difference1, difference2)
    elif difference1:
        return "removed=%s" % (difference1,)
    elif difference2:
        return "added=%s" % (difference2,)
    else:
        return "sets are equal"


def _check_process_schedule_idempotency(process, parameters):
    """Checks supplied parameters against an existing process object
    """
    for key, value in parameters.iteritems():
        # name is for decoration only. it's convenient to ignore it here.
        if key == "name":
            continue
        process_value = process.get(key)
        if process_value != value:
            raise BadRequestError(
                "process %s is %s but %s parameter value doesn't match request"
                % (process.upid, process.state, key))
