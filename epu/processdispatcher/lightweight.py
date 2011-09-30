import copy
from twisted.internet import defer

import ion.util.ionlog
import epu.states as InstanceStates

log = ion.util.ionlog.getLogger(__name__)


class ExecutionEngineRegistryEntry(object):
    def __init__(self, deployable_type, execution_engines):
        self.deployable_type = deployable_type
        self.execution_engines = list(execution_engines)


class ExecutionEngineRegistry(object):
    """Simple in-memory registry mapping deployable types to execution engines
    """
    def __init__(self):
        self.by_ee = {}
        self.by_dt = {}

    def add_entry(self, entry):
        self.by_dt[entry.deployable_type] = entry
        for ee in entry.execution_engines:
            self.by_ee[ee] = entry

    def get_by_engine_type(self, engine_type):
        return self.by_ee.get(engine_type)

    def get_by_deployable_type(self, deployable_type):
        return self.by_dt.get(deployable_type)


class DeployedNode(object):
    def __init__(self, node_id, dt, properties=None):
        self.node_id = node_id
        self.dt = dt
        self.properties = properties


class ExecutionEngineResource(object):
    def __init__(self, node_id, ee_id):
        self.node_id = node_id
        self.ee_id = ee_id

        self.last_heartbeat = None
        self.slot_count = 0
        self.processes = {}

    def intake_heartbeat(self, beat):
        pass


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

    def __init__(self, ee_registry):
        self.ee_registry = ee_registry

        self.processes = {}
        self.resources = {}
        self.nodes = {}

        self.queue = []


    def dispatch_process(self, epid, engine_type, description, subscribers, constraints=None, immediate=False):
        """Dispatch a new process into the system

        @param epid: unique process identifier
        @param engine_type: needed execution engine type
        @param description: description of what is started
        @param subscribers: where to send status updates of this process
        @param constraints: optional scheduling constraints (IaaS site? other stuff?)
        @param immediate: don't provision new resources if no slots are available
        @rtype: L{ProcessState}
        @return: description of process launch status


        This is an RPC-style call that returns quickly, as soon as a decision is made:

            1. If a matching slot is available, dispatch begins and a PENDING
               response is sent. Further updates are sent to subscribers.

            2. If no matching slot is available, behavior depends on immediate flag
               - If immediate is True, an error is returned
               - If immediate is False, a provision request is sent and
                 WAITING is returned. Further updates are sent to subscribers.

        At the point of return, the request is either pending (and guaranteed
        to be followed through til error or success), or has failed.


        Retry
        =====
        If a call to this operation times out without a reply, it can safely
        be retried. The epid and other parameters will be used to ensure that
        nothing is repeated. If the service fields an operation request that
        it thinks has already been acknowledged, it will return the current
        state of the process (or a defined AlreadyDidThatError if that is too
        difficult).
        """

        if epid in self.processes:
            return defer.succeed(self.processes[epid])

    def terminate_process(self, epid):
        """
        Kill a running process
        @param epid: ID of process
        @rtype: L{ProcessState}
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

    def dt_state(self, node_id, deployable_type, state, properties=None):
        """
        Handle updates about available instances of deployable types.

        @param dt_state: state information about DT(s)
        @return:

        This operation is the recipient of a "subscription" the PD makes to
        DT state updates. Calls to this operation are NOT RPC-style.

        This information is used for two purposes:

            1. To correlate EE agent heartbeats with a DT and various deploy
               information (site, allocation, security groups, etc).

            2. To detect EEs which have been killed due to underlying death
               of a resource (VM).
        """

        if state == InstanceStates.RUNNING:
            if node_id not in self.nodes:
                node = DeployedNode(node_id, deployable_type, properties)
                self.nodes[node_id] = node
        elif state in (InstanceStates.TERMINATING, InstanceStates.TERMINATED):
            #TODO reschedule processes running on node
            pass

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
            - slot_count - number of available slots
        """

        node_id = beat['node_id']
        processes = beat['processes']
        slot_count = int(beat['slot_count'])

        resource = self.resources.get(sender)
        if resource is None:
            # first heartbeat from this EE

            node = self.nodes.get(node_id)
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

                return defer.succeed(None)

            resource = ExecutionEngineResource(node_id, sender)
            self.resources[sender] = resource

            log.info("Got first heartbeat from EEAgent %s on node %s",
                     sender, node_id)

        # really we want to fold in changes?
        resource.processes = processes

        new_slots_available = slot_count > resource.slot_count
        resource.slot_count = slot_count

        if new_slots_available:
            self._consider_resource(resource)

        return defer.succeed(None)

    def dump(self):
        resources = {}
        processes = {}
        state = dict(resources=resources, processes=processes)

        for resource in self.resources.itervalues():
            resource_dict = dict(ee_id=resource.ee_id,
                                 node_id=resource.node_id,
                                 processes=copy.deepcopy(processes),
                                 slot_count=resource.slot_count)
            resources[resource.ee_id] = resource_dict

        #TODO processes

        return defer.succeed(state)

    def _consider_resource(self, resource):
        pass