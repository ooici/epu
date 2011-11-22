import logging

from epu.decisionengine.engineapi import Engine
from epu.epumanagement.health import InstanceHealthState
import epu.states as InstanceStates

log = logging.getLogger(__name__)

class MockProvisionerClient(object):
    """See the IProvisionerClient class.
    """
    def __init__(self):
        self.provision_count = 0
        self.terminate_node_count = 0
        self.launched_instance_ids = []
        self.terminated_instance_ids = []
        self.deployable_types_launched = []
        self.epum = None

    def _set_epum(self, epum):
        # circular ref, only in this mock/unit test situation
        self.epum = epum

    def provision(self, launch_id, deployable_type, launch_description, subscribers, vars=None):
        self.provision_count += 1
        #log.debug("provision() count %d" % self.provision_count)
        for group,item in launch_description.iteritems():
            self.launched_instance_ids.extend(item.instance_ids)
            for _ in item.instance_ids:
                self.deployable_types_launched.append(deployable_type)

    def terminate_launches(self, launches):
        log.debug("terminate_launches()")

    def terminate_nodes(self, nodes):
        self.terminate_node_count += len(nodes)
        self.terminated_instance_ids.extend(nodes)
        if self.epum:
            for node in nodes:
                content = {"node_id": node, "state": InstanceStates.TERMINATING}
                self.epum.msg_instance_info(None, content)

    def terminate_all(self, rpcwait=False, retries=5, poll=1.0):
        log.debug("terminate_all()")

    def dump_state(self, nodes, force_subscribe=None):
        log.debug("dump_state()")

class MockSubscriberNotifier(object):
    """See the ISubscriberNotifier class
    """
    def __init__(self):
        self.notify_by_name_called = 0
        self.receiver_names = []
        self.operations = []
        self.messages = []

    def notify_by_name(self, receiver_name, operation, message):
        """The name is translated into the appropriate messaging-layer object.
        @param receiver_name Message layer name
        @param operation The operation to call on that name
        @param message dict to send
        """
        self.notify_by_name_called += 1
        self.receiver_names.append(receiver_name)
        self.operations.append(operation)
        self.messages.append(message)

class MockOUAgentClient(object):
    """See the IOUAgentClient class
    """
    def __init__(self):
        self.epum = None
        self.dump_state_called = 0
        self.heartbeats_sent = 0
        self.respond_to_dump_state = False
        
    def dump_state(self, target_address, mock_timestamp=None):
        self.dump_state_called += 1
        if self.epum and self.respond_to_dump_state:
            # In Mock mode, we know that node_id and the OUAgent address are equal things, by convention
            content = {'node_id':target_address, 'state':InstanceHealthState.OK}
            self.epum.msg_heartbeat(None, content, timestamp=mock_timestamp)
            self.heartbeats_sent += 1

    def _set_epum(self, epum):
        # circular ref, only in this mock/unit test situation
        self.epum = epum

class MockDecisionEngine01(Engine):
    """
    Counts only
    """

    def __init__(self):
        Engine.__init__(self)
        self.initialize_count = 0
        self.initialize_conf = None
        self.decide_count = 0
        self.reconfigure_count = 0

    def initialize(self, control, state, conf=None):
        self.initialize_count += 1
        self.initialize_conf = conf

    def decide(self, *args):
        self.decide_count += 1

    def reconfigure(self, *args):
        self.reconfigure_count += 1

class MockDecisionEngine02(Engine):
    """
    Fails only
    """

    def __init__(self):
        Engine.__init__(self)
        self.initialize_count = 0
        self.initialize_conf = None
        self.decide_count = 0
        self.reconfigure_count = 0

    def initialize(self, control, state, conf=None):
        self.initialize_count += 1
        self.initialize_conf = conf

    def decide(self, *args):
        self.decide_count += 1
        raise Exception("decide disturbance")

    def reconfigure(self, *args):
        self.reconfigure_count += 1
        raise Exception("reconfigure disturbance")
