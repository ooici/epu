import epu.states as InstanceStates

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

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
    def notify_by_name(self, receiver_name):
        """The name is translated into the appropriate messaging-layer object.
        """
        log.debug("notify_by_name()")

    def notify_by_object(self, receiver_object):
        """Uses the appropriate messaging-layer object which the caller already has a
        reference to.
        """
        log.debug("notify_by_object()")

class MockDecisionEngine01(object):

    def __init__(self):
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
