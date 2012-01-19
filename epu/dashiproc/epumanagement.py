import logging

from dashi import bootstrap

from epu.epumanagement.test.mocks import MockOUAgentClient, MockProvisionerClient
from epu.epumanagement import EPUManagement
from epu.dashiproc.provisioner import ProvisionerClient
from epu.util import get_config_paths

log = logging.getLogger(__name__)

class EPUManagementService(object):
    """EPU management service interface

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    """

    def __init__(self):
        configs = ["service", "epumanagement"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        self.dashi = bootstrap.dashi_connect(self.CFG.epumanagement.service_name, self.CFG)

        # TODO: create ION class here or depend on epuagent repo as a dep
        ou_client = MockOUAgentClient()

        if self.CFG.epumanagement.has_key('mock_provisioner') and \
           self.CFG.epumanagement['mock_provisioner']:
            prov_client = MockProvisionerClient()
        else:
            prov_client = ProvisionerClient(self.dashi, handle_instance_state=False)

        self.epumanagement = EPUManagement(self.CFG.epumanagement, SubscriberNotifier(self.dashi),
                                           prov_client, ou_client)

        # hack to inject epum reference for mock prov client
        if isinstance(prov_client, MockProvisionerClient):
            prov_client._set_epum(self.epumanagement)

    def start(self):
        self.dashi.handle(self.register_need)
        self.dashi.handle(self.retire_node)
        self.dashi.handle(self.subscribe_dt)
        self.dashi.handle(self.unsubscribe_dt)
        self.dashi.handle(self.add_epu)
        self.dashi.handle(self.remove_epu)
        self.dashi.handle(self.reconfigure_epu)
        self.dashi.handle(self.ou_heartbeat)
        self.dashi.handle(self.instance_info)
        self.dashi.handle(self.sensor_info)

        # this may spawn some background threads
        self.epumanagement.initialize()

        # hack to load some epus at boot. later this should be client driven.
        initial_epus = self.CFG.epumanagement.initial_epus
        for epu_name, epu_config in initial_epus.iteritems():
            log.info("Loading EPU %s", epu_name)
            try:
                self.epumanagement.msg_add_epu(None, epu_name, epu_config)
            except Exception:
                log.exception("Failed to load EPU %s", epu_name)

        # blocks til dashi.cancel() is called
        self.dashi.consume()

    def register_need(self, dt_id, constraints, num_needed,
                      subscriber_name=None, subscriber_op=None):

        self.epumanagement.msg_register_need(None, dt_id, constraints,
                                             num_needed, subscriber_name,
                                             subscriber_op)

    def retire_node(self, node_id):
        self.epumanagement.msg_retire_node(None, node_id)

    def subscribe_dt(self, dt_id, subscriber_name, subscriber_op):
        self.epumanagement.msg_subscribe_dt(None, dt_id, subscriber_name, subscriber_op)

    def unsubscribe_dt(self, dt_id, subscriber_name):
        self.epumanagement.msg_unsubscribe_dt(None, dt_id, subscriber_name)

    def list_epus(self):
        """Return a list of EPUs in the system
        """
        return self.epumanagement.msg_list_epus()

    def describe_epu(self, epu_name):
        """Return a state structure for an EPU, or None
        """
        return self.epumanagement.msg_describe_epu(None, epu_name)

    def add_epu(self, epu_name, epu_config):
        self.epumanagement.msg_add_epu(None, epu_name, epu_config)

    def remove_epu(self, epu_name):
        self.epumanagement.msg_remove_epu(None, epu_name)

    def reconfigure_epu(self, epu_name, epu_config):
        self.epumanagement.msg_reconfigure_epu(None, epu_name, epu_config)

    def ou_heartbeat(self, heartbeat):
        self.epumanagement.msg_heartbeat(None, heartbeat) # epum parses

    def instance_info(self, record):
        self.epumanagement.msg_instance_info(None, record) # epum parses

    def sensor_info(self, info):
        self.epumanagement.msg_sensor_info(None, info) # epum parses

_unported_r1 = """
    @defer.inlineCallbacks
    def op_reconfigure_epu_rpc(self, content, headers, msg):
        yield self.epumanagement.msg_reconfigure_epu_rpc(None, content)
        yield self.reply_ok(msg, "")

    @defer.inlineCallbacks
    def op_de_state(self, content, headers, msg):
        state = self.core.de_state()
        extradict = {"state":state}
        cei_events.event(self.svc_name, "de_state", extra=extradict)
        yield self.reply_ok(msg, state)

    @defer.inlineCallbacks
    def op_whole_state(self, content, headers, msg):
        state = yield self.core.whole_state()
        yield self.reply_ok(msg, state)

    @defer.inlineCallbacks
    def op_node_error(self, content, headers, msg):
        node_id = content
        state = yield self.core.node_error(node_id)
        yield self.reply_ok(msg, state)
"""

class SubscriberNotifier(object):
    """See: ISubscriberNotifier
    """
    def __init__(self, dashi):
        self.dashi = dashi

    def notify_by_name(self, receiver_name, operation, message):
        """The name is translated into the appropriate messaging-layer object.
        @param receiver_name Message layer name
        @param operation The operation to call on that name
        @param message dict to send
        """
        self.dashi.fire(receiver_name, operation, args=message)


class EPUManagementClient(object):
    """See: IEpuManagementClient
    """
    def __init__(self, dashi, topic):
        self.dashi = dashi
        self.topic = topic

    def register_need(self, dt_id, constraints, num_needed,
                      subscriber_name=None, subscriber_op=None):

        self.dashi.fire(self.topic, "register_need", dt_id=dt_id,
                        constraints=constraints,
                        num_needed=num_needed,
                        subscriber_name=subscriber_name,
                        subscriber_op=subscriber_op)

    def retire_node(self, node_id):
        self.dashi.fire(self.topic, "retire_node", node_id=node_id)

    def subscribe_dt(self, dt_id, subscriber_name, subscriber_op):
        self.dashi.fire(self.topic, "subscribe_dt", dt_id=dt_id,
                        subscriber_name=subscriber_name,
                        subscriber_op=subscriber_op)

    def unsubscribe_dt(self, dt_id, subscriber_name):
        self.dashi.fire(self.topic, "unsubscribe_dt", dt_id=dt_id,
                        subscriber_name=subscriber_name)

    def list_epus(self):
        return self.dashi.call(self.topic, "list_epus")

    def describe_epu(self, epu_name):
        return self.dashi.call(self.topic, "describe_epu", epu_name=epu_name)

    def add_epu(self, epu_name, epu_config):
        self.dashi.call(self.topic, "add_epu", epu_name=epu_name,
                        epu_config=epu_config)

    def remove_epu(self, epu_name):
        self.dashi.call(self.topic, "remove_epu", epu_name=epu_name)

    def reconfigure_epu(self, epu_name, epu_config):
        self.dashi.call(self.topic, "reconfigure_epu", epu_name=epu_name,
                        epu_config=epu_config)

    def ou_heartbeat(self, heartbeat):
        self.dashi.fire(self.topic, "ou_heartbeat", heartbeat=heartbeat)

    def instance_info(self, record):
        self.dashi.fire(self.topic, "instance_info", record=record)

    def sensor_info(self, info):
        self.dashi.fire(self.topic, "sensor_info", info=info)



def main():
    logging.basicConfig(level=logging.DEBUG)
    epum = EPUManagementService()
    epum.start()
