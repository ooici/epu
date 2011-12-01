import logging
import os

from dashi import bootstrap

from epu.epumanagement.test.mocks import MockOUAgentClient
from epu.epumanagement import EPUManagement
from epu.epumanagement.clients import IProvisionerClient
#from epu.dashiproc.provisioner import ProvisionerClient
from epu.util import determine_path

log = logging.getLogger(__name__)

class EPUManagementService(object):
    """EPU management service interface

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    """

    def __init__(self):
        service_config = os.path.join(determine_path(), "config", "service.yml")
        provisioner_config = os.path.join(determine_path(), "config", "epumanagement.yml")
        config_files = [service_config, provisioner_config]
        self.CFG = bootstrap.configure(config_files)

        self.dashi = bootstrap.dashi_connect(self.CFG.epumanagement.service_name, self.CFG)

        # TODO: create ION class here or depend on epuagent repo as a dep
        ou_client = MockOUAgentClient()

        self.epumanagement = EPUManagement(self.CFG.epumanagement, SubscriberNotifier(self),
                                           IProvisionerClient(), ou_client)

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

    def add_epu(self, dt_id, epu_config):
        self.epumanagement.msg_add_epu(None, dt_id, epu_config)

    def remove_epu(self, dt_id):
        self.epumanagement.msg_remove_epu(None, dt_id)

    def reconfigure_epu(self, dt_id, epu_config):
        self.epumanagement.msg_add_epu(None, dt_id, epu_config)

    def ou_heartbeat(self, heartbeat):
        self.epumanagement.msg_heartbeat(None, heartbeat) # epum parses

    def instance_info(self, info):
        self.epumanagement.msg_instance_info(None, info) # epum parses

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
        self.dashi.fire(receiver_name, operation, message)


class EPUManagementClient(object):
    """See: IEpuManagementClient
    """
    def __init__(self, dashi):
        self.dashi = dashi

    # TODO: make operations e.g. "msg_add_epu" with full argument list that translate into
    # the 'content' bag for ION that in turn calls corresponding e.g. "op_add_epu" via ION



def main():
    epum = EPUManagementService()
    epum.start()