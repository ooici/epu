import logging

from dashi import bootstrap, DashiError

from epu.epumanagement.test.mocks import MockOUAgentClient, MockProvisionerClient
from epu.epumanagement import EPUManagement
from epu.dashiproc.provisioner import ProvisionerClient
from epu.util import get_config_paths
from epu.exceptions import UserNotPermittedError

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

        self.default_user = self.CFG.epumanagement.get('default_user')

        # TODO: create ION class here or depend on epuagent repo as a dep
        ou_client = MockOUAgentClient()

        if self.CFG.epumanagement.has_key('mock_provisioner') and \
           self.CFG.epumanagement['mock_provisioner']:
            prov_client = MockProvisionerClient()
        else:
            provisioner_topic = self.CFG.epumanagement.provisioner_service_name
            prov_client = ProvisionerClient(self.dashi, handle_instance_state=False, topic=provisioner_topic)

        self.epumanagement = EPUManagement(self.CFG.epumanagement, SubscriberNotifier(self.dashi),
                                           prov_client, ou_client)

        # hack to inject epum reference for mock prov client
        if isinstance(prov_client, MockProvisionerClient):
            prov_client._set_epum(self.epumanagement)

    def start(self):
        self.dashi.handle(self.subscribe_dt)
        self.dashi.handle(self.unsubscribe_dt)
        self.dashi.handle(self.add_epu)
        self.dashi.handle(self.remove_epu)
        self.dashi.handle(self.list_epus)
        self.dashi.handle(self.describe_epu)
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
                self.epumanagement.msg_add_epu(self.default_user, epu_name, epu_config)
            except Exception:
                log.exception("Failed to load EPU %s", epu_name)

        # blocks til dashi.cancel() is called
        self.dashi.consume()

    @property
    def default_user(self):
        if not self._default_user:
            msg = "Operation called for the default user, but none is defined."
            raise UserNotPermittedError(msg)
        else:
            return self._default_user

    @default_user.setter
    def default_user(self, default_user):
        self._default_user = default_user

    def subscribe_dt(self, dt_id, subscriber_name, subscriber_op):
        self.epumanagement.msg_subscribe_dt(None, dt_id, subscriber_name, subscriber_op)

    def unsubscribe_dt(self, dt_id, subscriber_name):
        self.epumanagement.msg_unsubscribe_dt(None, dt_id, subscriber_name)

    def list_epus(self, caller=None):
        """Return a list of EPUs in the system
        """
        caller = caller or self.default_user
        return self.epumanagement.msg_list_epus(caller=caller)

    def describe_epu(self, epu_name, caller=None):
        """Return a state structure for an EPU, or None
        """
        caller = caller or self.default_user
        try:
            return self.epumanagement.msg_describe_epu(caller, epu_name)
        except DashiError, e:
            exception_class, _, exception_message = str(e).partition(':')
            if exception_class == 'NotFoundError':
                raise NotFoundError("Unknown domain: %s" % epu_name)
            else:
                raise

    def add_epu(self, epu_name, epu_config, subscriber_name=None,
                subscriber_op=None, caller=None):
        caller = caller or self.default_user
        self.epumanagement.msg_add_epu(caller, epu_name, epu_config,
            subscriber_name=subscriber_name, subscriber_op=subscriber_op)

    def remove_epu(self, epu_name, caller=None):
        caller = caller or self.default_user
        self.epumanagement.msg_remove_epu(caller, epu_name)

    def reconfigure_epu(self, epu_name, epu_config, caller=None):
        caller = caller or self.default_user
        self.epumanagement.msg_reconfigure_epu(caller, epu_name, epu_config)

    def ou_heartbeat(self, heartbeat):
        self.epumanagement.msg_heartbeat(None, heartbeat) # epum parses

    def instance_info(self, record):
        self.epumanagement.msg_instance_info(None, record) # epum parses

    def sensor_info(self, info):
        self.epumanagement.msg_sensor_info(None, info) # epum parses


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

    def add_epu(self, epu_name, epu_config, subscriber_name=None,
                subscriber_op=None):
        self.dashi.call(self.topic, "add_epu", epu_name=epu_name,
            epu_config=epu_config, subscriber_name=subscriber_name,
            subscriber_op=subscriber_op)

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
