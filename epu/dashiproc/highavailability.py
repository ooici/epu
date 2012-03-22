import logging
import dashi.bootstrap as bootstrap

from dashi.util import LoopingCall

import epu.highavailability.policy as policy

from epu.highavailability.core import HighAvailabilityCore
from epu.epumanagement.test.mocks import MockProvisionerClient
from epu.dashiproc.processdispatcher import ProcessDispatcherClient
from epu.util import get_class, get_config_paths

log = logging.getLogger(__name__)

DEFAULT_TOPIC = "haservice"

policy_map = {
        'npreserving': policy.NPreservingPolicy,
}

class HighAvailabilityService(object):

    topic = DEFAULT_TOPIC
    started = False

    def __init__(self, *args, **kwargs):

        configs = ["service", "highavailability"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        exchange = kwargs.get('exchange')
        if exchange:
            self.CFG.server.amqp.exchange = exchange

        self.amqp_uri = kwargs.get('amqp_uri') or None
        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG, self.amqp_uri)

        process_dispatchers = (kwargs.get('process_dispatchers') or
                self.CFG.highavailability.processdispatchers)
        pd_client = self._make_pd_client(ProcessDispatcherClient, self.dashi)
        
        policy_name = self.CFG.highavailability.policy.name
        try:
            self.policy = policy_map[policy_name.lower()]
        except KeyError:
            raise Exception("HA Service doesn't support '%s' policy" % policy_name)

        policy_parameters = (kwargs.get('policy_parameters') or
                self.CFG.highavailability.policy.parameters)

        process_spec = (kwargs.get('process_spec') or 
                self.CFG.highavailability.process_spec)

        core = HighAvailabilityCore
        self.core = core(self.CFG.highavailability, pd_client,
                process_dispatchers, process_spec, self.policy)

    def start(self):

        log.info("starting high availability instance %s" % self)

        # Set up operations
        self.dashi.handle(self.reconfigure_policy)
        self.dashi.handle(self.dump)

        self.apply_policy_loop = LoopingCall(self.core.apply_policy)
        self.apply_policy_loop.start(5)

        self.started = True

        try:
            self.dashi.consume()
        except KeyboardInterrupt:
            self.apply_policy_loop.stop()
            log.warning("Caught terminate signal. Bye!")
        else:
            self.apply_policy_loop.stop()
            log.info("Exiting normally. Bye!")


    def reconfigure_policy(self, new_policy):
        """Service operation: Change the parameters of the policy used for service

        @param new_policy: parameters of policy
        @return:
        """
        self.core.reconfigure_policy(new_policy)

    def dump(self):
        """Dump state of ha core
        """
        return self.core.dump()

    @staticmethod
    def _make_pd_client(client_kls, dashi):
        """Returns a function that in turn returns a ProcessDispatcherClient
        that takes its name as its only argument. This is to avoid having 
        dashi specific things in the ha core
        """
        def make_pd_client(topic):
            return client_kls(dashi, topic)

        return make_pd_client

class HighAvailabilityServiceClient(object):

    def __init__(self, dashi, topic=None):

        self.dashi = dashi
        if not topic:
            self.topic = DEFAULT_TOPIC

    def reconfigure_policy(self, new_policy):
        """Service operation: Change number of instances to maintain
        """
        log.debug('reconfigure_policy: %s' % new_policy)
        self.dashi.call(self.topic, "reconfigure_policy", new_policy=new_policy)

    def dump(self):
        return self.dashi.call(self.topic, "dump")

def main():
    haservice = HighAvailabilityService()
    haservice.start()
