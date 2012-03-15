import gevent
import unittest

from epu.dashiproc.processdispatcher import ProcessDispatcherService
from epu.dashiproc.highavailability import HighAvailabilityService, HighAvailabilityServiceClient

class HighAvailabilityServiceTests(unittest.TestCase):

    amqp_uri = "memory://tests"

    def setUp(self):
        self.pd_names = ["pd_0", "pd_1"]
        self.pd_greenlets = []
        for pd_name in self.pd_names: 
            pd = ProcessDispatcherService(amqp_uri=self.amqp_uri, topic=pd_name)
            pd_greenlet = gevent.spawn(pd.start)
            self.pd_greenlets.append(pd_greenlet)

        policy_params = {'preserve_n': 0}
        self.haservice = HighAvailabilityService(policy_parameters=policy_params,
                amqp_uri=self.amqp_uri, process_dispatchers=self.pd_names)
        self.haservice_greenlet = gevent.spawn(self.haservice.start)
        self.haservice_client = HighAvailabilityServiceClient(self.haservice.dashi)

        gevent.sleep(0.05)

    def tearDown(self):
        self.haservice_greenlet.kill(exception=KeyboardInterrupt, block=True)
        for pd_greenlet in self.pd_greenlets:
            pd_greenlet.kill(exception=KeyboardInterrupt, block=True)

    def test_basic(self):
        params = {'preserve_n': 0}
        self.haservice_client.reconfigure_policy(params)

        gevent.sleep(4)
        assert self.haservice.core.policy_params == params
