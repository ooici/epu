import dashi.bootstrap as bootstrap
import unittest
from time import sleep

from dashi.util import LoopingCall

from epu.dashiproc.provisioner_query import ProvisionerQueryService

class TestProvisionerQueryService(unittest.TestCase):

    def setUp(self):

        LoopingCall.start = self._fake_start

        self.loop_interval = None
        self.query_called = False

    def _fake_start(self, interval, now=True):
        self.loop_interval = interval

    def _query_error(self):
        self.query_called = True
        raise Exception("expected")

    def _query_ok(self):
        self.query_called = True
        return

    def test_query(self):
        query = ProvisionerQueryService(interval_seconds = 5.0)
        bootstrap._start_methods([query.start], join=False)
        sleep(0) # yield to allow gevent to start query.start

        self.assertEqual(self.loop_interval, 5.0)

        query.client.query = self._query_ok
        query.query()

        self.assertTrue(self.query_called)

    def test_query_error(self):
        query = ProvisionerQueryService(interval_seconds = 5.0)
        bootstrap._start_methods([query.start], join=False)
        sleep(0) # yield to allow gevent to start query.start

        self.assertEqual(self.loop_interval, 5.0)

        query.client.query = self._query_error
        # no exception should bubble up
        query.query()
        self.assertTrue(self.query_called)
