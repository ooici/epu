from twisted.internet.task import LoopingCall
from twisted.internet import defer

from epu.ionproc.provisioner_query import ProvisionerQueryService
from ion.test.iontest import IonTestCase

class TestProvisionerQueryService(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):

        yield self._start_container()
        self.patch(LoopingCall, "start", self._fake_start)

        self.loop_interval = None
        self.query_called = False

    def _fake_start(self, interval, now=True):
        self.loop_interval = interval

    def _query_error(self):
        self.query_called = True
        return defer.fail(Exception("expected"))

    def _query_ok(self):
        self.query_called = True
        return defer.succeed(None)

    @defer.inlineCallbacks
    def test_query(self):
        query = ProvisionerQueryService(spawnargs={"interval_seconds": 5.0})
        yield self._spawn_process(query)

        self.assertEqual(self.loop_interval, 5.0)

        self.patch(query.client, "query", self._query_ok)
        query.query()

        self.assertTrue(self.query_called)

    @defer.inlineCallbacks
    def test_query_error(self):
        query = ProvisionerQueryService(spawnargs={"interval_seconds": 5.0})
        yield self._spawn_process(query)

        self.assertEqual(self.loop_interval, 5.0)

        self.patch(query.client, "query", self._query_error)
        # no exception should bubble up
        query.query()
        self.assertTrue(self.query_called)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()
