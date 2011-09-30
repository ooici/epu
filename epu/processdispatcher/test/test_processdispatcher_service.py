from ion.util import procutils
from twisted.internet import defer

from ion.test.iontest import IonTestCase
import ion.util.ionlog

from epu.ionproc.processdispatcher import ProcessDispatcherService, ProcessDispatcherClient
from epu.processdispatcher.test import FakeEEAgent
import epu.states as InstanceStates

log = ion.util.ionlog.getLogger(__name__)

class ProcessDispatcherServiceTests(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        self.pd = ProcessDispatcherService()
        self.pd_name = self.pd.get_scoped_name("system", self.pd.svc_name)

        yield self._spawn_process(self.pd)

        self.client = ProcessDispatcherClient()
        yield self.client.attach()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def _spawn_eeagent(self, node_id, slot_count,
                       heartbeat_dest=None,
                       heartbeat_op="ee_heartbeat"):
        if heartbeat_dest is None:
            heartbeat_dest = self.pd_name
        spawnargs = dict(node_id=node_id, heartbeat_dest=heartbeat_dest,
                         heartbeat_op=heartbeat_op, slot_count=slot_count)
        agent = FakeEEAgent(spawnargs=spawnargs)
        yield self._spawn_process(agent)
        defer.returnValue(agent)

    @defer.inlineCallbacks
    def _assert_pd_dump(self, fun):
        state = yield self.client.dump()
        log.debug("PD state: %s", state)
        fun(state)

    @defer.inlineCallbacks
    def _wait_assert_pd_dump(self, fun, attempts=10):
        tries = 0
        while True:
            try:
                yield self._assert_pd_dump(fun)
            except Exception:
                tries += 1
                if tries == attempts:
                    log.error("PD state assertion failing after %d attempts",
                              attempts)
                    raise
            else:
                return
            yield procutils.asleep(0.01)

    @defer.inlineCallbacks
    def test_basics(self):

        # create some fake nodes and tell PD about them
        nodes = ["node1", "node2", "node3"]

        for node in nodes:
            yield self.client.dt_state(node, "dt1", InstanceStates.RUNNING)

        # PD knows about these nodes but hasn't gotten a heartbeat yet

        # spawn the eeagents and tell them all to heartbeat
        eeagents = []
        for node in nodes:
            eeagent = yield self._spawn_eeagent(node, 4)
            yield eeagent.send_heartbeat()
            eeagents.append(eeagent)

        def assert_all_resources(state):
            eeagent_nodes = set()
            for resource in state['resources'].itervalues():
                eeagent_nodes.add(resource['node_id'])
            self.assertEqual(set(nodes), eeagent_nodes)

        yield self._wait_assert_pd_dump(assert_all_resources)

        spec = {"omg": "imaprocess"}

        procs = ["proc1", "proc2", "proc3"]
        for proc in procs:
            procstate = yield self.client.dispatch_process(proc, spec, None)
            self.assertEqual(procstate['epid'], proc)

        def assert_these_running(state):
            found = False
            for resource in state['resources'].itervalues():
                resource_procs = set(resource['processes'])
                if resource_procs:
                    if found:
                        self.fail("expected grouped processes")
                    self.assertEqual(resource_procs, set(procs))
                    found = True
            self.assertTrue(found)

        yield self._wait_assert_pd_dump(assert_these_running)

        # now terminate one process
        todie = procs.pop()
        procstate = yield self.client.terminate_process(todie)
        self.assertEqual(procstate['epid'], todie)

        yield self._wait_assert_pd_dump(assert_these_running)
