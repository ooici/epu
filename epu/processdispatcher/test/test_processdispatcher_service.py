"""
import logging
from collections import defaultdict
from ion.util import procutils
from twisted.internet import defer

from ion.test.iontest import IonTestCase

from epu.ionproc.processdispatcher import ProcessDispatcherService, ProcessDispatcherClient
from epu.processdispatcher.test import FakeEEAgent
from epu.states import InstanceState, ProcessState


log = logging.getLogger(__name__)

class ProcessDispatcherServiceTests(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

        self.pd = ProcessDispatcherService()
        self.pd_name = self.pd.get_scoped_name("system", self.pd.svc_name)

        yield self._spawn_process(self.pd)

        self.client = ProcessDispatcherClient()
        yield self.client.attach()

        self.eeagents = {}

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def _spawn_eeagent(self, node_id, slot_count, engine_type,
                       heartbeat_dest=None, heartbeat_op="ee_heartbeat"):
        if heartbeat_dest is None:
            heartbeat_dest = self.pd_name
        spawnargs = dict(node_id=node_id, heartbeat_dest=heartbeat_dest,
                         heartbeat_op=heartbeat_op, slot_count=slot_count,
                         engine_type=engine_type)
        agent = FakeEEAgent(spawnargs=spawnargs)
        yield self._spawn_process(agent)
        agent_name = agent.get_scoped_name("system", str(agent.backend_id))
        self.eeagents[agent_name] = agent

        yield agent.send_heartbeat()

        defer.returnValue(agent)

    @defer.inlineCallbacks
    def _get_eeagent_for_process(self, epid):
        state = yield self.client.dump()
        process = state['processes'][epid]

        attached = process['assigned']
        if attached is None:
            defer.returnValue(None)

        defer.returnValue(self.eeagents[attached])

    @defer.inlineCallbacks
    def _assert_pd_dump(self, fun, *args, **kwargs):
        state = yield self.client.dump()
        log.debug("PD state: %s", state)
        fun(state, *args, **kwargs)

    max_tries = 10
    @defer.inlineCallbacks
    def _wait_assert_pd_dump(self, fun, *args, **kwargs):
        tries = 0
        while True:
            try:
                yield self._assert_pd_dump(fun, *args, **kwargs)
            except Exception:
                tries += 1
                if tries == self.max_tries:
                    log.error("PD state assertion failing after %d attempts",
                              tries)
                    raise
            else:
                return
            yield procutils.asleep(0.01)

    @defer.inlineCallbacks
    def test_basics(self):

        # create some fake nodes and tell PD about them
        nodes = ["node1", "node2", "node3"]

        for node in nodes:
            yield self.client.dt_state(node, "dt1", InstanceState.RUNNING)

        # PD knows about these nodes but hasn't gotten a heartbeat yet

        # spawn the eeagents and tell them all to heartbeat
        for node in nodes:
            yield self._spawn_eeagent(node, 4, "engine1")

        def assert_all_resources(state):
            eeagent_nodes = set()
            for resource in state['resources'].itervalues():
                eeagent_nodes.add(resource['node_id'])
            self.assertEqual(set(nodes), eeagent_nodes)

        yield self._wait_assert_pd_dump(assert_all_resources)

        spec = {"omg": "imaprocess"}

        procs = ["proc1", "proc2", "proc3"]
        rounds = dict((epid, 0) for epid in procs)
        for proc in procs:
            procstate = yield self.client.dispatch_process(proc, spec, None)
            self.assertEqual(procstate['epid'], proc)

        yield self._wait_assert_pd_dump(self._assert_process_distribution,
                                        agent_counts=[3])

        # now terminate one process
        todie = procs.pop()
        procstate = yield self.client.terminate_process(todie)
        self.assertEqual(procstate['epid'], todie)

        yield self._wait_assert_pd_dump(self._assert_process_distribution,
                                        agent_counts=[2])

        def assert_process_rounds(state):
            for epid, expected_round in rounds.iteritems():
                self.assertEqual(state['processes'][epid]['round'],
                                 expected_round)

        yield self._wait_assert_pd_dump(assert_process_rounds)

        # "kill" a process in the backend eeagent
        fail_epid = procs[0]
        agent = yield self._get_eeagent_for_process(fail_epid)

        yield agent.fail_process(fail_epid)

        rounds[fail_epid] = 1

        yield self._wait_assert_pd_dump(assert_process_rounds)
        yield self._wait_assert_pd_dump(self._assert_process_distribution,
                                        agent_counts=[2])

    @defer.inlineCallbacks
    def test_queueing(self):

        #submit some processes before there are any resources available

        spec = {"omg": "imaprocess"}

        procs = ["proc1", "proc2", "proc3"]
        for proc in procs:
            procstate = yield self.client.dispatch_process(proc, spec, None)
            self.assertEqual(procstate['epid'], proc)

        yield self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.WAITING, procs)

        # add 2 nodes and a resource that supports 2 processes
        nodes = ["node1", "node2"]
        for node in nodes:
            yield self.client.dt_state(node, "dt1", InstanceState.RUNNING)

        yield self._spawn_eeagent(nodes[0], 2, "engine1")

        yield self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.RUNNING, procs[:2])
        yield self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.WAITING, procs[2:])

        # stand up a resource on the second node to support the other process
        yield self._spawn_eeagent(nodes[1], 2, "engine1")

        # all processes should now be running
        yield self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.RUNNING, procs)

    def _assert_process_states(self, dump, expected_state, epids):
        for epid in epids:
            process = dump['processes'][epid]
            assert process['state'] == expected_state, "%s: %s, expected %s!" % (
                epid, process['state'], expected_state)

    @defer.inlineCallbacks
    def test_node_death(self):
        # set up two nodes with two eeagents each

        nodes = ['node1', 'node2']
        for node in nodes:
            yield self.client.dt_state(node, "dt1", InstanceState.RUNNING)
            yield self._spawn_eeagent(node, 2, "engine1")
            yield self._spawn_eeagent(node, 2, "engine1")

        # 8 total slots are available, schedule 6 processes

        spec = {'omg': 'imaprocess'}
        procs = ['proc'+str(i+1) for i in range(6)]
        for proc in procs:
            yield self.client.dispatch_process(proc, spec, None)

        yield self._wait_assert_pd_dump(self._assert_process_distribution,
                                        node_counts=[4,2],
                                        agent_counts=[2,2,2],
                                        queued_count=0)

        # now kill one node
        yield self.client.dt_state(nodes[0], "dt1", InstanceState.TERMINATING)

        # procesess should be rescheduled. since we have 6 processes and only
        # 4 slots, 2 should be queued

        yield self._wait_assert_pd_dump(self._assert_process_distribution,
                                        node_counts=[4],
                                        agent_counts=[2,2],
                                        queued_count=2)


    def _assert_process_distribution(self, dump, nodes=None, node_counts=None,
                                     agents=None, agent_counts=None,
                                     queued=None, queued_count=None):
        #Assert the distribution of processes among nodes
        #node and agent counts are given as sequences of integers which are not
        #specific to a named node. So specifying node_counts=[4,3] will match
        #as long as you have 4 processes assigned to one node and 3 to another,
        #regardless of the node name
        found_queued = set()
        found_node = defaultdict(set)
        found_assigned = defaultdict(set)
        for process in dump['processes'].itervalues():
            epid = process['epid']
            assigned = process['assigned']

            if process['state'] == ProcessState.WAITING:
                found_queued.add(epid)
            elif assigned:
                node = dump['resources'][assigned]['node_id']
                found_node[node].add(epid)
                found_assigned[assigned].add(epid)

        if queued is not None:
            self.assertEqual(set(queued), found_queued)

        if queued_count is not None:
            self.assertEqual(len(found_queued), queued_count)

        if agents is not None:
            self.assertEqual(set(agents.keys()), set(found_assigned.keys()))
            for ee_id, processes in found_assigned.iteritems():
                self.assertEqual(set(agents[ee_id]), processes)

        if agent_counts is not None:
            assigned_lengths = [len(s) for s in found_assigned.itervalues()]
            self.assertEqual(sorted(assigned_lengths), sorted(agent_counts))

        if nodes is not None:
            self.assertEqual(set(nodes.keys()), set(found_node.keys()))
            for node_id, processes in found_node.iteritems():
                self.assertEqual(set(nodes[node_id]), processes)

        if node_counts is not None:
            node_lengths = [len(s) for s in found_node.itervalues()]
            self.assertEqual(sorted(node_lengths), sorted(node_counts))

    @defer.inlineCallbacks
    def test_immediate_process_reject(self):
        spec = {'omg': 'imaprocess'}
        yield self.client.dispatch_process("proc1", spec, None, immediate=True)

        # there are no resources so this process should be REJECTED immediately
        yield self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.REJECTED, ['proc1'])

    @defer.inlineCallbacks
    def test_constraints(self):

        nodes = ['node1', 'node2']
        yield self.client.dt_state(nodes[0], "dt1", InstanceState.RUNNING)
        yield self._spawn_eeagent(nodes[0], 2, "engine1")

        spec = {'omg': 'imaprocess'}
        proc1_constraints = dict(engine_type="engine1")
        proc2_constraints = dict(engine_type="engine2")

        yield self.client.dispatch_process("proc1", spec, None, proc1_constraints)
        yield self.client.dispatch_process("proc2", spec, None, proc2_constraints)

        # proc1 should be running on the node/agent, proc2 queued
        yield self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"]),
                                        queued=["proc2"])

        # launch another eeagent that supports proc2's engine_type
        yield self.client.dt_state(nodes[1], "dt1", InstanceState.RUNNING)
        yield self._spawn_eeagent(nodes[1], 2, "engine2")

        yield self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"],
                                                   node2=["proc2"]),
                                        queued=[])
"""