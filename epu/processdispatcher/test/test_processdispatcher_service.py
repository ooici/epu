import logging
import unittest
from collections import defaultdict

import gevent
from dashi import bootstrap, DashiConnection

from epu.dashiproc.processdispatcher import ProcessDispatcherService, ProcessDispatcherClient
from epu.processdispatcher.test.mocks import FakeEEAgent
from epu.processdispatcher.util import node_id_to_eeagent_name
from epu.processdispatcher.engines import EngineRegistry
from epu.states import InstanceState, ProcessState


log = logging.getLogger(__name__)

class ProcessDispatcherServiceTests(unittest.TestCase):

    amqp_uri = "memory://hello"

    engine_conf = {'engine1' : {'deployable_type' : 'dt1', 'slots' : 4},
                   'engine2' : {'deployable_type' : 'dt2', 'slots' : 4},
                   'engine3' : {'deployable_type' : 'dt3', 'slots' : 2},
                   'engine4' : {'deployable_type' : 'dt4', 'slots' : 2}}

    def setUp(self):

        DashiConnection.consumer_timeout = 0.01
        self.registry = EngineRegistry.from_config(self.engine_conf)
        self.pd = ProcessDispatcherService(amqp_uri=self.amqp_uri,
                                           registry=self.registry)
        self.pd_name = self.pd.topic
        self.pd_greenlet = gevent.spawn(self.pd.start)
        gevent.sleep(0.05)

        self.client = ProcessDispatcherClient(self.pd.dashi, self.pd_name)

        self.eeagents = {}

    def tearDown(self):
        self.pd.stop()
        for eeagent in self.eeagents.itervalues():
            eeagent.dashi.cancel()
            eeagent.dashi.disconnect()

        self.eeagents.clear()

    def _spawn_eeagent(self, node_id, slot_count, heartbeat_dest=None):
        if heartbeat_dest is None:
            heartbeat_dest = self.pd_name
        
        agent_name = node_id_to_eeagent_name(node_id)
        dashi = bootstrap.dashi_connect(agent_name,
                                        amqp_uri=self.amqp_uri)

        agent = FakeEEAgent(dashi, heartbeat_dest, node_id, slot_count)
        self.eeagents[agent_name] = agent
        gevent.spawn(agent.start)
        gevent.sleep(0.1) # hack to hopefully ensure consumer is bound TODO??

        agent.send_heartbeat()
        return agent

    def _get_eeagent_for_process(self, upid):
        state = self.client.dump()
        process = state['processes'][upid]

        attached = process['assigned']
        if attached is None:
            return None

        return self.eeagents[attached]

    def _assert_pd_dump(self, fun, *args, **kwargs):
        state = self.client.dump()
        log.debug("PD state: %s", state)
        fun(state, *args, **kwargs)

    max_tries = 10
    def _wait_assert_pd_dump(self, fun, *args, **kwargs):
        tries = 0
        while True:
            try:
                self._assert_pd_dump(fun, *args, **kwargs)
            except Exception:
                tries += 1
                if tries == self.max_tries:
                    log.error("PD state assertion failing after %d attempts",
                              tries)
                    raise
            else:
                return
            gevent.sleep(0.05)

    def test_basics(self):

        # create some fake nodes and tell PD about them
        nodes = ["node1", "node2", "node3"]

        for node in nodes:
            self.client.dt_state(node, "dt1", InstanceState.RUNNING)

        # PD knows about these nodes but hasn't gotten a heartbeat yet

        # spawn the eeagents and tell them all to heartbeat
        for node in nodes:
            self._spawn_eeagent(node, 4)

        def assert_all_resources(state):
            eeagent_nodes = set()
            for resource in state['resources'].itervalues():
                eeagent_nodes.add(resource['node_id'])
            self.assertEqual(set(nodes), eeagent_nodes)

        self._wait_assert_pd_dump(assert_all_resources)

        spec = {"run_type":"hats", "parameters": {}}

        procs = ["proc1", "proc2", "proc3"]
        rounds = dict((upid, 0) for upid in procs)
        for proc in procs:
            procstate = self.client.dispatch_process(proc, spec, None)
            self.assertEqual(procstate['upid'], proc)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                  agent_counts=[3])

        # now terminate one process
        todie = procs.pop()
        procstate = self.client.terminate_process(todie)
        self.assertEqual(procstate['upid'], todie)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        agent_counts=[2])

        def assert_process_rounds(state):
            for upid, expected_round in rounds.iteritems():
                self.assertEqual(state['processes'][upid]['round'],
                                 expected_round)

        self._wait_assert_pd_dump(assert_process_rounds)

        # "kill" a process in the backend eeagent
        fail_upid = procs[0]
        agent = self._get_eeagent_for_process(fail_upid)

        agent.fail_process(fail_upid)

        rounds[fail_upid] = 1

        self._wait_assert_pd_dump(assert_process_rounds)
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                  agent_counts=[2])

    def test_queueing(self):
        #submit some processes before there are any resources available

        spec = {"run_type":"hats", "parameters": {}}

        procs = ["proc1", "proc2", "proc3"]
        for proc in procs:
            procstate = self.client.dispatch_process(proc, spec, None)
            self.assertEqual(procstate['upid'], proc)

        self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.WAITING, procs)

        # add 2 nodes and a resource that supports 2 processes
        nodes = ["node1", "node2"]
        for node in nodes:
            self.client.dt_state(node, "dt3", InstanceState.RUNNING)

        self._spawn_eeagent(nodes[0], 2)

        self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.RUNNING, procs[:2])
        self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.WAITING, procs[2:])

        # stand up a resource on the second node to support the other process
        self._spawn_eeagent(nodes[1], 2)

        # all processes should now be running
        self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.RUNNING, procs)

    def _assert_process_states(self, dump, expected_state, upids):
        for upid in upids:
            process = dump['processes'][upid]
            assert process['state'] == expected_state, "%s: %s, expected %s!" % (
                upid, process['state'], expected_state)

    def test_node_death(self):
        # set up two nodes with 4 slots each

        nodes = ['node1', 'node2']
        for node in nodes:
            self.client.dt_state(node, "dt1", InstanceState.RUNNING)



        for node in nodes:
            self._spawn_eeagent(node, 4)

        # 8 total slots are available, schedule 6 processes

        spec = {"run_type":"hats", "parameters": {}}
        procs = ['proc'+str(i+1) for i in range(6)]
        for proc in procs:
            self.client.dispatch_process(proc, spec, None)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        node_counts=[4,2],
                                        queued_count=0)

        # now kill one node
        log.debug("killing node %s", nodes[0])
        self.client.dt_state(nodes[0], "dt1", InstanceState.TERMINATING)

        # procesess should be rescheduled. since we have 6 processes and only
        # 4 slots, 2 should be queued

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                  node_counts=[4],
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
            upid = process['upid']
            assigned = process['assigned']

            if process['state'] == ProcessState.WAITING:
                found_queued.add(upid)
            elif process['state'] == ProcessState.RUNNING:
                node = dump['resources'].get(assigned)
                self.assertIsNotNone(node)
                node_id = node['node_id']
                found_node[node_id].add(upid)
                found_assigned[assigned].add(upid)

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

    def test_immediate_process_reject(self):
        spec = {"run_type":"hats", "parameters": {}}
        self.client.dispatch_process("proc1", spec, None, immediate=True)

        # there are no resources so this process should be REJECTED immediately
        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.REJECTED, ['proc1'])

    def test_constraints(self):
        #raise unittest.SkipTest("constraints not yet supported")
        nodes = ['node1', 'node2']
        self.client.dt_state(nodes[0], "dt3", InstanceState.RUNNING)
        self._spawn_eeagent(nodes[0], 2)

        spec = {"run_type":"hats", "parameters": {}}
        proc1_constraints = dict(engine_type="engine3")
        proc2_constraints = dict(engine_type="engine4")

        self.client.dispatch_process("proc1", spec, None, proc1_constraints)
        self.client.dispatch_process("proc2", spec, None, proc2_constraints)

        # proc1 should be running on the node/agent, proc2 queued
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"]),
                                        queued=["proc2"])

        # launch another eeagent that supports proc2's engine_type
        self.client.dt_state(nodes[1], "dt4", InstanceState.RUNNING)
        self._spawn_eeagent(nodes[1], 2)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"],
                                                   node2=["proc2"]),
                                        queued=[])

class RabbitProcessDispatcherServiceTests(ProcessDispatcherServiceTests):
    amqp_uri = "amqp://guest:guest@127.0.0.1//"
