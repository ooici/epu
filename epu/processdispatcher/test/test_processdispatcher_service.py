import logging
import unittest
from collections import defaultdict
import random
import uuid
import threading

import gevent
from dashi import bootstrap, DashiConnection

from epu.dashiproc.processdispatcher import ProcessDispatcherService, \
    ProcessDispatcherClient, SubscriberNotifier
from epu.processdispatcher.test.mocks import FakeEEAgent, MockEPUMClient, MockNotifier
from epu.processdispatcher.util import node_id_to_eeagent_name
from epu.processdispatcher.engines import EngineRegistry
from epu.states import InstanceState, ProcessState
from epu.processdispatcher.store import ProcessRecord


log = logging.getLogger(__name__)

class ProcessDispatcherServiceTests(unittest.TestCase):

    amqp_uri = "memory://hello"

    engine_conf = {'engine1' : {'deployable_type' : 'dt1', 'slots' : 4}}

    def setUp(self):

        DashiConnection.consumer_timeout = 0.01
        self.registry = EngineRegistry.from_config(self.engine_conf)
        self.epum_client = MockEPUMClient()
        self.notifier = MockNotifier()
        self.pd = ProcessDispatcherService(amqp_uri=self.amqp_uri,
            registry=self.registry, epum_client=self.epum_client,
            notifier=self.notifier)

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

        processes_left = 3

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                  agent_counts=[processes_left])

        # now terminate one process
        todie = procs.pop()
        procstate = self.client.terminate_process(todie)
        self.assertEqual(procstate['upid'], todie)

        processes_left = 2

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        agent_counts=[processes_left])

        def assert_process_rounds(state):
            for upid, expected_round in rounds.iteritems():
                self.assertEqual(state['processes'][upid]['round'],
                                 expected_round)

        self._wait_assert_pd_dump(assert_process_rounds)

        # "kill" a process in the backend eeagent
        fail_upid = procs[0]
        agent = self._get_eeagent_for_process(fail_upid)

        agent.fail_process(fail_upid)

        processes_left = 1

        self._wait_assert_pd_dump(assert_process_rounds)
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                  agent_counts=[processes_left])

    def test_queueing(self):
        #submit some processes before there are any resources available

        spec = {"run_type":"hats", "parameters": {}}

        procs = ["proc1", "proc2", "proc3", "proc4", "proc5"]
        for proc in procs:
            procstate = self.client.dispatch_process(proc, spec, None)
            self.assertEqual(procstate['upid'], proc)

        for proc in procs:
            self.notifier.wait_for_state(proc, ProcessState.WAITING)
        self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.WAITING, procs)

        # add 2 nodes and a resource that supports 4 processes
        nodes = ["node1", "node2"]
        for node in nodes:
            self.client.dt_state(node, "dt1", InstanceState.RUNNING)

        self._spawn_eeagent(nodes[0], 4)

        for proc in procs[:4]:
            self.notifier.wait_for_state(proc, ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.RUNNING, procs[:4])
        for proc in procs[4:]:
            self.notifier.wait_for_state(proc, ProcessState.WAITING)
        self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.WAITING, procs[4:])

        # stand up a resource on the second node to support the other process
        self._spawn_eeagent(nodes[1], 4)

        # all processes should now be running
        for proc in procs:
            self.notifier.wait_for_state(proc, ProcessState.RUNNING)
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
        nodes = ['node1', 'node2']
        node1_properties = dict(hat_type="fedora")
        node2_properties = dict(hat_type="bowler")

        self.client.dt_state(nodes[0], "dt1", InstanceState.RUNNING,
            node1_properties)
        self._spawn_eeagent(nodes[0], 4)

        spec = {"run_type":"hats", "parameters": {}}
        proc1_constraints = dict(hat_type="fedora")
        proc2_constraints = dict(hat_type="bowler")

        self.client.dispatch_process("proc1", spec, None, proc1_constraints)
        self.client.dispatch_process("proc2", spec, None, proc2_constraints)

        # proc1 should be running on the node/agent, proc2 queued
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"]),
                                        queued=["proc2"])

        # launch another eeagent that supports proc2's engine_type
        self.client.dt_state(nodes[1], "dt1", InstanceState.RUNNING,
            node2_properties)
        self._spawn_eeagent(nodes[1], 4)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"],
                                                   node2=["proc2"]),
                                        queued=[])

    def test_describe(self):
        spec = {"run_type":"hats", "parameters": {}}

        self.client.dispatch_process("proc1", spec, None)

        processes = self.client.describe_processes()
        self.assertEqual(len(processes), 1)
        self.assertEqual(processes[0]['upid'], "proc1")

        proc1 = self.client.describe_process("proc1")
        self.assertEqual(proc1['upid'], "proc1")

        self.client.dispatch_process("proc2", spec, None)

        processes = self.client.describe_processes()
        self.assertEqual(len(processes), 2)

        if processes[0]['upid'] == "proc1":
            self.assertEqual(processes[1]['upid'], "proc2")
        elif processes[0]['upid'] == "proc2":
            self.assertEqual(processes[1]['upid'], "proc1")
        else:
            self.fail()

        proc1 = self.client.describe_process("proc1")
        self.assertEqual(proc1['upid'], "proc1")
        proc2 = self.client.describe_process("proc2")
        self.assertEqual(proc2['upid'], "proc2")

    def test_process_exited(self):
        node = "node1"
        self.client.dt_state(node, "dt1", InstanceState.RUNNING)
        self._spawn_eeagent(node, 1)

        spec = {"run_type":"hats", "parameters": {}}
        proc = "proc1"

        self.client.dispatch_process(proc, spec, None)

        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.RUNNING, [proc])

        agent = self._get_eeagent_for_process(proc)
        agent.exit_process(proc)
        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.EXITED, [proc])
        self.notifier.wait_for_state(proc, ProcessState.EXITED)

    def test_neediness(self, process_count=20, node_count=5):

        spec = {"run_type":"hats", "parameters": {}}

        procs = ["proc" + str(i) for i in range(process_count)]
        for proc in procs:
            procstate = self.client.dispatch_process(proc, spec, None)
            self.assertEqual(procstate['upid'], proc)

        self._wait_assert_pd_dump(self._assert_process_states,
            ProcessState.WAITING, procs)

        self.epum_client.assert_needs("dt1", range(node_count+1))
        self.epum_client.clear()

        # now provide nodes and resources, processes should start
        nodes = ["node" + str(i) for i in range(node_count)]
        for node in nodes:
            self.client.dt_state(node, "dt1", InstanceState.RUNNING)

        for node in nodes:
            self._spawn_eeagent(node, 4)

        self._wait_assert_pd_dump(self._assert_process_states,
            ProcessState.RUNNING, procs)

        # now kill all processes in a random order
        killlist = list(procs)
        random.shuffle(killlist)
        for proc in killlist:
            self.client.terminate_process(proc)

        self._wait_assert_pd_dump(self._assert_process_states,
            ProcessState.TERMINATED, procs)

        # all nodes should be retired
        with self.epum_client.condition:
            for _ in range(len(nodes)):
                if len(self.epum_client.retires) != len(nodes):
                    self.epum_client.condition.wait(0.1)
        self.assertEqual(set(self.epum_client.retires), set(nodes))



class RabbitProcessDispatcherServiceTests(ProcessDispatcherServiceTests):
    amqp_uri = "amqp://guest:guest@127.0.0.1//"

class SubscriberNotifierTests(unittest.TestCase):
    amqp_uri = "memory://hello"

    def setUp(self):
        self.condition = threading.Condition()
        self.process_states = []

        DashiConnection.consumer_timeout = 0.01
        self.name = "SubscriberNotifierTests" + uuid.uuid4().hex
        self.dashi = DashiConnection(self.name, self.amqp_uri, self.name)
        self.dashi.handle(self.process_state)

    def tearDown(self):
        self.dashi.cancel()

    def process_state(self, process):
        with self.condition:
            self.process_states.append(process)
            self.condition.notify_all()

    def test_notify_process(self):
        notifier = SubscriberNotifier(self.dashi)

        p1 = ProcessRecord.new(None, "p1", {"blah": "blah"},
            ProcessState.RUNNING, subscribers=[(self.name, "process_state")])

        notifier.notify_process(p1)
        self.dashi.consume(1, 1)
        self.assertEqual(len(self.process_states), 1)
        self.assertEqual(self.process_states[0]['upid'], "p1")
        self.assertEqual(self.process_states[0]['state'], ProcessState.RUNNING)

        p2 = ProcessRecord.new(None, "p2", {"blah": "blah"},
            ProcessState.PENDING, subscribers=[(self.name, "process_state")])
        notifier.notify_process(p2)
        self.dashi.consume(1, 1)
        self.assertEqual(len(self.process_states), 2)
        self.assertEqual(self.process_states[1]['upid'], "p2")
        self.assertEqual(self.process_states[1]['state'], ProcessState.PENDING)


class RabbitSubscriberNotifierTests(SubscriberNotifierTests):
    amqp_uri = "amqp://guest:guest@127.0.0.1//"
