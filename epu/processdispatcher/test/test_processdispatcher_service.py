# Copyright 2013 University of Chicago

import logging
import unittest
from collections import defaultdict
import random
import time
import uuid
import threading
from socket import timeout

from mock import patch
from dashi import bootstrap, DashiConnection

import epu.tevent as tevent
from epu.dashiproc.processdispatcher import ProcessDispatcherService, \
    ProcessDispatcherClient, SubscriberNotifier
from epu.processdispatcher.test.mocks import FakeEEAgent, MockEPUMClient, \
    MockNotifier, get_definition, get_domain_config, nosystemrestart_process_config, \
    minimum_time_between_starts_config
from epu.processdispatcher.engines import EngineRegistry, domain_id_from_engine
from epu.states import InstanceState, ProcessState
from epu.processdispatcher.store import ProcessRecord, ProcessDispatcherStore, ProcessDispatcherZooKeeperStore
from epu.processdispatcher.modes import QueueingMode, RestartMode
from epu.test import ZooKeeperTestMixin
from epu.test.util import wait


log = logging.getLogger(__name__)


class ProcessDispatcherServiceTests(unittest.TestCase):

    amqp_uri = "amqp://guest:guest@127.0.0.1//"

    engine_conf = {'engine1': {'slots': 4, 'base_need': 1},
                   'engine2': {'slots': 4}, 'engine3': {'slots': 4},
                   'engine4': {
                       'slots': 4, 'heartbeat_warning': 10, 'heartbeat_missing': 20,
                       'heartbeat_period': 1, 'base_need': 1}}
    default_engine = 'engine1'
    process_engines = {'a.b.C': 'engine3', 'a.b': 'engine2'}

    def setUp(self):

        DashiConnection.consumer_timeout = 0.01
        self.definition_id = "pd_definition"
        self.definition = get_definition()
        self.epum_client = MockEPUMClient()
        self.epum_client.add_domain_definition(self.definition_id, self.definition)

        self.store = self.setup_store()

        self.sysname = "test-sysname%s" % uuid.uuid4().hex

        self.start_pd()
        self.client = ProcessDispatcherClient(self.pd.dashi, self.pd_name)

        def waiter():
            try:
                self.client.list_definitions()
                return True
            except timeout:
                return False

        wait(waiter)

        self.process_definition_id = uuid.uuid4().hex
        self.client.create_definition(self.process_definition_id, "dtype",
            executable={"module": "some.module", "class": "SomeClass"},
            name="my_process")

        self.eeagents = {}
        self.eeagent_threads = {}

    def tearDown(self):
        self.stop_pd()
        self.teardown_store()
        self._kill_all_eeagents()

    def start_pd(self):
        self.registry = EngineRegistry.from_config(self.engine_conf,
            default=self.default_engine, process_engines=self.process_engines)
        self.notifier = MockNotifier()

        self.pd = ProcessDispatcherService(amqp_uri=self.amqp_uri,
            registry=self.registry, epum_client=self.epum_client,
            notifier=self.notifier, definition_id=self.definition_id,
            domain_config=get_domain_config(), store=self.store,
            sysname=self.sysname)

        self.pd_name = self.pd.topic
        self.pd_thread = tevent.spawn(self.pd.start)
        self.pd.ready_event.wait(60)

    def stop_pd(self):
        self.pd.stop()
        self.pd_thread.join()

    def setup_store(self):
        return ProcessDispatcherStore()

    def teardown_store(self):
        self.store.shutdown()

    def _spawn_eeagent(self, node_id, slot_count, heartbeat_dest=None):
        if heartbeat_dest is None:
            heartbeat_dest = self.pd_name

        agent_name = "eeagent_%s" % uuid.uuid4()
        dashi = bootstrap.dashi_connect(agent_name, sysname=self.sysname,
                                        amqp_uri=self.amqp_uri)

        agent = FakeEEAgent(agent_name, dashi, heartbeat_dest, node_id, slot_count)
        self.eeagents[agent_name] = agent
        self.eeagent_threads[agent_name] = tevent.spawn(agent.start)
        agent.ready_event.wait(10)

        agent.send_heartbeat()
        return agent

    def _kill_all_eeagents(self):
        for eeagent in self.eeagents.itervalues():
            eeagent.dashi.cancel()
            eeagent.dashi.disconnect()

        for eeagent_thread in self.eeagent_threads.itervalues():
            eeagent_thread.join()

        self.eeagents.clear()

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

    def assert_one_reconfigure(self, domain_id=None, preserve_n=None, retirees=None):
        if domain_id is not None:
            reconfigures = self.epum_client.reconfigures[domain_id]
        else:
            reconfigures = self.epum_client.reconfigures.values()
            self.assertTrue(reconfigures)
            reconfigures = reconfigures[0]
        self.assertEqual(len(reconfigures), 1)
        reconfigure = reconfigures[0]
        engine_conf = reconfigure['engine_conf']
        if preserve_n is not None:
            self.assertEqual(engine_conf['preserve_n'], preserve_n)
        if retirees is not None:
            retirables = engine_conf.get('retirable_nodes', [])
            self.assertEqual(set(retirables), set(retirees))

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
            time.sleep(0.05)

    def _wait_for_one_reconfigure(self, domain_id=None,):
        tries = 0
        while True:
            if domain_id is not None:
                reconfigures = self.epum_client.reconfigures[domain_id]
            else:
                reconfigures = self.epum_client.reconfigures.values()
                self.assertTrue(reconfigures)
                reconfigures = reconfigures[0]

            try:
                self.assertEqual(len(reconfigures), 1)
            except Exception:
                tries += 1
                if tries == self.max_tries:
                    log.error("Waiting for reconfigure failing after %d attempts",
                              tries)
                    raise
            else:
                return
            time.sleep(0.05)

    def test_basics(self):

        # create some fake nodes and tell PD about them
        nodes = ["node1", "node2", "node3"]
        domain_id = domain_id_from_engine("engine1")

        for node in nodes:
            self.client.node_state(node, domain_id, InstanceState.RUNNING)

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

        procs = ["proc1", "proc2", "proc3"]
        rounds = dict((upid, 0) for upid in procs)
        for proc in procs:
            procstate = self.client.schedule_process(proc,
                self.process_definition_id, None, restart_mode=RestartMode.NEVER)
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

    def test_terminate_when_waiting(self):
        """test_terminate_when_waiting
        submit a proc, wait for it to get to a waiting state, epum
        should then be configured to scale up, then terminate it.
        EPUM should be reconfigured to scale down at that point.
        """

        domain_id = domain_id_from_engine("engine2")
        self._wait_for_one_reconfigure(domain_id)
        self.assert_one_reconfigure(domain_id, 0, [])
        self.epum_client.clear()

        procs = ["proc1", "proc2", "proc3"]
        for proc in procs:
            procstate = self.client.schedule_process(proc,
                self.process_definition_id, None, restart_mode=RestartMode.NEVER,
                execution_engine_id="engine2")
            self.assertEqual(procstate['upid'], proc)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                  queued=procs)

        self._wait_for_one_reconfigure(domain_id)
        self.assert_one_reconfigure(domain_id, 1, [])
        self.epum_client.clear()

        # now terminate one process. Shouldn't have any reconfigs
        todie = procs.pop()
        procstate = self.client.terminate_process(todie)
        self.assertEqual(procstate['upid'], todie)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=procs)

        self.assertEqual(self.epum_client.reconfigures, {})

        # now kill the remaining procs. we should see a scale down
        for todie in procs:
            procstate = self.client.terminate_process(todie)
            self.assertEqual(procstate['upid'], todie)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=[])

        self._wait_for_one_reconfigure(domain_id)
        self.assert_one_reconfigure(domain_id, 0, [])
        self.epum_client.clear()

    def test_multiple_ee_per_node(self):

        # create some fake nodes and tell PD about them
        nodes = ["node1", "node2", "node3", "node4"]
        domain_id = domain_id_from_engine("engine1")

        for node in nodes:
            self.client.node_state(node, domain_id, InstanceState.RUNNING)

        # PD knows about these nodes but hasn't gotten a heartbeat yet

        # spawn two eeagents per node and tell them all to heartbeat
        for node in nodes:
            self._spawn_eeagent(node, 4)
            self._spawn_eeagent(node, 4)

        def assert_all_resources(state):
            eeagent_nodes = set()
            for resource in state['resources'].itervalues():
                eeagent_nodes.add(resource['node_id'])
            self.assertEqual(set(nodes), eeagent_nodes)

        self._wait_assert_pd_dump(assert_all_resources)

        procs = ["proc%s" % i for i in range(33)]

        def start_procs(n):
            started = []
            for proc in procs[:n]:
                print "Starting proc %s" % proc
                procstate = self.client.schedule_process(proc,
                self.process_definition_id, None)
                self.assertEqual(procstate['upid'], proc)
                started.append(proc)
            procs[:] = procs[n:]
            return started

        # start the first 5 processes. they should end up spread across the
        # two eeagents on just one node
        first_started = start_procs(5)

        agent_dist = [2, 3, 0, 0, 0, 0, 0, 0]
        node_dist = [5, 0, 0, 0]

        self._wait_assert_pd_dump(self._assert_process_distribution,
            agent_counts=agent_dist, node_counts=node_dist)

        # start 3 more. should fill up that first node
        start_procs(3)

        agent_dist = [4, 4, 0, 0, 0, 0, 0, 0]
        node_dist = [8, 0, 0, 0]
        self._wait_assert_pd_dump(self._assert_process_distribution,
            agent_counts=agent_dist, node_counts=node_dist)

        # two more should cause us to spread to a second node with a process
        # per eeagent
        start_procs(2)
        agent_dist = [4, 4, 1, 1, 0, 0, 0, 0]
        node_dist = [8, 2, 0, 0]
        self._wait_assert_pd_dump(self._assert_process_distribution,
            agent_counts=agent_dist, node_counts=node_dist)

        # now kill a process on the first node
        self.client.terminate_process(first_started[0])
        agent_dist = [3, 4, 1, 1, 0, 0, 0, 0]
        node_dist = [7, 2, 0, 0]
        self._wait_assert_pd_dump(self._assert_process_distribution,
            agent_counts=agent_dist, node_counts=node_dist)

        # and schedule two more. One new process should end up in the vacated
        # slot. The other should go to the second node.
        start_procs(2)
        agent_dist = [4, 4, 2, 1, 0, 0, 0, 0]
        node_dist = [8, 3, 0, 0]
        self._wait_assert_pd_dump(self._assert_process_distribution,
            agent_counts=agent_dist, node_counts=node_dist)

        # finally start the remaining 24 processes. They should fill up
        # all slots on all agents.
        start_procs(24)
        agent_dist = [4, 4, 4, 4, 4, 4, 4, 4]
        node_dist = [8, 8, 8, 8]
        self._wait_assert_pd_dump(self._assert_process_distribution,
            agent_counts=agent_dist, node_counts=node_dist)

    def test_requested_ee(self):
        self.client.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)
        self._spawn_eeagent("node1", 4)

        queued = []

        proc1_queueing_mode = QueueingMode.ALWAYS

        # ensure that procs that request nonexisting engine id get queued
        self.client.schedule_process("proc1", self.process_definition_id,
            queueing_mode=proc1_queueing_mode, execution_engine_id="engine2")

        # proc1 should be queued
        queued.append("proc1")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

        # ensure that procs that request existing engine id run
        self.client.schedule_process("proc2", self.process_definition_id,
                queueing_mode=proc1_queueing_mode, execution_engine_id="engine1")

        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        # ensure that procs that don't specify an engine id run
        self.client.schedule_process("proc3", self.process_definition_id,
                queueing_mode=proc1_queueing_mode)

        self.notifier.wait_for_state("proc3", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc3"])

        # now add an engine for proc1 and it should be scheduled
        self.client.node_state("node2", domain_id_from_engine("engine2"),
            InstanceState.RUNNING)
        self._spawn_eeagent("node2", 4)

        self.notifier.wait_for_state("proc1", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
            ProcessState.RUNNING, ["proc1"])

        # now launch another process for engine2. it should be scheduled too
        self.client.schedule_process("proc4", self.process_definition_id,
            queueing_mode=QueueingMode.NEVER, execution_engine_id="engine2")
        self.notifier.wait_for_state("proc4", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
            ProcessState.RUNNING, ["proc4"])

    def test_default_ee(self):
        self.client.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)
        self._spawn_eeagent("node1", 4)

        self.client.node_state("node2", domain_id_from_engine("engine2"),
            InstanceState.RUNNING)
        self._spawn_eeagent("node2", 4)

        # fill up all 4 slots on engine1 agent and launch one more proc
        for upid in ['p1', 'p2', 'p3', 'p4', 'p5']:
            self.client.schedule_process(upid, self.process_definition_id,
                queueing_mode=QueueingMode.ALWAYS)

        self.notifier.wait_for_state('p1', ProcessState.RUNNING)
        self.notifier.wait_for_state('p2', ProcessState.RUNNING)
        self.notifier.wait_for_state('p3', ProcessState.RUNNING)
        self.notifier.wait_for_state('p4', ProcessState.RUNNING)

        # p5 should be queued since it is not compatible with engine2
        self.notifier.wait_for_state('p5', ProcessState.WAITING)

        # now schedule p6 directly to engine2
        self.client.schedule_process("p6", self.process_definition_id,
            queueing_mode=QueueingMode.ALWAYS, execution_engine_id="engine2")
        self.notifier.wait_for_state('p1', ProcessState.RUNNING)

        # add another eeagent for engine1, p5 should run
        self.client.node_state("node3", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)
        self._spawn_eeagent("node3", 4)
        self.notifier.wait_for_state('p5', ProcessState.RUNNING)

    def test_process_engine_map(self):
        def1 = uuid.uuid4().hex
        self.client.create_definition(def1, "dtype",
            executable={"module": "a.b", "class": "C"},
            name="my_process")
        def2 = uuid.uuid4().hex
        self.client.create_definition(def2, "dtype",
            executable={"module": "a.b", "class": "D"},
            name="my_process")
        def3 = uuid.uuid4().hex
        self.client.create_definition(def3, "dtype",
            executable={"module": "a", "class": "B"},
            name="my_process")

        self.client.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)
        eeagent1 = self._spawn_eeagent("node1", 4)

        self.client.node_state("node2", domain_id_from_engine("engine2"),
            InstanceState.RUNNING)
        eeagent2 = self._spawn_eeagent("node2", 4)

        self.client.node_state("node3", domain_id_from_engine("engine3"),
            InstanceState.RUNNING)
        eeagent3 = self._spawn_eeagent("node3", 4)

        self.client.schedule_process("proc1", def1)
        self.client.schedule_process("proc2", def2)
        self.client.schedule_process("proc3", def3)

        self.notifier.wait_for_state("proc1", ProcessState.RUNNING)
        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self.notifier.wait_for_state("proc3", ProcessState.RUNNING)

        proc1 = self.client.describe_process("proc1")
        self.assertEqual(proc1['assigned'], eeagent3.name)

        proc2 = self.client.describe_process("proc2")
        self.assertEqual(proc2['assigned'], eeagent2.name)

        proc3 = self.client.describe_process("proc3")
        self.assertEqual(proc3['assigned'], eeagent1.name)

    def test_node_exclusive(self):
        node = "node1"
        domain_id = domain_id_from_engine('engine1')
        node_properties = dict(engine="fedora")
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)

        self._spawn_eeagent(node, 4)

        exclusive_attr = "hamsandwich"
        queued = []

        proc1_queueing_mode = QueueingMode.ALWAYS

        # Process should be scheduled, since no other procs have its
        # exclusive attribute
        self.client.schedule_process("proc1", self.process_definition_id,
                queueing_mode=proc1_queueing_mode,
                node_exclusive=exclusive_attr)

        self.notifier.wait_for_state("proc1", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc1"])

        # Process should be queued, because proc1 has the same attribute
        self.client.schedule_process("proc2", self.process_definition_id,
                queueing_mode=proc1_queueing_mode,
                node_exclusive=exclusive_attr)

        queued.append("proc2")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

        # Now kill the first process, and proc2 should run.
        self.client.terminate_process("proc1")
        queued.remove("proc2")
        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        # Process should be queued, because proc2 has the same attribute
        self.client.schedule_process("proc3", self.process_definition_id,
                queueing_mode=proc1_queueing_mode,
                node_exclusive=exclusive_attr)

        queued.append("proc3")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

        # Process should be scheduled, since no other procs have its
        # exclusive attribute
        other_exclusive_attr = "hummussandwich"
        self.client.schedule_process("proc4", self.process_definition_id,
                queueing_mode=proc1_queueing_mode,
                node_exclusive=other_exclusive_attr)

        self.notifier.wait_for_state("proc4", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc4"])

        # Now that we've started another node, waiting node should start
        node = "node2"
        node_properties = dict(engine="fedora")
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)

        self._spawn_eeagent(node, 4)

        self.notifier.wait_for_state("proc3", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc3"])

    def test_node_exclusive_bug(self):
        slots = 2

        node_1 = "node1"
        domain_id = domain_id_from_engine('engine1')
        node_properties = dict(engine="fedora")
        self.client.node_state(node_1, domain_id, InstanceState.RUNNING,
                node_properties)
        self._spawn_eeagent(node_1, slots)

        node_2 = "node2"
        domain_id = domain_id_from_engine('engine1')
        node_properties = dict(engine="fedora")
        self.client.node_state(node_2, domain_id, InstanceState.RUNNING,
                node_properties)
        self._spawn_eeagent(node_2, slots)

        node_3 = "node3"
        domain_id = domain_id_from_engine('engine1')
        node_properties = dict(engine="fedora")
        self.client.node_state(node_3, domain_id, InstanceState.RUNNING,
                node_properties)
        self._spawn_eeagent(node_3, slots)

        node_4 = "node4"
        domain_id = domain_id_from_engine('engine1')
        node_properties = dict(engine="fedora")
        self.client.node_state(node_4, domain_id, InstanceState.RUNNING,
                node_properties)
        self._spawn_eeagent(node_4, slots)

        pydap_xattr = "pydap"
        service_gateway_xattr = "service_gateway"

        queueing_mode = QueueingMode.START_ONLY

        # Process should be scheduled, since no other procs have its
        # exclusive attribute
        pydap_xattr_procs = []
        service_gateway_xattr_procs = []

        proc_1 = "proc_1"
        self.client.schedule_process(proc_1, self.process_definition_id,
                queueing_mode=queueing_mode,
                node_exclusive=pydap_xattr)
        pydap_xattr_procs.append(proc_1)

        proc_2 = "proc_2"
        self.client.schedule_process(proc_2, self.process_definition_id,
                queueing_mode=queueing_mode,
                node_exclusive=service_gateway_xattr)
        pydap_xattr_procs.append(proc_2)

        proc_3 = "proc_3"
        self.client.schedule_process(proc_3, self.process_definition_id,
                queueing_mode=queueing_mode,
                node_exclusive=service_gateway_xattr)
        service_gateway_xattr_procs.append(proc_1)

        proc_4 = "proc_4"
        self.client.schedule_process(proc_4, self.process_definition_id,
                queueing_mode=queueing_mode,
                node_exclusive=pydap_xattr)
        pydap_xattr_procs.append(proc_4)

        proc_5 = "proc_5"
        self.client.schedule_process(proc_5, self.process_definition_id,
                queueing_mode=queueing_mode,
                node_exclusive=service_gateway_xattr)
        service_gateway_xattr_procs.append(proc_5)

        proc_6 = "proc_6"
        self.client.schedule_process(proc_6, self.process_definition_id,
                queueing_mode=queueing_mode,
                node_exclusive=pydap_xattr)
        pydap_xattr_procs.append(proc_6)

        proc_7 = "proc_7"
        self.client.schedule_process(proc_7, self.process_definition_id,
                queueing_mode=queueing_mode,
                node_exclusive=service_gateway_xattr)
        service_gateway_xattr_procs.append(proc_7)

        proc_8 = "proc_8"
        self.client.schedule_process(proc_8, self.process_definition_id,
                queueing_mode=queueing_mode,
                node_exclusive=pydap_xattr)
        pydap_xattr_procs.append(proc_8)

        for proc in (pydap_xattr_procs + service_gateway_xattr_procs):
            self.notifier.wait_for_state(proc, ProcessState.RUNNING)
            self._wait_assert_pd_dump(self._assert_process_states,
                    ProcessState.RUNNING, [proc])

        self._wait_assert_pd_dump(self._assert_node_exclusive)

        self.client.terminate_process(proc_8)

        self._wait_assert_pd_dump(self._assert_node_exclusive)

    def test_node_exclusive_multiple_eeagents(self):
        node = "node1"
        domain_id = domain_id_from_engine('engine1')

        node_properties = dict(engine="fedora")
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)

        self._spawn_eeagent(node, 4)
        self._spawn_eeagent(node, 4)

        exclusive_attr = "hamsandwich"
        queued = []

        proc1_queueing_mode = QueueingMode.ALWAYS

        # Process should be scheduled, since no other procs have its
        # exclusive attribute
        self.client.schedule_process("proc1", self.process_definition_id,
                queueing_mode=proc1_queueing_mode,
                node_exclusive=exclusive_attr)

        self.notifier.wait_for_state("proc1", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc1"])

        # Process should be queued, because proc1 has the same attribute
        self.client.schedule_process("proc2", self.process_definition_id,
                queueing_mode=proc1_queueing_mode,
                node_exclusive=exclusive_attr)

        queued.append("proc2")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

        # Now kill the first process, and proc2 should run.
        self.client.terminate_process("proc1")
        queued.remove("proc2")
        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        # Process should be queued, because proc2 has the same attribute
        self.client.schedule_process("proc3", self.process_definition_id,
                queueing_mode=proc1_queueing_mode,
                node_exclusive=exclusive_attr)

        queued.append("proc3")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

        # Process should be scheduled, since no other procs have its
        # exclusive attribute
        other_exclusive_attr = "hummussandwich"
        self.client.schedule_process("proc4", self.process_definition_id,
                queueing_mode=proc1_queueing_mode,
                node_exclusive=other_exclusive_attr)

        self.notifier.wait_for_state("proc4", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc4"])

        # Now that we've started another node, waiting node should start
        node = "node2"
        node_properties = dict(engine="fedora")
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)

        self._spawn_eeagent(node, 4)

        self.notifier.wait_for_state("proc3", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc3"])

    def test_queueing(self):
        # submit some processes before there are any resources available

        procs = ["proc1", "proc2", "proc3", "proc4", "proc5"]
        for proc in procs:
            procstate = self.client.schedule_process(proc, self.process_definition_id)
            self.assertEqual(procstate['upid'], proc)

        for proc in procs:
            self.notifier.wait_for_state(proc, ProcessState.WAITING)
        self._wait_assert_pd_dump(self._assert_process_states,
                                        ProcessState.WAITING, procs)

        # add 2 nodes and a resource that supports 4 processes
        nodes = ["node1", "node2"]
        domain_id = domain_id_from_engine('engine1')

        for node in nodes:
            self.client.node_state(node, domain_id, InstanceState.RUNNING)

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
        domain_id = domain_id_from_engine('engine1')

        for node in nodes:
            self.client.node_state(node, domain_id, InstanceState.RUNNING)

        for node in nodes:
            self._spawn_eeagent(node, 4)

        # 8 total slots are available, schedule 6 processes

        procs = ['proc' + str(i + 1) for i in range(6)]

        # schedule the first process to never restart. it shouldn't come back.
        self.client.schedule_process(procs[0], self.process_definition_id,
            restart_mode=RestartMode.NEVER)

        # and the second process to restart on abnormal termination. it should
        # come back.
        self.client.schedule_process(procs[1], self.process_definition_id,
            restart_mode=RestartMode.ABNORMAL)

        for proc in procs[2:]:
            self.client.schedule_process(proc, self.process_definition_id)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        node_counts=[4, 2],
                                        queued_count=0)

        # now kill one node
        log.debug("killing node %s", nodes[0])
        self.client.node_state(nodes[0], domain_id, InstanceState.TERMINATING)

        # 5 procesess should be rescheduled. since we have 5 processes and only
        # 4 slots, 1 should be queued

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                  node_counts=[4],
                                  queued_count=1)

        # ensure that the correct process was not rescheduled
        self.notifier.wait_for_state(procs[0], ProcessState.FAILED)

    def _assert_process_distribution(self, dump, nodes=None, node_counts=None,
                                     agents=None, agent_counts=None,
                                     queued=None, queued_count=None,
                                     rejected=None, rejected_count=None):
        # Assert the distribution of processes among nodes
        # node and agent counts are given as sequences of integers which are not
        # specific to a named node. So specifying node_counts=[4,3] will match
        # as long as you have 4 processes assigned to one node and 3 to another,
        # regardless of the node name
        found_rejected = set()
        found_queued = set()
        found_node = defaultdict(set)
        found_assigned = defaultdict(set)
        for process in dump['processes'].itervalues():
            upid = process['upid']
            assigned = process['assigned']

            if process['state'] == ProcessState.WAITING:
                found_queued.add(upid)
            elif process['state'] == ProcessState.REJECTED:
                found_rejected.add(upid)
            elif process['state'] == ProcessState.RUNNING:
                resource = dump['resources'].get(assigned)
                self.assertIsNotNone(resource)
                node_id = resource['node_id']
                found_node[node_id].add(upid)
                found_assigned[assigned].add(upid)

        print "Queued: %s\nRejected: %s\n" % (queued, rejected)
        print "Found Queued: %s\nFound Rejected: %s\n" % (found_queued, found_rejected)

        if queued is not None:
            self.assertEqual(set(queued), found_queued)

        if queued_count is not None:
            self.assertEqual(len(found_queued), queued_count)

        if rejected is not None:
            self.assertEqual(set(rejected), found_rejected)

        if rejected_count is not None:
            self.assertEqual(len(found_rejected), rejected_count)

        if agents is not None:
            self.assertEqual(set(agents.keys()), set(found_assigned.keys()))
            for ee_id, processes in found_assigned.iteritems():
                self.assertEqual(set(agents[ee_id]), processes)

        if agent_counts is not None:
            assigned_lengths = [len(s) for s in found_assigned.itervalues()]
            # omit zero counts
            agent_counts = [count for count in agent_counts if count != 0]
            # print "%s =?= %s" % (agent_counts, assigned_lengths)
            self.assertEqual(sorted(assigned_lengths), sorted(agent_counts))

        if nodes is not None:
            self.assertEqual(set(nodes.keys()), set(found_node.keys()))
            for node_id, processes in found_node.iteritems():
                self.assertEqual(set(nodes[node_id]), processes)

        if node_counts is not None:
            node_lengths = [len(s) for s in found_node.itervalues()]
            # omit zero counts
            node_counts = [count for count in node_counts if count != 0]
            self.assertEqual(sorted(node_lengths), sorted(node_counts))

    def _assert_node_exclusive(self, dump):
        """assert that processes are distributed in a way consistent
        with the node exclusive properties of those processes
        """
        exclusive_dist = {}
        for proc_id, proc in dump['processes'].iteritems():
            if proc['state'] == '700-TERMINATED':
                continue
            assigned = proc.get('assigned')
            assert assigned is not None, proc

            node_exclusive = proc.get('node_exclusive')
            assert node_exclusive is not None

            if exclusive_dist.get(assigned) is None:
                exclusive_dist[assigned] = []
            exclusive_dist[assigned].append(node_exclusive)
            exclusive_dist[assigned].sort()

        for node, exclusives in exclusive_dist.iteritems():
            assert len(exclusives) == len(set(exclusives))

        exclusive_dist_nodes = {}
        exclusive_dist_resources = {}
        for node_id, node in dump['nodes'].iteritems():
            exclusive_dist_nodes[node_id] = node['node_exclusive']
            exclusive_dist_nodes[node_id].sort()
            for resource in node['resources']:
                exclusive_dist_resources[resource] = node['node_exclusive']
                exclusive_dist_resources[resource].sort()

        for node, exclusives in exclusive_dist_nodes.iteritems():
            assert len(exclusives) == len(set(exclusives))

        print "nodes: %s" % exclusive_dist_nodes
        print "resources: %s" % exclusive_dist_resources
        print "proc: %s" % exclusive_dist
        assert exclusive_dist == exclusive_dist_resources, "%s != %s" % (exclusive_dist, exclusive_dist_resources)
        return exclusive_dist

    def test_constraints(self):
        nodes = ['node1', 'node2']
        domain_id = domain_id_from_engine('engine1')
        node1_properties = dict(hat_type="fedora")
        node2_properties = dict(hat_type="bowler")

        self.client.node_state(nodes[0], domain_id, InstanceState.RUNNING,
            node1_properties)
        self._spawn_eeagent(nodes[0], 4)

        proc1_constraints = dict(hat_type="fedora")
        proc2_constraints = dict(hat_type="bowler")

        self.client.schedule_process("proc1", self.process_definition_id,
            constraints=proc1_constraints)
        self.client.schedule_process("proc2", self.process_definition_id,
            constraints=proc2_constraints)

        # proc1 should be running on the node/agent, proc2 queued
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"]),
                                        queued=["proc2"])

        # launch another eeagent that supports proc2's engine_type
        self.client.node_state(nodes[1], domain_id, InstanceState.RUNNING,
            node2_properties)
        self._spawn_eeagent(nodes[1], 4)

        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"],
                                                   node2=["proc2"]),
                                        queued=[])

    def test_queue_mode(self):

        constraints = dict(hat_type="fedora")
        queued = []
        rejected = []

        # Test QueueingMode.NEVER
        proc1_queueing_mode = QueueingMode.NEVER

        self.client.schedule_process("proc1", self.process_definition_id,
            constraints=constraints, queueing_mode=proc1_queueing_mode)

        # proc1 should be rejected
        rejected.append("proc1")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        rejected=rejected)

        # Test QueueingMode.ALWAYS
        proc2_queueing_mode = QueueingMode.ALWAYS

        self.client.schedule_process("proc2", self.process_definition_id,
            constraints=constraints, queueing_mode=proc2_queueing_mode)

        # proc2 should be queued
        queued.append("proc2")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

        # Test QueueingMode.START_ONLY
        proc3_queueing_mode = QueueingMode.START_ONLY
        proc3_restart_mode = RestartMode.ALWAYS

        self.client.schedule_process("proc3", self.process_definition_id,
            constraints=constraints, queueing_mode=proc3_queueing_mode,
            restart_mode=proc3_restart_mode)

        # proc3 should be queued, since its start_only
        queued.append("proc3")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

        node = "node1"
        domain_id = domain_id_from_engine('engine1')

        node_properties = dict(hat_type="fedora")
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)

        self._spawn_eeagent(node, 4)

        # we created a node, so it should now run
        self.notifier.wait_for_state("proc3", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc3"])

        log.debug("killing node %s", node)
        self._kill_all_eeagents()
        self.client.node_state(node, domain_id, InstanceState.TERMINATING)

        # proc3 should now be rejected, because its START_ONLY
        queued.remove("proc3")
        rejected.append("proc3")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        rejected=rejected)

        # Test QueueingMode.RESTART_ONLY

        # First test that its rejected if it doesn't start right away
        proc4_queueing_mode = QueueingMode.RESTART_ONLY
        proc4_restart_mode = RestartMode.ALWAYS

        self.client.schedule_process("proc4", self.process_definition_id,
            constraints=constraints, queueing_mode=proc4_queueing_mode,
            restart_mode=proc4_restart_mode)

        # proc4 should be rejected, since its RESTART_ONLY
        rejected.append("proc4")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        rejected=rejected)

        # Second test that if a proc starts, it'll get queued after it fails
        proc5_queueing_mode = QueueingMode.RESTART_ONLY
        proc5_restart_mode = RestartMode.ALWAYS

        # Start a node
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)
        self._spawn_eeagent(node, 4)

        self.client.schedule_process("proc5", self.process_definition_id,
            constraints=constraints, queueing_mode=proc5_queueing_mode,
            restart_mode=proc5_restart_mode)

        self.notifier.wait_for_state("proc5", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc5"])

        log.debug("killing node %s", node)
        self.client.node_state(node, domain_id, InstanceState.TERMINATING)
        self._kill_all_eeagents()

        # proc5 should be queued, since its RESTART_ONLY
        queued.append("proc5")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

    def test_restart_mode_never(self):

        constraints = dict(hat_type="fedora")

        # Start a node
        node = "node1"
        domain_id = domain_id_from_engine('engine1')
        node_properties = dict(hat_type="fedora")
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)
        eeagent = self._spawn_eeagent(node, 4)

        # Test RestartMode.NEVER
        proc1_queueing_mode = QueueingMode.ALWAYS
        proc1_restart_mode = RestartMode.NEVER

        self.client.schedule_process("proc1", self.process_definition_id,
            constraints=constraints, queueing_mode=proc1_queueing_mode,
            restart_mode=proc1_restart_mode)

        self.notifier.wait_for_state("proc1", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc1"])

        eeagent.fail_process("proc1")

        self.notifier.wait_for_state("proc1", ProcessState.FAILED)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.FAILED, ["proc1"])

    def test_restart_mode_always(self):

        constraints = dict(hat_type="fedora")
        queued = []

        # Start a node
        node = "node1"
        domain_id = domain_id_from_engine('engine1')
        node_properties = dict(hat_type="fedora")
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)
        eeagent = self._spawn_eeagent(node, 4)

        # Test RestartMode.ALWAYS
        proc2_queueing_mode = QueueingMode.ALWAYS
        proc2_restart_mode = RestartMode.ALWAYS

        self.client.schedule_process("proc2", self.process_definition_id,
            constraints=constraints, queueing_mode=proc2_queueing_mode,
            restart_mode=proc2_restart_mode, configuration={'process': {'minimum_time_between_starts': 0.1}})

        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        eeagent.exit_process("proc2")

        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        eeagent.fail_process("proc2")

        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        log.debug("killing node %s", node)
        self.client.node_state(node, domain_id, InstanceState.TERMINATING)
        self._kill_all_eeagents()

        # proc2 should be queued, since there are no more resources
        queued.append("proc2")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

    def test_restart_mode_abnormal(self):

        constraints = dict(hat_type="fedora")
        queued = []

        # Start a node
        node = "node1"
        domain_id = domain_id_from_engine('engine1')
        node_properties = dict(hat_type="fedora")
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)
        eeagent = self._spawn_eeagent(node, 4)

        # Test RestartMode.ABNORMAL
        proc2_queueing_mode = QueueingMode.ALWAYS
        proc2_restart_mode = RestartMode.ABNORMAL

        self.client.schedule_process("proc2", self.process_definition_id,
            constraints=constraints, queueing_mode=proc2_queueing_mode,
            restart_mode=proc2_restart_mode)

        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        eeagent.fail_process("proc2")

        # This can be very slow on buildbot, hence the long timeout
        self.notifier.wait_for_state("proc2", ProcessState.RUNNING, 60)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        log.debug("killing node %s", node)
        self.client.node_state(node, domain_id, InstanceState.TERMINATING)
        self._kill_all_eeagents()

        # proc2 should be queued, since there are no more resources
        queued.append("proc2")
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        queued=queued)

        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)
        eeagent = self._spawn_eeagent(node, 4)

        self.client.schedule_process("proc1", self.process_definition_id,
            constraints=constraints, queueing_mode=proc2_queueing_mode,
            restart_mode=proc2_restart_mode)

        self.notifier.wait_for_state("proc1", ProcessState.RUNNING)

        eeagent.exit_process("proc1")

        self.notifier.wait_for_state("proc1", ProcessState.EXITED)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.EXITED, ["proc1"])

    def test_start_count(self):

        nodes = ['node1']
        domain_id = domain_id_from_engine('engine1')
        node1_properties = dict(hat_type="fedora")

        self.client.node_state(nodes[0], domain_id, InstanceState.RUNNING,
            node1_properties)
        self._spawn_eeagent(nodes[0], 4)

        proc1_constraints = dict(hat_type="fedora")

        self.client.schedule_process("proc1", self.process_definition_id,
            constraints=proc1_constraints)

        # proc1 should be running on the node/agent, proc2 queued
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"]))

        proc = self.store.get_process(None, "proc1")
        self.assertEqual(proc.starts, 1)
        time.sleep(2)

        self.client.restart_process("proc1")
        # proc1 should be running on the node/agent, proc2 queued
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                        nodes=dict(node1=["proc1"]))
        proc = self.store.get_process(None, "proc1")
        self.assertEqual(proc.starts, 2)

    def test_minimum_time_between_starts(self):

        constraints = dict(hat_type="fedora")

        # Start a node
        node = "node1"
        domain_id = domain_id_from_engine('engine1')
        node_properties = dict(hat_type="fedora")
        self.client.node_state(node, domain_id, InstanceState.RUNNING,
                node_properties)
        eeagent = self._spawn_eeagent(node, 4)

        # Test RestartMode.ALWAYS
        queueing_mode = QueueingMode.ALWAYS
        restart_mode = RestartMode.ALWAYS

        default_time_to_throttle = 2
        time_to_throttle = 10

        self.client.schedule_process("proc1", self.process_definition_id,
            constraints=constraints, queueing_mode=queueing_mode,
            restart_mode=restart_mode)

        self.client.schedule_process("proc2", self.process_definition_id,
            constraints=constraints, queueing_mode=queueing_mode,
            restart_mode=restart_mode,
            configuration=minimum_time_between_starts_config(time_to_throttle))

        # Processes should start once without delay
        self.notifier.wait_for_state("proc1", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc1"])

        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        # Processes should be restarted once without delay
        eeagent.exit_process("proc1")
        eeagent.exit_process("proc2")

        self.notifier.wait_for_state("proc1", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc1"])
        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

        # The second time proc1 should be throttled for 2s (the default), and
        # proc2 should be throttled for the configured 5s
        eeagent.exit_process("proc1")
        eeagent.exit_process("proc2")

        self.notifier.wait_for_state("proc1", ProcessState.WAITING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.WAITING, ["proc1"])
        self.notifier.wait_for_state("proc2", ProcessState.WAITING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.WAITING, ["proc2"])

        # After waiting a few seconds, proc1 should be restarted
        time.sleep(default_time_to_throttle + 1)

        self.notifier.wait_for_state("proc1", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc1"])
        self.notifier.wait_for_state("proc2", ProcessState.WAITING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.WAITING, ["proc2"])

        # After a few more secs, proc2 should be restarted as well
        time.sleep(time_to_throttle - (default_time_to_throttle + 1) + 1)

        self.notifier.wait_for_state("proc2", ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                ProcessState.RUNNING, ["proc2"])

    def test_describe(self):

        self.client.schedule_process("proc1", self.process_definition_id)

        processes = self.client.describe_processes()
        self.assertEqual(len(processes), 1)
        self.assertEqual(processes[0]['upid'], "proc1")

        proc1 = self.client.describe_process("proc1")
        self.assertEqual(proc1['upid'], "proc1")

        self.client.schedule_process("proc2", self.process_definition_id)

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
        domain_id = domain_id_from_engine('engine1')
        self.client.node_state(node, domain_id, InstanceState.RUNNING)
        self._spawn_eeagent(node, 1)

        proc = "proc1"

        self.client.schedule_process(proc, self.process_definition_id)

        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.RUNNING, [proc])

        agent = self._get_eeagent_for_process(proc)
        agent.exit_process(proc)
        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.EXITED, [proc])
        self.notifier.wait_for_state(proc, ProcessState.EXITED)

    def test_neediness(self, process_count=20, node_count=5):

        procs = ["proc" + str(i) for i in range(process_count)]
        for proc in procs:
            procstate = self.client.schedule_process(proc,
                self.process_definition_id)
            self.assertEqual(procstate['upid'], proc)

        self._wait_assert_pd_dump(self._assert_process_states,
            ProcessState.WAITING, procs)

        for i in range(3):
            # retry this a few times to avoid a race between processes
            # hitting WAITING state and the needs being registered
            try:
                self.epum_client.assert_needs(range(node_count + 1),
                    domain_id_from_engine("engine1"))
                break
            except AssertionError:
                time.sleep(0.01)

        self.epum_client.clear()

        # now provide nodes and resources, processes should start
        nodes = ["node" + str(i) for i in range(node_count)]
        domain_id = domain_id_from_engine('engine1')
        for node in nodes:
            self.client.node_state(node, domain_id, InstanceState.RUNNING)

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

        for i in range(3):
            # retry this a few times to avoid a race between processes
            # hitting WAITING state and the needs being registered
            try:
                self.epum_client.assert_needs(range(node_count + 1),
                    domain_id_from_engine("engine1"))
                break
            except AssertionError:
                time.sleep(0.01)

    def test_definitions(self):
        self.client.create_definition("d1", "t1", "notepad.exe")

        d1 = self.client.describe_definition("d1")
        self.assertEqual(d1['definition_id'], "d1")
        self.assertEqual(d1['definition_type'], "t1")
        self.assertEqual(d1['executable'], "notepad.exe")

        self.client.update_definition("d1", "t1", "notepad2.exe")
        d1 = self.client.describe_definition("d1")
        self.assertEqual(d1['executable'], "notepad2.exe")

        d_list = self.client.list_definitions()
        self.assertIn("d1", d_list)

        self.client.remove_definition("d1")

    def test_reschedule_process(self):
        node = "node1"
        domain_id = domain_id_from_engine('engine1')
        self.client.node_state(node, domain_id, InstanceState.RUNNING)
        self._spawn_eeagent(node, 1)

        proc = "proc1"

        # start a process that is never restarted automatically.
        self.client.create_process(proc, self.process_definition_id)
        self.client.schedule_process(proc, restart_mode=RestartMode.NEVER)

        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.RUNNING, [proc])

        agent = self._get_eeagent_for_process(proc)
        agent.exit_process(proc)
        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.EXITED, [proc])
        self.notifier.wait_for_state(proc, ProcessState.EXITED)

        record = self.client.schedule_process(proc)
        self.assertEqual(record['state'], ProcessState.REQUESTED)

        self.notifier.wait_for_state(proc, ProcessState.RUNNING)

        # now fail the process. it should still be restartable.
        agent.fail_process(proc)

    def test_create_schedule(self):
        node = "node1"
        domain_id = domain_id_from_engine('engine1')
        self.client.node_state(node, domain_id, InstanceState.RUNNING)
        self._spawn_eeagent(node, 1)

        proc = "proc1"

        # create a process. it should be UNSCHEDULED until we schedule it
        self.client.create_process(proc, self.process_definition_id)

        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.UNSCHEDULED, [proc])

        # creating again is harmless
        self.client.create_process(proc, self.process_definition_id)

        # now schedule it
        self.client.schedule_process(proc)
        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.RUNNING, [proc])
        self.notifier.wait_for_state(proc, ProcessState.RUNNING)

        # scheduling again is harmless
        self.client.schedule_process(proc)

    def test_restart_system_boot(self):

        # set up some state in the PD before restart
        self.client.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)
        self._spawn_eeagent("node1", 4)

        procs = [('p1', RestartMode.ABNORMAL, None),
                 ('p2', RestartMode.ALWAYS, None),
                 ('p3', RestartMode.NEVER, None),
                 ('p4', RestartMode.ALWAYS, nosystemrestart_process_config()),
                 ('p5', RestartMode.ABNORMAL, None),
                 ('p6', RestartMode.ABNORMAL, nosystemrestart_process_config())]
        # fill up all 4 slots on engine1 agent and launch 2 more procs
        for upid, restart_mode, config in procs:
            self.client.schedule_process(upid, self.process_definition_id,
                queueing_mode=QueueingMode.ALWAYS, restart_mode=restart_mode,
                configuration=config)

        self.notifier.wait_for_state('p1', ProcessState.RUNNING)
        self.notifier.wait_for_state('p2', ProcessState.RUNNING)
        self.notifier.wait_for_state('p3', ProcessState.RUNNING)
        self.notifier.wait_for_state('p4', ProcessState.RUNNING)

        self.notifier.wait_for_state('p5', ProcessState.WAITING)
        self.notifier.wait_for_state('p6', ProcessState.WAITING)

        # now kill PD and eeagents. come back in system restart mode.
        self.stop_pd()
        self._kill_all_eeagents()

        self.store.initialize()
        self.store.set_system_boot(True)
        self.store.shutdown()

        self.start_pd()
        self.store.wait_initialized(timeout=20)

        # some processes should come back pending. others should fail out
        # due to their restart mode flag.
        self.notifier.wait_for_state('p1', ProcessState.UNSCHEDULED_PENDING)
        self.notifier.wait_for_state('p2', ProcessState.UNSCHEDULED_PENDING)
        self.notifier.wait_for_state('p3', ProcessState.TERMINATED)
        self.notifier.wait_for_state('p4', ProcessState.TERMINATED)
        self.notifier.wait_for_state('p5', ProcessState.UNSCHEDULED_PENDING)
        self.notifier.wait_for_state('p6', ProcessState.TERMINATED)

        # add resources back
        self.client.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)
        self._spawn_eeagent("node1", 4)

        # now launch a new process to make sure scheduling still works during
        # system boot mode
        self.client.schedule_process("p7", self.process_definition_id)

        # and restart a couple of the dead procs. one FAILED and one UNSCHEDULED_PENDING
        self.client.schedule_process("p1")
        self.client.schedule_process("p4")

        self.notifier.wait_for_state('p1', ProcessState.RUNNING)
        self.notifier.wait_for_state('p4', ProcessState.RUNNING)
        self.notifier.wait_for_state('p7', ProcessState.RUNNING)

        # finally, end system boot mode. the remaining 2 U-P procs should be scheduled
        self.client.set_system_boot(False)
        self._wait_assert_pd_dump(self._assert_process_distribution,
                                  node_counts=[4],
                                  queued_count=1)

        # one process will end up queued. doesn't matter which
        p2 = self.client.describe_process("p2")
        p5 = self.client.describe_process("p5")
        states = set([p2['state'], p5['state']])
        self.assertEqual(states, set([ProcessState.RUNNING, ProcessState.WAITING]))

    def test_missing_ee(self):
        """test_missing_ee

        Ensure that the PD kills lingering processes on eeagents after they've been
        evacuated.
        """

        # create some fake nodes and tell PD about them
        node_1 = "node1"
        domain_id = domain_id_from_engine("engine4")
        self.client.node_state(node_1, domain_id, InstanceState.RUNNING)

        # PD knows about this node but hasn't gotten a heartbeat yet

        # spawn the eeagents and tell them all to heartbeat
        eeagent_1 = self._spawn_eeagent(node_1, 1)

        def assert_all_resources(state):
            eeagent_nodes = set()
            for resource in state['resources'].itervalues():
                eeagent_nodes.add(resource['node_id'])
            self.assertEqual(set([node_1]), eeagent_nodes)

        self._wait_assert_pd_dump(assert_all_resources)
        time_to_throttle = 0

        self.client.schedule_process("p1", self.process_definition_id, execution_engine_id="engine4",
            configuration=minimum_time_between_starts_config(time_to_throttle))

        # Send a heartbeat to show the process is RUNNING, then wait for doctor
        # to mark the eeagent missing
        time.sleep(1)
        eeagent_1.send_heartbeat()
        self.notifier.wait_for_state('p1', ProcessState.RUNNING, timeout=30)
        self.notifier.wait_for_state('p1', ProcessState.WAITING, timeout=30)

        # Check that process is still 'Running' on the eeagent, even the PD has
        # since marked it failed
        eeagent_process = eeagent_1._get_process_with_upid('p1')
        self.assertEqual(eeagent_process['u_pid'], 'p1')
        self.assertEqual(eeagent_process['state'], ProcessState.RUNNING)
        self.assertEqual(eeagent_process['round'], 0)

        # Now send another heartbeat to start getting procs again
        eeagent_1.send_heartbeat()
        self.notifier.wait_for_state('p1', ProcessState.RUNNING, timeout=30)

        eeagent_process = eeagent_1._get_process_with_upid('p1')
        self.assertEqual(eeagent_process['u_pid'], 'p1')
        self.assertEqual(eeagent_process['state'], ProcessState.RUNNING)
        self.assertEqual(eeagent_process['round'], 1)

        # The pd should now have rescheduled the proc, and terminated the
        # lingering process
        self.assertEqual(len(eeagent_1.history), 1)
        terminated_history = eeagent_1.history[0]
        self.assertEqual(terminated_history['u_pid'], 'p1')
        self.assertEqual(terminated_history['state'], ProcessState.TERMINATED)
        self.assertEqual(terminated_history['round'], 0)

    def test_matchmaker_msg_retry(self):
        node = "node1"
        domain_id = domain_id_from_engine('engine1')
        self.client.node_state(node, domain_id, InstanceState.RUNNING)
        self._spawn_eeagent(node, 1)

        # sneak in and shorten retry time
        self.pd.matchmaker.process_launcher.retry_seconds = 0.5

        proc1 = "proc1"
        proc2 = "proc2"

        with patch.object(self.pd.matchmaker.process_launcher, "resource_client") as mock_resource_client:
            # first process request goes to mock, not real client but no error
            self.client.schedule_process(proc1, self.process_definition_id)

            # second process hits an error but that should not cause problems
            mock_resource_client.launch_process.side_effect = Exception("boom!")
            self.client.schedule_process(proc2, self.process_definition_id)

            self._wait_assert_pd_dump(self._assert_process_states,
                                      ProcessState.ASSIGNED, [proc1, proc2])
            self.assertEqual(mock_resource_client.launch_process.call_count, 2)

        # now resource client works again. those messages should be retried
        self.notifier.wait_for_state(proc1, ProcessState.RUNNING)
        self.notifier.wait_for_state(proc2, ProcessState.RUNNING)
        self._wait_assert_pd_dump(self._assert_process_states,
                                  ProcessState.RUNNING, [proc1, proc2])


class ProcessDispatcherServiceZooKeeperTests(ProcessDispatcherServiceTests, ZooKeeperTestMixin):

    # this runs all of the ProcessDispatcherService tests wih a ZK store

    def setup_store(self):

        self.setup_zookeeper(base_path_prefix="/processdispatcher_service_tests_")
        store = ProcessDispatcherZooKeeperStore(self.zk_hosts,
            self.zk_base_path, use_gevent=self.use_gevent)
        store.initialize()
        return store

    def teardown_store(self):
        if self.store:
            self.store.shutdown()
        self.teardown_zookeeper()


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
