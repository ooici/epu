# Copyright 2013 University of Chicago

import os
import unittest
import logging
import threading
import time
import uuid

from mock import patch

from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

import epu.tevent as tevent
from epu.processdispatcher.matchmaker import PDMatchmaker
from epu.processdispatcher.store import ProcessDispatcherStore, ProcessDispatcherZooKeeperStore
from epu.processdispatcher.test.mocks import MockResourceClient, \
    MockEPUMClient, MockNotifier, get_definition, get_domain_config
from epu.processdispatcher.store import ResourceRecord, ProcessRecord, NodeRecord
from epu.processdispatcher.engines import EngineRegistry, domain_id_from_engine
from epu.states import ProcessState, ProcessDispatcherState, ExecutionResourceState
from epu.processdispatcher.test.test_store import StoreTestMixin
from epu.processdispatcher.core import ProcessDispatcherCore
from epu.test import ZooKeeperTestMixin

log = logging.getLogger(__name__)


class PDMatchmakerTests(unittest.TestCase, StoreTestMixin):

    engine_conf = {
        'engine1': {
            'maximum_vms': 100,
            'slots': 1
        },
        'engine2': {
            'maximum_vms': 100,
            'slots': 2,
        },
        'engine3': {
            'maximum_vms': 100,
            'slots': 2,
            'replicas': 2
        },
        'engine4': {
            'maximum_vms': 100,
            'slots': 1,
            'spare_slots': 1
        }

    }

    process_engines = {
        'my.engine2': 'engine2'
    }

    def setUp(self):
        self.store = self.setup_store()
        self.resource_client = MockResourceClient()
        self.epum_client = MockEPUMClient()
        self.registry = EngineRegistry.from_config(
            self.engine_conf, default='engine1', process_engines=self.process_engines)
        self.notifier = MockNotifier()
        self.service_name = "some_pd"
        self.definition_id = "pd_definition"
        self.definition = get_definition()
        self.base_domain_config = get_domain_config()
        self.run_type = "fake_run_type"
        self.restart_throttling_config = {}
        self.dispatch_retry_seconds = 30

        self.core = ProcessDispatcherCore(self.store, self.registry,
            self.resource_client, self.notifier)

        self.epum_client.add_domain_definition(self.definition_id, self.definition)
        self.mm = PDMatchmaker(self.core, self.store, self.resource_client,
            self.registry, self.epum_client, self.notifier, self.service_name,
            self.definition_id, self.base_domain_config, self.run_type,
            self.restart_throttling_config, self.dispatch_retry_seconds)

        self.mmthread = None

    def tearDown(self):
        if self.mmthread:
            self.mm.cancel()
            self.mmthread.join()
            self.mmthread = None
        self.teardown_store()

    def setup_store(self):
        return ProcessDispatcherStore()

    def teardown_store(self):
        return

    def _run_in_thread(self):
        self.mmthread = tevent.spawn(self.mm.inaugurate)
        time.sleep(0.05)

    def test_run_cancel(self):
        self._run_in_thread()

        self.mm.cancel()
        self.mmthread.join()
        self.mmthread = None

    def test_match_writeconflict(self):
        self.mm.initialize()

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)
        self.store.enqueue_process(*p1key)

        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        # now update the resource record so the matchmake() attempt to write will conflict
        r1.assigned = ["hats"]
        self.store.update_resource(r1)

        # this should bail out without resetting the needs_matchmaking flag
        # or registering any need
        self.assertTrue(self.mm.needs_matchmaking)
        self.mm.matchmake()
        self.assertFalse(self.epum_client.reconfigures)
        self.assertTrue(self.mm.needs_matchmaking)

        r1copy = self.store.get_resource(r1.resource_id)
        self.assertRecordVersions(r1, r1copy)

    def test_match_notfound(self):
        self.mm.initialize()

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)
        self.store.enqueue_process(*p1key)

        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        # now update the resource record so the matchmake() attempt to write will conflict
        self.store.remove_resource("r1")

        # this should bail out without resetting the needs_matchmaking flag
        # or registering any need
        self.assertTrue(self.mm.needs_matchmaking)
        self.mm.matchmake()
        self.assertFalse(self.epum_client.reconfigures)
        self.assertTrue(self.mm.needs_matchmaking)

    def test_match_process_terminated(self):
        self.mm.initialize()

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
            ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)
        self.store.enqueue_process(*p1key)

        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        # now update the process record to be TERMINATED so that
        # MM should bail out of matching this process

        p1.state = ProcessState.TERMINATED
        self.store.update_process(p1)
        self.store.remove_queued_process(*p1key)

        self.mm.matchmake()

        p1 = self.store.get_process(None, "p1")
        self.assertEqual(p1.state, ProcessState.TERMINATED)

    def test_match1(self):
        self._run_in_thread()

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)

        self.store.enqueue_process(*p1key)

        self.wait_resource(r1.resource_id, lambda r: list(p1key) in r.assigned)
        time.sleep(0.05)
        self.resource_client.check_process_launched(p1, r1.resource_id)
        self.wait_process(p1.owner, p1.upid,
                          lambda p: p.assigned == r1.resource_id and
                                    p.state == ProcessState.ASSIGNED)

    def test_match_double_queued_process(self):

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)
        r2 = ResourceRecord.new("r2", "n1", 1, properties=props)
        self.store.add_resource(r2)

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)

        p2 = ProcessRecord.new(None, "p2", get_process_definition(),
                               ProcessState.REQUESTED)
        p2key = p2.get_key()
        self.store.add_process(p2)

        # enqueue p1 repeatedly. make sure that doesn't bomb anything
        self.store.enqueue_process(*p1key)
        self.store.enqueue_process(*p1key)
        self.store.enqueue_process(*p2key)
        self.store.enqueue_process(*p1key)

        self._run_in_thread()
        self.wait_process(p1.owner, p1.upid,
                          lambda p: p.state == ProcessState.ASSIGNED)
        self.wait_process(p2.owner, p2.upid,
                          lambda p: p.state == ProcessState.ASSIGNED)

    def test_node_exclusive_bug(self):
        """test_node_exclusive_bug

        If two processes with the same node exclusive attribute where scheduled
        in the same matchmaking cycle, they could be scheduled to the same
        resource, due to a caching issue. This test tests the fix.
        """
        self.mm.initialize()

        n1 = NodeRecord.new("n1", "d1")
        self.store.add_node(n1)

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 2, properties=props)
        self.store.add_resource(r1)

        n2 = NodeRecord.new("n2", "d1")
        self.store.add_node(n2)

        props = {"engine": "engine1"}
        r2 = ResourceRecord.new("r2", "n2", 2, properties=props)
        self.store.add_resource(r2)

        xattr_1 = "port5000"
        constraints = {}
        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints,
                               node_exclusive=xattr_1)
        p1key = p1.get_key()
        self.store.add_process(p1)
        self.store.enqueue_process(*p1key)

        p2 = ProcessRecord.new(None, "p2", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints,
                               node_exclusive=xattr_1)
        p2key = p2.get_key()
        self.store.add_process(p2)
        self.store.enqueue_process(*p2key)

        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        self.mm.matchmake()

        # Ensure these processes are ASSIGNED and scheduled to different nodes

        p1 = self.store.get_process(None, "p1")
        p2 = self.store.get_process(None, "p2")
        self.assertNotEqual(p1.assigned, p2.assigned)

    def test_node_exclusive(self):
        self._run_in_thread()

        n1 = NodeRecord.new("n1", "d1")
        self.store.add_node(n1)

        props = {"engine": "engine1"}
        n1_r1 = ResourceRecord.new("n1_r1", "n1", 2, properties=props)
        self.store.add_resource(n1_r1)

        n1_r2 = ResourceRecord.new("n1_r2", "n1", 2, properties=props)
        self.store.add_resource(n1_r2)

        xattr_1 = "port5000"
        constraints = {}
        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints,
                               node_exclusive=xattr_1)
        p1key = p1.get_key()
        self.store.add_process(p1)
        self.store.enqueue_process(*p1key)

        # The first process should be assigned, since nothing else needs this
        # attr
        # TODO: it's possible that this could be assigned to n1_r2, but hopefully not
        self.wait_resource(n1_r1.resource_id, lambda r: list(p1key) in r.assigned)
        time.sleep(0.05)
        self.resource_client.check_process_launched(p1, n1_r1.resource_id)
        self.wait_process(p1.owner, p1.upid,
                          lambda p: p.assigned == n1_r1.resource_id and
                                    p.state == ProcessState.ASSIGNED)

        p2 = ProcessRecord.new(None, "p2", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints,
                               node_exclusive=xattr_1)
        p2key = p2.get_key()
        self.store.add_process(p2)
        self.store.enqueue_process(*p2key)

        # The second process should wait, since first process wants this attr
        # as well
        self.wait_process(p2.owner, p2.upid,
                          lambda p: p.state == ProcessState.WAITING)

        # If we start another node, we should see that second process be
        # scheduled
        n2 = NodeRecord.new("n2", "d1")
        self.store.add_node(n2)

        props = {"engine": "engine1"}
        n2_r1 = ResourceRecord.new("n2_r1", "n2", 2, properties=props)
        self.store.add_resource(n2_r1)

        props = {"engine": "engine1"}
        n2_r2 = ResourceRecord.new("n2_r2", "n2", 2, properties=props)
        self.store.add_resource(n2_r2)

        # The second process should now be assigned
        self.wait_resource(n2_r1.resource_id, lambda r: list(p2key) in r.assigned)
        time.sleep(0.05)
        self.resource_client.check_process_launched(p2, n2_r1.resource_id)
        self.wait_process(p2.owner, p2.upid,
                          lambda p: p.assigned == n2_r1.resource_id and
                                    p.state == ProcessState.ASSIGNED)

        # Now we submit another process with a different exclusive attribute
        # It should be assigned right away
        xattr_2 = "port5001"
        constraints = {}
        p3 = ProcessRecord.new(None, "p3", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints,
                               node_exclusive=xattr_2)
        p3key = p3.get_key()
        self.store.add_process(p3)
        self.store.enqueue_process(*p3key)

        p3_resource = None
        for resource in [n1_r1, n1_r2, n2_r1, n2_r2]:
            try:
                self.wait_resource(resource.resource_id, lambda r: list(p3key) in r.assigned,
                    timeout=0.5)
            except Exception:
                continue
            time.sleep(0.05)
            self.resource_client.check_process_launched(p3, resource.resource_id)
            self.wait_process(p3.owner, p3.upid,
                              lambda p: p.assigned == resource.resource_id and
                                        p.state == ProcessState.ASSIGNED)
            p3_resource = resource

        self.assertIsNotNone(p3_resource)

        # Now submit a fourth process, which should be scheduled to a different
        # node from p3
        p4 = ProcessRecord.new(None, "p4", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints,
                               node_exclusive=xattr_2)
        p4key = p4.get_key()
        self.store.add_process(p4)
        self.store.enqueue_process(*p4key)

        p4_resource = None
        for resource in [n1_r1, n1_r2, n2_r1, n2_r2]:
            try:
                self.wait_resource(resource.resource_id, lambda r: list(p4key) in r.assigned,
                    timeout=0.5)
            except Exception:
                continue
            time.sleep(0.05)
            self.resource_client.check_process_launched(p4, resource.resource_id)
            self.wait_process(p4.owner, p4.upid,
                              lambda p: p.assigned == resource.resource_id and
                                        p.state == ProcessState.ASSIGNED)
            p4_resource = resource

        self.assertIsNotNone(p4_resource)

        self.assertNotEqual(p3_resource.node_id, p4_resource.node_id)

    def test_node_filo(self):
        """test_node_filo

        We prioritize shutting down the newest VMs as a workaround for OOI
        Testing strategy
        """
        self.mm.initialize()

        n1 = NodeRecord.new("n1", "d1")
        self.store.add_node(n1)

        props = {"engine": "engine4"}
        r1 = ResourceRecord.new("r1", "n1", 2, properties=props)
        self.store.add_resource(r1)

        n2 = NodeRecord.new("n2", "d1")
        self.store.add_node(n2)

        props = {"engine": "engine4"}
        r2 = ResourceRecord.new("r2", "n2", 2, properties=props)
        self.store.add_resource(r2)

        constraints = {"engine": "engine4"}
        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints)
        p1key = p1.get_key()
        self.store.add_process(p1)
        self.store.enqueue_process(*p1key)

        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        self.mm.register_needs()
        self.epum_client.clear()

        self.mm.queued_processes = []

        self.mm.register_needs()
        conf = self.epum_client.reconfigures['pd_domain_engine4'][0]
        retired_nodes = conf['engine_conf']['retirable_nodes']
        assert len(retired_nodes) == 1

        # This should be the second node we started
        assert retired_nodes[0] == "n2"

    def test_match_copy_hostname(self):
        self._run_in_thread()

        props = {"engine": "engine1", "hostname": "vm123"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
            ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)

        self.store.enqueue_process(*p1key)
        self.wait_process(p1.owner, p1.upid,
            lambda p: p.assigned == r1.resource_id and
                      p.state == ProcessState.ASSIGNED)
        p1 = self.store.get_process(None, "p1")
        self.assertEqual(p1.hostname, "vm123")

    def test_waiting(self):
        self._run_in_thread()

        # not-immediate process enqueued while there are no resources

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)

        self.store.enqueue_process(*p1key)
        self.wait_process(None, "p1", lambda p: p.state == ProcessState.WAITING)

        # now give it a resource. it should be scheduled
        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        self.wait_resource(r1.resource_id, lambda r: list(p1key) in r.assigned)
        time.sleep(0.05)
        self.resource_client.check_process_launched(p1, r1.resource_id)

    def test_process_terminated(self):
        self._run_in_thread()

        event = threading.Event()

        # we set up a resource and a matching process that should be assigned
        # to it. we will simulate marking the process TERMINATED out-of-band
        # and ensure that is recognized before the dispatch.

        # when the matchmaker attempts to update the process, sneak in an update
        # first so the matchmaker request conflicts
        original_update_process = self.store.update_process

        def patched_update_process(process):
            original = self.store.get_process(process.owner, process.upid)
            original.state = ProcessState.TERMINATED
            original_update_process(original)

            try:
                original_update_process(process)
            finally:
                event.set()

        self.store.update_process = patched_update_process

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
            ProcessState.REQUESTED)
        self.store.add_process(p1)
        self.store.enqueue_process(*p1.key)

        # now give it a resource. it should be matched but in the meantime
        # the process will be terminated
        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        # wait for MM to hit our update conflict, kill it, and check that it
        # appropriately backed out the allocation
        assert event.wait(5)
        self.mm.cancel()
        self.mmthread.join()
        self.mmthread = None

        resource = self.store.get_resource("r1")
        self.assertEqual(len(resource.assigned), 0)

        self.assertEqual(self.resource_client.launch_count, 0)

    def test_process_already_assigned(self):

        # this is a recovery situation, probably. The process is assigned
        # to a resource already at the start of the matchmaker run, but
        # the process record hasn't been updated to reflect that. The
        # situation should be detected and handled by matchmaker.

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
            ProcessState.REQUESTED)
        self.store.add_process(p1)
        self.store.enqueue_process(*p1.key)

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        r1.assigned.append(p1.key)
        self.store.add_resource(r1)

        self._run_in_thread()

        self.wait_process(None, "p1", lambda p: p.state == ProcessState.ASSIGNED)

        r1 = self.store.get_resource("r1")
        self.assertEqual(len(r1.assigned), 1)
        self.assertTrue(r1.is_assigned(p1.owner, p1.upid, p1.round))
        self.assertEqual(r1.available_slots, 0)

    def test_assigned_process_not_rematched(self):
        # Processes are moved to the ASSIGNED state when assigned by the matchmaker.
        # Though they remain in the queue, they should not be rematched
        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
            ProcessState.ASSIGNED, assigned="r1")
        self.store.add_process(p1)
        self.store.enqueue_process(*p1.key)

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        r1.assigned.append(p1.key)
        self.store.add_resource(r1)

        self.mm.initialize()
        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        self.mm.process_launcher.pending_process_dispatches[p1.key] = time.time()

        # kinda dirty: patch out an internal MM function and ensure it isn't called
        with patch.object(self.mm, '_handle_matched_process') as m:
            self.mm.matchmake()

            assert not m.called, "mm has calls: %s" % (m.call_args,)

        self.assertEqual(len(self.mm.process_launcher.pending_process_dispatches), 1)

    def test_assigned_process_not_rematched_but_retried(self):
        # Processes are moved to the ASSIGNED state when assigned by the matchmaker.
        # Though they remain in the queue, they should not be rematched
        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
            ProcessState.ASSIGNED, assigned="r1")
        self.store.add_process(p1)
        self.store.enqueue_process(*p1.key)

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        r1.assigned.append(p1.key)
        self.store.add_resource(r1)

        self.mm.initialize()
        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        # kinda dirty: patch out an internal MM function and ensure it isn't called
        with patch.object(self.mm, '_handle_matched_process') as m:
            self.mm.matchmake()

            assert not m.called, "mm has calls: %s" % (m.call_args,)

        self.assertIn(p1.key, self.mm.process_launcher.pending_process_dispatches)

    def test_wait_resource(self):
        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)
        self.wait_resource("r1", lambda r: r.resource_id == "r1")

        def makeitso():
            r1.slot_count = 2
            self.store.update_resource(r1)

        tevent.spawn(makeitso)

        self.wait_resource("r1", lambda r: r.slot_count == 2)

    def test_disabled_resource(self):
        self._run_in_thread()

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        r1.state = ExecutionResourceState.DISABLED

        self.store.add_resource(r1)
        self.wait_resource("r1", lambda r: r.resource_id == "r1")

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED)
        p1key = p1.key
        self.store.add_process(p1)
        self.store.enqueue_process(*p1key)

        # the resource matches but it is disabled, process should
        # remain in the queue
        self.wait_process(p1.owner, p1.upid,
                          lambda p: p.state == ProcessState.WAITING)

    def test_queueing_order(self):
        self._run_in_thread()

        procnames = []
        # queue 10 processes
        for i in range(10):
            proc = ProcessRecord.new(None, "proc" + str(i), get_process_definition(),
                                     ProcessState.REQUESTED)
            prockey = proc.key
            self.store.add_process(proc)
            self.store.enqueue_process(*prockey)
            self.epum_client.clear()

            self.wait_process(proc.owner, proc.upid,
                              lambda p: p.state == ProcessState.WAITING)
            procnames.append(proc.upid)

            # potentially retry a few times to account for race between process
            # state updates and need reconfigures
            for i in range(5):
                try:
                    self.assert_one_reconfigure(preserve_n=i + 1, retirees=[])
                    break
                except AssertionError:
                    time.sleep(0.01)

            self.epum_client.clear()

        # now add 10 resources each with 1 slot. processes should start in order
        for i in range(10):
            props = {"engine": "engine1"}
            res = ResourceRecord.new("res" + str(i), "node" + str(i), 1,
                    properties=props)
            self.store.add_resource(res)

            self.wait_process(None, procnames[i],
                              lambda p: p.state >= ProcessState.ASSIGNED and
                                        p.assigned == res.resource_id)

        # finally doublecheck that launch requests happened in order too
        for i in range(5):
            try:
                self.assertEqual(self.resource_client.launch_count, 10)
                for i, launch in enumerate(self.resource_client.launches):
                    self.assertEqual(launch[0], "res" + str(i))
                    self.assertEqual(launch[1], "proc" + str(i))

                break
            except AssertionError:
                time.sleep(0.01)

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

    def enqueue_n_processes(self, n_processes, engine_id):
        pkeys = []
        for pid in range(n_processes):
            upid = uuid.uuid4().hex
            p = ProcessRecord.new(None, upid, get_process_definition(),
                ProcessState.REQUESTED, constraints={'engine': engine_id})
            pkey = p.get_key()
            pkeys.append(pkey)
            self.store.add_process(p)
            self.store.enqueue_process(*pkey)
            self.mm.queued_processes.append(pkey)
        return pkeys

    def create_n_pending_processes(self, n_processes, engine_id=None, module=None):
        if engine_id is not None:
            constraints = {'engine': engine_id}
        else:
            constraints = {}
        pkeys = []
        for pid in range(n_processes):
            upid = uuid.uuid4().hex
            p = ProcessRecord.new(None, upid, get_process_definition(module=module),
                ProcessState.UNSCHEDULED_PENDING, constraints=constraints)
            self.store.add_process(p)
            pkey = p.get_key()
            pkeys.append(pkey)
        return pkeys

    def enqueue_pending_processes(self):
        """enqueue_pending_processes

        Normally, this would be done by the doctor in a full system
        """
        pkeys = []
        for pid in self.store.get_process_ids():
            owner, upid = pid
            process = self.store.get_process(owner, upid)
            if process.state == ProcessState.UNSCHEDULED_PENDING:
                pkey = process.get_key()
                process.state = ProcessState.REQUESTED
                self.store.update_process(process)
                self.store.enqueue_process(*pkey)
                self.mm.queued_processes.append(pkey)
                pkeys.append(pkey)
        return pkeys

    def create_engine_resources(self, engine_id, node_count=1, assignments=None):
        engine_spec = self.registry.get_engine_by_id(engine_id)
        assert len(assignments) <= engine_spec.slots * engine_spec.replicas * node_count

        records = []
        for i in range(node_count):
            node_id = uuid.uuid4().hex
            props = {"engine": engine_id}
            for i in range(engine_spec.replicas):
                res = ResourceRecord.new(uuid.uuid4().hex, node_id,
                    engine_spec.slots, properties=props)
                records.append(res)
                res.metadata['version'] = 0
                self.mm.resources[res.resource_id] = res

                # use fake process ids in the assigned list, til it matters
                if len(assignments) <= engine_spec.slots:
                    res.assigned = list(assignments)
                    assignments = []
                else:
                    res.assigned = assignments[:engine_spec.slots]
                    assignments[:] = assignments[engine_spec.slots:]

                print "added resource: %s" % res
        return records

    def test_engine_config(self):
        self.mm.cancel()
        maximum_vms = 10
        base_iaas_allocation = 'm1.small'
        engine_iaas_allocation = 'm1.medium'
        self.base_domain_config = {
            'engine_conf': {
                'iaas_allocation': base_iaas_allocation
            }
        }

        engine_conf = {
            'engine1': {
                'slots': 1,
                'iaas_allocation': engine_iaas_allocation,
                'maximum_vms': maximum_vms
            },
            'engine2': {
                'slots': 2,
                'iaas_allocation': engine_iaas_allocation,
                'maximum_vms': maximum_vms
            },
        }

        self.registry = EngineRegistry.from_config(engine_conf, default='engine1')
        self.mm = PDMatchmaker(self.core, self.store, self.resource_client,
            self.registry, self.epum_client, self.notifier, self.service_name,
            self.definition_id, self.base_domain_config, self.run_type,
            self.restart_throttling_config)

        self.mm.initialize()
        self.assertEqual(len(self.epum_client.domains), len(engine_conf.keys()))

        engine1_domain_conf = self.epum_client.domains['pd_domain_engine1']['engine_conf']
        engine2_domain_conf = self.epum_client.domains['pd_domain_engine2']['engine_conf']

        self.assertEqual(engine1_domain_conf['iaas_allocation'], engine_iaas_allocation)
        self.assertEqual(engine2_domain_conf['iaas_allocation'], engine_iaas_allocation)

        self.assertEqual(engine1_domain_conf['maximum_vms'], maximum_vms)
        self.assertEqual(engine2_domain_conf['maximum_vms'], maximum_vms)

    def test_needs(self):
        self.mm.initialize()

        self.assertFalse(self.epum_client.reconfigures)
        self.assertEqual(len(self.epum_client.domains), len(self.engine_conf.keys()))

        for engine_id in self.engine_conf:
            domain_id = domain_id_from_engine(engine_id)
            self.assertEqual(self.epum_client.domain_subs[domain_id],
                [(self.service_name, "node_state")])

        self.mm.register_needs()
        for engine_id in self.engine_conf:
            domain_id = domain_id_from_engine(engine_id)
            engine = self.engine_conf[engine_id]
            if engine.get('spare_slots', 0) == 0:
                self.assert_one_reconfigure(domain_id, 0, [])
        self.epum_client.clear()

        engine1_domain_id = domain_id_from_engine("engine1")
        engine2_domain_id = domain_id_from_engine("engine2")
        engine3_domain_id = domain_id_from_engine("engine3")
        engine4_domain_id = domain_id_from_engine("engine4")

        # engine1 has 1 slot and 1 replica per node, expect a VM per process
        engine1_procs = self.enqueue_n_processes(10, "engine1")

        # engine2 has 2 slots and 1 replica per node, expect a VM per 2 processes
        engine2_procs = self.enqueue_n_processes(10, "engine2")

        # engine3 has 2 slots and 2 replicas per node, expect a VM per 4 processes
        engine3_procs = self.enqueue_n_processes(10, "engine3")

        # engine4 has 2 slots and 1 replica per node, and a
        # minimum of 1 free slot, expect a VM per process + 1
        engine4_procs = self.enqueue_n_processes(10, "engine4")

        self.mm.register_needs()
        self.assert_one_reconfigure(engine1_domain_id, 10, [])
        self.assert_one_reconfigure(engine2_domain_id, 5, [])
        self.assert_one_reconfigure(engine3_domain_id, 3, [])
        self.assert_one_reconfigure(engine4_domain_id, 11, [])
        self.epum_client.clear()

        # now add some resources with assigned processes
        # and removed queued processes. need shouldn't change.
        engine1_resources = self.create_engine_resources("engine1",
            node_count=10, assignments=engine1_procs)
        self.assertEqual(len(engine1_resources), 10)
        engine2_resources = self.create_engine_resources("engine2",
            node_count=5, assignments=engine2_procs)
        self.assertEqual(len(engine2_resources), 5)
        engine3_resources = self.create_engine_resources("engine3",
            node_count=3, assignments=engine3_procs)
        self.assertEqual(len(engine3_resources), 6)
        engine4_resources = self.create_engine_resources("engine4",
            node_count=11, assignments=engine4_procs)
        self.assertEqual(len(engine4_resources), 11)
        self.mm.queued_processes = []

        self.mm.register_needs()
        self.assertFalse(self.epum_client.reconfigures)

        # now try scale down

        # empty 2 resources from engine1. 2 nodes should be terminated.
        engine1_retirees = set()
        for resource in engine1_resources[:2]:
            engine1_retirees.add(resource.node_id)
            resource.assigned = []

        # empty 2 resources from engine2. 2 nodes should be terminated
        engine2_retirees = set()
        for resource in engine2_resources[:2]:
            engine2_retirees.add(resource.node_id)
            resource.assigned = []

        # empty 3 resources from engine3.  1 node should be terminated
        for resource in engine3_resources[:3]:
            resource.assigned = []
        engine3_retirees = set([engine3_resources[0].node_id])

        # empty 2 resources from engine4.  2 nodes should be terminated
        engine4_retirees = set()
        for resource in engine4_resources:
            if len(resource.assigned) > 0:
                engine4_retirees.add(resource.node_id)
                resource.assigned = []

            if len(engine4_retirees) >= 2:
                break

        self.mm.register_needs()
        self.assert_one_reconfigure(engine1_domain_id, 8,
            engine1_retirees)
        self.assert_one_reconfigure(engine2_domain_id, 3,
            engine2_retirees)
        self.assert_one_reconfigure(engine3_domain_id, 2,
            engine3_retirees)
        # Note that we cannot check which nodes have retired, since the spare
        # one may be terminated
        self.assert_one_reconfigure(engine4_domain_id, 9)
        self.epum_client.clear()

    def test_needs_maximum_vms(self):
        self.mm.initialize()

        self.assertFalse(self.epum_client.reconfigures)
        self.assertEqual(len(self.epum_client.domains), len(self.engine_conf.keys()))

        for engine_id in self.engine_conf:
            domain_id = domain_id_from_engine(engine_id)
            self.assertEqual(self.epum_client.domain_subs[domain_id],
                [(self.service_name, "node_state")])

        self.mm.register_needs()
        for engine_id in self.engine_conf:
            domain_id = domain_id_from_engine(engine_id)
            engine = self.engine_conf[engine_id]
            if engine.get('spare_slots', 0) == 0:
                self.assert_one_reconfigure(domain_id, 0, [])
        self.epum_client.clear()

        engine1_domain_id = domain_id_from_engine("engine1")
        engine2_domain_id = domain_id_from_engine("engine2")

        # engine1 has 1 slot and 1 replica per node, expect a VM per process
        self.enqueue_n_processes(10, "engine1")

        # engine2 has 2 slots and 1 replica per node, expect a VM per 2 processes
        self.enqueue_n_processes(10, "engine2")

        self.mm.register_needs()
        self.assert_one_reconfigure(engine1_domain_id, 10, [])
        self.assert_one_reconfigure(engine2_domain_id, 5, [])
        self.epum_client.clear()

        # engine1 has 1 slot and 1 replica per node, expect a VM per process, normally
        self.enqueue_n_processes(1000, "engine1")

        # engine2 has 2 slots and 1 replica per node, expect a VM per 2 processes, normally
        self.enqueue_n_processes(1000, "engine2")

        # But, we set a maximum of 100 VMs, so even though we have thousands of processes
        # queued, we only start 100 VMs total
        self.mm.register_needs()
        self.assert_one_reconfigure(engine1_domain_id, 100, [])
        self.assert_one_reconfigure(engine2_domain_id, 100, [])
        self.epum_client.clear()

    def test_needs_unscheduled_pending(self):

        # engine1 has 1 slot, expect a VM per process
        engine1_pending_procs = self.create_n_pending_processes(10, "engine1")

        # engine2 has 2 slots, expect a VM per 2 processes
        engine2_pending_procs = self.create_n_pending_processes(5, "engine2")
        engine2_pending_procs += self.create_n_pending_processes(5, module="my.engine2")

        # Normally this is done by the doctor, but we do it manually here,
        # since there is no doctor in this test env
        self.store.set_initialized()
        self.store.set_pd_state(ProcessDispatcherState.SYSTEM_BOOTING)

        self.mm.initialize()

        self.assertFalse(self.epum_client.reconfigures)
        self.assertEqual(len(self.epum_client.domains), len(self.engine_conf.keys()))

        for engine_id in self.engine_conf:
            domain_id = domain_id_from_engine(engine_id)
            self.assertEqual(self.epum_client.domain_subs[domain_id],
                [(self.service_name, "node_state")])

        engine1_domain_id = domain_id_from_engine("engine1")
        engine2_domain_id = domain_id_from_engine("engine2")

        self.mm.register_needs()

        # we should see VMs even though we have no queued procs
        self.assert_one_reconfigure(engine1_domain_id, 10, [])
        self.assert_one_reconfigure(engine2_domain_id, 5, [])

        self.epum_client.clear()

        # engine1 has 1 slot, expect a VM per process
        engine1_queued_procs = self.enqueue_n_processes(10, "engine1")

        # engine2 has 2 slots, expect a VM per 2 processes
        engine2_queued_procs = self.enqueue_n_processes(10, "engine2")

        self.mm.register_needs()

        # we should see for the queued and pending procs
        self.assert_one_reconfigure(engine1_domain_id, 20, [])
        self.assert_one_reconfigure(engine2_domain_id, 10, [])
        self.epum_client.clear()

        # When we enqueue the procs and mark the PD OK
        self.enqueue_pending_processes()
        self.mm.register_needs()

        # The matchmaker won't have checked for the updated pending procs
        self.assertEqual(len(self.mm.unscheduled_pending_processes), 20)

        # But there should be no change to requested VMs, since we should
        # deduplicate processes
        self.assertFalse(self.epum_client.reconfigures)

        self.store.set_pd_state(ProcessDispatcherState.OK)

        self.mm.register_needs()

        # The matchmaker should have no pending processes
        self.assertEqual(len(self.mm.unscheduled_pending_processes), 0)

        # There should be no change to requested VMs
        self.assertFalse(self.epum_client.reconfigures)

        self.epum_client.clear()

        # now add some resources with assigned processes
        # and removed queued processes. need shouldn't change.
        engine1_procs = engine1_queued_procs + engine1_pending_procs
        engine2_procs = engine2_queued_procs + engine2_pending_procs
        engine1_resources = self.create_engine_resources("engine1",
            node_count=20, assignments=engine1_procs)
        self.assertEqual(len(engine1_resources), 20)
        engine2_resources = self.create_engine_resources("engine2",
            node_count=10, assignments=engine2_procs)
        self.assertEqual(len(engine2_resources), 10)
        self.mm.queued_processes = []

        self.mm.register_needs()
        self.assertFalse(self.epum_client.reconfigures)

        # empty resources from engine1. all nodes should be terminated.
        engine1_retirees = set()
        for resource in engine1_resources:
            engine1_retirees.add(resource.node_id)
            resource.assigned = []

        # empty resources from engine2. all nodes should be terminated
        engine2_retirees = set()
        for resource in engine2_resources:
            engine2_retirees.add(resource.node_id)
            resource.assigned = []

        self.mm.register_needs()

        # we should see for the queued and pending procs
        self.assert_one_reconfigure(engine1_domain_id, 0, engine1_retirees)
        self.assert_one_reconfigure(engine2_domain_id, 0, engine2_retirees)
        self.epum_client.clear()

    def test_needs_duplicate_process(self):

        # ensure processes represented in queue and in a resource are not
        # counted twice. This situation arises in between process and resource
        # record updates.
        self.mm.initialize()

        self.assertFalse(self.epum_client.reconfigures)
        self.assertEqual(len(self.epum_client.domains), len(self.engine_conf.keys()))

        for engine_id in self.engine_conf:
            domain_id = domain_id_from_engine(engine_id)
            self.assertEqual(self.epum_client.domain_subs[domain_id],
                [(self.service_name, "node_state")])

        self.mm.register_needs()
        for engine_id in self.engine_conf:
            domain_id = domain_id_from_engine(engine_id)
            engine = self.engine_conf[engine_id]
            if engine.get('spare_slots', 0) == 0:
                self.assert_one_reconfigure(domain_id, 0, [])
        self.epum_client.clear()

        engine1_domain_id = domain_id_from_engine("engine1")

        engine1_procs = self.enqueue_n_processes(10, "engine1")

        one_process_key = engine1_procs[0]
        owner, upid, rround = one_process_key

        self.mm.register_needs()
        self.assert_one_reconfigure(engine1_domain_id, 10, [])
        self.epum_client.clear()

        # now add some resources with assigned processes
        # and removed queued processes. need shouldn't change.
        engine1_resources = self.create_engine_resources("engine1",
            node_count=10, assignments=engine1_procs)
        self.assertEqual(len(engine1_resources), 10)
        self.mm.queued_processes = []

        self.mm.register_needs()
        self.assertFalse(self.epum_client.reconfigures)

        # now pretend one process fails and is requeued
        # the requue can happen before the resource update so we
        # simulate this to ensure that the process isn't counted twice

        self.mm.queued_processes = [(owner, upid, rround + 1)]
        self.mm.register_needs()
        self.assertFalse(self.epum_client.reconfigures)

    @attr('INT')
    def test_engine_types(self):
        self._run_in_thread()

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        constraints = {"engine": "engine2"}
        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints)
        p1key = p1.get_key()
        self.store.add_process(p1)

        self.store.enqueue_process(*p1key)

        # We don't have a resource that can run this yet
        timed_out = False
        try:
            self.wait_resource(r1.resource_id, lambda r: list(p1key) in r.assigned,
                    timeout=2)
        except Exception:
            timed_out = True
        assert timed_out

        props = {"engine": "engine2"}
        r2 = ResourceRecord.new("r2", "n2", 1, properties=props)
        self.store.add_resource(r2)

        self.wait_resource(r2.resource_id, lambda r: list(p1key) in r.assigned)

        time.sleep(0.05)
        self.resource_client.check_process_launched(p1, r2.resource_id)
        self.wait_process(p1.owner, p1.upid,
                          lambda p: p.assigned == r2.resource_id and
                                    p.state == ProcessState.ASSIGNED)

    @attr('INT')
    def test_default_engine_types(self):
        self._run_in_thread()

        props = {"engine": self.registry.default}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)

        self.store.enqueue_process(*p1key)

        self.wait_resource(r1.resource_id, lambda r: list(p1key) in r.assigned)

        time.sleep(0.05)
        self.resource_client.check_process_launched(p1, r1.resource_id)
        self.wait_process(p1.owner, p1.upid,
                          lambda p: p.assigned == r1.resource_id and
                                    p.state == ProcessState.ASSIGNED)

    @attr('INT')
    def test_stale_procs(self):
        """test that the matchmaker doesn't try to schedule stale procs

        A stale proc is one that the matchmaker has attempted to scale before
        while the state have the resources hasn't changed.
        """
        if not os.environ.get('INT'):
            raise SkipTest("Skip slow integration test")

        self.mm.initialize()

        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)
        self.store.enqueue_process(*p1key)

        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        self.assertTrue(self.mm.needs_matchmaking)
        self.mm.matchmake()
        self.assertFalse(self.mm.needs_matchmaking)
        self.assertTrue(len(self.mm.stale_processes) > 0)

        self.mm._get_queued_processes()
        self.mm._get_resource_set()
        self.assertFalse(self.mm.needs_matchmaking)
        self.assertTrue(len(self.mm.stale_processes) > 0)

        p2 = ProcessRecord.new(None, "p2", get_process_definition(),
                               ProcessState.REQUESTED)
        p2key = p2.get_key()
        self.store.add_process(p2)
        self.store.enqueue_process(*p2key)

        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        self.assertTrue(self.mm.needs_matchmaking)
        self.assertTrue(len(self.mm.stale_processes) > 0)
        self.assertTrue(len(self.mm.queued_processes) > len(self.mm.stale_processes))

        self.mm.matchmake()

        self.assertFalse(self.mm.needs_matchmaking)
        self.assertTrue(len(self.mm.queued_processes) == len(self.mm.stale_processes))

        # Add a resource, and ensure that stale procs get dumped
        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        self.mm._get_queued_processes()
        self.mm._get_resources()
        self.mm._get_resource_set()

        self.assertTrue(len(self.mm.stale_processes) == 0)

    @attr('INT')
    def test_stale_optimization(self):
        # DL: not sure this test is really relevant anymore. It often fails
        # against zookeeper because the ratio isn't as good.
        raise SkipTest("Skip manual optimization test")

        from time import clock

        self.mm.initialize()

        n_start_proc = 10000

        for i in range(0, n_start_proc):

            p = ProcessRecord.new(None, "p%s" % i, get_process_definition(),
                                   ProcessState.REQUESTED)
            pkey = p.get_key()
            self.store.add_process(p)
            self.store.enqueue_process(*pkey)

        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        self.assertTrue(self.mm.needs_matchmaking)
        unoptimized_start = clock()
        self.mm.matchmake()
        unoptimized_end = clock()
        unoptimized_time = unoptimized_end - unoptimized_start
        self.assertFalse(self.mm.needs_matchmaking)
        self.assertTrue(len(self.mm.stale_processes) > 0)

        p = ProcessRecord.new(None, "px", get_process_definition(),
                               ProcessState.REQUESTED)
        pkey = p.get_key()
        self.store.add_process(p)
        self.store.enqueue_process(*pkey)

        # sneak into MM and force it to update this info from the store
        self.mm._get_queued_processes()
        self.mm._get_resource_set()

        self.assertTrue(self.mm.needs_matchmaking)
        optimized_start = clock()
        self.mm.matchmake()
        optimized_end = clock()
        optimized_time = optimized_end - optimized_start

        if optimized_time > 0:
            ratio = unoptimized_time / optimized_time
            print "Unoptimised Time: %s Optimised Time: %s ratio: %s" % (
                unoptimized_time, optimized_time, ratio)
            self.assertTrue(ratio >= 100,
                    "Our optimized matchmake didn't have a 100 fold improvement")
        else:
            print "optimized_time was zero. hmm"

        # Add a resource, and ensure that matchmake time is unoptimized
        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.store.add_resource(r1)

        self.mm._get_queued_processes()
        self.mm._get_resources()
        self.mm._get_resource_set()

        self.assertTrue(self.mm.needs_matchmaking)
        addresource_start = clock()
        self.mm.matchmake()
        addresource_end = clock()
        addresource_time = addresource_end - addresource_start

        optimized_addresource_ratio = unoptimized_time / addresource_time
        print "Add resource ratio: %s" % optimized_addresource_ratio
        msg = "After adding a resource, matchmaking should be of the same order"
        self.assertTrue(optimized_addresource_ratio < 10, msg)


class PDMatchmakerZooKeeperTests(PDMatchmakerTests, ZooKeeperTestMixin):
    def setup_store(self):
        self.setup_zookeeper(base_path_prefix="/matchmaker_tests_" + uuid.uuid4().hex)
        store = ProcessDispatcherZooKeeperStore(self.zk_hosts,
            self.zk_base_path, use_gevent=self.use_gevent)
        store.initialize()

        return store

    def teardown_store(self):
        if self.store:
            self.store.shutdown()

        self.teardown_zookeeper()


def get_process_definition(module=None):
    if module is None:
        module = "some.fake.path"
    return {"name": "hats", "executable": {"module": module,
                                           "url": "uri://something",
                                           "class": "SomeFakeClass"}}
