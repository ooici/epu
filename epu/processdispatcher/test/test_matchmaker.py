import os
import unittest
import logging
import threading
import time
import uuid

import epu.tevent as tevent


from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from epu.processdispatcher.matchmaker import PDMatchmaker
from epu.processdispatcher.store import ProcessDispatcherStore, ProcessDispatcherZooKeeperStore
from epu.processdispatcher.test.mocks import MockResourceClient, \
    MockEPUMClient, MockNotifier, get_definition, get_domain_config
from epu.processdispatcher.store import ResourceRecord, ProcessRecord
from epu.processdispatcher.engines import EngineRegistry
from epu.states import ProcessState
from epu.processdispatcher.test.test_store import StoreTestMixin
from epu.test import ZooKeeperTestMixin

log = logging.getLogger(__name__)


class PDMatchmakerTests(unittest.TestCase, StoreTestMixin):

    engine_conf = {
        'engine1': {
            'deployable_type': 'dt1',
            'slots': 1
        },
        'engine2': {
            'deployable_type': 'dt2',
            'slots': 1
        }
    }

    def setUp(self):
        self.store = self.setup_store()
        self.resource_client = MockResourceClient()
        self.epum_client = MockEPUMClient()
        self.registry = EngineRegistry.from_config(self.engine_conf, default='engine1')
        self.notifier = MockNotifier()
        self.service_name = "some_pd"
        self.definition_id = "pd_definition"
        self.definition = get_definition()
        self.base_domain_config = get_domain_config()
        self.run_type = "fake_run_type"

        self.epum_client.add_domain_definition(self.definition_id, self.definition)
        self.mm = PDMatchmaker(self.store, self.resource_client,
            self.registry, self.epum_client, self.notifier, self.service_name,
            self.definition_id, self.base_domain_config, self.run_type)

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
                                    p.state == ProcessState.PENDING)

    def test_node_exclusive(self):
        self._run_in_thread()

        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 2, properties=props)
        self.store.add_resource(r1)

        xattr = "port5000"
        constraints = {}
        p1 = ProcessRecord.new(None, "p1", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints,
                               node_exclusive=xattr)
        p1key = p1.get_key()
        self.store.add_process(p1)
        self.store.enqueue_process(*p1key)

        # The first process should be assigned, since nothing else needs this
        # attr
        self.wait_resource(r1.resource_id, lambda r: list(p1key) in r.assigned)
        time.sleep(0.05)
        self.resource_client.check_process_launched(p1, r1.resource_id)
        self.wait_process(p1.owner, p1.upid,
                          lambda p: p.assigned == r1.resource_id and
                                    p.state == ProcessState.PENDING)

        p2 = ProcessRecord.new(None, "p2", get_process_definition(),
                               ProcessState.REQUESTED, constraints=constraints,
                               node_exclusive=xattr)
        p2key = p2.get_key()
        self.store.add_process(p2)
        self.store.enqueue_process(*p2key)

        # The second process should wait, since first process wants this attr
        # as well
        self.wait_process(p2.owner, p2.upid,
                          lambda p: p.state == ProcessState.WAITING)

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
                      p.state == ProcessState.PENDING)
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

        self.wait_process(None, "p1", lambda p: p.state == ProcessState.PENDING)

        r1 = self.store.get_resource("r1")
        self.assertEqual(len(r1.assigned), 1)
        self.assertTrue(r1.is_assigned(p1.owner, p1.upid, p1.round))
        self.assertEqual(r1.available_slots, 0)

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
        r1.enabled = False

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

            time.sleep(0.05)
            self.assert_one_reconfigure(preserve_n=i + 1, retirees=[])
            self.epum_client.clear()

        # now add 10 resources each with 1 slot. processes should start in order
        for i in range(10):
            props = {"engine": "engine1"}
            res = ResourceRecord.new("res" + str(i), "node" + str(i), 1,
                    properties=props)
            self.store.add_resource(res)

            self.wait_process(None, procnames[i],
                              lambda p: p.state == ProcessState.PENDING and
                                        p.assigned == res.resource_id)

        # finally doublecheck that launch requests happened in order too
        self.assertEqual(self.resource_client.launch_count, 10)
        for i, launch in enumerate(self.resource_client.launches):
            self.assertEqual(launch[0], "res" + str(i))
            self.assertEqual(launch[1], "proc" + str(i))

    def assert_one_reconfigure(self, domain_id=None, preserve_n=None, retirees=None):
        if domain_id is not None:
            reconfigures = self.epum_client.reconfigures[domain_id]
        else:
            reconfigures = self.epum_client.reconfigures.values()[0]
        self.assertEqual(len(reconfigures), 1)
        reconfigure = reconfigures[0]
        engine_conf = reconfigure['engine_conf']
        if preserve_n is not None:
            self.assertEqual(engine_conf['preserve_n'], preserve_n)
        if retirees is not None:
            retirables = engine_conf.get('retirable_nodes', [])
            self.assertEqual(set(retirables), set(retirees))

    def test_needs(self):
        n_processes = 10
        n_engines = len(self.engine_conf.keys())

        self.mm.initialize()

        self.assertFalse(self.epum_client.reconfigures)
        self.assertEqual(len(self.epum_client.domains), n_engines)
        domain_id = self.epum_client.domains.keys()[0]
        self.assertEqual(self.epum_client.domain_subs[domain_id],
            [(self.service_name, "dt_state")])

        self.mm.register_needs()
        self.assert_one_reconfigure(domain_id, 0, [])
        self.epum_client.clear()

        # pretend to queue n_processes
        for pid in range(n_processes):
            p = ProcessRecord.new(None, "%d" % pid, get_process_definition(),
                               ProcessState.REQUESTED)
            pkey = p.get_key()
            self.store.add_process(p)
            self.store.enqueue_process(*pkey)
            self.mm.queued_processes.append(pkey)

        self.mm.register_needs()
        self.assert_one_reconfigure(domain_id, 10, [])
        self.epum_client.clear()
        print self.mm.queued_processes

        # now add some resources with assigned processes
        # and removed queued processes. need shouldn't change.
        for i in range(n_processes):
            props = {"engine": "engine1"}
            res = ResourceRecord.new("res" + str(i), "node" + str(i), 1,
                    properties=props)
            res.metadata['version'] = 0
            res.assigned = [i]
            self.mm.resources[res.resource_id] = res
        self.mm.queued_processes = []

        self.mm.register_needs()
        self.assertFalse(self.epum_client.reconfigures)

        # now try scale down
        n_to_retire = 3
        expected_retired_nodes = set()
        for resource in self.mm.resources.values()[:n_to_retire]:
            expected_retired_nodes.add(resource.node_id)
            resource.assigned = []

        self.mm.register_needs()
        self.assert_one_reconfigure(domain_id, n_processes - n_to_retire,
            expected_retired_nodes)
        self.epum_client.clear()

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
                                    p.state == ProcessState.PENDING)

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
                                    p.state == ProcessState.PENDING)

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
        if not os.environ.get('INT'):
            raise SkipTest("Skip slow integration test")

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



def get_process_definition():
    return {"name": "hats", "executable": {"module": "some.fake.path",
                                           "class": "SomeFakeClass"}}

