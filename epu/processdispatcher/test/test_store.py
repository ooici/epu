# Copyright 2013 University of Chicago

import threading
import unittest
from functools import partial
import time
import random
import logging
import os

from kazoo.exceptions import KazooException

from epu.exceptions import NotFoundError, WriteConflictError
from epu.processdispatcher.store import ResourceRecord, ProcessDispatcherStore,\
    ProcessDispatcherZooKeeperStore, ProcessDefinitionRecord
from epu.test import ZooKeeperTestMixin, MockLeader, SocatProxyRestartWrapper

log = logging.getLogger(__name__)


# noinspection PyUnresolvedReferences
class StoreTestMixin(object):
    def assertRecordVersions(self, first, second):
        self.assertEqual(first.metadata['version'], second.metadata['version'])

    def wait_resource(self, resource_id, pred, timeout=5):
        wait_store(partial(self.store.get_resource, resource_id), pred, timeout)

    def wait_process(self, owner, upid, pred, timeout=5):
        wait_store(partial(self.store.get_process, owner, upid), pred, timeout)


def wait_store(query, pred, timeout=1):
    condition = threading.Condition()

    def watcher(*args):
        with condition:
            condition.notify_all()

    start = time.time()
    with condition:
        while not pred(query(watcher=watcher)):
            if time.time() - start >= timeout:
                raise Exception("timeout")
            condition.wait(timeout)


class ProcessDispatcherStoreTests(unittest.TestCase, StoreTestMixin):

    ZK_HOSTS = "localhost:2181"

    def setUp(self):
        self.store = ProcessDispatcherStore()

    def test_queued_processes(self):

        source = [("u1", "proc1", 0), ("u1", "proc2", 1), ("u2", "proc1", 0),
            ("u2", "proc2", 0), ("u3", "proc3", 3)]

        for key in source:
            self.store.enqueue_process(*key)

        queued = self.store.get_queued_processes()
        self.assertEqual(source, queued)

        toremove = source.pop()
        self.store.remove_queued_process(*toremove)

        queued = self.store.get_queued_processes()
        self.assertEqual(source, queued)

    def assertProcessDefinitionsEqual(self, d1, d2):
        attrs = ('definition_id', 'definition_type', 'executable',
                             'name', 'description', 'version')
        for attr in attrs:
            self.assertEqual(getattr(d1, attr), getattr(d2, attr))

    def test_add_update_remove_definition(self):

        d1 = ProcessDefinitionRecord.new("d1", "t1", "notepad.exe", "proc1")
        d2 = ProcessDefinitionRecord.new("d2", "t2", "cat", "proc2")
        d3 = ProcessDefinitionRecord.new("d3", "t3", "cat" * 2000000, "proc3")

        self.store.add_definition(d1)
        self.store.add_definition(d2)
        if self.__class__ == ProcessDispatcherZooKeeperStoreTests:
            with self.assertRaises(ValueError):
                self.store.add_definition(d3)

        # adding again should get a WriteConflict
        self.assertRaises(WriteConflictError, self.store.add_definition, d1)

        all_ids = self.store.list_definition_ids()
        self.assertEqual(set(all_ids), set(["d1", "d2"]))

        got_d1 = self.store.get_definition("d1")
        self.assertProcessDefinitionsEqual(d1, got_d1)

        got_d2 = self.store.get_definition("d2")
        self.assertProcessDefinitionsEqual(d2, got_d2)

        self.assertIsNone(self.store.get_definition("d3"))

        d1.executable = "ps"
        self.store.update_definition(d1)
        got_d1 = self.store.get_definition("d1")
        self.assertProcessDefinitionsEqual(d1, got_d1)

        self.store.remove_definition("d1")
        self.store.remove_definition("d2")

        self.assertRaises(NotFoundError, self.store.update_definition, d2)

        self.assertFalse(self.store.list_definition_ids())

        self.assertIsNone(self.store.get_definition("d1"))
        self.assertIsNone(self.store.get_definition("neverexisted"))

    def test_not_unicode(self):
        d1 = ProcessDefinitionRecord.new("d1", "t1", "notepad.exe", "proc1")
        self.store.add_definition(d1)
        got_d1 = self.store.get_definition("d1")

        # ensure strings don't come back as unicode
        self.assertIsInstance(got_d1.definition_id, str)
        self.assertIsInstance(got_d1.name, str)


class ProcessDispatcherZooKeeperStoreTests(ProcessDispatcherStoreTests, ZooKeeperTestMixin):

    def setUp(self):
        self.setup_zookeeper("/processdispatcher_store_tests_")
        self.store = ProcessDispatcherZooKeeperStore(self.zk_hosts,
            self.zk_base_path, use_gevent=self.use_gevent)
        self.store.initialize()

    def tearDown(self):
        self.store.shutdown()
        self.teardown_zookeeper()


class ProcessDispatcherZooKeeperStoreProxyTests(ProcessDispatcherStoreTests, ZooKeeperTestMixin):

    def setUp(self):
        self.setup_zookeeper("/processdispatcher_store_tests_", use_proxy=True)
        self.store = ProcessDispatcherZooKeeperStore(self.zk_hosts,
            self.zk_base_path, use_gevent=self.use_gevent, timeout=2.0)
        self.store.initialize()

    def tearDown(self):
        if not self.proxy.running:
            self.proxy.start()
        self.store.shutdown()
        self.teardown_zookeeper()

    def test_elections_connection(self):

        matchmaker = MockLeader()
        doctor = MockLeader()
        self.store.contend_matchmaker(matchmaker)
        self.store.contend_doctor(doctor)

        matchmaker.wait_running()
        doctor.wait_running()

        # now kill the connection
        self.proxy.stop()
        matchmaker.wait_cancelled(5)
        doctor.wait_cancelled(5)

        # wait for session to expire
        time.sleep(3)

        # start connection back up. leaders should resume. eventually.
        self.proxy.start()

        matchmaker.wait_running(60)
        doctor.wait_running(60)

    def test_elections_under_siege(self, coups=10):
        # repeatedly kill and restart ZK connections with varying delays.
        # make sure we come out of it with leaders in the end.

        if not os.environ.get('NIGHTLYINT'):
            raise unittest.SkipTest("Slow integration test")

        matchmaker = MockLeader()
        doctor = MockLeader()
        self.store.contend_matchmaker(matchmaker)
        self.store.contend_doctor(doctor)

        for i in range(coups):
            sleep_time = random.uniform(0.0, 4.0)
            log.debug("Enjoying %s seconds of peace", sleep_time)
            time.sleep(sleep_time)

            self.proxy.stop()

            sleep_time = random.uniform(0.0, 5.0)
            log.debug("Enduring %s seconds of anarchy", sleep_time)
            time.sleep(sleep_time)

            self.proxy.start()

        # ensure leaders eventually recover
        matchmaker.wait_running(60)
        doctor.wait_running(60)


class ProcessDispatcherZooKeeperStoreProxyKillsTests(ProcessDispatcherStoreTests, ZooKeeperTestMixin):

    # this runs all of the ProcessDispatcherStoreTests tests plus any
    # ZK-specific ones, but uses a proxy in front of ZK and restarts
    # the proxy before each call to the store. The effect is that for each store
    # operation, the first call to kazoo fails with a connection error, but the
    # client should handle that and retry

    def setUp(self):
        self.setup_zookeeper(base_path_prefix="/processdispatcher_store_tests_", use_proxy=True)
        self.real_store = ProcessDispatcherZooKeeperStore(self.zk_hosts,
            self.zk_base_path, use_gevent=self.use_gevent)

        self.real_store.initialize()

        # have the tests use a wrapped store that restarts the connection before each call
        self.store = SocatProxyRestartWrapper(self.proxy, self.real_store)

    def tearDown(self):
        self.teardown_zookeeper()

    def test_the_fixture(self):
        # make sure test fixture actually works like we think

        def fake_operation():
            self.store.kazoo.get("/")
        self.real_store.fake_operation = fake_operation

        self.assertRaises(KazooException, self.store.fake_operation)


class RecordTests(unittest.TestCase):
    def test_resource_record(self):
        props = {"engine": "engine1", "resource_id": "r1"}
        r = ResourceRecord.new("r1", "n1", 1, properties=props)
        self.assertEqual(r.available_slots, 1)
        self.assertEqual(r.properties, props)
        r.assigned.append('proc1')
        self.assertEqual(r.available_slots, 0)

    def test_record_metadata(self):
        props = {"engine": "engine1"}
        r1 = ResourceRecord.new("r1", "n1", 1, properties=props)
        r1.metadata['version'] = 0

        r2 = ResourceRecord.new("r2", "n1", 1, properties=props)
        r2.metadata['version'] = 1

        r1_dict_copy = dict(r1)
        r2_dict_copy = dict(r2)

        self.assertEqual(r1.metadata['version'], 0)
        self.assertEqual(r2.metadata['version'], 1)
        self.assertNotIn('metadata', r1_dict_copy)
        self.assertNotIn('metadata', r2_dict_copy)
