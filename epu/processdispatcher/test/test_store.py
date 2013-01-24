import threading
import unittest
from functools import partial
import time

from epu.exceptions import NotFoundError, WriteConflictError
from epu.processdispatcher.store import ResourceRecord, ProcessDispatcherStore,\
    ProcessDispatcherZooKeeperStore, ProcessDefinitionRecord
from epu.test import ZooKeeperTestMixin


#noinspection PyUnresolvedReferences
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

        self.store.add_definition(d1)
        self.store.add_definition(d2)

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


class ProcessDispatcherZooKeeperStoreTests(ProcessDispatcherStoreTests, ZooKeeperTestMixin):

    def setUp(self):
        self.setup_zookeeper("/processdispatcher_store_tests_")
        self.store = ProcessDispatcherZooKeeperStore(self.zk_hosts,
            self.zk_base_path, use_gevent=self.use_gevent)
        self.store.initialize()

    def tearDown(self):
        self.store.shutdown()
        self.teardown_zookeeper()


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
