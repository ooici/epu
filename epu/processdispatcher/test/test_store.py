import threading
import unittest
from functools import partial
import time

from epu.processdispatcher.store import ResourceRecord, ProcessDispatcherStore

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


class RecordTests(unittest.TestCase):
    def test_resource_record(self):
        r = ResourceRecord.new("r1", "n1", 1)
        self.assertEqual(r.available_slots, 1)
        self.assertEqual(r.properties, {})
        r.assigned.append('proc1')
        self.assertEqual(r.available_slots, 0)

    def test_record_metadata(self):
        r1 = ResourceRecord.new("r1", "n1", 1)
        r1.metadata['version'] = 0

        r2 = ResourceRecord.new("r2", "n1", 1)
        r2.metadata['version'] = 1

        r1_dict_copy = dict(r1)
        r2_dict_copy = dict(r2)


        self.assertEqual(r1.metadata['version'], 0)
        self.assertEqual(r2.metadata['version'], 1)
        self.assertNotIn('metadata', r1_dict_copy)
        self.assertNotIn('metadata', r2_dict_copy)
