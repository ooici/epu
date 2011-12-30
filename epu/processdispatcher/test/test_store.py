import threading
import unittest
from functools import partial
import time

from epu.processdispatcher.store import ResourceRecord

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


class RecordTests(unittest.TestCase):
    def test_resource_record(self):
        r = ResourceRecord.new("r1", "n1", 1)
        self.assertEqual(r.available_slots, 1)
        self.assertEqual(r.properties, {})
        r.assigned.append('proc1')
        self.assertEqual(r.available_slots, 0)
