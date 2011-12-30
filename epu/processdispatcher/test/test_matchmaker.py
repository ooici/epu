import unittest
import logging
import threading

import gevent
from gevent.event import Event

from epu.processdispatcher.matchmaker import PDMatchmaker
from epu.processdispatcher.store import ProcessDispatcherStore
from epu.processdispatcher.test.mocks import MockResourceClient
from epu.processdispatcher.store import ResourceRecord, ProcessRecord
from epu.states import ProcessState

log = logging.getLogger(__name__)

class PDMatchmakerTests(unittest.TestCase):

    def setUp(self):
        self.store = ProcessDispatcherStore()
        self.resource_client = MockResourceClient()
        self.mm = PDMatchmaker(self.store, self.resource_client)

        self.mmthread = None

    def tearDown(self):
        if self.mmthread:
            self.mm.cancel()
            self.mmthread.join()
            self.mmthread = None

    def _run_in_thread(self):
        self.mm.initialize()

        self.mmthread = gevent.spawn(self.mm.run)

    def test_run_cancel(self):
        self._run_in_thread()

        self.mm.cancel()
        self.mmthread.join()
        self.mmthread = None

    def test_match1(self):
        self._run_in_thread()

        r1 = ResourceRecord.new("r1", "n1", 1)
        self.store.add_resource(r1)

        p1 = ProcessRecord.new(None, "p1", get_process_spec(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)

        self.store.enqueue_process(*p1key)

        event = Event()
        def watcher(resource_id):
            assert resource_id == r1.resource_id
            event.set()

        gotresource = self.store.get_resource(r1.resource_id, watcher=watcher)
        if not p1key in gotresource.assigned:
            event.wait(1)
            gotresource = self.store.get_resource(r1.resource_id, watcher=watcher)
            self.assertIn(list(p1key), gotresource.assigned)

def get_process_spec():
    return {"run_type":"hats", "parameters": {}}