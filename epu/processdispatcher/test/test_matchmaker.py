import unittest
import logging

import gevent
import gevent.thread

from epu.processdispatcher.matchmaker import PDMatchmaker
from epu.processdispatcher.store import ProcessDispatcherStore
from epu.processdispatcher.test.mocks import MockResourceClient
from epu.processdispatcher.store import ResourceRecord, ProcessRecord
from epu.states import ProcessState
from epu.processdispatcher.test.test_store import StoreTestMixin

log = logging.getLogger(__name__)

class PDMatchmakerTests(unittest.TestCase, StoreTestMixin):
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

    def test_match_writeconflict(self):
        self.mm.initialize()
        r1 = ResourceRecord.new("r1", "n1", 1)
        self.store.add_resource(r1)

        p1 = ProcessRecord.new(None, "p1", get_process_spec(),
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
        self.assertTrue(self.mm.needs_matchmaking)
        self.mm.matchmake()
        self.assertTrue(self.mm.needs_matchmaking)

        r1copy = self.store.get_resource(r1.resource_id)
        self.assertRecordVersions(r1, r1copy)

    def test_match1(self):
        self._run_in_thread()

        r1 = ResourceRecord.new("r1", "n1", 1)
        self.store.add_resource(r1)

        p1 = ProcessRecord.new(None, "p1", get_process_spec(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)

        self.store.enqueue_process(*p1key)

        self.wait_resource(r1.resource_id, lambda r: list(p1key) in r.assigned)
        self.resource_client.check_process_launched(p1, r1.resource_id)
        self.wait_process(p1.owner, p1.upid,
                          lambda p: p.assigned == r1.resource_id and
                                    p.state == ProcessState.PENDING)

    def test_waiting(self):
        self._run_in_thread()

        # not-immediate process enqueued while there are no resources

        p1 = ProcessRecord.new(None, "p1", get_process_spec(),
                               ProcessState.REQUESTED)
        p1key = p1.get_key()
        self.store.add_process(p1)

        self.store.enqueue_process(*p1key)
        self.wait_process(None, "p1", lambda p: p.state == ProcessState.WAITING)

        # now give it a resource. it should be scheduled
        r1 = ResourceRecord.new("r1", "n1", 1)
        self.store.add_resource(r1)

        self.wait_resource(r1.resource_id, lambda r: list(p1key) in r.assigned)
        self.resource_client.check_process_launched(p1, r1.resource_id)

    def test_immediate(self):
        self._run_in_thread()

        p1 = ProcessRecord.new(None, "p1", get_process_spec(),
                               ProcessState.REQUESTED, immediate=True)
        p1key = p1.get_key()
        self.store.add_process(p1)

        self.store.enqueue_process(*p1key)
        self.wait_process(None, "p1", lambda p: p.state == ProcessState.REJECTED)

        # process should be removed from queue
        self.assertFalse(self.store.get_queued_processes())

    def test_wait_resource(self):
        r1 = ResourceRecord.new("r1", "n1", 1)
        self.store.add_resource(r1)
        self.wait_resource("r1", lambda r: r.resource_id == "r1")

        def makeitso():
            r1.slot_count = 2
            self.store.update_resource(r1)

        gevent.spawn_later(0, makeitso)
        self.wait_resource("r1", lambda r: r.slot_count == 2)

    def test_disabled_resource(self):
        self._run_in_thread()

        r1 = ResourceRecord.new("r1", "n1", 1)
        r1.enabled = False

        self.store.add_resource(r1)
        self.wait_resource("r1", lambda r: r.resource_id == "r1")

        p1 = ProcessRecord.new(None, "p1", get_process_spec(),
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
            proc = ProcessRecord.new(None, "proc"+str(i), get_process_spec(),
                                     ProcessState.REQUESTED)
            prockey = proc.key
            self.store.add_process(proc)
            self.store.enqueue_process(*prockey)

            self.wait_process(proc.owner, proc.upid,
                              lambda p: p.state == ProcessState.WAITING)
            procnames.append(proc.upid)

        # now add 10 resources each with 1 slot. processes should start in order
        for i in range(10):
            res = ResourceRecord.new("res"+str(i), "node"+str(i), 1)
            self.store.add_resource(res)

            self.wait_process(None, procnames[i],
                              lambda p: p.state == ProcessState.PENDING and
                                        p.assigned == res.resource_id)

        # finally doublecheck that launch requests happened in order too
        self.assertEqual(self.resource_client.launch_count, 10)
        for i, launch in enumerate(self.resource_client.launches):
            self.assertEqual(launch[0], "res"+str(i))
            self.assertEqual(launch[1], "proc"+str(i))


def get_process_spec():
    return {"run_type":"hats", "parameters": {}}