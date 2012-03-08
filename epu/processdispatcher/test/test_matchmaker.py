import unittest
import logging
import threading

import gevent
import gevent.thread

from epu.processdispatcher.matchmaker import PDMatchmaker
from epu.processdispatcher.store import ProcessDispatcherStore
from epu.processdispatcher.test.mocks import MockResourceClient, MockEPUMClient
from epu.processdispatcher.store import ResourceRecord, ProcessRecord
from epu.processdispatcher.engines import EngineRegistry
from epu.states import ProcessState
from epu.processdispatcher.test.test_store import StoreTestMixin

log = logging.getLogger(__name__)

class PDMatchmakerTests(unittest.TestCase, StoreTestMixin):

    engine_conf = {'engine1' : {'deployable_type' : 'dt1', 'slots' : 1}}

    def setUp(self):
        self.store = ProcessDispatcherStore()
        self.resource_client = MockResourceClient()
        self.epum_client = MockEPUMClient()
        self.registry = EngineRegistry.from_config(self.engine_conf)

        self.mm = PDMatchmaker(self.store, self.resource_client,
            self.registry, self.epum_client)

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
        # or registering any need
        self.assertTrue(self.mm.needs_matchmaking)
        self.mm.matchmake()
        self.assertFalse(self.epum_client.needs)
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

        self.assertEqual(len(self.epum_client.needs), 1)

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

        p1 = ProcessRecord.new(None, "p1", get_process_spec(),
            ProcessState.REQUESTED)
        self.store.add_process(p1)
        self.store.enqueue_process(*p1.key)

        # now give it a resource. it should be matched but in the meantime
        # the process will be terminated
        r1 = ResourceRecord.new("r1", "n1", 1)
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

        p1 = ProcessRecord.new(None, "p1", get_process_spec(),
            ProcessState.REQUESTED)
        self.store.add_process(p1)
        self.store.enqueue_process(*p1.key)

        r1 = ResourceRecord.new("r1", "n1", 1)
        r1.assigned.append(p1.key)
        self.store.add_resource(r1)

        self._run_in_thread()

        self.wait_process(None, "p1", lambda p: p.state == ProcessState.PENDING)

        r1 = self.store.get_resource("r1")
        self.assertEqual(len(r1.assigned), 1)
        self.assertTrue(r1.is_assigned(p1.owner, p1.upid, p1.round))
        self.assertEqual(r1.available_slots, 0)

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

        # ensure we can still matchmake another process.
        # encountered a bug where rejecting a process left the
        # matchmaker in a broken state.

        p2 = ProcessRecord.new(None, "p2", get_process_spec(),
            ProcessState.REQUESTED)
        self.store.add_process(p2)
        self.store.enqueue_process(*p2.get_key())
        self.wait_process(None, "p2", lambda p: p.state == ProcessState.WAITING)

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

            self.assertEqual(len(self.epum_client.needs), i+1)
            dt, constraints, count = self.epum_client.needs[-1]
            self.assertEqual(dt, 'dt1')
            self.assertEqual(count, i+1)

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