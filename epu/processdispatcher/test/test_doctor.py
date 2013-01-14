import unittest
import logging
import time
import uuid

import epu.tevent as tevent

from epu.processdispatcher.doctor import PDDoctor
from epu.processdispatcher.core import ProcessDispatcherCore
from epu.processdispatcher.modes import RestartMode
from epu.processdispatcher.store import ProcessDispatcherStore, ProcessDispatcherZooKeeperStore
from epu.processdispatcher.test.mocks import MockResourceClient, MockNotifier
from epu.processdispatcher.store import ProcessRecord
from epu.processdispatcher.engines import EngineRegistry, domain_id_from_engine
from epu.states import ProcessState, InstanceState, ProcessDispatcherState
from epu.processdispatcher.test.test_store import StoreTestMixin
from epu.test import ZooKeeperTestMixin
from epu.test.util import wait

log = logging.getLogger(__name__)


class PDDoctorTests(unittest.TestCase, StoreTestMixin):

    engine_conf = {'engine1': {'slots': 4}}

    def setUp(self):
        self.store = self.setup_store()
        self.registry = EngineRegistry.from_config(self.engine_conf)
        self.resource_client = MockResourceClient()
        self.notifier = MockNotifier()
        self.core = ProcessDispatcherCore(self.store, self.registry,
            self.resource_client, self.notifier)
        self.doctor = PDDoctor(self.core, self.store)

        self.docthread = None

    def tearDown(self):
        if self.docthread:
            self.doctor.cancel()
            self.docthread.join()
            self.docthread = None
        self.teardown_store()

    def setup_store(self):
        return ProcessDispatcherStore()

    def teardown_store(self):
        return

    def _run_in_thread(self):
        self.docthread = tevent.spawn(self.doctor.inaugurate)
        time.sleep(0.05)

    def test_uninitialized_system_boot_with_state(self):
        self.store.set_system_boot(True)
        self.core.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)
        resource_id = "eeagent_1"
        self.core.ee_heartbeat(resource_id, make_beat("node1"))

        p0 = ProcessRecord.new(None, "proc0", {}, ProcessState.RUNNING,
                assigned=resource_id,
                restart_mode=RestartMode.ALWAYS_EXCEPT_SYSTEM_RESTART)
        self.store.add_process(p0)
        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.RUNNING,
                assigned=resource_id)
        self.store.add_process(p1)
        p2 = ProcessRecord.new(None, "proc2", {}, ProcessState.PENDING,
            assigned=resource_id)
        self.store.add_process(p2)
        p3 = ProcessRecord.new(None, "proc3", {}, ProcessState.TERMINATING,
            assigned=resource_id)
        self.store.add_process(p3)

        # this one shouldn't restart
        p4 = ProcessRecord.new(None, "proc4", {}, ProcessState.RUNNING,
                assigned=resource_id,
                restart_mode=RestartMode.ABNORMAL_EXCEPT_SYSTEM_RESTART)
        self.store.add_process(p4)

        # non-running proceses should also potentially be restarted on boot
        p5 = ProcessRecord.new(None, "proc5", {}, ProcessState.WAITING)
        self.store.add_process(p5)
        self.store.enqueue_process(*p5.key)
        p6 = ProcessRecord.new(None, "proc6", {}, ProcessState.REQUESTED)
        self.store.add_process(p6)

        #not this one, due to RestartMode
        p7 = ProcessRecord.new(None, "proc7", {}, ProcessState.REQUESTED,
            restart_mode=RestartMode.ALWAYS_EXCEPT_SYSTEM_RESTART)
        self.store.add_process(p7)
        self.store.enqueue_process(*p7.key)

        resource = self.store.get_resource(resource_id)
        resource.assigned = [p0.key, p1.key, p2.key, p3.key, p4.key]
        self.store.update_resource(resource)

        restartable_procs = ["proc1", "proc2", "proc5", "proc6"]
        failed_procs = ["proc0", "proc4", "proc7"]

        self._run_in_thread()

        assert self.store.wait_initialized(timeout=10)

        self.assertEqual(len(self.store.get_queued_processes()), 0)
        self.assertEqual(len(self.store.get_node_ids()), 0)
        self.assertEqual(len(self.store.get_resource_ids()), 0)

        for proc in restartable_procs:
            self.assertEqual(self.store.get_process(None, proc).state,
                             ProcessState.UNSCHEDULED_PENDING)
        for proc in failed_procs:
            self.assertEqual(self.store.get_process(None, proc).state,
                             ProcessState.FAILED)
        self.assertEqual(self.store.get_process(None, "proc3").state,
                         ProcessState.TERMINATED)

        self.assertEqual(self.store.get_pd_state(),
                         ProcessDispatcherState.SYSTEM_BOOTING)

        # now end system boot
        self.store.set_system_boot(False)

        wait(lambda: self.store.get_pd_state() == ProcessDispatcherState.OK)

        # check that pending processes were correctly rescheduled
        self.assertEqual(len(self.store.get_queued_processes()), len(restartable_procs))
        for proc in restartable_procs:
            self.assertEqual(self.store.get_process(None, proc).state,
                             ProcessState.REQUESTED)

    def test_uninitialized_system_boot_without_state(self):
        self.store.set_system_boot(True)
        self._run_in_thread()

        assert self.store.wait_initialized(timeout=10)
        self.assertEqual(self.store.get_pd_state(),
                         ProcessDispatcherState.SYSTEM_BOOTING)
        self.store.set_system_boot(False)
        wait(lambda: self.store.get_pd_state() == ProcessDispatcherState.OK)

    def test_uninitialized_not_system_boot_with_procs(self):
        # tests the case where doctor arrives to an uninitialized system
        # that is not doing a system boot. HOWEVER, there are procs in the
        # UNSCHEDULED_PENDING state. This would likely only happen if the
        # PD died during system boot and recovered after the system boot flag
        # was turned off. Very small window, but possible.

        p0 = ProcessRecord.new(None, "proc0", {}, ProcessState.UNSCHEDULED_PENDING)
        self.store.add_process(p0)
        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.UNSCHEDULED_PENDING)
        self.store.add_process(p1)
        p2 = ProcessRecord.new(None, "proc2", {}, ProcessState.UNSCHEDULED_PENDING)
        self.store.add_process(p2)

        restartable_procs = ["proc0", "proc1", "proc2"]
        self._run_in_thread()

        assert self.store.wait_initialized(timeout=10)
        self.assertEqual(self.store.get_pd_state(),
                         ProcessDispatcherState.OK)

        # check that pending processes were correctly rescheduled
        self.assertEqual(len(self.store.get_queued_processes()), len(restartable_procs))
        for proc in restartable_procs:
            self.assertEqual(self.store.get_process(None, proc).state,
                             ProcessState.REQUESTED)

    def test_uninitialized_not_system_boot_without_procs(self):
        # tests the case where doctor arrives to an uninitialized system
        # that is not doing a system boot and has no UNSCHEDULED_PENDING procs.
        # this is likely a recovery from all-PD-workers failing and resuming in
        # a running system, or Zookeeper issue
        self._run_in_thread()

        assert self.store.wait_initialized(timeout=10)
        self.assertEqual(self.store.get_pd_state(),
                         ProcessDispatcherState.OK)

    def test_initialized_system_boot_with_procs(self):
        # tests the case where just the doctor dies in the middle of system boot
        # but after a doctor has already declared the system initialized. In this
        # case we have processes in the UNSCHEDULED_PENDING state that should be
        # rescheduled once system boot ends.

        self.store.set_system_boot(True)
        self.store.set_initialized()
        self.store.set_pd_state(ProcessDispatcherState.SYSTEM_BOOTING)

        p0 = ProcessRecord.new(None, "proc0", {}, ProcessState.UNSCHEDULED_PENDING)
        self.store.add_process(p0)
        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.UNSCHEDULED_PENDING)
        self.store.add_process(p1)
        p2 = ProcessRecord.new(None, "proc2", {}, ProcessState.UNSCHEDULED_PENDING)
        self.store.add_process(p2)

        restartable_procs = ["proc0", "proc1", "proc2"]
        self._run_in_thread()

        # now end system boot
        self.store.set_system_boot(False)

        wait(lambda: self.store.get_pd_state() == ProcessDispatcherState.OK)

        # check that pending processes were correctly rescheduled
        self.assertEqual(len(self.store.get_queued_processes()), len(restartable_procs))
        for proc in restartable_procs:
            self.assertEqual(self.store.get_process(None, proc).state,
                             ProcessState.REQUESTED)

    def test_initialized_system_boot_without_procs(self):
        # tests the case where just the doctor dies in the middle of system boot
        # but after a doctor has already declared the system initialized. In this
        # case we have no processes to schedule on system boot completion.

        self.store.set_system_boot(True)
        self.store.set_initialized()
        self.store.set_pd_state(ProcessDispatcherState.SYSTEM_BOOTING)

        self._run_in_thread()

        # now end system boot
        self.store.set_system_boot(False)

        wait(lambda: self.store.get_pd_state() == ProcessDispatcherState.OK)

    def test_initialized_not_system_boot(self):
        # recover into an already initialized and booted system. this is likely
        # a recovery from a doctor failure while the rest of the system was still
        # alive.
        self.store.set_initialized()
        self.store.set_pd_state(ProcessDispatcherState.OK)

        self._run_in_thread()

        # we have nothing really to check here, yet. but at least we can make sure
        # the process is cancellable.


class PDDoctorZooKeeperTests(PDDoctorTests, ZooKeeperTestMixin):
    def setup_store(self):
        self.setup_zookeeper(base_path_prefix="/doctor_tests_" + uuid.uuid4().hex)
        store = ProcessDispatcherZooKeeperStore(self.zk_hosts,
            self.zk_base_path, use_gevent=self.use_gevent)
        store.initialize()

        return store

    def teardown_store(self):
        if self.store:
            self.store.shutdown()

        self.teardown_zookeeper()


def make_beat(node_id, processes=None):
    return {"node_id": node_id, "processes": processes or []}
