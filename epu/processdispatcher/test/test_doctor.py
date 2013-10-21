# Copyright 2013 University of Chicago

import unittest
import logging
import time
import uuid
from datetime import timedelta, datetime

from mock import Mock

import epu.tevent as tevent
from epu.processdispatcher.doctor import PDDoctor, ExecutionResourceMonitor
from epu.processdispatcher.core import ProcessDispatcherCore
from epu.processdispatcher.modes import RestartMode
from epu.processdispatcher.store import ProcessDispatcherStore, ProcessDispatcherZooKeeperStore
from epu.processdispatcher.test.mocks import MockResourceClient, MockNotifier
from epu.processdispatcher.store import ProcessRecord
from epu.processdispatcher.engines import EngineRegistry, domain_id_from_engine
from epu.states import ProcessState, InstanceState, ProcessDispatcherState, ExecutionResourceState
from epu.processdispatcher.test.test_store import StoreTestMixin
from epu.processdispatcher.test.mocks import nosystemrestart_process_config, make_beat
from epu.test import ZooKeeperTestMixin
from epu.test.util import wait
from epu.util import UTC


log = logging.getLogger(__name__)


class PDDoctorTests(unittest.TestCase, StoreTestMixin):

    engine_conf = {'engine1': {'slots': 4, 'heartbeat_period': 5,
                   'heartbeat_warning': 10, 'heartbeat_missing': 20}}

    def setUp(self):
        self.store = self.setup_store()
        self.registry = EngineRegistry.from_config(self.engine_conf)
        self.resource_client = MockResourceClient()
        self.notifier = MockNotifier()
        self.core = ProcessDispatcherCore(self.store, self.registry,
            self.resource_client, self.notifier)
        self.doctor = PDDoctor(self.core, self.store)

        self.docthread = None

        self.monitor = None

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
                configuration=nosystemrestart_process_config(),
                assigned=resource_id,
                restart_mode=RestartMode.ALWAYS)
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
                configuration=nosystemrestart_process_config(),
                assigned=resource_id,
                restart_mode=RestartMode.ABNORMAL)
        self.store.add_process(p4)

        # non-running proceses should also potentially be restarted on boot
        p5 = ProcessRecord.new(None, "proc5", {}, ProcessState.WAITING)
        self.store.add_process(p5)
        self.store.enqueue_process(*p5.key)
        p6 = ProcessRecord.new(None, "proc6", {}, ProcessState.REQUESTED)
        self.store.add_process(p6)

        # not this one, due to RestartMode
        p7 = ProcessRecord.new(None, "proc7", {}, ProcessState.REQUESTED,
            configuration=nosystemrestart_process_config(),
            restart_mode=RestartMode.ALWAYS)
        self.store.add_process(p7)
        self.store.enqueue_process(*p7.key)

        resource = self.store.get_resource(resource_id)
        resource.assigned = [p0.key, p1.key, p2.key, p3.key, p4.key]
        self.store.update_resource(resource)

        restartable_procs = ["proc1", "proc2", "proc5", "proc6"]
        dead_procs = ["proc0", "proc4", "proc7"]

        self._run_in_thread()

        assert self.store.wait_initialized(timeout=10)

        self.assertEqual(len(self.store.get_queued_processes()), 0)
        self.assertEqual(len(self.store.get_node_ids()), 0)
        self.assertEqual(len(self.store.get_resource_ids()), 0)

        for proc in restartable_procs:
            self.assertEqual(self.store.get_process(None, proc).state,
                             ProcessState.UNSCHEDULED_PENDING)
        for proc in dead_procs:
            self.assertEqual(self.store.get_process(None, proc).state,
                             ProcessState.TERMINATED)
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

    def test_monitor_thread(self):
        self._run_in_thread()

        assert self.store.wait_initialized(timeout=10)
        self.assertEqual(self.store.get_pd_state(),
                         ProcessDispatcherState.OK)

        self.assertIsNotNone(self.doctor.monitor)
        monitor_thread = self.doctor.monitor_thread
        self.assertIsNotNone(monitor_thread)
        self.assertTrue(monitor_thread.is_alive())

        # now cancel doctor. monitor should stop too
        self.doctor.cancel()
        wait(lambda: not monitor_thread.is_alive())

    def _setup_resource_monitor(self):
        self.monitor = ExecutionResourceMonitor(self.core, self.store)
        return self.monitor

    def _send_heartbeat(self, resource_id, node_id, timestamp):
        self.core.ee_heartbeat(resource_id, make_beat(node_id, timestamp=timestamp))

    def assert_monitor_cycle(self, expected_delay, resource_states=None):
        self.assertEqual(expected_delay, self.monitor.monitor_cycle())

        if resource_states:
            for resource_id, expected_state in resource_states.iteritems():
                found_state = self.store.get_resource(resource_id).state
                if found_state != expected_state:
                    self.fail("Resource %s state = %s. Expected %s" % (resource_id,
                        found_state, expected_state))

    def test_resource_monitor(self):
        t0 = datetime(2012, 3, 13, 9, 30, 0, tzinfo=UTC)
        mock_now = Mock()
        mock_now.return_value = t0

        def increment_now(seconds):
            t = mock_now.return_value + timedelta(seconds=seconds)
            mock_now.return_value = t
            log.debug("THE TIME IS NOW: %s", t)
            return t

        monitor = self._setup_resource_monitor()
        monitor._now_func = mock_now

        # before there are any resources, monitor should work but return a None delay
        self.assertIsNone(monitor.monitor_cycle())

        self.core.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)

        # 3 resources. all report in at t0
        r1, r2, r3 = "eeagent_1", "eeagent_2", "eeagent_3"
        self._send_heartbeat(r1, "node1", t0)
        self._send_heartbeat(r2, "node1", t0)
        self._send_heartbeat(r3, "node1", t0)

        states = {r1: ExecutionResourceState.OK, r2: ExecutionResourceState.OK,
                  r3: ExecutionResourceState.OK}

        self.assert_monitor_cycle(10, states)

        t1 = increment_now(5)  # :05
        # heartbeat comes in for r1 5 seconds later
        self._send_heartbeat(r1, "node1", t1)

        self.assert_monitor_cycle(5, states)

        increment_now(5)  # :10

        # no heartbeats for r2 and r3. they should be marked WARNING
        states[r2] = ExecutionResourceState.WARNING
        states[r3] = ExecutionResourceState.WARNING
        self.assert_monitor_cycle(5, states)

        increment_now(4)  # :14

        # r2 gets a heartbeat through, but its timestamp puts it still in the warning threshold
        self._send_heartbeat(r2, "node1", t0 + timedelta(seconds=1))

        self.assert_monitor_cycle(1, states)

        increment_now(6)  # :20

        # r1 should go warning, r3 should go missing
        states[r1] = ExecutionResourceState.WARNING
        states[r3] = ExecutionResourceState.MISSING
        self.assert_monitor_cycle(4, states)

        t2 = increment_now(3)  # :23
        self._send_heartbeat(r1, "node1", t2)
        states[r1] = ExecutionResourceState.OK
        self.assert_monitor_cycle(1, states)

        t3 = increment_now(2)  # :25
        self._send_heartbeat(r3, "node1", t3)
        states[r2] = ExecutionResourceState.MISSING
        states[r3] = ExecutionResourceState.OK
        self.assert_monitor_cycle(8, states)

        increment_now(5)  # :30
        # hearbeat r2 enough to go back to WARNING, but still late
        self.core.ee_heartbeat(r2, make_beat("node1", timestamp=t0 + timedelta(seconds=15)))
        self._send_heartbeat(r2, "node1", t0 + timedelta(seconds=15))
        states[r2] = ExecutionResourceState.WARNING
        self.assert_monitor_cycle(3, states)

        t4 = increment_now(5)  # :35
        # disable r2 and heartbeat r1 and r3 (heartbeats arrive late, but that's ok)
        self._send_heartbeat(r1, "node1", t4)
        self._send_heartbeat(r3, "node1", t4)
        self.core.resource_change_state(self.store.get_resource(r2),
            ExecutionResourceState.DISABLED)

        states[r2] = ExecutionResourceState.DISABLED
        self.assert_monitor_cycle(10, states)


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

    def assert_monitor_cycle(self, *args, **kwargs):
        # occasionally ZK doesn't fire the watches quick enough.
        # so we add a little sleep before each cycle
        time.sleep(0.05)
        super(PDDoctorZooKeeperTests, self).assert_monitor_cycle(*args, **kwargs)


class PDDoctorMockTests(unittest.TestCase):

    engine_conf = {'engine1': {'slots': 4, 'heartbeat_period': 5,
                   'heartbeat_warning': 10, 'heartbeat_missing': 20}}

    def setUp(self):
        self.store = Mock()
        self.store.get_pd_state = Mock(return_value=ProcessDispatcherState.OK)
        self.registry = EngineRegistry.from_config(self.engine_conf)
        self.resource_client = MockResourceClient()
        self.notifier = MockNotifier()
        self.core = ProcessDispatcherCore(self.store, self.registry,
            self.resource_client, self.notifier)
        self.doctor = PDDoctor(self.core, self.store)

        self.docthread = None

        self.monitor = None

    def tearDown(self):
        if self.docthread:
            self.doctor.cancel()
            self.docthread.join()
            self.docthread = None

    def test_initialize(self):
        self.doctor.is_leader = True
        self.doctor.config[self.doctor.CONFIG_MONITOR_HEARTBEATS] = False

        def stop_being_leader_in_one_sec():
            time.sleep(1)
            with self.doctor.condition:
                self.doctor.is_leader = False
                self.doctor.condition.notify_all()

        tevent.spawn(stop_being_leader_in_one_sec)
        self.doctor._initialize()
