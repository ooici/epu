import unittest
import uuid

from epu.states import InstanceState, ProcessState
from epu.processdispatcher.core import ProcessDispatcherCore
from epu.processdispatcher.store import ProcessDispatcherStore, ProcessRecord
from epu.processdispatcher.engines import EngineRegistry, domain_id_from_engine
from epu.processdispatcher.test.mocks import MockResourceClient, MockNotifier
from epu.processdispatcher.modes import RestartMode, QueueingMode
from epu.exceptions import NotFoundError, BadRequestError


class ProcessDispatcherCoreTests(unittest.TestCase):

    engine_conf = {'engine1': {'slots': 4},
                   'engine2': {'slots': 4},
                   'engine3': {'slots': 2},
                   'engine4': {'slots': 2}}

    def setUp(self):
        self.store = self.get_store()
        self.registry = EngineRegistry.from_config(self.engine_conf)
        self.resource_client = MockResourceClient()
        self.notifier = MockNotifier()
        self.core = ProcessDispatcherCore(self.store, self.registry,
            self.resource_client, self.notifier)

    def get_store(self):
        return ProcessDispatcherStore()

    def test_add_remove_node(self):
        self.core.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)

        node = self.store.get_node("node1")
        self.assertTrue(node is not None)
        self.assertEqual(node.node_id, "node1")
        self.assertEqual(node.domain_id, domain_id_from_engine("engine1"))

        self.core.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.TERMINATING)
        node = self.store.get_node("node1")
        self.assertTrue(node is None)

        # this shouldn't cause any problems even though node is gone
        self.core.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.TERMINATED)

    def test_add_remove_node_with_resource(self):
        self.core.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)
        resource_id = "eeagent_1"
        self.core.ee_heartbeat(resource_id, make_beat("node1"))

        resource = self.store.get_resource(resource_id)
        self.assertIsNotNone(resource)
        self.assertTrue(resource.enabled)

        # now send a terminated state for the node. resource should be removed.
        self.core.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.TERMINATED)

        self.assertTrue(self.store.get_resource(resource_id) is None)
        self.assertTrue(self.store.get_node("node1") is None)

    def test_add_remove_node_with_resource_and_processes(self):
        self.core.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.RUNNING)
        resource_id = "eeagent_1"
        self.core.ee_heartbeat(resource_id, make_beat("node1"))

        # set up a few of processes on the resource
        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.RUNNING,
                assigned=resource_id)
        self.store.add_process(p1)
        p2 = ProcessRecord.new(None, "proc2", {}, ProcessState.PENDING,
            assigned=resource_id)
        self.store.add_process(p2)
        p3 = ProcessRecord.new(None, "proc3", {}, ProcessState.TERMINATING,
            assigned=resource_id)
        self.store.add_process(p3)

        resource = self.store.get_resource(resource_id)
        resource.assigned = [p1.key, p2.key, p3.key]
        self.store.update_resource(resource)

        # now send a terminated state for the node. resource should be removed.
        self.core.node_state("node1", domain_id_from_engine("engine1"),
            InstanceState.TERMINATED)

        self.assertTrue(self.store.get_resource(resource_id) is None)
        self.assertTrue(self.store.get_node("node1") is None)

        queued_processes = set(self.store.get_queued_processes())

        # these two should have been rescheduled
        for procname in ("proc1", "proc2"):
            proc = self.store.get_process(None, procname)
            self.assertEqual(proc.state, ProcessState.DIED_REQUESTED)
            self.assertEqual(proc.round, 1)
            self.assertIn(proc.key, queued_processes)
            self.notifier.assert_process_state(procname, ProcessState.DIED_REQUESTED)

        # this one should be terminated
        proc3 = self.store.get_process(None, "proc3")
        self.assertEqual(proc3.state, ProcessState.TERMINATED)
        self.assertEqual(proc3.round, 0)
        self.assertNotIn(proc3.key, queued_processes)
        self.notifier.assert_process_state("proc3", ProcessState.TERMINATED)

    def test_terminate_unassigned_process(self):
        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.WAITING)
        self.store.add_process(p1)
        self.store.enqueue_process(*p1.key)

        gotproc = self.core.terminate_process(None, "proc1")

        self.assertEqual(gotproc.upid, "proc1")
        self.assertEqual(gotproc.state, ProcessState.TERMINATED)

        p1 = self.store.get_process(None, "proc1")
        self.assertEqual(p1.state, ProcessState.TERMINATED)

        # should be gone from queue too
        self.assertFalse(self.store.get_queued_processes())

    def test_process_subscribers(self):
        proc = "proc1"
        definition = "def1"
        subscribers = [('destination', 'operation')]
        self.core.create_definition(definition, None, None)
        self.core.create_process(None, proc, definition)
        self.core.schedule_process(None, proc, subscribers=subscribers)

        record = self.store.get_process(None, proc)

        self.assertEqual(len(record.subscribers), len(subscribers))
        for a, b in zip(record.subscribers, subscribers):
            self.assertEqual(a[0], b[0])
            self.assertEqual(a[1], b[1])

    def test_schedule_notfound(self):

        # scheduling an unknown process
        proc = "proc1"
        with self.assertRaises(NotFoundError):
            self.core.schedule_process(None, proc)

    def test_schedule_new_process(self):
        proc = "proc1"
        definition = "def1"
        self.core.create_definition(definition, None, None)

        process = self.core.schedule_process(None, proc, definition)
        self.assertEqual(process.state, ProcessState.REQUESTED)
        self.assertEqual(process.upid, proc)

    def test_create_idempotency(self):
        proc = "proc1"
        definition = "def1"
        another_definition = "def2"
        self.core.create_definition(definition, None, None)
        self.core.create_definition(another_definition, None, None)

        process = self.core.create_process(None, proc, definition)
        self.assertEqual(process.state, ProcessState.UNSCHEDULED)
        self.assertEqual(process.upid, proc)

        # calling again is fine
        process = self.core.create_process(None, proc, definition)
        self.assertEqual(process.state, ProcessState.UNSCHEDULED)
        self.assertEqual(process.upid, proc)

        # with a different definition is not fine
        with self.assertRaises(BadRequestError):
            self.core.create_process(None, proc, another_definition)

        # nor with a different name
        with self.assertRaises(BadRequestError):
            self.core.create_process(None, proc, definition, name="hats")

    def test_schedule_idempotency(self):
        proc = "proc1"
        definition = "def1"

        self.core.create_definition(definition, None, None)

        process = self.core.create_process(None, proc, definition)
        self.assertEqual(process.state, ProcessState.UNSCHEDULED)
        self.assertEqual(process.upid, proc)

        process = self.core.schedule_process(None, proc)
        self.assertEqual(process.state, ProcessState.REQUESTED)
        self.assertEqual(process.upid, proc)

        # calling again is fine
        process = self.core.schedule_process(None, proc)
        self.assertEqual(process.state, ProcessState.REQUESTED)
        self.assertEqual(process.upid, proc)

        # with a different parameter is not fine
        with self.assertRaises(BadRequestError):
            self.core.schedule_process(None, proc,
                restart_mode=RestartMode.ALWAYS)

        with self.assertRaises(BadRequestError):
            self.core.schedule_process(None, proc,
                queueing_mode=QueueingMode.START_ONLY)

    def test_schedule_idempotency_procname(self):
        proc = "proc1"
        definition = "def1"

        self.core.create_definition(definition, None, None)

        # special case: changing process name is ok
        process = self.core.create_process(None, proc, definition,
            name="name1")
        self.assertEqual(process.state, ProcessState.UNSCHEDULED)
        self.assertEqual(process.upid, proc)

        process = self.core.schedule_process(None, proc,
            name="name2")
        self.assertEqual(process.state, ProcessState.REQUESTED)
        self.assertEqual(process.upid, proc)

        # special case: different process name is ok
        process = self.core.schedule_process(None, proc,
            name="name3")
        self.assertEqual(process.state, ProcessState.REQUESTED)
        self.assertEqual(process.upid, proc)

    def test_process_should_restart(self):
        definition = "def1"

        self.core.create_definition(definition, None, None)

        abnormal_states = (ProcessState.TERMINATED, ProcessState.TERMINATING,
            ProcessState.FAILED)
        all_states = (ProcessState.TERMINATED, ProcessState.TERMINATING,
            ProcessState.FAILED, ProcessState.EXITED)
        # default behavior is to restart processes that exit abnormally
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition)
        for state in abnormal_states:
            self.assertTrue(self.core.process_should_restart(process, state))
            # system restart mode doesn't matter
            self.assertTrue(self.core.process_should_restart(process, state,
                is_system_restart=True))
        self.assertFalse(self.core.process_should_restart(process,
            ProcessState.EXITED))
        self.assertFalse(self.core.process_should_restart(process,
            ProcessState.EXITED, is_system_restart=True))

        # same with explicit RestartMode.ABNORMAL specified
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.ABNORMAL)
        for state in abnormal_states:
            self.assertTrue(self.core.process_should_restart(process, state))
            self.assertTrue(self.core.process_should_restart(process, state,
                is_system_restart=True))
        self.assertFalse(self.core.process_should_restart(process,
            ProcessState.EXITED))
        self.assertFalse(self.core.process_should_restart(process,
            ProcessState.EXITED, is_system_restart=True))

        #RestartMode.NEVER
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.NEVER)
        for state in all_states:
            self.assertFalse(self.core.process_should_restart(process, state))
            self.assertFalse(self.core.process_should_restart(process, state,
                is_system_restart=True))

        #RestartMode.ALWAYS
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.ALWAYS)
        for state in all_states:
            self.assertTrue(self.core.process_should_restart(process, state))
            self.assertTrue(self.core.process_should_restart(process, state,
                is_system_restart=True))

        #RestartMode.ALWAYS_EXCEPT_SYSTEM_RESTART
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.ALWAYS_EXCEPT_SYSTEM_RESTART)
        for state in all_states:
            self.assertTrue(self.core.process_should_restart(process, state))
            self.assertFalse(self.core.process_should_restart(process, state,
                is_system_restart=True))

        #RestartMode.ABNORMAL_EXCEPT_SYSTEM_RESTART
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.ABNORMAL_EXCEPT_SYSTEM_RESTART)
        for state in abnormal_states:
            self.assertTrue(self.core.process_should_restart(process, state))
            self.assertFalse(self.core.process_should_restart(process, state,
                is_system_restart=True))
        self.assertFalse(self.core.process_should_restart(process,
            ProcessState.EXITED))
        self.assertFalse(self.core.process_should_restart(process,
            ProcessState.EXITED, is_system_restart=True))


def make_beat(node_id, processes=None):
    return {"node_id": node_id, "processes": processes or []}
