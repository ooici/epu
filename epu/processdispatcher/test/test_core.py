# Copyright 2013 University of Chicago

import unittest
import uuid

from mock import Mock

from epu.states import InstanceState, ProcessState, ExecutionResourceState
from epu.processdispatcher.core import ProcessDispatcherCore
from epu.processdispatcher.store import ProcessDispatcherStore, ProcessRecord
from epu.processdispatcher.engines import EngineRegistry, domain_id_from_engine
from epu.processdispatcher.test.mocks import nosystemrestart_process_config, \
    MockNotifier, make_beat
from epu.processdispatcher.modes import RestartMode, QueueingMode
from epu.exceptions import NotFoundError, BadRequestError
from epu.util import parse_datetime


class ProcessDispatcherCoreTests(unittest.TestCase):

    engine_conf = {'engine1': {'slots': 4},
                   'engine2': {'slots': 4},
                   'engine3': {'slots': 2},
                   'engine4': {'slots': 2}}

    def setUp(self):
        self.store = self.get_store()
        self.registry = EngineRegistry.from_config(self.engine_conf)
        self.resource_client = Mock()
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
        self.assertEqual(resource.state, ExecutionResourceState.OK)

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

    def test_terminate_not_found(self):
        # process which doesn't exist

        with self.assertRaises(NotFoundError):
            self.core.terminate_process(None, 'notarealprocess')

    def test_terminate_terminal_process(self):
        # processes which are already in a terminal state shouldn't change
        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.UNSCHEDULED)
        p2 = ProcessRecord.new(None, "proc2", {}, ProcessState.UNSCHEDULED_PENDING)
        p3 = ProcessRecord.new(None, "proc3", {}, ProcessState.TERMINATED)
        p4 = ProcessRecord.new(None, "proc4", {}, ProcessState.EXITED)
        p5 = ProcessRecord.new(None, "proc5", {}, ProcessState.FAILED)
        p6 = ProcessRecord.new(None, "proc6", {}, ProcessState.REJECTED)

        for p in (p1, p2, p3, p4, p5, p6):
            self.store.add_process(p)

        for p in (p1, p2, p3, p4, p5, p6):
            gotproc = self.core.terminate_process(None, p.upid)

            self.assertEqual(gotproc.upid, p.upid)
            self.assertEqual(gotproc.state, p.state)

            p1 = self.store.get_process(None, p.upid)
            self.assertEqual(p1.state, p.state)
        self.assertEqual(self.resource_client.call_count, 0)

    def test_terminate_unassigned_process(self):
        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.WAITING)
        self.store.add_process(p1)
        self.store.enqueue_process(*p1.key)

        gotproc = self.core.terminate_process(None, "proc1")

        self.assertEqual(gotproc.upid, "proc1")
        self.assertEqual(gotproc.state, ProcessState.TERMINATED)

        p1 = self.store.get_process(None, "proc1")
        self.assertEqual(p1.state, ProcessState.TERMINATED)
        self.notifier.assert_process_state("proc1", ProcessState.TERMINATED)

        # should be gone from queue too
        self.assertFalse(self.store.get_queued_processes())
        self.assertEqual(self.resource_client.call_count, 0)

    def test_terminate_raciness(self):
        # ensure process is TERMINATING before resource client is called

        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.RUNNING)
        p1.assigned = "hats"
        self.store.add_process(p1)

        def assert_process_terminating(resource_id, upid, round):
            self.assertEqual(resource_id, "hats")
            self.assertEqual(upid, "proc1")
            process = self.store.get_process(None, upid)
            self.assertEqual(process.state, ProcessState.TERMINATING)

        self.resource_client.terminate_process.side_effect = assert_process_terminating

        self.core.terminate_process(None, "proc1")

        self.resource_client.terminate_process.assert_called_once_with(
            "hats", "proc1", 0)
        self.notifier.assert_process_state("proc1", ProcessState.TERMINATING)

    def test_terminate_assigned(self):
        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.ASSIGNED)
        p1.assigned = "hats"
        self.store.add_process(p1)
        self.core.terminate_process(None, "proc1")

        self.resource_client.terminate_process.assert_called_once_with(
            "hats", "proc1", 0)
        self.notifier.assert_process_state("proc1", ProcessState.TERMINATING)

    def test_terminate_retry(self):
        # try to kill a process that is already terminating
        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.TERMINATING)
        p1.assigned = "hats"
        self.store.add_process(p1)
        self.core.terminate_process(None, "proc1")

        self.resource_client.terminate_process.assert_called_once_with(
            "hats", "proc1", 0)
        self.notifier.assert_no_process_state()

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

        # RestartMode.NEVER
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.NEVER)
        for state in all_states:
            self.assertFalse(self.core.process_should_restart(process, state))
            self.assertFalse(self.core.process_should_restart(process, state,
                is_system_restart=True))

        # RestartMode.ALWAYS
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.ALWAYS)
        for state in all_states:
            self.assertTrue(self.core.process_should_restart(process, state))
            self.assertTrue(self.core.process_should_restart(process, state,
                is_system_restart=True))

        # RestartMode.ALWAYS with process.omit_from_system_restart
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.ALWAYS,
            configuration=nosystemrestart_process_config())
        for state in all_states:
            self.assertTrue(self.core.process_should_restart(process, state))
            self.assertFalse(self.core.process_should_restart(process, state,
                is_system_restart=True))

        # RestartMode.ABNORMAL with process.omit_from_system_restart
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.ABNORMAL,
            configuration=nosystemrestart_process_config())
        for state in abnormal_states:
            self.assertTrue(self.core.process_should_restart(process, state))
            self.assertFalse(self.core.process_should_restart(process, state,
                is_system_restart=True))
        self.assertFalse(self.core.process_should_restart(process,
            ProcessState.EXITED))
        self.assertFalse(self.core.process_should_restart(process,
            ProcessState.EXITED, is_system_restart=True))

        # ensure that a process with a busted config doesn't raise an error
        process = self.core.schedule_process(None, uuid.uuid4().hex, definition,
            restart_mode=RestartMode.ALWAYS,
            configuration={'process': ['what is a list doing here??']})
        for state in all_states:
            self.assertTrue(self.core.process_should_restart(process, state))

    def test_heartbeat_node_update_race(self):

        # test processing two beats simultaneously, for eeagents in the same node.
        # check that they don't collide updating the node record
        node_id = uuid.uuid4().hex
        self.core.node_state(node_id, domain_id_from_engine("engine1"),
            InstanceState.RUNNING)

        beat = make_beat(node_id)

        # this beat gets injected while the other is in the midst of processing
        sneaky_beat = make_beat(node_id)

        # when the PD attempts to update the process, sneak in an update
        # first so the request conflicts
        original_update_node = self.store.update_node

        def patched_update_node(node):
            # unpatch ourself first so we don't recurse forever
            self.store.update_node = original_update_node

            self.core.ee_heartbeat("eeagent2", sneaky_beat)
            original_update_node(node)

        self.store.update_node = patched_update_node

        self.core.ee_heartbeat("eeagent1", beat)

        node = self.store.get_node(node_id)
        self.assertEqual(set(["eeagent1", "eeagent2"]), set(node.resources))

    def test_heartbeat_node_removed(self):

        # test processing a heartbeat where node is removed partway through
        node_id = uuid.uuid4().hex
        self.core.node_state(node_id, domain_id_from_engine("engine1"),
            InstanceState.RUNNING)

        beat = make_beat(node_id)

        original_update_node = self.store.update_node

        def patched_update_node(node):
            # unpatch ourself first so we don't recurse forever
            self.store.update_node = original_update_node
            self.store.remove_node(node.node_id)
            original_update_node(node)

        self.store.update_node = patched_update_node

        # this shouldn't blow up, and no resource should be added
        self.core.ee_heartbeat("eeagent1", beat)
        self.assertEqual(self.store.get_resource("eeagent1"), None)

    def test_heartbeat_timestamps(self):

        # test processing a heartbeat where node is removed partway through
        node_id = uuid.uuid4().hex
        self.core.node_state(node_id, domain_id_from_engine("engine1"),
            InstanceState.RUNNING)

        d1 = parse_datetime("2013-04-02T19:37:57.617734+00:00")
        d2 = parse_datetime("2013-04-02T19:38:57.617734+00:00")
        d3 = parse_datetime("2013-04-02T19:39:57.617734+00:00")

        self.core.ee_heartbeat("eeagent1", make_beat(node_id, timestamp=d1.isoformat()))

        resource = self.store.get_resource("eeagent1")
        self.assertEqual(resource.last_heartbeat_datetime, d1)

        self.core.ee_heartbeat("eeagent1", make_beat(node_id, timestamp=d3.isoformat()))
        resource = self.store.get_resource("eeagent1")
        self.assertEqual(resource.last_heartbeat_datetime, d3)

        # out of order hbeat. time shouln't be updated
        self.core.ee_heartbeat("eeagent1", make_beat(node_id, timestamp=d2.isoformat()))
        resource = self.store.get_resource("eeagent1")
        self.assertEqual(resource.last_heartbeat_datetime, d3)

    def test_get_process_constraints(self):
        """test_get_process_constraints

        ensure that order of precedence of engine ids is correct. Should be:

        1. process target - when a process is scheduled, an execution_engine_id
        can be specified in the request's ProcessTarget object. If specified,
        this EE is used.
        2. process/engine mappings - the CEI Launch YML file contains a
        process_engines mapping of process packages to EE names. If the process'
        module matches an entry in this configuration, the associated EE is
        chosen. This format is described below.
        3. default execution engine - the CEI Launch YML file also must specify
        a default_execution_engine value. This is used as a last resort.
        """

        self.registry.set_process_engine_mapping("my", "engine4")
        self.registry.default = "engine1"

        process_definition = {
            'executable': {
                'module': 'my.test',
                'class': 'MyClass'
            }
        }
        process_constraints = {
            'engine': 'mostimportantengine'
        }

        p1 = ProcessRecord.new(None, "proc1", {}, ProcessState.PENDING)
        constraints = self.core.get_process_constraints(p1)
        self.assertEqual(constraints['engine'], self.registry.default)

        p3 = ProcessRecord.new(None, "proc3", process_definition, ProcessState.PENDING,
                constraints=process_constraints)
        constraints = self.core.get_process_constraints(p3)
        self.assertEqual(constraints['engine'], "mostimportantengine")
