import unittest

from epu.states import InstanceState, ProcessState
from epu.processdispatcher.core import ProcessDispatcherCore
from epu.processdispatcher.store import ProcessDispatcherStore, ProcessRecord
from epu.processdispatcher.engines import EngineRegistry
from epu.processdispatcher.test.mocks import MockResourceClient, MockEPUMClient, MockNotifier
from epu.processdispatcher.util import node_id_to_eeagent_name


class ProcessDispatcherCoreTests(unittest.TestCase):

    engine_conf = {'engine1': {'deployable_type': 'dt1', 'slots': 4},
                   'engine2': {'deployable_type': 'dt2', 'slots': 4},
                   'engine3': {'deployable_type': 'dt3', 'slots': 2},
                   'engine4': {'deployable_type': 'dt4', 'slots': 2}}

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
        self.core.dt_state("node1", "dt1", InstanceState.RUNNING)

        node = self.store.get_node("node1")
        self.assertTrue(node is not None)
        self.assertEqual(node.node_id, "node1")
        self.assertEqual(node.deployable_type, "dt1")

        self.core.dt_state("node1", "dt1", InstanceState.TERMINATING)
        node = self.store.get_node("node1")
        self.assertTrue(node is None)

        # this shouldn't cause any problems even though node is gone
        self.core.dt_state("node1", "dt1", InstanceState.TERMINATED)

    def test_add_remove_node_with_resource(self):
        self.core.dt_state("node1", "dt1", InstanceState.RUNNING)
        resource_id = node_id_to_eeagent_name("node1")
        self.core.ee_heartbeart(resource_id, make_beat())

        resource = self.store.get_resource(resource_id)
        self.assertTrue(resource.enabled)

        # now send a terminated state for the node. resource should be removed.
        self.core.dt_state("node1", "dt1", InstanceState.TERMINATED)

        self.assertTrue(self.store.get_resource(resource_id) is None)
        self.assertTrue(self.store.get_node("node1") is None)

    def test_add_remove_node_with_resource_and_processes(self):
        self.core.dt_state("node1", "dt1", InstanceState.RUNNING)
        resource_id = node_id_to_eeagent_name("node1")
        self.core.ee_heartbeart(resource_id, make_beat())

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
        self.core.dt_state("node1", "dt1", InstanceState.TERMINATED)

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
        spec = {"run_type": "hats", "parameters": {}}
        proc = "proc1"
        subscribers = [('destination', 'operation')]
        self.core.dispatch_process(None, proc, spec, subscribers)

        record = self.store.get_process(None, proc)

        self.assertEqual(len(record.subscribers), len(subscribers))
        for a, b in zip(record.subscribers, subscribers):
            self.assertEqual(a[0], b[0])
            self.assertEqual(a[1], b[1])


def make_beat(processes=None):
    return {"processes": processes or []}
