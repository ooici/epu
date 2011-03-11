import unittest
import uuid

from epu import states as InstanceStates

from epu.epucontroller.health import HealthMonitor, NodeHealthState

class HeartbeatMonitorTests(unittest.TestCase):
    def setUp(self):
        self.monitor = HealthMonitor(boot_seconds=10, missing_seconds=5,
                                        zombie_seconds=10)

    def test_basic(self):

        nodes = [str(uuid.uuid4()) for i in range(3)]
        n1, n2, n3 = nodes

        # not using real timestamps
        now = 0

        for n in nodes:
            self.monitor.node_state(n, InstanceStates.RUNNING, timestamp=now)

        # all nodes are running but haven't been heard from
        self.assertNodeState(NodeHealthState.UNKNOWN, *nodes)
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.UNKNOWN, *nodes)

        now = 5
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.UNKNOWN, *nodes)

        # first heartbeat to n1
        self.ok_heartbeat(n1, now)
        self.assertNodeState(NodeHealthState.OK, n1)

        now  = 10
        self.monitor.update(now)

        self.assertNodeState(NodeHealthState.OK, n1)
        self.assertNodeState(NodeHealthState.UNKNOWN, n2, n3)

        self.ok_heartbeat(n1, now) # n1 makes it in under the wire
        self.ok_heartbeat(n2, now)
        now = 11
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.OK, n1, n2)
        self.assertNodeState(NodeHealthState.MISSING, n3)

        self.ok_heartbeat(n3, now)
        self.assertNodeState(NodeHealthState.OK, *nodes)

        # ok don't hear from n2 for a while, should go missing
        now = 13
        self.ok_heartbeat(n1, now)

        now = 16
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.OK, n1, n3)
        self.assertNodeState(NodeHealthState.MISSING, n2)

        self.ok_heartbeat(n2, now)
        self.assertNodeState(NodeHealthState.OK, *nodes)

        now = 20

        # roll all nodes to terminated in IaaS
        for n in nodes:
            self.monitor.node_state(n, InstanceStates.TERMINATED, now)

        # been longer than missing window for n1 but shouldn't matter
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.OK, *nodes)

        now = 30
        self.ok_heartbeat(n1, now)
        self.monitor.update(now)
        # not a zombie yet
        self.assertNodeState(NodeHealthState.OK, *nodes)

        now = 31
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.OK, n1)
        self.assertRaises(KeyError, self.monitor.__getitem__, n2)
        self.assertRaises(KeyError, self.monitor.__getitem__, n3)

        self.ok_heartbeat(n1, now)
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.ZOMBIE, n1)

    def test_error(self):
        node = str(uuid.uuid4())

        now = 1
        self.monitor.node_state(node, InstanceStates.RUNNING, now)
        self.ok_heartbeat(node, now)
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.OK, node)

        now = 5
        self.err_heartbeat(node, now)
        self.assertNodeState(NodeHealthState.MONITOR_ERROR, node)
        self.assertEqual(self.monitor[node].error, 'faiiiill')

        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.MONITOR_ERROR, node)

    def test_process_error(self):
        node = str(uuid.uuid4())

        now = 1
        self.monitor.node_state(node, InstanceStates.RUNNING, now)
        self.ok_heartbeat(node, now)
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.OK, node)

        now = 5
        procs = [{'name' : 'proc1', 'stderr' : 'faaaaaail', 'state' : 100,
                  'exitcode' : -1, 'stop_timestamp' : 25242}]
        self.err_heartbeat(node, now, procs)
        self.monitor.update(now)
        self.assertNodeState(NodeHealthState.PROCESS_ERROR, node)
        self.assertEqual('faaaaaail', self.monitor[node].process_errors[0]['stderr'])
        procs[0].pop('stderr')

        now = 8
        self.err_heartbeat(node, now, procs)
        self.assertNodeState(NodeHealthState.PROCESS_ERROR, node)
        self.assertEqual('faaaaaail', self.monitor[node].process_errors[0]['stderr'])


    def assertNodeState(self, state, *node_ids):
        for n in node_ids:
            self.assertEqual(state, self.monitor[n].state)

    def ok_heartbeat(self, node_id, timestamp):
        msg = {'node_id' : node_id, 'timestamp' : timestamp,
            'state' : NodeHealthState.OK}
        self.monitor.new_heartbeat(msg, timestamp)

    def err_heartbeat(self, node_id, timestamp, procs=None):

        msg = {'node_id' : node_id, 'timestamp' : timestamp,}
        if procs:
            msg['state'] = NodeHealthState.PROCESS_ERROR
            msg['failed_processes'] = procs
        else:
            msg['state'] = NodeHealthState.MONITOR_ERROR
            msg['error'] = 'faiiiill'

        self.monitor.new_heartbeat(msg, timestamp)

