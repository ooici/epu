from twisted.trial import unittest
from twisted.internet import defer
import uuid

from epu import states as InstanceStates
from epu.test import Mock
from epu.epucontroller.health import HealthMonitor, InstanceHealthState

class FakeState(object):
    def __init__(self):
        self.health = {}
        self.instances = {}

    def new_fake_instance_state(self, instance_id, state, state_time,
                                health=None, errors=None):
        if health is None:
            if instance_id in self.instances:
                health = self.instances[instance_id].health
            else:
                health = InstanceHealthState.UNKNOWN

        if errors is None and instance_id in self.instances:
            errors = self.instances[instance_id].errors

        self.instances[instance_id] = Mock(instance_id=instance_id,
                                           state=state,
                                           state_time=state_time,
                                           health=health,
                                           errors=errors)

    def new_instance_health(self, instance_id, health_state, errors=None):
        self.health[instance_id] = (health_state, errors)
        self.instances[instance_id].health = health_state
        self.instances[instance_id].errors = errors
        return defer.succeed(None)


class HeartbeatMonitorTests(unittest.TestCase):
    def setUp(self):
        self.state = FakeState()
        self.monitor = HealthMonitor(self.state, boot_seconds=10,
                                     missing_seconds=5, zombie_seconds=10)

    @defer.inlineCallbacks
    def test_basic(self):

        nodes = [str(uuid.uuid4()) for i in range(3)]
        n1, n2, n3 = nodes

        # not using real timestamps
        now = 0

        for n in nodes:
            self.state.new_fake_instance_state(n, InstanceStates.RUNNING, now)

        # all nodes are running but haven't been heard from
        self.assertNodeState(InstanceHealthState.UNKNOWN, *nodes)
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.UNKNOWN, *nodes)

        now = 5
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.UNKNOWN, *nodes)

        # first heartbeat to n1
        yield self.ok_heartbeat(n1, now)
        self.assertNodeState(InstanceHealthState.OK, n1)

        now  = 10
        yield self.monitor.update(now)

        self.assertNodeState(InstanceHealthState.OK, n1)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3)

        yield self.ok_heartbeat(n1, now) # n1 makes it in under the wire
        yield self.ok_heartbeat(n2, now)
        now = 11
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.OK, n1, n2)
        self.assertNodeState(InstanceHealthState.MISSING, n3)

        yield self.ok_heartbeat(n3, now)
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        # ok don't hear from n2 for a while, should go missing
        now = 13
        yield self.ok_heartbeat(n1, now)

        now = 16
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.OK, n1, n3)
        self.assertNodeState(InstanceHealthState.MISSING, n2)

        yield self.ok_heartbeat(n2, now)
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        now = 20

        # roll all nodes to terminated in IaaS
        for n in nodes:
            self.state.new_fake_instance_state(n, InstanceStates.TERMINATED, now)

        # been longer than missing window for n1 but shouldn't matter
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        now = 30
        yield self.ok_heartbeat(n1, now)
        yield self.monitor.update(now)
        # not a zombie yet
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        now = 31
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.OK, n1)

        yield self.ok_heartbeat(n1, now)
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n1)

    @defer.inlineCallbacks
    def test_error(self):
        node = str(uuid.uuid4())

        now = 1
        self.state.new_fake_instance_state(node, InstanceStates.RUNNING, now)
        yield self.ok_heartbeat(node, now)
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.OK, node)

        now = 5
        yield self.err_heartbeat(node, now)
        self.assertNodeState(InstanceHealthState.MONITOR_ERROR, node)
        errors = self.state.instances[node].errors
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], 'faiiiill')

        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.MONITOR_ERROR, node)

    def test_process_error(self):
        node = str(uuid.uuid4())

        now = 1
        self.state.new_fake_instance_state(node, InstanceStates.RUNNING, now)
        yield self.ok_heartbeat(node, now)
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.OK, node)

        now = 5
        procs = [{'name' : 'proc1', 'stderr' : 'faaaaaail', 'state' : 100,
                  'exitcode' : -1, 'stop_timestamp' : 25242}]
        yield self.err_heartbeat(node, now, procs)
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, node)
        self.assertEqual('faaaaaail', self.monitor[node].errors[0]['stderr'])
        procs[0].pop('stderr')

        now = 8
        yield self.err_heartbeat(node, now, procs)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, node)
        self.assertEqual('faaaaaail', self.monitor[node].errors[0]['stderr'])

    def assertNodeState(self, state, *node_ids):
        for n in node_ids:
            self.assertEqual(state, self.state.instances[n].health)

    def ok_heartbeat(self, node_id, timestamp):
        msg = {'node_id' : node_id, 'timestamp' : timestamp,
            'state' : InstanceHealthState.OK}
        return self.monitor.new_heartbeat(msg, timestamp)

    def err_heartbeat(self, node_id, timestamp, procs=None):

        msg = {'node_id' : node_id, 'timestamp' : timestamp,}
        if procs:
            msg['state'] = InstanceHealthState.PROCESS_ERROR
            msg['failed_processes'] = procs
        else:
            msg['state'] = InstanceHealthState.MONITOR_ERROR
            msg['error'] = 'faiiiill'

        return self.monitor.new_heartbeat(msg, timestamp)

