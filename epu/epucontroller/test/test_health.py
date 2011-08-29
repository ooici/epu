from epu.epucontroller.controller_store import ControllerStore
from twisted.trial import unittest
from twisted.internet import defer
import uuid

from epu import states as InstanceStates
from epu.epucontroller.controller_core import ControllerCoreState, CoreInstance
from epu.epucontroller.health import HealthMonitor, InstanceHealthState

class FakeState(ControllerCoreState):
    def new_fake_instance_state(self, instance_id, state, state_time,
                                health=None, errors=None):
        if health is None:
            if instance_id in self.instances:
                health = self.instances[instance_id].health
            else:
                health = InstanceHealthState.UNKNOWN

        if errors is None and instance_id in self.instances:
            errors = self.instances[instance_id].errors

        self.instances[instance_id] = CoreInstance(instance_id=instance_id,
                                                   launch_id="thelaunch",
                                                   site="chicago",
                                                   allocation="big",
                                           state=state,
                                           state_time=state_time,
                                           health=health,
                                           errors=errors)


class HeartbeatMonitorTests(unittest.TestCase):
    def setUp(self):
        self.state = FakeState(ControllerStore())
        self.monitor = None

    @defer.inlineCallbacks
    def test_recovery(self):

        self.monitor = HealthMonitor(self.state,
                                     starting_seconds=10, boot_seconds=10,
                                     missing_seconds=5, zombie_seconds=10,
                                     init_time=100)
        self.monitor._RECOVERY_BUFFER_SECONDS = 10
        nodes = ["n" + str(i+1) for i in range(8)]
        n1, n2, n3, n4, n5, n6, n7, n8 = nodes

        # set up some instances that reached their iaas_state before the
        # init time (100)

        # this one has been running for well longer than the missing timeout
        # and we will have not received a heartbeat. It shouldn't be marked
        # MISSING until more than 5 seconds after the init_time
        self.state.new_fake_instance_state(n1, InstanceStates.RUNNING, 50,
                                           InstanceHealthState.OK)

        # this has been running for 10 seconds before the init time but we
        # have never received a heartbeat. It should be marked as MISSING
        # after the boot timeout expires, starting from the init time.
        self.state.new_fake_instance_state(n2, InstanceStates.RUNNING, 90,
                                           InstanceHealthState.UNKNOWN)

        # is terminated and nothing should happen
        self.state.new_fake_instance_state(n3, InstanceStates.TERMINATED, 90,
                                           InstanceHealthState.UNKNOWN)

        # this one will get a heartbeat at 110, just before it would be
        # marked MISSING
        self.state.new_fake_instance_state(n4, InstanceStates.RUNNING, 95,
                                           InstanceHealthState.UNKNOWN)

        # this one will get a heartbeat at 105, just before it would be
        # marked MISSING
        self.state.new_fake_instance_state(n5, InstanceStates.RUNNING, 95,
                                           InstanceHealthState.OK)

        # this instance was already marked as errored before the recovery
        self.state.new_fake_instance_state(n6, InstanceStates.RUNNING, 95,
                                           InstanceHealthState.PROCESS_ERROR)

        # this instance was a ZOMBIE, it should be initially marked back as
        # UNKNOWN and then if a heartbeat arrives it should be ZOMBIE again
        self.state.new_fake_instance_state(n7, InstanceStates.TERMINATED, 80,
                                           InstanceHealthState.ZOMBIE)

        # this instance is STARTED but past the timeout. it gets a window of
        # time after recovery for its state updates to come in before it is
        # marked as MISSING
        self.state.new_fake_instance_state(n8, InstanceStates.STARTED, 80,
                                           InstanceHealthState.UNKNOWN)

        yield self.monitor.update(100)
        self.assertNodeState(InstanceHealthState.OK, n1, n5)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4, n7, n8)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)

        yield self.monitor.update(105)
        self.assertNodeState(InstanceHealthState.OK, n1, n5)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4, n7, n8)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)

        self.ok_heartbeat(n5, 105)
        self.ok_heartbeat(n7, 105) # this one will be relabeled as a zombie

        self.err_heartbeat(n6, 105, procs=['a'])
        yield self.monitor.update(106)
        self.assertNodeState(InstanceHealthState.OK, n5)
        self.assertNodeState(InstanceHealthState.MISSING, n1)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4, n8)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n7)

        self.ok_heartbeat(n5, 110)
        yield self.monitor.update(110)
        self.assertNodeState(InstanceHealthState.OK, n5)
        self.assertNodeState(InstanceHealthState.MISSING, n1)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4, n8)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n7)

        self.ok_heartbeat(n4, 110)
        self.err_heartbeat(n6, 110, procs=['a'])
        yield self.monitor.update(111)
        self.assertNodeState(InstanceHealthState.OK, n5, n4)
        self.assertNodeState(InstanceHealthState.MISSING, n1, n2, n8)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n3)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n7)


    @defer.inlineCallbacks
    def test_starting_timeout(self):
        self.monitor = HealthMonitor(self.state, starting_seconds=10,
                                     boot_seconds=10, missing_seconds=5,
                                     zombie_seconds=10, init_time=0)

        nodes = ["n" + str(i+1) for i in range(2)]
        n1, n2 = nodes

        self.state.new_fake_instance_state(n1, InstanceStates.STARTED, 10,
                                           InstanceHealthState.UNKNOWN)
        self.state.new_fake_instance_state(n2, InstanceStates.STARTED, 10,
                                           InstanceHealthState.UNKNOWN)

        yield self.monitor.update(10)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n1, n2)

        yield self.monitor.update(20)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n1, n2)

        #n1 comes in under the wire
        self.state.new_fake_instance_state(n1, InstanceStates.RUNNING, 20,
                                           InstanceHealthState.UNKNOWN)

        yield self.monitor.update(21)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n1)
        self.assertNodeState(InstanceHealthState.MISSING, n2)


    @defer.inlineCallbacks
    def test_basic(self):
        self.monitor = HealthMonitor(self.state, boot_seconds=10,
                                     missing_seconds=5, zombie_seconds=10,
                                     init_time=0)

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

        now = 42
        yield self.monitor.update(now)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n1)

    @defer.inlineCallbacks
    def test_error(self):
        self.monitor = HealthMonitor(self.state, boot_seconds=10,
                                     missing_seconds=5, zombie_seconds=10,
                                     init_time=0)

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

    @defer.inlineCallbacks
    def test_process_error(self):
        self.monitor = HealthMonitor(self.state, boot_seconds=10,
                                     missing_seconds=5, zombie_seconds=10,
                                     init_time=0)

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
        errors = self.state.instances[node].errors
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]['stderr'], 'faaaaaail')
        procs[0].pop('stderr')

        now = 8
        yield self.err_heartbeat(node, now, procs)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, node)
        errors = self.state.instances[node].errors
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]['stderr'], 'faaaaaail')

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

