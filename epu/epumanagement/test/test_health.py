from twisted.trial import unittest
from twisted.internet import defer
import uuid

from epu import states as InstanceStates
from epu.decisionengine.impls.simplest import CONF_PRESERVE_N
from epu.epumanagement import EPUManagement
from epu.epumanagement.conf import *
from epu.epumanagement.store import ControllerStore, EPUState
from epu.epumanagement.core import CoreInstance
from epu.epumanagement.health import InstanceHealthState, TESTCONF_HEALTH_INIT_TIME
from epu.epumanagement.test.mocks import MockOUAgentClient, MockProvisionerClient, MockSubscriberNotifier
from epu.epumanagement.test.test_epumanagement import MOCK_PKG

class FakeState(EPUState):
    def new_fake_instance_state(self, instance_id, state, state_time,
                                health=None, errors=None):
        if health is None:
            if instance_id in self.instances:
                health = self.instances[instance_id].health
            else:
                health = InstanceHealthState.UNKNOWN

        if errors is None and instance_id in self.instances:
            errors = self.instances[instance_id].errors

        instance = CoreInstance(instance_id=instance_id, launch_id="thelaunch",
                                site="chicago", allocation="big", state=state,
                                state_time=state_time, health=health, errors=errors)
        self._add_instance(instance)

class HeartbeatMonitorTests(unittest.TestCase):
    def setUp(self):
        self.epu_name = "epuX"
        epu_config = self._epu_config(health_init_time=100)
        self.state = FakeState(None, self.epu_name, epu_config, backing_store=ControllerStore())

        initial_conf = {EPUM_INITIALCONF_PERSISTENCE: "memory",
                        EPUM_INITIALCONF_EXTERNAL_DECIDE: True}
        self.notifier = MockSubscriberNotifier()
        self.provisioner_client = MockProvisionerClient()
        self.ou_client = MockOUAgentClient()
        self.epum = EPUManagement(initial_conf, self.notifier, self.provisioner_client, self.ou_client)

        # inject the FakeState instance directly instead of using msg_add_epu()
        self.epum.epum_store.epus[self.epu_name] = self.state

    def _epu_config(self, health_init_time=0):
        general = {EPUM_CONF_ENGINE_CLASS: MOCK_PKG + ".MockDecisionEngine01"}
        health = {EPUM_CONF_HEALTH_MONITOR: True, EPUM_CONF_HEALTH_BOOT: 10,
                  EPUM_CONF_HEALTH_MISSING: 5, EPUM_CONF_HEALTH_ZOMBIE: 10,
                  TESTCONF_HEALTH_INIT_TIME: health_init_time}
        engine = {CONF_PRESERVE_N:1}
        return {EPUM_CONF_GENERAL:general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}

    @defer.inlineCallbacks
    def test_recovery(self):
        yield self.epum.initialize()
        epu_config = self._epu_config(health_init_time=100)
        yield self.epum.msg_reconfigure_epu(None, self.epu_name, epu_config)

        nodes = ["n" + str(i+1) for i in range(7)]
        n1, n2, n3, n4, n5, n6, n7 = nodes

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

        yield self.epum._doctor_appt(100)
        self.assertNodeState(InstanceHealthState.OK, n1, n5)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4, n7)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)

        yield self.epum._doctor_appt(105)
        self.assertNodeState(InstanceHealthState.OK, n1, n5)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4, n7)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)

        self.ok_heartbeat(n5, 105)
        self.ok_heartbeat(n7, 105) # this one will be relabeled as a zombie

        self.err_heartbeat(n6, 105, procs=['a'])
        yield self.epum._doctor_appt(106)
        self.assertNodeState(InstanceHealthState.OK, n5)
        self.assertNodeState(InstanceHealthState.MISSING, n1)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n7)

        self.ok_heartbeat(n5, 110)
        yield self.epum._doctor_appt(110)
        self.assertNodeState(InstanceHealthState.OK, n5)
        self.assertNodeState(InstanceHealthState.MISSING, n1)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n7)

        self.ok_heartbeat(n4, 110)
        self.err_heartbeat(n6, 110, procs=['a'])
        yield self.epum._doctor_appt(111)
        self.assertNodeState(InstanceHealthState.OK, n5, n4)
        self.assertNodeState(InstanceHealthState.MISSING, n1, n2)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n3)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n7)

    @defer.inlineCallbacks
    def test_basic(self):
        yield self.epum.initialize()
        yield self.epum.msg_reconfigure_epu(None, self.epu_name, self._epu_config())
        
        nodes = [str(uuid.uuid4()) for i in range(3)]
        n1, n2, n3 = nodes

        # not using real timestamps
        now = 0

        for n in nodes:
            self.state.new_fake_instance_state(n, InstanceStates.RUNNING, now)

        # all nodes are running but haven't been heard from
        self.assertNodeState(InstanceHealthState.UNKNOWN, *nodes)
        yield self.epum._doctor_appt(now)
        self.assertEquals(0, self.epum.doctor.monitors[self.epu_name].init_time)
        self.assertNodeState(InstanceHealthState.UNKNOWN, *nodes)

        now = 5
        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.UNKNOWN, *nodes)

        # first heartbeat to n1
        yield self.ok_heartbeat(n1, now)
        self.assertNodeState(InstanceHealthState.OK, n1)

        now  = 10
        yield self.epum._doctor_appt(now)

        self.assertNodeState(InstanceHealthState.OK, n1)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3)

        yield self.ok_heartbeat(n1, now) # n1 makes it in under the wire
        yield self.ok_heartbeat(n2, now)
        now = 11
        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, n1, n2)
        self.assertNodeState(InstanceHealthState.MISSING, n3)

        yield self.ok_heartbeat(n3, now)
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        # ok don't hear from n2 for a while, should go missing
        now = 13
        yield self.ok_heartbeat(n1, now)

        now = 16
        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, n1, n3)
        self.assertNodeState(InstanceHealthState.MISSING, n2)

        yield self.ok_heartbeat(n2, now)
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        now = 20

        # roll all nodes to terminated in IaaS
        for n in nodes:
            self.state.new_fake_instance_state(n, InstanceStates.TERMINATED, now)

        # been longer than missing window for n1 but shouldn't matter
        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        now = 30
        yield self.ok_heartbeat(n1, now)
        yield self.epum._doctor_appt(now)
        # not a zombie yet
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        now = 31
        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, n1)

        yield self.ok_heartbeat(n1, now)
        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n1)

        now = 42
        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n1)

    @defer.inlineCallbacks
    def test_error(self):
        yield self.epum.initialize()
        yield self.epum.msg_reconfigure_epu(None, self.epu_name, self._epu_config())

        node = str(uuid.uuid4())

        now = 1
        self.state.new_fake_instance_state(node, InstanceStates.RUNNING, now)
        yield self.ok_heartbeat(node, now)
        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, node)

        now = 5
        yield self.err_heartbeat(node, now)
        self.assertNodeState(InstanceHealthState.MONITOR_ERROR, node)
        errors = self.state.instances[node].errors
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], 'faiiiill')

        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.MONITOR_ERROR, node)

    @defer.inlineCallbacks
    def test_process_error(self):
        yield self.epum.initialize()
        yield self.epum.msg_reconfigure_epu(None, self.epu_name, self._epu_config())

        node = str(uuid.uuid4())

        now = 1
        self.state.new_fake_instance_state(node, InstanceStates.RUNNING, now)
        yield self.ok_heartbeat(node, now)
        yield self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, node)

        now = 5
        procs = [{'name' : 'proc1', 'stderr' : 'faaaaaail', 'state' : 100,
                  'exitcode' : -1, 'stop_timestamp' : 25242}]
        yield self.err_heartbeat(node, now, procs)
        yield self.epum._doctor_appt(now)
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

    @defer.inlineCallbacks
    def ok_heartbeat(self, node_id, timestamp):
        msg = {'node_id' : node_id, 'timestamp' : timestamp,
            'state' : InstanceHealthState.OK}
        yield self.epum.msg_heartbeat(None, msg, timestamp=timestamp)

    @defer.inlineCallbacks
    def err_heartbeat(self, node_id, timestamp, procs=None):

        msg = {'node_id' : node_id, 'timestamp' : timestamp,}
        if procs:
            msg['state'] = InstanceHealthState.PROCESS_ERROR
            msg['failed_processes'] = procs
        else:
            msg['state'] = InstanceHealthState.MONITOR_ERROR
            msg['error'] = 'faiiiill'

        yield self.epum.msg_heartbeat(None, msg, timestamp=timestamp)
