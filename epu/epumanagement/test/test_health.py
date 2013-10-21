# Copyright 2013 University of Chicago

import unittest
import uuid

from epu.states import InstanceState, InstanceHealthState
from epu.decisionengine.impls.simplest import CONF_PRESERVE_N
from epu.epumanagement import EPUManagement
from epu.epumanagement.conf import EPUM_INITIALCONF_EXTERNAL_DECIDE,\
    EPUM_DEFAULT_SERVICE_NAME, EPUM_CONF_ENGINE_CLASS, EPUM_CONF_HEALTH_MONITOR,\
    EPUM_CONF_GENERAL, EPUM_CONF_HEALTH, EPUM_CONF_ENGINE, EPUM_CONF_HEALTH_BOOT,\
    EPUM_CONF_HEALTH_MISSING, EPUM_CONF_HEALTH_ZOMBIE, EPUM_CONF_HEALTH_REALLY_MISSING
from epu.epumanagement.store import LocalDomainStore, LocalEPUMStore
from epu.epumanagement.core import CoreInstance
from epu.epumanagement.health import TESTCONF_HEALTH_INIT_TIME
from epu.epumanagement.test.mocks import MockOUAgentClient, MockProvisionerClient,\
    MockSubscriberNotifier, MockDTRSClient
from epu.epumanagement.test.test_epumanagement import MOCK_PKG


class FakeDomainStore(LocalDomainStore):
    def new_fake_instance_state(self, instance_id, state, state_time,
                                health=None, errors=None):
        instance = self.get_instance(instance_id)
        if health is None:
            if instance:
                health = instance.health
            else:
                health = InstanceHealthState.UNKNOWN

        if errors is None and instance and instance.errors:
            errors = instance.errors

        newinstance = CoreInstance(instance_id=instance_id, launch_id="thelaunch",
                                site="chicago", allocation="big", state=state,
                                state_time=state_time, health=health, errors=errors)
        if instance:
            self.update_instance(newinstance, previous=instance)
        else:
            self.add_instance(newinstance)


class HeartbeatMonitorTests(unittest.TestCase):
    def setUp(self):
        self.domain_name = "epuX"
        self.domain_owner = "david"
        self.domain_key = (self.domain_owner, self.domain_name)
        config = self._dom_config(health_init_time=100)
        self.state = FakeDomainStore(self.domain_owner, self.domain_name, config)

        initial_conf = {EPUM_INITIALCONF_EXTERNAL_DECIDE: True}
        self.notifier = MockSubscriberNotifier()
        self.provisioner_client = MockProvisionerClient()
        self.dtrs_client = MockDTRSClient()
        self.ou_client = MockOUAgentClient()
        self.epum_store = LocalEPUMStore(EPUM_DEFAULT_SERVICE_NAME)
        self.epum_store.initialize()
        self.epum = EPUManagement(
            initial_conf, self.notifier, self.provisioner_client,
            self.ou_client, self.dtrs_client, store=self.epum_store)
        self.provisioner_client._set_epum(self.epum)
        self.ou_client._set_epum(self.epum)

        # inject the FakeState instance directly instead of using msg_add_domain()
        self.epum.epum_store.domains[(self.domain_owner, self.domain_name)] = self.state

    def _dom_config(self, health_init_time=0):
        general = {EPUM_CONF_ENGINE_CLASS: MOCK_PKG + ".MockDecisionEngine01"}
        health = {EPUM_CONF_HEALTH_MONITOR: True, EPUM_CONF_HEALTH_BOOT: 10,
                  EPUM_CONF_HEALTH_MISSING: 5, EPUM_CONF_HEALTH_ZOMBIE: 10,
                  EPUM_CONF_HEALTH_REALLY_MISSING: 3,
                  TESTCONF_HEALTH_INIT_TIME: health_init_time}
        engine = {CONF_PRESERVE_N: 1}
        return {EPUM_CONF_GENERAL: general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}

    def test_recovery(self):
        self.epum.initialize()
        dom_config = self._dom_config(health_init_time=100)
        self.epum.msg_reconfigure_domain(self.domain_owner, self.domain_name, dom_config)

        nodes = ["n" + str(i + 1) for i in range(7)]
        n1, n2, n3, n4, n5, n6, n7 = nodes

        # set up some instances that reached their iaas_state before the
        # init time (100)

        # this one has been running for well longer than the missing timeout
        # and we will have not received a heartbeat. It shouldn't be marked
        # OUT_OF_CONTACT until more than 5 seconds after the init_time
        self.state.new_fake_instance_state(n1, InstanceState.RUNNING, 50,
                                           InstanceHealthState.OK)

        # this has been running for 10 seconds before the init time but we
        # have never received a heartbeat. It should be marked as OUT_OF_CONTACT
        # after the boot timeout expires, starting from the init time.
        self.state.new_fake_instance_state(n2, InstanceState.RUNNING, 90,
                                           InstanceHealthState.UNKNOWN)

        # is terminated and nothing should happen
        self.state.new_fake_instance_state(n3, InstanceState.TERMINATED, 90,
                                           InstanceHealthState.UNKNOWN)

        # this one will get a heartbeat at 110, just before it would be
        # marked OUT_OF_CONTACT
        self.state.new_fake_instance_state(n4, InstanceState.RUNNING, 95,
                                           InstanceHealthState.UNKNOWN)

        # this one will get a heartbeat at 105, just before it would be
        # marked OUT_OF_CONTACT
        self.state.new_fake_instance_state(n5, InstanceState.RUNNING, 95,
                                           InstanceHealthState.OK)

        # this instance was already marked as errored before the recovery
        self.state.new_fake_instance_state(n6, InstanceState.RUNNING, 95,
                                           InstanceHealthState.PROCESS_ERROR)

        # this instance was a ZOMBIE, it should be initially marked back as
        # UNKNOWN and then if a heartbeat arrives it should be ZOMBIE again
        self.state.new_fake_instance_state(n7, InstanceState.TERMINATED, 80,
                                           InstanceHealthState.ZOMBIE)

        self.epum._doctor_appt(100)
        self.assertNodeState(InstanceHealthState.OK, n1, n5)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4, n7)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)

        self.epum._doctor_appt(105)
        self.assertNodeState(InstanceHealthState.OK, n1, n5)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4, n7)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)

        self.ok_heartbeat(n5, 105)
        self.ok_heartbeat(n7, 105)  # this one will be relabeled as a zombie

        self.err_heartbeat(n6, 105, procs=['a'])
        self.epum._doctor_appt(106)
        self.assertNodeState(InstanceHealthState.OK, n5)
        self.assertNodeState(InstanceHealthState.OUT_OF_CONTACT, n1)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n7)

        self.ok_heartbeat(n5, 110)
        self.epum._doctor_appt(110)
        self.assertNodeState(InstanceHealthState.OK, n5)

        # n1 has now been "out of contact" too long and is past the "really missing"
        # threshold, so it should now be MISSING
        self.assertNodeState(InstanceHealthState.MISSING, n1)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3, n4)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n7)

        self.ok_heartbeat(n4, 110)
        self.err_heartbeat(n6, 110, procs=['a'])
        self.epum._doctor_appt(111)
        self.assertNodeState(InstanceHealthState.OK, n5, n4)
        self.assertNodeState(InstanceHealthState.MISSING, n1)
        self.assertNodeState(InstanceHealthState.OUT_OF_CONTACT, n2)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n3)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, n6)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n7)

    def test_basic(self):
        self.epum.initialize()
        self.epum.msg_reconfigure_domain(self.domain_owner, self.domain_name, self._dom_config())

        nodes = [str(uuid.uuid4()) for i in range(3)]
        n1, n2, n3 = nodes

        # not using real timestamps
        now = 0

        for n in nodes:
            self.state.new_fake_instance_state(n, InstanceState.RUNNING, now)

        # all nodes are running but haven't been heard from
        self.assertNodeState(InstanceHealthState.UNKNOWN, *nodes)
        self.epum._doctor_appt(now)
        self.assertEquals(0, self.epum.doctor.monitors[self.domain_key].init_time)
        self.assertNodeState(InstanceHealthState.UNKNOWN, *nodes)

        now = 5
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.UNKNOWN, *nodes)

        # first heartbeat to n1
        self.ok_heartbeat(n1, now)
        self.assertNodeState(InstanceHealthState.OK, n1)

        now = 10
        self.epum._doctor_appt(now)

        self.assertNodeState(InstanceHealthState.OK, n1)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n2, n3)

        self.ok_heartbeat(n1, now)  # n1 makes it in under the wire
        self.ok_heartbeat(n2, now)
        now = 11
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, n1, n2)
        self.assertNodeState(InstanceHealthState.OUT_OF_CONTACT, n3)

        self.ok_heartbeat(n3, now)
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        # ok don't hear from n2 for a while, should go missing
        now = 13
        self.ok_heartbeat(n1, now)

        now = 16
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, n1, n3)
        self.assertNodeState(InstanceHealthState.OUT_OF_CONTACT, n2)

        self.ok_heartbeat(n2, now)
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        now = 20

        # roll all nodes to terminated in IaaS
        for n in nodes:
            self.state.new_fake_instance_state(n, InstanceState.TERMINATED, now)

        # been longer than missing window for n1 but shouldn't matter
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        now = 30
        self.ok_heartbeat(n1, now)
        self.epum._doctor_appt(now)
        # not a zombie yet
        self.assertNodeState(InstanceHealthState.OK, *nodes)

        now = 31
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, n1)

        self.ok_heartbeat(n1, now)
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.ZOMBIE, n1)

        now = 42
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.UNKNOWN, n1)

    def test_error(self):
        self.epum.initialize()
        self.epum.msg_reconfigure_domain(self.domain_owner, self.domain_name, self._dom_config())

        node = str(uuid.uuid4())

        now = 1
        self.state.new_fake_instance_state(node, InstanceState.RUNNING, now)
        self.ok_heartbeat(node, now)
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, node)

        now = 5
        self.err_heartbeat(node, now)
        self.assertNodeState(InstanceHealthState.MONITOR_ERROR, node)
        errors = self.state.instances[node].errors
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0], 'faiiiill')

        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.MONITOR_ERROR, node)

    def test_process_error(self):
        self.epum.initialize()
        self.epum.msg_reconfigure_domain(self.domain_owner, self.domain_name, self._dom_config())

        node = str(uuid.uuid4())

        now = 1
        self.state.new_fake_instance_state(node, InstanceState.RUNNING, now)
        self.ok_heartbeat(node, now)
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.OK, node)

        now = 5
        procs = [{'name': 'proc1', 'stderr': 'faaaaaail', 'state': 100,
                  'exitcode': -1, 'stop_timestamp': 25242}]
        self.err_heartbeat(node, now, procs)
        self.epum._doctor_appt(now)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, node)
        errors = self.state.instances[node].errors
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]['stderr'], 'faaaaaail')
        procs[0].pop('stderr')

        now = 8
        self.err_heartbeat(node, now, procs)
        self.assertNodeState(InstanceHealthState.PROCESS_ERROR, node)
        errors = self.state.instances[node].errors
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]['stderr'], 'faaaaaail')

    def test_defibulator(self):
        self.epum.initialize()
        dom_config = self._dom_config(health_init_time=100)
        self.epum.msg_reconfigure_domain(self.domain_owner, self.domain_name, dom_config)

        self.ou_client.dump_state_called = 0
        self.ou_client.heartbeats_sent = 0
        self.ou_client.respond_to_dump_state = True

        # set up an instance that reached its iaas_state before the init time (100)
        n1 = "n1"

        # has been running for well longer than the missing timeout and we will
        # have not received a heartbeat. It shouldn't be marked OUT_OF_CONTACT
        # until more than 5 seconds after the init_time
        self.state.new_fake_instance_state(n1, InstanceState.RUNNING, 50,
                                           InstanceHealthState.OK)

        self.epum._doctor_appt(100)
        self.assertNodeState(InstanceHealthState.OK, n1)
        self.epum._doctor_appt(105)
        self.assertNodeState(InstanceHealthState.OK, n1)

        self.assertEquals(0, self.ou_client.dump_state_called)
        self.assertEquals(0, self.ou_client.heartbeats_sent)
        self.epum._doctor_appt(106)
        # back to OK
        self.assertNodeState(InstanceHealthState.OK, n1)
        self.assertEquals(1, self.ou_client.dump_state_called)
        self.assertEquals(1, self.ou_client.heartbeats_sent)

    def test_defibulator_failure(self):
        self.epum.initialize()
        dom_config = self._dom_config(health_init_time=100)
        self.epum.msg_reconfigure_domain(self.domain_owner, self.domain_name, dom_config)

        self.ou_client.dump_state_called = 0
        self.ou_client.heartbeats_sent = 0
        self.ou_client.respond_to_dump_state = False  # i.e., the node is really gone

        # set up an instance that reached its iaas_state before the init time (100)
        n1 = "Poor Yorick"

        # has been running for well longer than the missing timeout and we will
        # have not received a heartbeat. It shouldn't be marked OUT_OF_CONTACT
        # until more than 5 seconds after the init_time
        self.state.new_fake_instance_state(n1, InstanceState.RUNNING, 50,
                                           InstanceHealthState.OK)

        self.epum._doctor_appt(100)
        self.assertNodeState(InstanceHealthState.OK, n1)
        self.epum._doctor_appt(105)
        self.assertNodeState(InstanceHealthState.OK, n1)

        self.assertEquals(0, self.ou_client.dump_state_called)
        self.assertEquals(0, self.ou_client.heartbeats_sent)
        self.epum._doctor_appt(106)
        self.assertNodeState(InstanceHealthState.OUT_OF_CONTACT, n1)
        self.assertEquals(1, self.ou_client.dump_state_called)
        self.assertEquals(0, self.ou_client.heartbeats_sent)

        self.epum._doctor_appt(110)
        self.assertNodeState(InstanceHealthState.MISSING, n1)
        self.assertEquals(1, self.ou_client.dump_state_called)
        self.assertEquals(0, self.ou_client.heartbeats_sent)

    # ----------------------------------------------------------------------------------

    def assertNodeState(self, state, *node_ids):
        for n in node_ids:
            self.assertEqual(state, self.state.instances[n].health)

    def ok_heartbeat(self, node_id, timestamp):
        msg = {'node_id': node_id, 'timestamp': timestamp,
            'state': InstanceHealthState.OK}
        self.epum.msg_heartbeat(None, msg, timestamp=timestamp)

    def err_heartbeat(self, node_id, timestamp, procs=None):

        msg = {'node_id': node_id, 'timestamp': timestamp, }
        if procs:
            msg['state'] = InstanceHealthState.PROCESS_ERROR
            msg['failed_processes'] = procs
        else:
            msg['state'] = InstanceHealthState.MONITOR_ERROR
            msg['error'] = 'faiiiill'

        self.epum.msg_heartbeat(None, msg, timestamp=timestamp)
