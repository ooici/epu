# Copyright 2013 University of Chicago

import logging
import itertools
import uuid
import unittest
import threading

from mock import Mock

from epu.decisionengine.impls.simplest import CONF_PRESERVE_N
from epu.epumanagement.conf import *  # noqa
from epu.epumanagement.store import LocalDomainStore, ZooKeeperDomainStore
from epu.states import InstanceState, InstanceHealthState
from epu.epumanagement.decider import ControllerCoreControl
from epu.epumanagement.core import EngineState, CoreInstance
from epu.epumanagement.test.mocks import MockProvisionerClient
from epu.test import ZooKeeperTestMixin
from epu.exceptions import WriteConflictError

log = logging.getLogger(__name__)


class BaseControllerStateTests(unittest.TestCase):
    """Base test class with utility functions.
    """

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.domain = None

    def assertInstance(self, instance_id, **kwargs):
        instance = self.domain.get_instance(instance_id)
        for key, value in kwargs.iteritems():
            self.assertEqual(getattr(instance, key), value)

        instance = self.domain.get_instance(instance_id)
        for key, value in kwargs.iteritems():
            self.assertEqual(getattr(instance, key), value)

    def new_instance(self, time, extravars=None):
        launch_id = str(uuid.uuid4())
        instance_id = str(uuid.uuid4())
        self.domain.new_instance_launch("dtid", instance_id, launch_id,
            "chicago", "big", timestamp=time, extravars=extravars)
        return launch_id, instance_id

    def new_instance_state(self, launch_id, instance_id, state, time):
        msg = dict(node_id=instance_id, launch_id=launch_id, site="chicago",
                   allocation="big", state=state)
        self.domain.new_instance_state(msg, time)


class ControllerStateStoreTests(BaseControllerStateTests):
    """ControllerCoreState tests that can use either storage implementation.
    """
    def setUp(self):
        self.domain = LocalDomainStore("david", "domain1", {})

    def test_instances(self):
        launch_id = str(uuid.uuid4())
        instance_id = str(uuid.uuid4())
        self.domain.new_instance_launch("dtid", instance_id, launch_id,
                                             "chicago", "big", timestamp=1)

        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceState.REQUESTING,
                            state_time=1, health=InstanceHealthState.UNKNOWN)

        msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceState.STARTED)
        self.domain.new_instance_state(msg, timestamp=2)

        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceState.STARTED,
                            state_time=2, health=InstanceHealthState.UNKNOWN)

        # bring in a health update
        self.domain.new_instance_health(instance_id,
                                             InstanceHealthState.OK,
                                             errors=['blah'])
        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceState.STARTED,
                            state_time=2, health=InstanceHealthState.OK,
                            errors=['blah'])

        # another instance state change should preserve health info
        msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceState.RUNNING)

        self.domain.new_instance_state(msg, timestamp=3)
        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceState.RUNNING,
                            state_time=3, health=InstanceHealthState.OK,
                            errors=['blah'])

        all_instances = self.domain.get_instance_ids()
        all_instances = set(all_instances)
        self.assertEqual(len(all_instances), 1)
        self.assertIn(instance_id, all_instances)

    def test_instance_update_conflict(self):
        launch_id = str(uuid.uuid4())
        instance_id = str(uuid.uuid4())
        self.domain.new_instance_launch("dtid", instance_id, launch_id,
                                             "chicago", "big", timestamp=1)

        sneaky_msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceState.PENDING)

        # patch in a function that sneaks in an instance record update just
        # before a requested update. This simulates the case where two EPUM
        # workers are competing to update the same instance.
        original_update_instance = self.domain.update_instance

        patch_called = threading.Event()

        def patched_update_instance(*args, **kwargs):
            patch_called.set()
            # unpatch ourself first so we don't recurse forever
            self.domain.update_instance = original_update_instance

            self.domain.new_instance_state(sneaky_msg, timestamp=2)
            original_update_instance(*args, **kwargs)
        self.domain.update_instance = patched_update_instance

        # send our "real" update. should get a conflict
        msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceState.STARTED)

        with self.assertRaises(WriteConflictError):
            self.domain.new_instance_state(msg, timestamp=2)

        assert patch_called.is_set()


class ZooKeeperControllerStateStoreTests(ControllerStateStoreTests, ZooKeeperTestMixin):

    # this runs all of the ControllerStateStoreTests tests plus any
    # ZK-specific ones

    def setUp(self):
        self.setup_zookeeper("/epum_store_tests_")
        self.addCleanup(self.teardown_zookeeper)
        self.domain = ZooKeeperDomainStore("david", "domain1", self.kazoo,
            self.kazoo.retry, self.zk_base_path)


class ControllerCoreStateTests(BaseControllerStateTests):
    """ControllerCoreState tests that only use in memory store

    They test things basically peripheral to actual persistence.
    """
    def setUp(self):
        self.domain = LocalDomainStore("david", "domain1", {})

    def test_instance_extravars(self):
        """extravars get carried forward from the initial instance state

        (when they don't arrive in state updates)
        """
        extravars = {'iwant': 'asandwich', '4': 'real'}
        launch_id, instance_id = self.new_instance(1,
                                                         extravars=extravars)
        self.new_instance_state(launch_id, instance_id,
                                      InstanceState.RUNNING, 2)

        instance = self.domain.get_instance(instance_id)
        self.assertEqual(instance.instance_id, instance_id)
        self.assertEqual(instance.state, InstanceState.RUNNING)
        self.assertEqual(instance.extravars, extravars)

    def test_incomplete_instance_message(self):
        launch_id, instance_id = self.new_instance(1)

        # now fake a response like we'd get from provisioner dump_state
        # when it has no knowledge of instance
        record = {"node_id": instance_id, "state": InstanceState.FAILED}
        self.domain.new_instance_state(record, timestamp=2)

        instance = self.domain.get_instance(instance_id)
        for k in ('instance_id', 'launch_id', 'site', 'allocation', 'state'):
            self.assertIn(k, instance)

    def test_get_engine_state(self):

        launch_id1, instance_id1 = self.new_instance(1)
        launch_id2, instance_id2 = self.new_instance(1)
        self.new_instance_state(launch_id1, instance_id1, InstanceState.RUNNING, 2)
        es = self.domain.get_engine_state()

        # check instances

        # TODO instance change tracking not supported
#        self.assertEqual(len(es.instance_changes), 2)
#        self.assertIn(instance_id1, es.instance_changes)
#        self.assertIn(instance_id2, es.instance_changes)
#        self.assertEqual(len(es.instance_changes[instance_id1]), 2)
#        self.assertEqual(len(es.instance_changes[instance_id2]), 1)
        self.assertEqual(es.instances[instance_id1].state, InstanceState.RUNNING)
        self.assertEqual(es.instances[instance_id2].state, InstanceState.REQUESTING)

        # ensure that next time around there are no changes but state is same
        es = self.domain.get_engine_state()
#        self.assertEqual(len(es.instance_changes), 0)
        self.assertEqual(es.instances[instance_id1].state,
                         InstanceState.RUNNING)
        self.assertEqual(es.instances[instance_id2].state,
                         InstanceState.REQUESTING)

    def _cleared_instance_health(self, instance_state):
        launch_id, instance_id = self.new_instance(5)
        self.new_instance_state(launch_id, instance_id,
                                      InstanceState.RUNNING, 6)

        self.domain.new_instance_health(instance_id,
            InstanceHealthState.PROCESS_ERROR, error_time=123,
            errors=['blah'])

        self.assertInstance(instance_id, state=InstanceState.RUNNING,
                            health=InstanceHealthState.PROCESS_ERROR,
                            error_time=123,
                            errors=['blah'])

        # terminate the instance and its health state should be cleared
        # but error should remain, for postmortem let's say?
        self.new_instance_state(launch_id, instance_id,
                                      instance_state, 7)
        self.assertInstance(instance_id, state=instance_state,
                            health=InstanceHealthState.UNKNOWN,
                            error_time=123,
                            errors=['blah'])
        inst = self.domain.get_instance(instance_id)
        log.debug(inst.health)

    def test_terminating_cleared_instance_health(self):
        return self._cleared_instance_health(InstanceState.TERMINATING)

    def test_terminated_cleared_instance_health(self):
        return self._cleared_instance_health(InstanceState.TERMINATED)

    def test_failed_cleared_instance_health(self):
        return self._cleared_instance_health(InstanceState.FAILED)

    def test_out_of_order_instance(self):
        launch_id, instance_id = self.new_instance(5)
        self.new_instance_state(launch_id, instance_id,
                                      InstanceState.STARTED, 6)

        # instances cannot go back in state
        self.new_instance_state(launch_id, instance_id,
                                      InstanceState.REQUESTED, 6)

        self.assertEqual(self.domain.get_instance(instance_id).state,
            InstanceState.STARTED)


class ZooKeeperControllerCoreStateStoreTests(ControllerCoreStateTests, ZooKeeperTestMixin):

    # this runs all of the ControllerCoreStateTests tests plus any
    # ZK-specific ones

    def setUp(self):
        self.setup_zookeeper("/epum_store_tests_")
        self.addCleanup(self.teardown_zookeeper)
        self.domain = ZooKeeperDomainStore("david", "domain1", self.kazoo,
            self.kazoo.retry, self.zk_base_path)


class EngineStateTests(unittest.TestCase):

    def test_instances(self):
        i1 = [Mock(instance_id="i1", state=state)
              for state in (InstanceState.REQUESTING,
                            InstanceState.REQUESTED,
                            InstanceState.PENDING,
                            InstanceState.RUNNING)]
        i2 = [Mock(instance_id="i2", state=state)
              for state in (InstanceState.REQUESTING,
                            InstanceState.REQUESTED,
                            InstanceState.FAILED)]
        i3 = [Mock(instance_id="i3", state=state)
              for state in InstanceState.REQUESTING, InstanceState.PENDING]

        changes = dict(i1=i1, i2=i2, i3=i3)
        instances = dict(i1=i1[-1], i2=i2[-1], i3=i3[-1])
        es = EngineState()
        es.instance_changes = changes
        es.instances = instances

        self.assertEqual(es.get_instance("i1").state, InstanceState.RUNNING)
        self.assertEqual(es.get_instance("i2").state, InstanceState.FAILED)
        self.assertEqual(es.get_instance("i3").state, InstanceState.PENDING)
        self.assertEqual(es.get_instance("i4"), None)  # there is no i4
        self.assertEqual(len(es.get_instance_changes("i1")), 4)
        self.assertEqual(len(es.get_instance_changes("i2")), 3)
        self.assertEqual(len(es.get_instance_changes("i3")), 2)
        self.assertEqual(es.get_instance_changes("i4"), [])

        all_changes = es.get_instance_changes()
        changeset = set((change.instance_id, change.state) for change in all_changes)
        for item in itertools.chain(i1, i2, i3):
            self.assertIn((item.instance_id, item.state), changeset)

        failed = es.get_instances_by_state(InstanceState.FAILED)
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].instance_id, "i2")
        self.assertEqual(failed[0].state, InstanceState.FAILED)

        pending2running = es.get_instances_by_state(InstanceState.PENDING,
                                                    InstanceState.RUNNING)
        self.assertEqual(len(pending2running), 2)
        ids = (pending2running[0].instance_id, pending2running[1].instance_id)
        self.assertIn("i1", ids)
        self.assertIn("i3", ids)

        pending = es.get_pending_instances()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].instance_id, "i3")

    def test_instance_health(self):
        i1 = Mock(instance_id="i1", state=InstanceState.RUNNING,
                  health=InstanceHealthState.OK)
        i2 = Mock(instance_id="i2", state=InstanceState.FAILED,
                  health=InstanceHealthState.OK)
        i3 = Mock(instance_id="i3", state=InstanceState.TERMINATED,
                  health=InstanceHealthState.MISSING)

        instances = dict(i1=i1, i2=i2, i3=i3)
        es = EngineState()
        es.instances = instances

        healthy = es.get_healthy_instances()
        self.assertEqual(healthy, [i1])

        unhealthy = es.get_unhealthy_instances()
        self.assertFalse(unhealthy)

        i1.health = InstanceHealthState.MISSING
        healthy = es.get_healthy_instances()
        self.assertFalse(healthy)
        unhealthy = es.get_unhealthy_instances()
        self.assertEqual(unhealthy, [i1])

    def test_instance_health2(self):
        i1 = Mock(instance_id="i1", state=InstanceState.RUNNING,
                  health=InstanceHealthState.OK)
        i2 = Mock(instance_id="i2", state=InstanceState.RUNNING_FAILED,
                  health=InstanceHealthState.OK)
        i3 = Mock(instance_id="i3", state=InstanceState.RUNNING_FAILED,
                  health=InstanceHealthState.MISSING)

        instances = dict(i1=i1, i2=i2, i3=i3)
        es = EngineState()
        es.instances = instances

        healthy = es.get_healthy_instances()
        self.assertEqual(healthy, [i1])

        unhealthy = es.get_unhealthy_instances()
        self.assertTrue(i2 in unhealthy)
        self.assertTrue(i3 in unhealthy)
        self.assertEqual(2, len(unhealthy))

        # Should not matter if health is present or not, it's RUNNING_FAILED
        i3.health = InstanceHealthState.MISSING
        unhealthy = es.get_unhealthy_instances()
        self.assertTrue(i2 in unhealthy)
        self.assertTrue(i3 in unhealthy)


class ControllerCoreControlTests(unittest.TestCase):

    def _config_simplest_domain_conf(self, n_preserving):
        """Get 'simplest' EPU conf with specified NPreserving policy
        """
        engine_class = "epu.decisionengine.impls.simplest.SimplestEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        engine = {CONF_PRESERVE_N: n_preserving}
        return {EPUM_CONF_GENERAL: general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}

    def setUp(self):
        self.provisioner = MockProvisionerClient()
        config = self._config_simplest_domain_conf(1)
        self.state = LocalDomainStore('david', "epu1", config)
        self.prov_vars = {"foo": "bar"}
        self.controller_name = "fakey"
        self.control = ControllerCoreControl(self.provisioner, self.state,
                                             self.prov_vars,
                                             self.controller_name)

    def test_configure_1(self):
        self.control.configure(None)
        self.assertEqual(self.control.prov_vars, self.prov_vars)

    def test_configure_2(self):
        self.control.configure({})
        self.assertEqual(self.control.prov_vars, self.prov_vars)

    def test_configure_3(self):
        params = {PROVISIONER_VARS_KEY: {"blah": "blah"}}
        self.control.configure(params)
        self.assertEqual(self.control.prov_vars, {"blah": "blah"})

    def test_launch(self):
        launch_id, instance_ids = self.control.launch("dt", "chicago",
            "small", extravars={"v1": 1})

        self.assertEqual(len(instance_ids), 1)

        # check that right info got added to state
        instance_id = instance_ids[0]
        instance = self.state.get_instance(instance_id)
        self.assertEqual(instance.instance_id, instance_id)
        self.assertEqual(instance.launch_id, launch_id)
        self.assertEqual(instance.site, "chicago")
        self.assertEqual(instance.allocation, "small")
        self.assertEqual(instance.extravars, {"v1": 1})

        # and provisionerclient called
        self.assertEqual(len(self.provisioner.launches), 1)
        launch = self.provisioner.launches[0]
        self.assertEqual(launch['launch_id'], launch_id)
        self.assertEqual(launch['dt'], "dt")
        # vars are merged result
        self.assertEqual(launch['vars']['foo'], "bar")
        self.assertEqual(launch['vars']['v1'], 1)
        self.assertEqual(launch['site'], "chicago")
        self.assertEqual(launch['allocation'], "small")


class CoreInstanceTests(unittest.TestCase):
    def test_instance_version(self):
        instance = CoreInstance(instance_id="i1", launch_id="l1",
            allocation="a1", site="s1", state=InstanceState.RUNNING)

        self.assertEqual(instance._version, None)

        self.assertNotIn("_version", instance)
        self.assertNotIn("version", instance.keys())

        instance.set_version(1)
        self.assertEqual(instance._version, 1)

        self.assertNotIn("_version", instance)
        self.assertNotIn("version", instance.keys())
        d = dict(instance.iteritems())
        self.assertNotIn("_version", d)
