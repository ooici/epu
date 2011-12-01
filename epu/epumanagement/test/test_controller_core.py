import logging
import itertools
import uuid
import unittest

from epu.decisionengine.impls.simplest import CONF_PRESERVE_N
from epu.epumanagement.conf import *
from epu.epumanagement.forengine import SensorItem, LaunchItem
from epu.epumanagement.store import ControllerStore, EPUState
from epu.states import InstanceStates, InstanceHealthState
from epu.epumanagement.decider import ControllerCoreControl
from epu.epumanagement.core import EngineState, CoreInstance
from epu.test import Mock

log = logging.getLogger(__name__)

class BaseControllerStateTests(unittest.TestCase):
    """Base test class with utility functions.
    """

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.store = None
        self.state = None

    def assertInstance(self, instance_id, **kwargs):
        instance = self.store.get_instance(instance_id)
        for key,value in kwargs.iteritems():
            self.assertEqual(getattr(instance, key), value)

        instance = self.state.instances[instance_id]
        for key,value in kwargs.iteritems():
            self.assertEqual(getattr(instance, key), value)

    def assertSensor(self, sensor_id, timestamp, value):
        sensoritem = self.store.get_sensor(sensor_id)
        self.assertEqual(sensoritem.sensor_id, sensor_id)
        self.assertEqual(sensoritem.time, timestamp)
        self.assertEqual(sensoritem.value, value)

        sensoritem = self.state.sensors[sensor_id]
        self.assertEqual(sensoritem.sensor_id, sensor_id)
        self.assertEqual(sensoritem.time, timestamp)
        self.assertEqual(sensoritem.value, value)

    def new_instance(self, time, extravars=None):
        launch_id = str(uuid.uuid4())
        instance_id = str(uuid.uuid4())
        self.state.new_instance_launch("dtid", instance_id, launch_id,
                                             "chicago", "big", timestamp=time,
                                             extravars=extravars)
        return launch_id, instance_id

    def new_instance_state(self, launch_id, instance_id, state, time):
        msg = dict(node_id=instance_id, launch_id=launch_id, site="chicago",
                   allocation="big", state=state)
        self.state.new_instance_state(msg, time)


class ControllerStateStoreTests(BaseControllerStateTests):
    """ControllerCoreState tests that can use either storage implementation.

    Subclassed below to use cassandra.
    """
    def setUp(self):
        self.store = self.get_store()
        self.state = EPUState(None, "epu1", {}, backing_store=self.store)

    def get_store(self):
        return ControllerStore()

    def test_sensors(self):
        sensor_id = "sandwich_meter" # how many sandwiches??

        msg = dict(sensor_id=sensor_id, time=1, value=100)
        self.state.new_sensor_item(msg)

        self.assertSensor(sensor_id, 1, 100)

        msg = dict(sensor_id=sensor_id, time=2, value=101)
        self.state.new_sensor_item(msg)
        self.assertSensor(sensor_id, 2, 101)

        all_sensors = self.store.get_sensor_ids()
        all_sensors = set(all_sensors)
        self.assertEqual(len(all_sensors), 1)
        self.assertIn(sensor_id, all_sensors)
    
    def test_instances(self):
        launch_id = str(uuid.uuid4())
        instance_id = str(uuid.uuid4())
        self.state.new_instance_launch("dtid", instance_id, launch_id,
                                             "chicago", "big", timestamp=1)

        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.REQUESTING,
                            state_time=1, health=InstanceHealthState.UNKNOWN)

        msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceStates.STARTED)
        self.state.new_instance_state(msg, timestamp=2)

        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.STARTED,
                            state_time=2, health=InstanceHealthState.UNKNOWN)

        # bring in a health update
        self.state.new_instance_health(instance_id,
                                             InstanceHealthState.OK,
                                             errors=['blah'])
        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.STARTED,
                            state_time=2, health=InstanceHealthState.OK,
                            errors=['blah'])

        # another instance state change should preserve health info
        msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceStates.RUNNING)

        self.state.new_instance_state(msg, timestamp=3)
        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.RUNNING,
                            state_time=3, health=InstanceHealthState.OK,
                            errors=['blah'])

        all_instances = self.store.get_instance_ids()
        all_instances = set(all_instances)
        self.assertEqual(len(all_instances), 1)
        self.assertIn(instance_id, all_instances)

    def test_recovery(self):

        # put some values in the store directly
        self.store.add_sensor(SensorItem("s1", 100, "s1v1"))
        self.store.add_sensor(SensorItem("s2", 100, "s2v1"))
        self.store.add_sensor(SensorItem("s1", 200, "s1v2"))

        d1 = dict(instance_id="i1", launch_id="l1", allocation="big",
                  site="cleveland", state=InstanceStates.PENDING)
        self.store.add_instance(CoreInstance.from_dict(d1))
        d2 = dict(instance_id="i2", launch_id="l2", allocation="big",
                  site="cleveland", state=InstanceStates.PENDING)
        self.store.add_instance(CoreInstance.from_dict(d2))

        d2['state'] = InstanceStates.RUNNING
        self.store.add_instance(CoreInstance.from_dict(d2))

        # recovery should bring them into state
        self.state.recover()
        self.assertSensor("s1", 200, "s1v2")
        self.assertSensor("s2", 100, "s2v1")
        self.assertInstance("i1", launch_id="l1", allocation="big",
                  site="cleveland", state=InstanceStates.PENDING)
        self.assertInstance("i2", launch_id="l2", allocation="big",
                  site="cleveland", state=InstanceStates.RUNNING)

    def test_recover_nothing(self):

        #ensure recover() works when the store is empty

        self.state.recover()
        self.assertEqual(len(self.state.instances), 0)
        self.assertEqual(len(self.state.sensors), 0)

class ControllerCoreStateTests(BaseControllerStateTests):
    """ControllerCoreState tests that only use in memory store

    They test things basically peripheral to actual persistence.
    """
    def setUp(self):
        self.store = ControllerStore()
        self.state = EPUState(None, "epu1", {}, backing_store=self.store)

    def test_bad_sensors(self):
        #badly formatted sensors shouldn't break the world

        bads = [dict(sesnor_id="bad", time=1, value=34),
                ['not','even','a','dict!'],
                None,
                142,
                "this is just a string",
                dict(sensor_id="almost", time=1),
                dict(sensor_id="soclose", value=7)]

        for bad in bads:
            self.state.new_sensor_item(bad)

    def test_instance_extravars(self):
        """extravars get carried forward from the initial instance state

        (when they don't arrive in state updates)
        """
        extravars = {'iwant': 'asandwich', 4: 'real'}
        launch_id, instance_id = self.new_instance(1,
                                                         extravars=extravars)
        self.new_instance_state(launch_id, instance_id,
                                      InstanceStates.RUNNING, 2)

        instance = self.state.instances[instance_id]
        self.assertEqual(instance.instance_id, instance_id)
        self.assertEqual(instance.state, InstanceStates.RUNNING)
        self.assertEqual(instance.extravars, extravars)

    def test_incomplete_instance_message(self):
        launch_id, instance_id = self.new_instance(1)

        # now fake a response like we'd get from provisioner dump_state
        # when it has no knowledge of instance
        record = {"node_id":instance_id, "state":InstanceStates.FAILED}
        self.state.new_instance_state(record, timestamp=2)

        instance = self.state.instances[instance_id]
        for k in ('instance_id', 'launch_id', 'site', 'allocation', 'state'):
            self.assertIn(k, instance)

    def test_get_engine_state(self):
        self.state.new_sensor_item(dict(sensor_id="s1", time=1, value="a"))
        self.state.new_sensor_item(dict(sensor_id="s1", time=2, value="b"))
        self.state.new_sensor_item(dict(sensor_id="s2", time=2, value="a"))

        launch_id1, instance_id1 = self.new_instance(1)
        launch_id2, instance_id2 = self.new_instance(1)
        self.new_instance_state(launch_id1, instance_id1, InstanceStates.RUNNING, 2)
        es = self.state.get_engine_state()

        #check instances
        self.assertEqual(len(es.instance_changes), 2)
        self.assertIn(instance_id1, es.instance_changes)
        self.assertIn(instance_id2, es.instance_changes)
        self.assertEqual(len(es.instance_changes[instance_id1]), 2)
        self.assertEqual(len(es.instance_changes[instance_id2]), 1)
        self.assertEqual(es.instances[instance_id1].state, InstanceStates.RUNNING)
        self.assertEqual(es.instances[instance_id2].state, InstanceStates.REQUESTING)

        #check sensors
        self.assertEqual(len(es.sensor_changes), 2)
        self.assertIn("s1", es.sensor_changes)
        self.assertIn("s2", es.sensor_changes)
        self.assertEqual(len(es.sensor_changes["s1"]), 2)
        self.assertEqual(es.sensors["s1"].value, "b")
        self.assertEqual(es.sensors["s2"].value, "a")

        # ensure that next time around there are no changes but state is same
        es = self.state.get_engine_state()
        self.assertEqual(len(es.instance_changes), 0)
        self.assertEqual(es.instances[instance_id1].state,
                         InstanceStates.RUNNING)
        self.assertEqual(es.instances[instance_id2].state,
                         InstanceStates.REQUESTING)
        self.assertEqual(len(es.sensor_changes), 0)
        self.assertEqual(es.sensors["s1"].value, "b")
        self.assertEqual(es.sensors["s2"].value, "a")

    def _cleared_instance_health(self, instance_state):
        launch_id, instance_id = self.new_instance(5)
        self.new_instance_state(launch_id, instance_id,
                                      InstanceStates.RUNNING, 6)

        self.state.new_instance_health(instance_id,
                                             InstanceHealthState.PROCESS_ERROR,
                                             error_time=123,
                                             errors=['blah'])

        self.assertInstance(instance_id, state=InstanceStates.RUNNING,
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
        inst = self.store.get_instance(instance_id)
        log.debug(inst.health)

    def test_terminating_cleared_instance_health(self):
        return self._cleared_instance_health(InstanceStates.TERMINATING)

    def test_terminated_cleared_instance_health(self):
        return self._cleared_instance_health(InstanceStates.TERMINATED)

    def test_failed_cleared_instance_health(self):
        return self._cleared_instance_health(InstanceStates.FAILED)

    def test_out_of_order_instance(self):
        launch_id, instance_id = self.new_instance(5)
        self.new_instance_state(launch_id, instance_id,
                                      InstanceStates.STARTED, 6)

        # instances cannot go back in state
        self.new_instance_state(launch_id, instance_id,
                                      InstanceStates.REQUESTED, 6)

        self.assertEqual(self.state.instances[instance_id].state,
                         InstanceStates.STARTED)

    def test_out_of_order_sensor(self):
        sensor_id = "sandwich_meter" # how many sandwiches??

        msg = dict(sensor_id=sensor_id, time=100, value=100)
        self.state.new_sensor_item(msg)

        msg = dict(sensor_id=sensor_id, time=90, value=200)
        self.state.new_sensor_item(msg)

        self.assertSensor(sensor_id, 100, 100)
        self.assertSensor(sensor_id, 100, 100)
        self.assertEqual(len(self.state.pending_sensors[sensor_id]), 2)


class EngineStateTests(unittest.TestCase):
    def test_sensors(self):
        s1 = [SensorItem("s1", i, "v" + str(i)) for i in range(3)]
        s2 = [SensorItem("s2", i, "v" + str(i)) for i in range(5)]

        changes = {'s1' : s1, 's2' : s2}
        sensors = {'s1' : s1[-1], 's2' : s2[-1]}
        es = EngineState()
        es.sensor_changes = changes
        es.sensors = sensors

        self.assertEqual(es.get_sensor("s1").value, "v2")
        self.assertEqual(es.get_sensor("s2").value, "v4")
        self.assertEqual(es.get_sensor("s3"), None) #there is no s3
        self.assertEqual(len(es.get_sensor_changes("s1")), 3)
        self.assertEqual(len(es.get_sensor_changes("s2")), 5)
        self.assertEqual(es.get_sensor_changes("s3"), [])

        all_changes = es.get_sensor_changes()
        for item in itertools.chain(s1, s2):
            self.assertIn(item, all_changes)

    def test_instances(self):
        i1 = [Mock(instance_id="i1", state=state)
              for state in (InstanceStates.REQUESTING,
                            InstanceStates.REQUESTED,
                            InstanceStates.PENDING,
                            InstanceStates.RUNNING)]
        i2 = [Mock(instance_id="i2", state=state)
              for state in (InstanceStates.REQUESTING,
                            InstanceStates.REQUESTED,
                            InstanceStates.FAILED)]
        i3 = [Mock(instance_id="i3", state=state)
              for state in InstanceStates.REQUESTING, InstanceStates.PENDING]

        changes = dict(i1=i1, i2=i2, i3=i3)
        instances = dict(i1=i1[-1], i2=i2[-1], i3=i3[-1])
        es = EngineState()
        es.instance_changes = changes
        es.instances = instances

        self.assertEqual(es.get_instance("i1").state, InstanceStates.RUNNING)
        self.assertEqual(es.get_instance("i2").state, InstanceStates.FAILED)
        self.assertEqual(es.get_instance("i3").state, InstanceStates.PENDING)
        self.assertEqual(es.get_instance("i4"), None) # there is no i4
        self.assertEqual(len(es.get_instance_changes("i1")), 4)
        self.assertEqual(len(es.get_instance_changes("i2")), 3)
        self.assertEqual(len(es.get_instance_changes("i3")), 2)
        self.assertEqual(es.get_instance_changes("i4"), [])

        all_changes = es.get_instance_changes()
        changeset = set((change.instance_id, change.state) for change in all_changes)
        for item in itertools.chain(i1, i2, i3):
            self.assertIn((item.instance_id, item.state), changeset)

        failed = es.get_instances_by_state(InstanceStates.FAILED)
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].instance_id, "i2")
        self.assertEqual(failed[0].state, InstanceStates.FAILED)

        pending2running = es.get_instances_by_state(InstanceStates.PENDING,
                                                    InstanceStates.RUNNING)
        self.assertEqual(len(pending2running), 2)
        ids = (pending2running[0].instance_id, pending2running[1].instance_id)
        self.assertIn("i1", ids)
        self.assertIn("i3", ids)

        pending = es.get_pending_instances()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].instance_id, "i3")

    def test_instance_health(self):
        i1 = Mock(instance_id="i1", state=InstanceStates.RUNNING,
                  health=InstanceHealthState.OK)
        i2 = Mock(instance_id="i2", state=InstanceStates.FAILED,
                  health=InstanceHealthState.OK)
        i3 = Mock(instance_id="i3", state=InstanceStates.TERMINATED,
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
        i1 = Mock(instance_id="i1", state=InstanceStates.RUNNING,
                  health=InstanceHealthState.OK)
        i2 = Mock(instance_id="i2", state=InstanceStates.RUNNING_FAILED,
                  health=InstanceHealthState.OK)
        i3 = Mock(instance_id="i3", state=InstanceStates.RUNNING_FAILED,
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

    def _config_simplest_epuconf(self, n_preserving):
        """Get 'simplest' EPU conf with specified NPreserving policy
        """
        engine_class = "epu.decisionengine.impls.simplest.SimplestEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        engine = {CONF_PRESERVE_N:n_preserving}
        return {EPUM_CONF_GENERAL:general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}

    def setUp(self):
        self.provisioner = FakeProvisionerClient()
        epuconfig = self._config_simplest_epuconf(1)
        self.state = EPUState(None, "epu1", epuconfig)
        self.prov_vars = {"foo" : "bar"}
        self.controller_name = "fakey"
        self.control = ControllerCoreControl(self.provisioner, self.state,
                                             self.prov_vars,
                                             self.controller_name)

    def test_configure_1(self):
        self.control.configure(None)
        self.assertEqual(self.control.sleep_seconds, 5.0)
        self.assertEqual(self.control.prov_vars, self.prov_vars)

    def test_configure_2(self):
        self.control.configure({})
        self.assertEqual(self.control.sleep_seconds, 5.0)
        self.assertEqual(self.control.prov_vars, self.prov_vars)

    def test_configure_3(self):
        params = {"timed-pulse-irregular" : 3000,
                  PROVISIONER_VARS_KEY : {"blah": "blah"}}
        self.control.configure(params)
        self.assertEqual(self.control.sleep_seconds, 3.0)
        self.assertEqual(self.control.prov_vars, {"blah": "blah"})

    def test_launch(self):
        desc = {'i1' : LaunchItem(1, "small", "chicago", None)}
        launch_id, launch_desc = self.control.launch("dt", desc, extravars={"v1": 1})

        instance_ids = launch_desc['i1'].instance_ids
        self.assertEqual(len(instance_ids), 1)

        #check that right info got added to state
        instance_id = instance_ids[0]
        instance = self.state.instances[instance_id]
        self.assertEqual(instance.instance_id, instance_id)
        self.assertEqual(instance.launch_id, launch_id)
        self.assertEqual(instance.site, "chicago")
        self.assertEqual(instance.allocation, "small")

        # and provisionerclient called
        self.assertEqual(len(self.provisioner.launches), 1)
        launch = self.provisioner.launches[0]
        self.assertEqual(launch['launch_id'], launch_id)
        self.assertEqual(launch['dt'], "dt")
        # vars are merged result
        self.assertEqual(launch['vars']['foo'], "bar")
        self.assertEqual(launch['vars']['v1'], 1)
        self.assertEqual(launch['subscribers'], (self.controller_name,))
        self.assertEqual(launch['launch_description'], launch_desc)

        
class FakeProvisionerClient(object):
    def __init__(self):
        self.launches = []
        self.dump_state_reqs = []

    def provision(self, launch_id, deployable_type, launch_description,
                  subscribers, vars=None):
        record = dict(launch_id=launch_id, dt=deployable_type,
                      launch_description=launch_description,
                      subscribers=subscribers, vars=vars)
        self.launches.append(record)

    def dump_state(self, nodes, force_subscribe=None):
        self.dump_state_reqs.append(nodes)

