from collections import defaultdict
import uuid
from twisted.trial import unittest
from twisted.internet import defer
from epu.epucontroller.health import InstanceHealthState

import epu.states as InstanceStates
from epu.epucontroller.controller_core import ControllerCore, \
    PROVISIONER_VARS_KEY, MONITOR_HEALTH_KEY, HEALTH_BOOT_KEY, \
    HEALTH_ZOMBIE_KEY, HEALTH_MISSING_KEY, ControllerCoreState

class ControllerCoreTests(unittest.TestCase):

    def setUp(self):
        self.prov_client = FakeProvisionerClient()
        self.prov_vars = {"a" : "b"}

    def test_setup_nohealth(self):
        core = ControllerCore(self.prov_client, "%s.FakeEngine" % __name__,
                              "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars})
        self.assertEqual(core.health_monitor, None)
        #should be ignored (no exception)
        core.new_heartbeat('notarealheartbeat')

    def test_setup_nohealth2(self):
        core = ControllerCore(self.prov_client, "%s.FakeEngine" % __name__,
                              "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars,
                               MONITOR_HEALTH_KEY : False
                               })
        self.assertEqual(core.health_monitor, None)
        #should be ignored (no exception)
        core.new_heartbeat('notarealheartbeat')

    def test_setup_health(self):
        core = ControllerCore(self.prov_client, "%s.FakeEngine" % __name__,
                              "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars,
                               MONITOR_HEALTH_KEY : True, HEALTH_BOOT_KEY:1,
                               HEALTH_ZOMBIE_KEY:2, HEALTH_MISSING_KEY:3
                               })
        health = core.health_monitor
        self.assertNotEqual(health, None)
        self.assertEqual(health.boot_timeout, 1)
        self.assertEqual(health.zombie_timeout, 2)
        self.assertEqual(health.missing_timeout, 3)

class ControllerCoreStateTests(unittest.TestCase):
    def setUp(self):
        self.store = self.get_store()
        self.state = ControllerCoreState(self.store)

    def get_store(self):
        return FakeControllerStore()


    @defer.inlineCallbacks
    def test_sensors(self):
        sensor_id = "sandwich_meter" # how many sandwiches??

        msg = dict(sensor_id=sensor_id, time=1, value=100)
        yield self.state.new_sensor_item(msg)

        self.assertEqual(self.store.sensor_counts[sensor_id], 1)
        self.assertSensor(sensor_id, 1, 100)

        msg = dict(sensor_id=sensor_id, time=2, value=101)
        yield self.state.new_sensor_item(msg)
        self.assertEqual(self.store.sensor_counts[sensor_id], 2)
        self.assertSensor(sensor_id, 2, 101)

    @defer.inlineCallbacks
    def test_bad_sensors(self):
        #badly formatted sensors shouldn't break the world

        bads = [dict(sesnor_id="bad", timestamp=1, value=34),
                ['not','even','a','dict!'],
                None,
                142,
                "this is just a string",
                dict(sensor_id="almost", timestamp=1),
                dict(sensor_id="soclose", value=7)]

        for bad in bads:
            yield self.state.new_sensor_item(bad)
    
    @defer.inlineCallbacks
    def test_instances(self):
        launch_id = str(uuid.uuid4())
        instance_id = str(uuid.uuid4())
        yield self.state.new_instance_launch(instance_id, launch_id,
                                             "chicago", "big", timestamp=1)

        self.assertEqual(self.store.instance_counts[instance_id], 1)
        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.REQUESTING,
                            state_time=1, health=InstanceHealthState.UNKNOWN)

        msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceStates.STARTED)
        yield self.state.new_instance_state(msg, timestamp=2)

        self.assertEqual(self.store.instance_counts[instance_id], 2)
        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.STARTED,
                            state_time=2, health=InstanceHealthState.UNKNOWN)

        # bring in a health update
        yield self.state.new_instance_health(instance_id,
                                             InstanceHealthState.OK,
                                             errors=['blah'])
        self.assertEqual(self.store.instance_counts[instance_id], 3)
        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.STARTED,
                            state_time=2, health=InstanceHealthState.OK,
                            errors=['blah'])

        # another instance state change should preserve health info
        msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceStates.RUNNING)

        yield self.state.new_instance_state(msg, timestamp=3)
        self.assertEqual(self.store.instance_counts[instance_id], 4)
        self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.RUNNING,
                            state_time=3, health=InstanceHealthState.OK,
                            errors=['blah'])

    def assertInstance(self, instance_id, **kwargs):
        instance = self.store.instances[instance_id]
        for key,value in kwargs.iteritems():
            self.assertEqual(getattr(instance, key), value)

        instance = self.state.instances[instance_id]
        for key,value in kwargs.iteritems():
            self.assertEqual(getattr(instance, key), value)

    def assertSensor(self, sensor_id, timestamp, value):
        sensoritem = self.store.sensors[sensor_id]
        self.assertEqual(sensoritem.sensor_id, sensor_id)
        self.assertEqual(sensoritem.time, timestamp)
        self.assertEqual(sensoritem.value, value)

        sensoritem = self.state.sensors[sensor_id]
        self.assertEqual(sensoritem.sensor_id, sensor_id)
        self.assertEqual(sensoritem.time, timestamp)
        self.assertEqual(sensoritem.value, value)


class FakeControllerStore(object):
    def __init__(self):
        self.instances = {}
        self.instance_counts = defaultdict(int)
        self.sensors = {}
        self.sensor_counts = defaultdict(int)

    def add_instance(self, instance):
        self.instances[instance.instance_id] = instance
        self.instance_counts[instance.instance_id] += 1
        return defer.succeed(None)

    def add_sensor(self, sensor):
        self.sensors[sensor.sensor_id] = sensor
        self.sensor_counts[sensor.sensor_id] += 1
        return defer.succeed(None)


class FakeProvisionerClient(object):
    pass

class FakeEngine(object):
    def initialize(self, *args):
        pass
