from collections import defaultdict
import itertools
import uuid
from twisted.trial import unittest
from twisted.internet import defer
from epu.epucontroller.forengine import SensorItem
from epu.epucontroller.health import InstanceHealthState

import epu.states as InstanceStates
from epu.epucontroller.controller_core import ControllerCore, \
    PROVISIONER_VARS_KEY, MONITOR_HEALTH_KEY, HEALTH_BOOT_KEY, \
    HEALTH_ZOMBIE_KEY, HEALTH_MISSING_KEY, ControllerCoreState, EngineState, CoreInstance
from epu.test import Mock

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

        bads = [dict(sesnor_id="bad", time=1, value=34),
                ['not','even','a','dict!'],
                None,
                142,
                "this is just a string",
                dict(sensor_id="almost", time=1),
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

    @defer.inlineCallbacks
    def test_get_engine_state(self):
        self.state.new_sensor_item(dict(sensor_id="s1", time=1, value="a"))
        self.state.new_sensor_item(dict(sensor_id="s1", time=2, value="b"))
        self.state.new_sensor_item(dict(sensor_id="s2", time=2, value="a"))

        launch_id1, instance_id1 = yield self.new_instance(1)
        launch_id2, instance_id2 = yield self.new_instance(1)
        yield self.new_instance_state(launch_id1, instance_id1, InstanceStates.RUNNING, 2)
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
        self.assertEqual(es.instances[instance_id1].state, InstanceStates.RUNNING)
        self.assertEqual(es.instances[instance_id2].state, InstanceStates.REQUESTING)
        self.assertEqual(len(es.sensor_changes), 0)
        self.assertEqual(es.sensors["s1"].value, "b")
        self.assertEqual(es.sensors["s2"].value, "a")

    @defer.inlineCallbacks
    def new_instance(self, time):
        launch_id = str(uuid.uuid4())
        instance_id = str(uuid.uuid4())
        yield self.state.new_instance_launch(instance_id, launch_id,
                                             "chicago", "big", timestamp=time)
        defer.returnValue((launch_id, instance_id))

    @defer.inlineCallbacks
    def new_instance_state(self, launch_id, instance_id, state, time):
        msg = dict(node_id=instance_id, launch_id=launch_id, site="chicago",
                   allocation="big", state=state)
        yield self.state.new_instance_state(msg, time)

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
