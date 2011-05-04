import itertools
import uuid
from twisted.trial import unittest
from twisted.internet import defer
from ion.util.itv_decorator import itv
from ion.core import ioninit

from epu.epucontroller.controller_store import ControllerStore, CassandraControllerStore
from epu.epucontroller.forengine import SensorItem
from epu.epucontroller.health import InstanceHealthState

import epu.states as InstanceStates
from epu.epucontroller.controller_core import ControllerCore, \
    PROVISIONER_VARS_KEY, MONITOR_HEALTH_KEY, HEALTH_BOOT_KEY, \
    HEALTH_ZOMBIE_KEY, HEALTH_MISSING_KEY, ControllerCoreState, EngineState, CoreInstance
from epu.test import Mock

CONF = ioninit.config(__name__)


class ControllerCoreTests(unittest.TestCase):
    ENGINE = "%s.FakeEngine" % __name__

    def setUp(self):
        self.prov_client = FakeProvisionerClient()
        self.prov_vars = {"a" : "b"}

    def test_setup_nohealth(self):
        core = ControllerCore(self.prov_client, self.ENGINE, "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars})
        self.assertEqual(core.health_monitor, None)
        #should be ignored (no exception)
        core.new_heartbeat('notarealheartbeat')

    def test_setup_nohealth2(self):
        core = ControllerCore(self.prov_client, self.ENGINE, "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars,
                               MONITOR_HEALTH_KEY : False
                               })
        self.assertEqual(core.health_monitor, None)
        #should be ignored (no exception)
        core.new_heartbeat('notarealheartbeat')

    def test_setup_health(self):
        core = ControllerCore(self.prov_client, self.ENGINE, "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars,
                               MONITOR_HEALTH_KEY : True, HEALTH_BOOT_KEY:1,
                               HEALTH_ZOMBIE_KEY:2, HEALTH_MISSING_KEY:3
                               })
        health = core.health_monitor
        self.assertNotEqual(health, None)
        self.assertEqual(health.boot_timeout, 1)
        self.assertEqual(health.zombie_timeout, 2)
        self.assertEqual(health.missing_timeout, 3)

    def test_initialize(self):
        state = FakeControllerState()
        core = ControllerCore(self.prov_client, self.ENGINE, "controller",
                              state=state)

        yield core.run_initialize()
        self.assertEqual(state.recover_count, 1)
        self.assertEqual(core.engine.initialize_count, 1)


class BaseControllerStateTests(unittest.TestCase):
    """Base test class with utility functions.

    Subclassed below to group into tests that should run on in-memory,
    cassandra, or both.
    """

    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
        self.store = None
        self.state = None

    def assertInstance(self, instance_id, **kwargs):
        instance = yield self.store.get_instance(instance_id)
        for key,value in kwargs.iteritems():
            self.assertEqual(getattr(instance, key), value)

        instance = self.state.instances[instance_id]
        for key,value in kwargs.iteritems():
            self.assertEqual(getattr(instance, key), value)

    def assertSensor(self, sensor_id, timestamp, value):
        sensoritem = yield self.store.get_sensor(sensor_id)
        self.assertEqual(sensoritem.sensor_id, sensor_id)
        self.assertEqual(sensoritem.time, timestamp)
        self.assertEqual(sensoritem.value, value)

        sensoritem = self.state.sensors[sensor_id]
        self.assertEqual(sensoritem.sensor_id, sensor_id)
        self.assertEqual(sensoritem.time, timestamp)
        self.assertEqual(sensoritem.value, value)

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


class ControllerStateStoreTests(BaseControllerStateTests):
    """ControllerCoreState tests that can use either storage implementation.

    Subclassed below to use cassandra.
    """
    @defer.inlineCallbacks
    def setUp(self):
        self.store = yield self.get_store()
        self.state = ControllerCoreState(self.store)

    def get_store(self):
        return defer.succeed(ControllerStore())

    @defer.inlineCallbacks
    def test_sensors(self):
        sensor_id = "sandwich_meter" # how many sandwiches??

        msg = dict(sensor_id=sensor_id, time=1, value=100)
        yield self.state.new_sensor_item(msg)

        yield self.assertSensor(sensor_id, 1, 100)

        msg = dict(sensor_id=sensor_id, time=2, value=101)
        yield self.state.new_sensor_item(msg)
        yield self.assertSensor(sensor_id, 2, 101)

        all_sensors = yield self.store.get_sensor_ids()
        all_sensors = set(all_sensors)
        self.assertEqual(len(all_sensors), 1)
        self.assertIn(sensor_id, all_sensors)
    
    @defer.inlineCallbacks
    def test_instances(self):
        launch_id = str(uuid.uuid4())
        instance_id = str(uuid.uuid4())
        yield self.state.new_instance_launch(instance_id, launch_id,
                                             "chicago", "big", timestamp=1)

        yield self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.REQUESTING,
                            state_time=1, health=InstanceHealthState.UNKNOWN)

        msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceStates.STARTED)
        yield self.state.new_instance_state(msg, timestamp=2)

        yield self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.STARTED,
                            state_time=2, health=InstanceHealthState.UNKNOWN)

        # bring in a health update
        yield self.state.new_instance_health(instance_id,
                                             InstanceHealthState.OK,
                                             errors=['blah'])
        yield self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.STARTED,
                            state_time=2, health=InstanceHealthState.OK,
                            errors=['blah'])

        # another instance state change should preserve health info
        msg = dict(node_id=instance_id, launch_id=launch_id,
                   site="chicago", allocation="big",
                   state=InstanceStates.RUNNING)

        yield self.state.new_instance_state(msg, timestamp=3)
        yield self.assertInstance(instance_id, launch_id=launch_id, site="chicago",
                            allocation="big", state=InstanceStates.RUNNING,
                            state_time=3, health=InstanceHealthState.OK,
                            errors=['blah'])

        all_instances = yield self.store.get_instance_ids()
        all_instances = set(all_instances)
        self.assertEqual(len(all_instances), 1)
        self.assertIn(instance_id, all_instances)

    @defer.inlineCallbacks
    def test_recovery(self):

        # put some values in the store directly
        yield self.store.add_sensor(SensorItem("s1", 100, "s1v1"))
        yield self.store.add_sensor(SensorItem("s2", 100, "s2v1"))
        yield self.store.add_sensor(SensorItem("s1", 200, "s1v2"))

        d1 = dict(instance_id="i1", launch_id="l1", allocation="big",
                  site="cleveland", state=InstanceStates.PENDING)
        yield self.store.add_instance(CoreInstance(**d1))
        d2 = dict(instance_id="i2", launch_id="l2", allocation="big",
                  site="cleveland", state=InstanceStates.PENDING)
        yield self.store.add_instance(CoreInstance(**d2))

        d2['state'] = InstanceStates.RUNNING
        yield self.store.add_instance(CoreInstance(**d2))

        # recovery should bring them into state
        yield self.state.recover()
        self.assertSensor("s1", 200, "s1v2")
        self.assertSensor("s2", 00, "s1v1")
        self.assertInstance("i1", launch_id="l1", allocation="big",
                  site="cleveland", state=InstanceStates.PENDING)
        self.assertInstance("i2", launch_id="l2", allocation="big",
                  site="cleveland", state=InstanceStates.RUNNING)

    @defer.inlineCallbacks
    def test_recover_nothing(self):

        #ensure recover() works when the store is empty

        yield self.state.recover()
        self.assertEqual(len(self.state.instances), 0)
        self.assertEqual(len(self.state.sensors), 0)

        
class CassandraControllerCoreStateStoreTests(ControllerStateStoreTests):
    """ControllerCoreState tests that can use either storage implementation.

    Subclassed to use cassandra.
    """
    def get_store(self):
        return self.get_cassandra_store()

    @itv(CONF)
    def get_cassandra_store(self):
        controller_name = str(uuid.uuid4())[:8]
        store = CassandraControllerStore(controller_name,
                                         "localhost",
                                         9160,
                                         "ooiuser",
                                         "oceans11",
                                         "ControllerTests",
                                         CoreInstance,
                                         SensorItem)
        store.initialize()
        store.activate()
        return defer.succeed(store)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.store.terminate()


class ControllerCoreStateTests(BaseControllerStateTests):
    """ControllerCoreState tests that only use in memory store

    They test things basically peripheral to actual persistence.
    """
    def setUp(self):
        self.store = ControllerStore()
        self.state = ControllerCoreState(self.store)

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
        self.assertEqual(es.instances[instance_id1].state,
                         InstanceStates.RUNNING)
        self.assertEqual(es.instances[instance_id2].state,
                         InstanceStates.REQUESTING)
        self.assertEqual(len(es.sensor_changes), 0)
        self.assertEqual(es.sensors["s1"].value, "b")
        self.assertEqual(es.sensors["s2"].value, "a")

    @defer.inlineCallbacks
    def test_out_of_order_instance(self):
        launch_id, instance_id = yield self.new_instance(5)
        yield self.new_instance_state(launch_id, instance_id,
                                      InstanceStates.STARTED, 6)

        # instances cannot go back in state
        yield self.new_instance_state(launch_id, instance_id,
                                      InstanceStates.REQUESTED, 6)

        self.assertEqual(self.state.instances[instance_id].state,
                         InstanceStates.STARTED)

    @defer.inlineCallbacks
    def test_out_of_order_sensor(self):
        sensor_id = "sandwich_meter" # how many sandwiches??

        msg = dict(sensor_id=sensor_id, time=100, value=100)
        yield self.state.new_sensor_item(msg)

        msg = dict(sensor_id=sensor_id, time=90, value=200)
        yield self.state.new_sensor_item(msg)

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


class FakeProvisionerClient(object):
    pass

class FakeEngine(object):

    def __init__(self):
        self.initialize_count = 0
        self.decide_count = 0
        self.reconfigure_count = 0

    def initialize(self, *args):
        self.initialize_count += 1

    def decide(self, *args):
        self.decide_count += 1
        
    def reconfigure(self, *args):
        self.reconfigure_count += 1


class FakeControllerState(object):
    def __init__(self):
        self.recover_count = 0

    def recover(self):
        self.recover_count += 1
        return defer.succeed(None)