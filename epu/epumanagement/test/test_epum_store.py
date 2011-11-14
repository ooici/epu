import uuid
from twisted.internet import defer
from twisted.trial import unittest
from epu.decisionengine.impls.simplest import CONF_PRESERVE_N
from epu.epumanagement.core import CoreInstance
from epu.epumanagement.forengine import SensorItem
from epu.epumanagement.store import EPUMStore, ControllerStore
from epu.epumanagement.conf import *
import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)

class EPUStoreBasicTests(unittest.TestCase):

    def setUp(self):
        initial_conf = {EPUM_INITIALCONF_PERSISTENCE:"memory"}
        self.store = EPUMStore(initial_conf)

    @defer.inlineCallbacks
    def test_simple_add(self):
        epu_config = {}
        yield self.store.create_new_epu("caller01", "testing01", epu_config)
        epu = yield self.store.get_epu_state("testing01")
        self.assertEqual("testing01", epu.epu_name)
        self.assertEqual("caller01", epu.creator)

        # try to create again, should be name clash
        self.assertFailure(self.store.create_new_epu("caller01", "testing01", epu_config), ValueError)

    @defer.inlineCallbacks
    def test_epu_configs(self):
        """
        Create one EPU with a certain configuration.  Test that initial conf and
        later conf additions work properly.
        """
        engine_class = "epu.decisionengine.impls.simplest.SimplestEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        engine = {CONF_PRESERVE_N:2, }
        epu_config = {EPUM_CONF_GENERAL:general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}
        yield self.store.create_new_epu(None, "testing02", epu_config)
        epu = yield self.store.get_epu_state("testing02")

        general_out = yield epu.get_general_conf()
        self.assertTrue(isinstance(general_out, dict))
        self.assertTrue(general_out.has_key(EPUM_CONF_ENGINE_CLASS))
        self.assertEqual(engine_class, general_out[EPUM_CONF_ENGINE_CLASS])

        engine_out = yield epu.get_engine_conf()
        self.assertTrue(isinstance(engine_out, dict))
        self.assertTrue(engine_out.has_key(CONF_PRESERVE_N))
        self.assertEqual(2, engine_out[CONF_PRESERVE_N])

        health_out = yield epu.get_health_conf()
        self.assertTrue(isinstance(health_out, dict))
        self.assertTrue(health_out.has_key(EPUM_CONF_HEALTH_MONITOR))
        self.assertEqual(False, health_out[EPUM_CONF_HEALTH_MONITOR])
        health_enabled = yield epu.is_health_enabled()
        self.assertFalse(health_enabled)

class ControllerStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = ControllerStore()

    @defer.inlineCallbacks
    def test_config(self):
        empty = yield self.store.get_config()
        self.assertIsInstance(empty, dict)
        self.assertFalse(empty)

        empty = yield self.store.get_config(keys=('not','real', 'keys'))
        self.assertIsInstance(empty, dict)
        self.assertFalse(empty)

        yield self.store.add_config({'a_string' : 'thisisastring',
                                     'a_list' : [1,2,3], 'a_number' : 1.23})
        cfg = yield self.store.get_config(keys=['a_string'])
        self.assertEqual(cfg, {'a_string' : 'thisisastring'})

        cfg = yield self.store.get_config()
        self.assertEqual(cfg, {'a_string' : 'thisisastring',
                                     'a_list' : [1,2,3], 'a_number' : 1.23})

        yield self.store.add_config({'a_dict' : {"akey": {'fpp' : 'bar'}, "blah" : 5},
                                     "a_list" : [4,5,6]})

        cfg = yield self.store.get_config()
        self.assertEqual(cfg, {'a_string' : 'thisisastring',
                                     'a_list' : [4,5,6], 'a_number' : 1.23,
                                     'a_dict' : {"akey": {'fpp' : 'bar'}, "blah" : 5}})

        cfg = yield self.store.get_config(keys=('a_list', 'a_number'))
        self.assertEqual(cfg, {'a_list' : [4,5,6], 'a_number' : 1.23})

    @defer.inlineCallbacks
    def test_instances_put_get_3(self):
        yield self._instances_put_get(3)

    @defer.inlineCallbacks
    def test_instances_put_get_100(self):
        yield self._instances_put_get(100)

    @defer.inlineCallbacks
    def test_instances_put_get_301(self):
        yield self._instances_put_get(301)

    @defer.inlineCallbacks
    def _instances_put_get(self, count):
        instances = []
        instance_ids = set()
        for i in range(count):
            instance = CoreInstance(instance_id=str(uuid.uuid4()), launch_id=str(uuid.uuid4()),
                                    site="Chicago", allocation="small", state="Illinois")
            instances.append(instance)
            instance_ids.add(instance.instance_id)
            yield self.store.add_instance(instance)

        found_ids = yield self.store.get_instance_ids()
        found_ids = set(found_ids)
        log.debug("Put %d instances, got %d instance IDs", count, len(found_ids))
        self.assertEqual(len(found_ids), len(instance_ids))
        self.assertEqual(found_ids, instance_ids)

        # could go on to verify each instance record

    @defer.inlineCallbacks
    def test_sensors_put_get_3(self):
        yield self._sensors_put_get(3)

    @defer.inlineCallbacks
    def test_sensors_put_get_100(self):
        yield self._sensors_put_get(100)

    @defer.inlineCallbacks
    def test_sensors_put_get_301(self):
        yield self._sensors_put_get(301)

    @defer.inlineCallbacks
    def _sensors_put_get(self, count):
        sensors = []
        sensor_ids = set()
        for i in range(count):
            sensor = SensorItem(str(uuid.uuid4()), i, str(i))
            sensors.append(sensor)
            sensor_ids.add(sensor.sensor_id)
            yield self.store.add_sensor(sensor)

        found_ids = yield self.store.get_sensor_ids()
        found_ids = set(found_ids)
        log.debug("Put %d sensors, got %d sensor IDs", count, len(found_ids))
        self.assertEqual(len(found_ids), len(sensor_ids))
        self.assertEqual(found_ids, sensor_ids)
