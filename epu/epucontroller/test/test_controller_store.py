import uuid

from twisted.internet import defer
from twisted.trial.unittest import TestCase

import ion.util.ionlog

from epu import cassandra
from epu.epucontroller.controller_core import CoreInstance
from epu.epucontroller.controller_store import CassandraControllerStore, ControllerStore
from epu.epucontroller.forengine import SensorItem
from epu.test import cassandra_test

log = ion.util.ionlog.getLogger(__name__)

class CassandraFixture(object):
    def __init__(self):
        self.store = None
        self.cassandra_manager = None

    @defer.inlineCallbacks
    def setup(self):
        cf_defs = CassandraControllerStore.get_column_families()
        ks = cassandra.get_keyspace(cf_defs)
        self.cassandra_manager = cassandra.CassandraSchemaManager(ks)

        yield self.cassandra_manager.create()

        controller_name = str(uuid.uuid4())[:8]
        host, port = cassandra.get_host_port()
        username, password = cassandra.get_credentials()
        self.store = CassandraControllerStore(controller_name, host, port,
                                              username, password, ks.name,
                                              CoreInstance, SensorItem)
        self.store.initialize()
        self.store.activate()
        defer.returnValue(self.store)

    @defer.inlineCallbacks
    def teardown(self):
        if self.store:
            self.store.terminate()

        yield self.cassandra_manager.teardown()
        self.cassandra_manager.disconnect()
        self.store  = None
        self.cassandra_manager = None


class ControllerStoreTests(TestCase):
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

class CassandraControllerStoreTests(ControllerStoreTests):

    @cassandra_test
    @defer.inlineCallbacks
    def setUp(self):
        self.cassandra_fixture = CassandraFixture()
        self.store = yield self.cassandra_fixture.setup()
        self.store._PAGE_SIZE = 100


    def tearDown(self):
        if self.cassandra_fixture:
            return self.cassandra_fixture.teardown()

