from collections import defaultdict
import uuid
import struct
import simplejson as json

from ion.util.tcp_connections import TCPConnection
from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory
from twisted.internet import defer


class ControllerStore(object):
    """In memory "persistence" for EPU Controller state

    The same interface is used for real persistence.
    """

    def __init__(self):
        self.instances = defaultdict(list)
        self.sensors = defaultdict(list)

    def add_instance(self, instance):
        """Adds a new instance object to persistence
        @param instance Instance to add
        @retval Deferred
        """
        instance_id = instance.instance_id
        self.instances[instance_id].append(instance_id)
        return defer.succeed(None)

    def get_instance_ids(self):
        """Retrieves a list of known instances

        @retval Deferred of list of instance IDs
        """
        return defer.succeed(self.instances.keys())

    def get_instance(self, instance_id):
        """Retrieves the latest instance object for the specified id
        @param instance_id ID of instance to retrieve
        @retval Deferred of Instance object or None
        """
        return defer.succeed(self.instances.get(instance_id))

    def add_sensor(self, sensor):
        """Adds a new sensor object to persistence
        @param sensor Sensor to add
        @retval Deferred
        """
        sensor_id = sensor.sensor_id
        self.sensors[sensor_id].append(sensor_id)
        return defer.succeed(None)

    def get_sensor_ids(self):
        """Retrieves a list of known sensors

        @retval Deferred of list of sensor IDs
        """
        return defer.succeed(self.sensors.keys())

    def get_sensor(self, sensor_id):
        """Retrieve the latest sensor item for the specified sensor

        @param sensor_id ID of the sensor item to retrieve
        @retval Deferred of SensorItem object or None
        """
        return defer.succeed(self.sensors.get(sensor_id))


class CassandraControllerStore(TCPConnection):
    """Cassandra persistence for EPU controller state

    All EPU controllers within a system share the same column families.


    The "known" CFs hold a list of known instance IDs and sensor IDs for each
    EPU controller. The value is irrelevant and inserts are of course
    idempotent. These are used for figuring out all information about a
    controller without walking the entire instances and sensors CFs.
    
    ControllerKnownInstances = {
        Controller1 = {
            instance_id_1,
            instance_id_2
        },
        Controller2 = {
            instance_id_1,
            instance_id_2
        }
    }

    ControllerKnownSensors = {
        Controller1 = {
            sensor_1,
            sensor_2
        },
        Controller2 = {
            sensor_1,
            sensor_2
        }
    }

    All records for an instance are held in a single row. The column keys
    are TimeUUIDs. Because instance records have more complicated ordering
    than time, it is necessary to ensure a record is actually new before
    inserting. This is safe since in the current architecture there is only
    one writer for controller.

    One limitation is that out-of-order records cannot be inserted in the
    store. So if the messaging layer provides the STARTED state record for
    an instance *after* the RUNNING record, it cannot be stored and must
    be dropped. The correct state will be preserved, but not all of history
    will be.

    ControllerInstances = {               #comparator = TimeUUIDType
        Controller1Instance1 = {
            TimeUUID1 : 'the actual record',
            TimeUUID2 : 'the actual record',
            TimeUUID3 : 'the actual record',
        },
        Controller2Instance1 = {
            TimeUUID1 : 'the actual record',
            TimeUUID2 : 'the actual record',
            TimeUUID3 : 'the actual record',
        }
    }


    Sensor records are stored similarly. Instead of a TimeUUID, they use
    longs as keys which are likely to be a timestamp. Again, the controller
    must check the timestamp and not treat the most recently arrived value
    as the latest. However it can still write older values as they will be
    correctly inserted into history.

    ControllerSensors = {                 #comparator = LongType
        Controller1Sensor1 = {
            timestamp1 : 'sensor message',
            timestamp2 : 'sensor message'
        }
    }
    """

    def __init__(self, controller_name, host, port, username, password,
                 keyspace, instance_factory, sensor_item_factory,
                 instance_cf="ControllerInstances",
                 instance_id_cf="ControllerKnownInstances",
                 sensor_cf="ControllerSensors",
                 sensor_id_cf="ControllerKnownSensors"):

        self.controller_name = str(controller_name)
        # keep a set of known instances and sensors so we can save on
        # unnecessary inserts to the controller instance/sensor lists
        self.seen_instances = set()
        self.seen_sensors = set()

        self.instance_factory = instance_factory
        self.sensor_item_factory = sensor_item_factory

        authorization_dictionary = {'username': username, 'password': password}

        self.keyspace = keyspace
        self.created_keyspace = False
        self.created_column_families = False
        ### Create the twisted factory for the TCP connection
        self.manager = ManagedCassandraClientFactory(
                credentials=authorization_dictionary,
                check_api_version=True, keyspace=keyspace)

        # Call the initialization of the Managed TCP connection base class
        TCPConnection.__init__(self, host, port, self.manager)
        self.client = CassandraClient(self.manager)

        self.instance_cf = instance_cf
        self.instance_id_cf = instance_id_cf
        self.sensor_cf = sensor_cf
        self.sensor_id_cf = sensor_id_cf

    @defer.inlineCallbacks
    def add_instance(self, instance):
        """Adds a new instance object to persistence
        @param instance Instance to add
        @retval Deferred
        """

        instance_id = str(instance.instance_id)
        if instance_id not in self.seen_instances:
            yield self.client.insert(self.controller_name, self.instance_id_cf,
                                     "", column=instance_id)

        key = self.controller_name + instance_id
        value = json.dumps(dict(instance.iteritems()))
        col = uuid.uuid1().bytes
        yield self.client.insert(key, self.instance_cf, value, column=col)

    @defer.inlineCallbacks
    def get_instance_ids(self):
        """Retrieves a list of known instances

        @retval Deferred of list of instance IDs
        """
        slice = yield self.client.get_slice(self.controller_name,
                                            self.instance_id_cf)
        if slice:
            ret = [col.column.name for col in slice]
        else:
            ret = []
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def get_instance(self, instance_id):
        """Retrieves the latest instance object for the specified id
        @param instance_id ID of instance to retrieve
        @retval Deferred of Instance object or None
        """

        key = self.controller_name + str(instance_id)
        slice = yield self.client.get_slice(key, self.instance_cf,
                                          reverse=True, count=1)

        if slice and slice.columns:
            d = json.loads(slice.columns[0].value)
            ret = self.instance_factory(**d)
        else:
            ret = None
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def add_sensor(self, sensor):
        """Adds a new sensor object to persistence
        @param sensor Sensor to add
        @retval Deferred
        """

        sensor_id = str(sensor.sensor_id)
        if sensor_id not in self.seen_sensors:
            yield self.client.insert(self.controller_name, self.sensor_id_cf,
                                     "", column=sensor_id)

        key = self.controller_name + sensor_id
        value = json.dumps(sensor.value)
        col = struct.pack('!Q', int(sensor.value))
        yield self.client.insert(key, self.sensor_cf, value, column=col)

    @defer.inlineCallbacks
    def get_sensor_ids(self):
        """Retrieves a list of known sensors

        @retval Deferred of list of sensor IDs
        """
        slice = yield self.client.get_slice(self.controller_name,
                                            self.sensor_id_cf)

        if slice:
            ret = [col.column.name for col in slice]
        else:
            ret = []
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def get_sensor(self, sensor_id):
        """Retrieve the latest sensor item for the specified sensor

        @param sensor_id ID of the sensor item to retrieve
        @retval Deferred of SensorItem object or None
        """
        key = self.controller_name + str(sensor_id)
        slice = yield self.client.get_slice(key, self.sensor_cf,
                                          reverse=True, count=1)

        if slice and slice.columns:
            col = slice.columns[0]
            val = json.loads(col.value)
            ret = self.sensor_item_factory(sensor_id, int(col.timestamp), val)
        else:
            ret = None

        defer.returnValue(ret)

    def on_deactivate(self, *args, **kwargs):
        self.manager.shutdown()

    def on_terminate(self, *args, **kwargs):
        self.manager.shutdown()
