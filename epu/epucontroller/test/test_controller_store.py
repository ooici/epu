import uuid

from twisted.internet import defer

from epu import cassandra
from epu.epucontroller.controller_core import CoreInstance
from epu.epucontroller.controller_store import CassandraControllerStore
from epu.epucontroller.forengine import SensorItem

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

