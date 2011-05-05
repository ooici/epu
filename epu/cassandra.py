import sys

from twisted.internet import reactor, defer

from telephus.cassandra.ttypes import NotFoundException, KsDef
from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory

from ion.core import ioninit

class CassandraSchemaManager(object):
    """Manages creation and destruction of cassandra schemas.

    Useful for both testing and production
    """

    def __init__(self, keyspace_def, error_if_existing=False):
        self.keyspace_def = keyspace_def
        self.error_if_existing=error_if_existing
        self.created_keyspace = False
        self.created_cfs = []

        self.client = None
        self.manager = None
        self.connector = None

    def connect(self, host=None, port=9160, username=None, password=None):
        if not host:
            host, port = get_host_port()

        if username or password:
            if not (username and password):
                raise CassandraConfigurationError(
                    "Specify both username and password or neither")
        else:
            username, password = get_credentials()
        authz = dict(username=username, password=password)

        self.manager = ManagedCassandraClientFactory(credentials=authz,
                                                     check_api_version=True)
        self.connector = reactor.connectTCP(host, port, self.manager)
        self.client = CassandraClient(self.manager)

    def disconnect(self):
        if self.manager:
            self.manager.shutdown()
        if self.connector:
            self.connector.disconnect()

    @defer.inlineCallbacks
    def create(self):
        if not self.client:
            self.connect()

        keyspace = self.keyspace_def

        try:
            existing = yield self.client.describe_keyspace(keyspace.name)
        except NotFoundException:
            existing = None

        # keyspace already exists
        if existing:
            yield self.client.set_keyspace(keyspace.name)
            _compare_ks_properties(existing, keyspace)

            existing_cfs = dict((cf.name, cf) for cf in existing.cf_defs)

            for cf in keyspace.cf_defs:
                if cf.name in existing_cfs:
                    _compare_cf_properties(existing_cfs[cf.name], cf)
                else:
                    if cf.keyspace != keyspace.name:
                        raise CassandraConfigurationError(
                            "CF %s has wrong keyspace name", cf.name)
                    self.created_cfs.append(cf.name)
                    yield self.client.system_add_column_family(cf)
        else:
            self.created_keyspace = True
            yield self.client.system_add_keyspace(keyspace)
            yield self.client.set_keyspace(keyspace.name)

    @defer.inlineCallbacks
    def teardown(self):
        if self.created_keyspace:
            yield self.client.system_drop_keyspace(self.keyspace_def.name)

        elif self.created_cfs:
            for cf in self.created_cfs:
                yield self.client.system_drop_column_family(cf)


def _compare_ks_properties(existing, desired):
    for prop in ('name', 'strategy_class', 'replication_factor'):
        desired_val = getattr(desired, prop)
        actual_val = getattr(existing, prop)
        if desired_val is not None and actual_val != desired_val:
            raise CassandraConfigurationError(
                "Cannot modify existing keyspace. %s differs." % prop)

def _compare_cf_properties(existing, desired):
    for prop in ('column_type', 'comparator_type', 'subcomparator_type',
                 'comment', 'default_validation_class'):
        desired_val = getattr(desired, prop)
        actual_val = getattr(existing, prop)
        if desired_val is not None and actual_val != desired_val:
            raise CassandraConfigurationError(
                "Cannot modify existing column family. %s differs: %s vs %s"
                %(prop, desired_val, actual_val))

        
class CassandraConfigurationError(Exception):
    """Error setting up cassandra connection or schema
    """
    def __str__(self):
        return str(self.args[0])

CONF = None
CONF_NAME = "epu.cassandra"
def _init_config():
    global CONF
    if CONF is None:
        CONF = ioninit.config(CONF_NAME)

def get_credentials():
    _init_config()

    try:
        username = CONF['username']
        password = CONF['password']
    except KeyError:
        raise CassandraConfigurationError("Missing Cassandra credentials")
    return username, password

def get_host_port():
    _init_config()

    host = CONF.getValue('hostname')
    if not host:
        raise CassandraConfigurationError("Missing Cassandra hostname")

    port = CONF.getValue('port', 9160)
    try:
        port = int(port)
    except ValueError:
        raise CassandraConfigurationError("Invalid Cassandra port: %s" % port)
    return host,port

def get_keyspace_name():
    _init_config()

    keyspace = CONF.getValue('keyspace')
    if not keyspace:
        raise CassandraConfigurationError("Missing Cassandra keyspace")

    return keyspace

def get_keyspace(cf_defs, name=None):
    if not name:
        name = get_keyspace_name()
    for cf in cf_defs:
        cf.keyspace = name
    return KsDef(name, replication_factor=1, cf_defs=cf_defs,
                 strategy_class="org.apache.cassandra.locator.SimpleStrategy")

def get_epu_keyspace_definition():
    """Gathers column family definitions from EPU components
    """
    name = get_keyspace_name()

    from epu.provisioner.store import CassandraProvisionerStore
    provisioner_cfs = CassandraProvisionerStore.get_column_families(name)

    from epu.epucontroller.controller_store import CassandraControllerStore
    controller_cfs = CassandraControllerStore.get_column_families(name)

    all_cfs = []
    all_cfs.extend(provisioner_cfs)
    all_cfs.extend(controller_cfs)

    return get_keyspace(all_cfs)

@defer.inlineCallbacks
def run_schematool():
    global exit_status
    exit_status = 1
    mgr = None
    try:
        ks_def = get_epu_keyspace_definition()
        mgr = CassandraSchemaManager(ks_def)
        yield mgr.create()
        exit_status = 0
    except CassandraConfigurationError,e:
        print >>sys.stderr, "Problem wih cassandra setup: %s" % e
    finally:
        try:
            if mgr:
                yield mgr.disconnect()
        finally:
            reactor.callLater(0, shut_it_down)

def shut_it_down():
    if reactor.running:
        reactor.stop()

def main():
    global exit_status
    exit_status = 1
    # creates schema for epu controller and provisioner
    run_schematool()
    reactor.run()
    return exit_status

if __name__ == '__main__':
    sys.exit(main())