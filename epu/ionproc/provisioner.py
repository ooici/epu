#!/usr/bin/env python

import ion.util.ionlog
from ion.util.state_object import BasicLifecycleObject

log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer #, reactor
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.process.process import ProcessFactory
from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc
from ion.core import ioninit

from epu.provisioner.store import ProvisionerStore, CassandraProvisionerStore
from epu.provisioner.core import ProvisionerCore
from epu.ionproc.dtrs import DeployableTypeRegistryClient
from epu import cei_events

class ProvisionerService(ServiceProcess):
    """Provisioner service interface
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='provisioner', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        cei_events.event("provisioner", "init_begin", log)
        store = self.spawn_args.get('store')

        if not store:
            store = yield self._get_cassandra_store()

        if not store:
            log.info("Using in-memory Provisioner store")
            store = ProvisionerStore()

        self.store = store

        notifier = self.spawn_args.get('notifier')
        self.notifier = notifier or ProvisionerNotifier(self)
        
        self.dtrs = DeployableTypeRegistryClient(self)

        kwargs = {}
        for k in ('nimbus_key', 'nimbus_secret', 'ec2_key', 'ec2_secret'):
            kwargs[k] = self.spawn_args.get(k)

        self.core = ProvisionerCore(self.store, self.notifier, self.dtrs,
                                    **kwargs)
        cei_events.event("provisioner", "init_end", log)

        # operator can disable new launches
        self.enabled = True

    def slc_terminate(self):
        if self.store and isinstance(self.store, BasicLifecycleObject):
            log.debug("Terminating store process")
            self.store.terminate()


    @defer.inlineCallbacks
    def _get_cassandra_store(self):
        info = self.spawn_args.get('cassandra_store')
        if not info:
            defer.returnValue(None)

        host = info['host']
        port = int(info['port'])
        username = info['username']
        password = info['password']
        keyspace = info['keyspace']
        prefix = info.get('prefix')

        log.info('Using Cassandra Provisioner store')
        store = CassandraProvisionerStore(host, port, username, password,
                                          prefix=prefix)
        store.initialize()
        store.activate()
        yield store.assure_schema(keyspace)
        defer.returnValue(store)

    @defer.inlineCallbacks
    def op_provision(self, content, headers, msg):
        """Service operation: Provision a taskable resource
        """
        log.debug("op_provision content:"+str(content))

        if not self.enabled:
            log.error('Provisioner is DISABLED. Ignoring provision request!')
            defer.returnValue(None)

        launch, nodes = yield self.core.prepare_provision(content)

        # now we can ACK the request as it is safe in datastore

        # set up a callLater to fulfill the request after the ack. Would be
        # cleaner to have explicit ack control.
        #reactor.callLater(0, self.core.execute_provision_request, launch, nodes)
        yield self.core.execute_provision(launch, nodes)

    @defer.inlineCallbacks
    def op_terminate_nodes(self, content, headers, msg):
        """Service operation: Terminate one or more nodes
        """
        log.debug('op_terminate_nodes content:'+str(content))

        #expecting one or more node IDs
        if not isinstance(content, list):
            content = [content]

        yield self.core.mark_nodes_terminating(content)

        #reactor.callLater(0, self.core.terminate_nodes, content)
        yield self.core.terminate_nodes(content)

    @defer.inlineCallbacks
    def op_terminate_launches(self, content, headers, msg):
        """Service operation: Terminate one or more launches
        """
        log.debug('op_terminate_launches content:'+str(content))

        #expecting one or more launch IDs
        if not isinstance(content, list):
            content = [content]

        for launch in content:
            yield self.core.mark_launch_terminating(launch)

        #reactor.callLater(0, self.core.terminate_launches, content)
        yield self.core.terminate_launches(content)

    @defer.inlineCallbacks
    def op_query(self, content, headers, msg):
        """Service operation: query IaaS  and send updates to subscribers.
        """
        # immediate ACK is desired
        #reactor.callLater(0, self.core.query_nodes, content)
        yield self.core.query_nodes(content)
        if msg:
            yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_terminate_all(self, content, headers, msg):
        """Service operation: terminate all running instances
        """
        log.critical('Disabling provisioner, future requests will be ignored')
        self.enabled = False

        yield self.core.terminate_all()



class ProvisionerClient(ServiceClient):
    """
    Client for provisioning deployable types
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "provisioner"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def provision(self, launch_id, deployable_type, launch_description, vars=None):
        """Provisions a deployable type
        """
        yield self._check_init()

        nodes = {}
        for nodename, item in launch_description.iteritems():
            nodes[nodename] = {'ids' : item.instance_ids,
                    'site' : item.site,
                    'allocation' : item.allocation_id,
                    'data' : item.data}
        sa = yield self.proc.get_scoped_name('system', 'sensor_aggregator')
        request = {'deployable_type' : deployable_type,
                'launch_id' : launch_id,
                'nodes' : nodes,
                'subscribers' : [sa],
                'vars' : vars}
        log.debug('Sending provision request: ' + str(request))
        yield self.send('provision', request)

    @defer.inlineCallbacks
    def query(self, rpc=False):
        """Triggers a query operation in the provisioner. Node updates
        are not sent in reply, but are instead sent to subscribers
        (most likely a sensor aggregator).
        """
        yield self._check_init()
        log.debug('Sending query request to provisioner')
        
        # optionally send query in rpc-style, in which case this method's 
        # Deferred will not be fired util provisioner has a response from
        # all underlying IaaS. Right now this is only used in tests.
        if rpc:
            yield self.rpc_send('query', None)
        else:
            yield self.send('query', None)

    @defer.inlineCallbacks
    def terminate_launches(self, launches):
        """Terminates one or more launches
        """
        yield self._check_init()
        log.debug('Sending terminate_launches request to provisioner')
        yield self.send('terminate_launches', launches)

    @defer.inlineCallbacks
    def terminate_nodes(self, nodes):
        """Terminates one or more nodes
        """
        yield self._check_init()
        log.debug('Sending terminate_nodes request to provisioner')
        yield self.send('terminate_nodes', nodes)

    @defer.inlineCallbacks
    def terminate_all(self):
        """Terminate all running nodes and disable provisioner
        """
        yield self._check_init()
        log.critical('Sending terminate_all request to provisioner')
        yield self.send('terminate_all', None)


class ProvisionerNotifier(object):
    """Abstraction for sending node updates to subscribers.
    """
    def __init__(self, process):
        self.process = process

    @defer.inlineCallbacks
    def send_record(self, record, subscribers, operation='node_status'):
        """Send a single node record to all subscribers.
        """
        log.debug('Sending status record about node %s to %s',
                record['node_id'], repr(subscribers))
        for sub in subscribers:
            yield self.process.send(sub, operation, record)

    @defer.inlineCallbacks
    def send_records(self, records, subscribers, operation='node_status'):
        """Send a set of node records to all subscribers.
        """
        for rec in records:
            yield self.send_record(rec, subscribers, operation)

# Spawn of the process using the module name
factory = ProcessFactory(ProvisionerService)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('EPU Provisioner starting, startup type "%s"' % starttype)

    conf = ioninit.config(__name__)


    # Required services.
    proc = [{'name': 'provisioner',
             'module': __name__,
             'class': ProvisionerService.__name__,
             'spawnargs': {
                 'nimbus_key' : conf['nimbus_key'],
                 'nimbus_secret' : conf.getValue['nimbus_secret'],
                 'ec2_key' : conf['ec2_key'],
                 'ec2_secret' : conf.getValue['ec2_secret'],

                 #TODO add logic to grab cassandra info from config
                 }
            },
    ]

    app_supv_desc = ProcessDesc(name='Provisioner app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':proc})

    supv_id = yield app_supv_desc.spawn()

    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('EPU Provisioner stopping, state "%s"' % str(state))
    supdesc = state[0]
    # Return the deferred
    return supdesc.terminate()
