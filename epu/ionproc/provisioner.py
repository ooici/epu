#!/usr/bin/env python

import ion.util.ionlog


from twisted.internet import defer #, reactor

from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.process.process import ProcessFactory
from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc
from ion.core import ioninit

from epu.util import get_class
from epu.provisioner.store import ProvisionerStore, CassandraProvisionerStore
from epu.provisioner.core import ProvisionerCore, ProvisionerContextClient
from epu.ionproc.dtrs import DeployableTypeRegistryClient
from epu import cei_events
from epu import states

log = ion.util.ionlog.getLogger(__name__)

class ProvisionerService(ServiceProcess):
    """Provisioner service interface
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='provisioner', version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):
        cei_events.event("provisioner", "init_begin", log)

        try:
            store = self.spawn_args['store']
            site_drivers = self.spawn_args['site_drivers']
            context_client = self.spawn_args['context_client']
        except KeyError,e:
            raise KeyError("Missing provisioner spawn_arg: " + str(e))

        self.store = store

        notifier = self.spawn_args.get('notifier')
        self.notifier = notifier or ProvisionerNotifier(self)
        self.dtrs = DeployableTypeRegistryClient(self)

        self.core = ProvisionerCore(self.store, self.notifier, self.dtrs,
                                    site_drivers, context_client)
        yield self.core.recover()
        cei_events.event("provisioner", "init_end", log)

        # operator can disable new launches
        self.enabled = True

    def slc_terminate(self):
        if self.store and hasattr(self.store, "disconnect"):
            log.debug("Terminating store process")
            self.store.disconnect()

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

        if launch['state'] != states.FAILED:
            yield self.core.execute_provision(launch, nodes)
        else:
            log.warn("Launch %s couldn't be prepared, not executing",
                     launch['launch_id'])

    @defer.inlineCallbacks
    def op_terminate_nodes(self, content, headers, msg):
        """Service operation: Terminate one or more nodes
        """
        log.debug('op_terminate_nodes content:'+str(content))

        yield self.core.mark_nodes_terminating(content)

        #reactor.callLater(0, self.core.terminate_nodes, content)
        yield self.core.terminate_nodes(content)

    @defer.inlineCallbacks
    def op_terminate_launches(self, content, headers, msg):
        """Service operation: Terminate one or more launches
        """
        log.debug('op_terminate_launches content:'+str(content))

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
        yield self.core.query(content)
        if msg:
            yield self.reply_ok(msg)

    @defer.inlineCallbacks
    def op_terminate_all(self, content, headers, msg):
        """Service operation: terminate all running instances
        """
        log.critical('Terminate all initiated.')
        log.critical('Disabling provisioner, future requests will be ignored')
        self.enabled = False

        yield self.core.terminate_all()

    @defer.inlineCallbacks
    def op_terminate_all_rpc(self, content, headers, msg):
        """Service operation: terminate all running instances if that has not been initiated yet.
        Return True if all running instances have been terminated.
        """
        if self.enabled:
            log.critical('Terminate all RPC initiated.')
            log.critical('Disabling provisioner, future requests will be ignored')
            self.enabled = False
            yield self.core.terminate_all()
        else:
            log.critical('Terminate all RPC checkup.')

        state = yield self.core.check_terminate_all()
        yield self.reply_ok(msg, state)

    @defer.inlineCallbacks
    def op_dump_state(self, content, headers, msg):
        """Service operation: (re)send state information to subscribers
        """
        nodes = content.get('nodes')
        if not nodes:
            log.error("Got dump_state request without a nodes list")
        else:
            yield self.core.dump_state(nodes)


class ProvisionerClient(ServiceClient):
    """
    Client for provisioning deployable types
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "provisioner"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def provision(self, launch_id, deployable_type, launch_description,
                  subscribers, vars=None):
        """Provisions a deployable type
        """
        yield self._check_init()

        nodes = {}
        for nodename, item in launch_description.iteritems():
            nodes[nodename] = {'ids' : item.instance_ids,
                    'site' : item.site,
                    'allocation' : item.allocation_id,
                    'data' : item.data}

        request = {'deployable_type' : deployable_type,
                'launch_id' : launch_id,
                'nodes' : nodes,
                'subscribers' : subscribers,
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
    def terminate_all(self, rpcwait=False):
        """Terminate all running nodes and disable provisioner
        If rpcwait is True, the operation returns a True/False response whether or not all nodes have been terminated yet
        """
        yield self._check_init()
        if not rpcwait:
            log.critical('Sending terminate_all request to provisioner')
            yield self.send('terminate_all', None)
        else:
            terminated = False
            while not terminated:
                log.critical('Sending terminate_all RPC request to provisioner')
                (terminated, headers, msg) = yield self.rpc_send('terminate_all_rpc', None)
                log.critical('All terminated: %s' % terminated)

    @defer.inlineCallbacks
    def dump_state(self, nodes):
        """
        """
        yield self._check_init()
        log.debug('Sending dump_state request to provisioner')
        yield self.send('dump_state', dict(nodes=nodes))


class ProvisionerNotifier(object):
    """Abstraction for sending node updates to subscribers.
    """
    def __init__(self, process):
        self.process = process

    @defer.inlineCallbacks
    def send_record(self, record, subscribers, operation='instance_state'):
        """Send a single node record to all subscribers.
        """
        log.debug('Sending state %s record for node %s to %s',
                record['state'], record['node_id'], repr(subscribers))
        for sub in subscribers:
            yield self.process.send(sub, operation, record)

    @defer.inlineCallbacks
    def send_records(self, records, subscribers, operation='instance_state'):
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

    proc = [{'name': 'provisioner',
             'module': __name__,
             'class': ProvisionerService.__name__,
             'spawnargs': {
                 'query_period' : conf.getValue('query_period'),
                 'store' : get_provisioner_store(conf),
                 'site_drivers' : get_site_drivers(conf.getValue('sites')),
                 'context_client' : get_context_client(conf)}}]

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

def get_cassandra_store(host, username, password, keyspace, port=None, prefix=""):
    store = CassandraProvisionerStore(host, port or 9160, username, password,
                                      keyspace, prefix)
    store.connect()
    return store

def get_provisioner_store(conf):
    cassandra_host = conf.getValue('cassandra_hostname')
    if cassandra_host:
        log.info("Using cassandra store. host: %s", cassandra_host)
        try:
            store = get_cassandra_store(cassandra_host,
                                        conf['cassandra_username'],
                                        conf['cassandra_password'],
                                        conf.getValue('cassandra_keyspace'),
                                        conf.getValue('cassandra_port'))
        except KeyError,e:
            raise KeyError("Provisioner config missing: " + str(e))
    else:
        log.info("Using in-memory Provisioner store")
        store = ProvisionerStore()
    return store

def get_context_client(conf):
    try:
        return ProvisionerContextClient(conf['context_uri'],
                                        conf['context_key'],
                                        conf['context_secret'])
    except KeyError,e:
        raise KeyError("Provisioner config missing: " + str(e))

def get_site_drivers(site_config):
    """Loads a dict of IaaS drivers from a config block
    """
    if site_config is None or not isinstance(site_config, dict):
        raise ValueError("expecting dict of IaaS driver configs")
    drivers = {}
    for site, spec in site_config.iteritems():
        try:
            cls_name = spec["driver_class"]
            cls_kwargs = spec["driver_kwargs"]
            log.debug("Loading IaaS driver %s", cls_name)
            cls = get_class(cls_name)
            driver = cls(**cls_kwargs)
            drivers[site] = driver
        except KeyError,e:
            raise KeyError("IaaS site description '%s' missing key '%s'" % (site, str(e)))
    return drivers
