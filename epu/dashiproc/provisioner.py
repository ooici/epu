
try:
    import gevent
    import gevent.monkey
except:
    #gevent not available
    pass

from dashi import DashiConnection
from dashi.bootstrap import Service

from epu.provisioner.store import ProvisionerStore
from epu.provisioner.core import ProvisionerCore, ProvisionerContextClient

from epu import states
from epu.util import get_class

class ProvisionerService(Service):

    topic = "provisioner"

    def __init__(self, *args, **kwargs):

        super(ProvisionerService, self).__init__(*args, **kwargs)
        self.log = self.get_logger()
        self.quit = False

        self.store = self._get_provisioner_store()
        notifier = kwargs.get('notifier')
        self.notifier = notifier or ProvisionerNotifier(self)
        dtrs = kwargs.get('dtrs')
        self.dtrs = dtrs or DeployableTypeRegistryClient(self)
        context_client = self._get_context_client()
        site_drivers = self._get_site_drivers()

        try:
            self._enable_gevent()
        except:
            self.log.warning("gevent not available. Falling back to threading")

        self.core = ProvisionerCore(self.store, self.notifier, self.dtrs,
                                    site_drivers, context_client, logger=self.log)
        self.core.recover()

        self.enabled = True


    def start(self):

        # Set up operations
        self.dashi.handle(self.provision)
        self.dashi.handle(self.query)
        self.dashi.handle(self.terminate_nodes)
        self.dashi.handle(self.terminate_launches)

        self._start(methods=[self.dashi.consume], join=False)
        self._start(methods=[self.sleep])

    def _start(self, methods=[], join=True):

        threads = []
        for method in methods:
            thread = Thread(target=method)
            thread.daemon = True
            threads.append(thread)
            thread.start()
        
        if not join:
            return

        try:
            while len(threads) > 0:
                for t in threads:
                    t.join(1)
                    if not t.isAlive():
                        threads.remove(t)
                             
        except KeyboardInterrupt:
            self.log.info("%s killed" % self.__class__.__name__)

    def _start_gevent(self, methods=[], join=True):

        greenlets = []
        for method in methods:
            glet = gevent.spawn(method)
            greenlets.append(glet)

        if join:
            try:
                gevent.joinall(greenlets)
            except KeyboardInterrupt:
                gevent.killall(greenlets)

    def sleep(self):
        """sleep function to keep provisioner alive
        """
        import time
        while not self.quit:
            time.sleep(1)

    def provision(self, request):

        if not self.enabled:
            log.error('Provisioner is DISABLED. Ignoring provision request!')
            return None

        launch, nodes = self.core.prepare_provision(request)
        self.log.info("Got launch '%s' \nGot nodes '%s'" % (launch, nodes))

        if launch['state'] != states.FAILED: 
            self.core.execute_provision(launch, nodes) 
        else: 
            self.log.warn("Launch %s couldn't be prepared, not executing", 
                    launch['launch_id']) 


    def terminate_nodes(self):

        self.log.info("terminate_nodes")


    def terminate_launches(self):

        self.log.info("terminate_launches")

    def query(self):
        """Service operation: query IaaS  and send updates to subscribers.
        """
        # immediate ACK is desired
        #reactor.callLater(0, self.core.query_nodes, content)
        self.core.query()

        # TODO: implement this for dashi
        # peek into headers to determine if request is RPC. RPC is used in
        # some tests.
        #if headers and headers.get('protocol') == 'rpc':
            #self.reply_ok(msg, True)

    def terminate_all(self):
        """Service operation: terminate all running instances
        """
        self.log.critical('Terminate all initiated.')
        self.log.critical('Disabling provisioner, future requests will be ignored')
        self.enabled = False

        yield self.core.terminate_all()
        

    def terminate_all_rpc(self):

        self.log.info("terminate_all_rpc")

    def dump_state(self):

        self.log.info("dump_state")

    def _enable_gevent(self):
        """enables gevent and swaps out standard threading for gevent
        greenlet threading

        throws an exception if gevent isn't available
        """
        gevent.monkey.patch_all()
        self._start = self._start_gevent

    def _get_provisioner_store(self):

        cassandra = self.CFG.get("cassandra")
        if cassandra:
            #TODO: add support for cassandra
            raise Exception("Cassandra store not implemented yet")
        else:
            self.log.info("Using in-memory Provisioner store")
            store = ProvisionerStore()
        return store

    def _get_context_client(self):
        try:
            return ProvisionerContextClient(self.CFG.context.uri,
                                            self.CFG.context.key,
                                            self.CFG.context.secret)
        except AttributeError,e:
            raise AttributeError("Provisioner config missing: " + str(e))

    def _get_site_drivers(self):
        """Loads a dict of IaaS drivers from a config block
        """
        try:
            sites = self.CFG.sites
            if not sites:
                raise ValueError("expecting dict of IaaS driver configs")
            drivers = {}
            for site, spec in sites.iteritems():
                try:
                    cls_name = spec["driver_class"]
                    cls_kwargs = spec["driver_kwargs"]
                    self.log.debug("Loading IaaS driver %s", cls_name)
                    cls = get_class(cls_name)
                    driver = cls(**cls_kwargs)
                    drivers[site] = driver
                except KeyError,e:
                    raise KeyError("IaaS site description '%s' missing key '%s'" % (site, str(e)))
        except AttributeError,e:
            raise AttributeError("Provisioner config missing: " + str(e))
        
        return drivers


class ProvisionerClient(Service):

    topic = "client"

    def __init__(self, *args, **kwargs):

        super(ProvisionerClient, self).__init__(*args, **kwargs)
        self.log = self.get_logger()

        self.amqp_uri = "amqp://%s:%s@%s/%s" % (
                        self.CFG.server.amqp.username,
                        self.CFG.server.amqp.password,
                        self.CFG.server.amqp.host,
                        self.CFG.server.amqp.vhost,
                        )
        self.dashi = DashiConnection(self.topic, self.amqp_uri, self.topic)

    def terminate_nodes(self):
        self.dashi.call("provisioner", "terminate_nodes")

    def terminate_launches(self):
        self.dashi.call("provisioner", "terminate_launches")

    def terminate_all(self):
        self.dashi.fire("provisioner", "terminate_all")

    def provision(self, launch_id, deployable_type, launch_description,
                  subscribers, vars=None):
        """Provisions a deployable type
        """

        nodes = {}
        self.log.info(launch_description)
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

        self.dashi.call("provisioner", "provision", request=request)

    def query(self):
        """Triggers a query operation in the provisioner. Node updates
        are not sent in reply, but are instead sent to subscribers
        (most likely a sensor aggregator).
        """

        self.log.debug('Sending query request to provisioner')
        
        #TODO: implement this in Dashi
        # optionally send query in rpc-style, in which case this method's 
        # Deferred will not be fired util provisioner has a response from
        # all underlying IaaS. Right now this is only used in tests.
        #if rpc:
            #(content, headers, msg) = yield self.rpc_send('query', None)
            #defer.returnValue(content)
        #else:
            #yield self.send('query', None)

        self.dashi.call("provisioner", "query")


class ProvisionerNotifier(object):
    """Abstraction for sending node updates to subscribers.
    """
    def __init__(self, process):
        self.process = process

    def send_record(self, record, subscribers, operation='instance_state'):
        """Send a single node record to all subscribers.
        """
        #TODO:
        #log.debug('Sending state %s record for node %s to %s',
                #record['state'], record['node_id'], repr(subscribers))
        if subscribers:
            for sub in subscribers:
                self.process.send(sub, operation, record)

    def send_records(self, records, subscribers, operation='instance_state'):
        """Send a set of node records to all subscribers.
        """
        for rec in records:
            self.send_record(rec, subscribers, operation)



