import uuid
from dashi.util import LoopingCall
import os.path
import logging

from dashi import DashiConnection
import dashi.bootstrap as bootstrap

from epu.provisioner.store import ProvisionerStore
from epu.provisioner.core import ProvisionerCore, ProvisionerContextClient
from epu.states import InstanceState
from epu.util import get_class, get_config_paths

logging.basicConfig(level=logging.DEBUG)

class ProvisionerService(object):

    topic = "provisioner"

    def __init__(self, *args, **kwargs):

        configs = ["service", "provisioner"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        self.log = logging.getLogger()

        store = kwargs.get('store')
        self.store = store or self._get_provisioner_store()

        notifier = kwargs.get('notifier')
        self.notifier = notifier or ProvisionerNotifier(self)

        dtrs = kwargs.get('dtrs')
        self.dtrs = dtrs or self._get_dtrs()

        context_client = kwargs.get('context_client')
        context_client = context_client or self._get_context_client()

        site_drivers = kwargs.get('site_drivers')
        site_drivers = site_drivers or self._get_site_drivers(self.CFG.sites)

        amqp_uri = kwargs.get('amqp_uri')
        self.amqp_uri = amqp_uri

        core = kwargs.get('core')
        core = core or self._get_core()

        try:
            bootstrap.enable_gevent()
        except Exception:
            self.log.warning("gevent not available. Falling back to threading")

        self.core = core(self.store, self.notifier, self.dtrs,
                         site_drivers, context_client, logger=self.log)
        self.core.recover()
        self.enabled = True
        self.quit = False

        # DL allowing sending query messages to ourselves for now, since we are
        # limited to a single instance again. This simplifies deployment as no
        # separate query service is required.
        query_period = float(self.CFG.provisioner.get('query_period', 0))
        if query_period:
            self.query_period = query_period
            self.query_looping_call = LoopingCall(self._query_loop)
        else:
            self.query_looping_call = None

        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG, self.amqp_uri)

    def start(self):

        self.log.info("starting provisioner instance %s" % self)

        # Set up operations
        self.dashi.handle(self.provision)
        self.dashi.handle(self.query)
        self.dashi.handle(self.terminate_all)
        self.dashi.handle(self.terminate_nodes)
        self.dashi.handle(self.terminate_launches)
        self.dashi.handle(self.dump_state)

        if self.query_looping_call:
            self.log.debug("Starting query loop: %s second period",
                           self.query_period)
            self.query_looping_call.start(self.query_period)

        try:
            self.dashi.consume()
        except KeyboardInterrupt:
            self.log.warning("Caught terminate signal. Bye!")
        else:
            self.log.info("Exiting normally. Bye!")


    def sleep(self):
        """sleep function to keep provisioner alive
        """
        import time
        while not self.quit:
            time.sleep(1)

    def provision(self, request):
        """Service operation: Provision a taskable resource
        """

        if not self.enabled:
            self.log.error('Provisioner is DISABLED. Ignoring provision request!')
            return None

        launch, nodes = self.core.prepare_provision(request)

        if launch['state'] != InstanceState.FAILED:
            self.core.execute_provision(launch, nodes) 
        else: 
            self.log.warn("Launch %s couldn't be prepared, not executing", 
                    launch['launch_id']) 


    def terminate_nodes(self, nodes):
        """Service operation: Terminate one or more nodes
        """

        self.log.debug('op_terminate_nodes content:'+str(nodes))
        self.core.mark_nodes_terminating(nodes)
        self.core.terminate_nodes(nodes)

    def terminate_launches(self, launches):
        """Service operation: Terminate one or more launches
        """
        self.log.debug('op_terminate_launches content:'+str(launches))

        for launch in launches:
            self.core.mark_launch_terminating(launch)

        self.core.terminate_launches(launches)

    def query(self):
        """Service operation: query IaaS  and send updates to subscribers.
        """
        try:
            self.core.query()
            return True
        except Exception:
            return False

    def terminate_all(self):
        """Service operation: terminate all running instances
        """
        self.log.critical('Terminate all initiated.')
        self.log.critical('Disabling provisioner, future requests will be ignored')
        self.enabled = False

        self.core.terminate_all()
        

    def dump_state(self, nodes=None, force_subscribe=False):
        """Service operation: (re)send state information to subscribers
        """
        if not nodes:
            self.log.error("Got dump_state request without a nodes list")
        else:
            self.core.dump_state(nodes, force_subscribe=force_subscribe)


    def _query_loop(self):
        """Method used in looping call to send query messages to ourself.

        Used in simplified deployments when there is no standalone provisioner query
        service
        """
        try:
            self.dashi.fire(self.topic, "query")
        except Exception, e:
            self.log.error("Error sending provisioner query request: %s", e,
                                  exc_info=True)

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

    def _get_core(self):

        try:
            core_name = self.CFG.provisioner['core']
        except KeyError, e:
            return ProvisionerCore

        core = get_class(core_name)
        return core

    def _get_dtrs(self):

        dtrs_name = self.CFG.provisioner['dtrs']
        dt = self.CFG.provisioner['dt_path']
        cookbooks = self.CFG.provisioner.get('cookbooks_path')
        dtrs_class = get_class(dtrs_name)
        dtrs = dtrs_class(dt=dt, cookbooks=cookbooks)

        return dtrs


    @staticmethod
    def _get_site_drivers(sites):
        """Loads a dict of IaaS drivers from a config block
        """

        drivers = {}
        for site, spec in sites.iteritems():
            try:
                cls_name = spec["driver_class"]
                cls_kwargs = spec["driver_kwargs"]
                cls = get_class(cls_name)
                driver = cls(**cls_kwargs)
                drivers[site] = driver
            except KeyError,e:
                raise KeyError("IaaS site description '%s' missing key '%s'" % (site, str(e)))
        
        return drivers


class ProvisionerClient(object):

    def __init__(self, dashi, handle_instance_state=True):

        self.log = logging.getLogger()
        self.dashi = dashi

        if handle_instance_state:
            self.dashi.handle(self.instance_state)

    def terminate_nodes(self, nodes):
        """Service operation: Terminate one or more nodes
        """
        self.log.debug('op_terminate_nodes nodes:'+str(nodes))
        self.dashi.fire("provisioner", "terminate_nodes", nodes=nodes)

    def terminate_launches(self, launches):
        self.dashi.fire("provisioner", "terminate_launches", launches=launches)

    def terminate_all(self, rpcwait=False):
        if rpcwait:
            self.dashi.call("provisioner", "terminate_all")
        else:
            self.dashi.fire("provisioner", "terminate_all")

    def provision(self, launch_id, deployable_type, launch_description,
                  subscribers, vars=None):
        """Provisions a deployable type
        """

        nodes = {}
        for nodename, item in launch_description.iteritems():
            nodes[nodename] = {'ids' : item.instance_ids,
                    'site' : item.site,
                    'allocation' : item.allocation_id,
                    'data' : item.data,
                    'vagrant_box' : getattr(item, 'vagrant_box', None)}

        request = {'deployable_type' : deployable_type,
                'launch_id' : launch_id,
                'nodes' : nodes,
                'subscribers' : subscribers,
                'vars' : vars}

        self.dashi.fire("provisioner", "provision", request=request)

    def query(self, rpc=False):
        """Triggers a query operation in the provisioner. Node updates
        are not sent in reply, but are instead sent to subscribers
        (most likely a sensor aggregator).
        """

        self.log.debug('Sending query request to provisioner')
        
        # optionally send query in rpc-style, in which case this method 
        # will not return until provisioner has a response from
        # all underlying IaaS. Right now this is only used in tests.
        if rpc:
            return self.dashi.call("provisioner", "query")
        else:
            self.dashi.fire("provisioner", "query")


    def dump_state(self, nodes=None, force_subscribe=None):
        self.log.debug('Sending dump_state request to provisioner')
        self.dashi.fire('provisioner', 'dump_state', nodes=nodes, force_subscribe=force_subscribe)

    def instance_state(self, record):
        self.log.info("Got instance state: %s" % record)

class ProvisionerNotifier(object):
    """Abstraction for sending node updates to subscribers.
    """
    def __init__(self, process):
        self.process = process

    def send_record(self, record, subscribers, operation='instance_info'):
        """Send a single node record to all subscribers.
        """
        self.process.log.debug('Sending state %s record for node %s to %s',
                record['state'], record['node_id'], repr(subscribers))
        if subscribers:
            for sub in subscribers:
                self.process.dashi.fire(sub, operation, record=record)

    def send_records(self, records, subscribers, operation='instance_info'):
        """Send a set of node records to all subscribers.
        """
        for rec in records:
            self.send_record(rec, subscribers, operation)

def main():
    provisioner = ProvisionerService()
    provisioner.start()
