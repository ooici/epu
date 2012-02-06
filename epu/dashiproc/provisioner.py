import logging

import dashi.bootstrap as bootstrap

from epu.provisioner.store import ProvisionerStore
from epu.provisioner.core import ProvisionerCore, ProvisionerContextClient
from epu.provisioner.leader import ProvisionerLeader
from epu.states import InstanceState
from epu.util import get_class, get_config_paths

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

class ProvisionerService(object):

    topic = "provisioner"

    def __init__(self, *args, **kwargs):

        configs = ["service", "provisioner"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        store = kwargs.get('store')
        self.store = store or self._get_provisioner_store()

        notifier = kwargs.get('notifier')
        self.notifier = notifier or ProvisionerNotifier(self)

        dtrs = kwargs.get('dtrs')
        self.dtrs = dtrs or self._get_dtrs()

        context_client = kwargs.get('context_client')
        context_client = context_client or self._get_context_client()

        site_drivers = kwargs.get('site_drivers')
        site_drivers = site_drivers or self._get_site_drivers(self.CFG.get('sites'))

        amqp_uri = kwargs.get('amqp_uri')
        self.amqp_uri = amqp_uri

        core = kwargs.get('core')
        core = core or self._get_core()

        self.core = core(self.store, self.notifier, self.dtrs,
                         site_drivers, context_client)
        self.core.recover()
        self.enabled = True

        leader = kwargs.get('leader')
        self.leader = leader or ProvisionerLeader(self.store, self.core)

        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG, self.amqp_uri)

    def start(self):

        log.info("starting provisioner instance %s" % self)

        # Set up operations
        self.dashi.handle(self.provision)
        self.dashi.handle(self.terminate_all)
        self.dashi.handle(self.terminate_nodes)
        self.dashi.handle(self.dump_state)
        self.dashi.handle(self.describe_nodes)

        self.leader.initialize()

        try:
            self.dashi.consume()
        except KeyboardInterrupt:
            log.warning("Caught terminate signal. Bye!")
        else:
            log.info("Exiting normally. Bye!")


    def provision(self, launch_id, deployable_type, instance_ids, subscribers,
                  site=None, allocation=None, vars=None):

        if not self.enabled:
            log.error('Provisioner is DISABLED. Ignoring provision request!')
            return None

        launch, nodes = self.core.prepare_provision(launch_id, deployable_type,
            instance_ids, subscribers, site, allocation, vars)

        if launch['state'] != InstanceState.FAILED:
            self.core.execute_provision(launch, nodes) 
        else: 
            log.warn("Launch %s couldn't be prepared, not executing", 
                    launch['launch_id']) 


    def terminate_nodes(self, nodes):
        """Service operation: Terminate one or more nodes
        """

        log.debug('op_terminate_nodes content:'+str(nodes))
        self.core.mark_nodes_terminating(nodes)
        self.core.terminate_nodes(nodes)

    def terminate_all(self):
        """Service operation: terminate all running instances
        """
        log.critical('Terminate all initiated.')
        log.critical('Disabling provisioner, future requests will be ignored')
        self.enabled = False

        self.core.terminate_all()
        
    def describe_nodes(self, nodes=None):
        """Service operation: return state records for nodes managed by the provisioner

        @param nodes: sequence of node IDs. If empty or None, all nodes will be described.
        @return: list of node records
        """
        return self.core.describe_nodes(nodes)

    def dump_state(self, nodes=None, force_subscribe=False):
        """Service operation: (re)send state information to subscribers
        """
        if not nodes:
            log.error("Got dump_state request without a nodes list")
        else:
            self.core.dump_state(nodes, force_subscribe=force_subscribe)

    def _get_provisioner_store(self):

        cassandra = self.CFG.get("cassandra")
        if cassandra:
            #TODO: add support for cassandra
            raise Exception("Cassandra store not implemented yet")
        else:
            log.info("Using in-memory Provisioner store")
            store = ProvisionerStore()
        return store

    def _get_context_client(self):
        if not self.CFG.get('context'):
            log.warning("No context configuration provided.")
            return None
        try:
            return ProvisionerContextClient(self.CFG.context.uri,
                                            self.CFG.context.key,
                                            self.CFG.context.secret)
        except AttributeError,e:
            raise AttributeError("Provisioner config missing: " + str(e))

    def _get_core(self):

        try:
            core_name = self.CFG.provisioner['core']
        except KeyError:
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

        if not sites:
            log.warning("No sites configured")
            return None

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

        self.dashi = dashi

        if handle_instance_state:
            self.dashi.handle(self.instance_state)

    def terminate_nodes(self, nodes):
        """Service operation: Terminate one or more nodes
        """
        log.debug('op_terminate_nodes nodes:'+str(nodes))
        self.dashi.fire("provisioner", "terminate_nodes", nodes=nodes)

    def terminate_all(self, rpcwait=False):
        if rpcwait:
            self.dashi.call("provisioner", "terminate_all")
        else:
            self.dashi.fire("provisioner", "terminate_all")

    def provision(self, launch_id, instance_ids, deployable_type, subscribers,
                  site=None, allocation=None, vars=None, **extras):
        """Provisions a deployable type
        """
        if len(instance_ids) != 1:
            raise ValueError("only single-instance launches are supported now")

        self.dashi.fire("provisioner", "provision", launch_id=launch_id,
            deployable_type=deployable_type, instance_ids=instance_ids,
            subscribers=subscribers, site=site, allocation=allocation,
            vars=vars, **extras)

    def dump_state(self, nodes=None, force_subscribe=None):
        log.debug('Sending dump_state request to provisioner')
        self.dashi.fire('provisioner', 'dump_state', nodes=nodes, force_subscribe=force_subscribe)

    def describe_nodes(self, nodes=None):
        """Query state records for nodes managed by the provisioner

        @param nodes: sequence of node IDs. If empty or None, all nodes will be described.
        @return: list of node records
        """
        return self.dashi.call('provisioner', 'describe_nodes', nodes=nodes)

    def instance_state(self, record):
        log.info("Got instance state: %s" % record)


class ProvisionerNotifier(object):
    """Abstraction for sending node updates to subscribers.
    """
    def __init__(self, process):
        self.process = process

    def send_record(self, record, subscribers, operation='instance_info'):
        """Send a single node record to all subscribers.
        """
        log.debug('Sending state %s record for node %s to %s',
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
