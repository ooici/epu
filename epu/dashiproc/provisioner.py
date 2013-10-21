import threading
import logging
import time

import dashi.bootstrap as bootstrap
try:
    from statsd import StatsClient
except ImportError:
    StatsClient = None

from epu.dashiproc.dtrs import DTRSClient
from epu.provisioner.store import get_provisioner_store, sanitize_record
from epu.provisioner.core import ProvisionerCore, ProvisionerContextClient
from epu.provisioner.leader import ProvisionerLeader
from epu.states import InstanceState
from epu.util import get_class, get_config_paths
from epu.exceptions import UserNotPermittedError
import epu.dashiproc

log = logging.getLogger(__name__)


class ProvisionerService(object):

    def __init__(self, *args, **kwargs):

        configs = ["service", "provisioner"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        ssl_no_host_check = kwargs.get('ssl_no_host_check')
        if ssl_no_host_check is None:
            ssl_no_host_check = self.CFG.get('ssl_no_host_check')
        if ssl_no_host_check:
            import libcloud.security
            libcloud.security.VERIFY_SSL_CERT = False

        store = kwargs.get('store')
        self.proc_name = self.CFG.provisioner.get('proc_name', "")
        self.store = store or get_provisioner_store(self.CFG, proc_name=self.proc_name)
        self.store.initialize()

        notifier = kwargs.get('notifier')
        epum_topic = self.CFG.provisioner.epu_management_service_name
        self.notifier = notifier or ProvisionerNotifier(self, [epum_topic])

        amqp_uri = kwargs.get('amqp_uri')
        self.amqp_uri = amqp_uri

        self.topic = self.CFG.provisioner.get('service_name')

        self.sysname = kwargs.get('sysname')

        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG, self.amqp_uri, self.sysname)

        statsd_cfg = kwargs.get('statsd')
        statsd_cfg = statsd_cfg or self.CFG.get('statsd')

        dtrs = kwargs.get('dtrs')
        dtrs_topic = self.CFG.provisioner.dtrs_service_name
        self.dtrs = dtrs or self._get_dtrs(dtrs_topic, statsd_cfg=statsd_cfg, client_name=self.topic)

        contextualization_disabled = kwargs.get('contextualization_disabled')
        if contextualization_disabled is None:
            contextualization_disabled = self.CFG.get('contextualization_disabled')
        if not contextualization_disabled:
            context_client = kwargs.get('context_client')
            context_client = context_client or self._get_context_client()
        else:
            context_client = None

        default_user = kwargs.get('default_user')
        self.default_user = default_user or self.CFG.provisioner.get('default_user')

        iaas_timeout = kwargs.get('iaas_timeout')
        iaas_timeout = iaas_timeout or self.CFG.provisioner.get('iaas_timeout')

        instance_ready_timeout = kwargs.get('instance_ready_timeout')
        instance_ready_timeout = instance_ready_timeout or self.CFG.provisioner.get('instance_ready_timeout')

        record_reaping_max_age = kwargs.get('record_reaping_max_age')
        record_reaping_max_age = record_reaping_max_age or self.CFG.provisioner.get('record_reaping_max_age')

        core = kwargs.get('core')
        core = core or self._get_core()

        self.core = core(self.store, self.notifier, self.dtrs, context_client,
                iaas_timeout=iaas_timeout, statsd_cfg=statsd_cfg, instance_ready_timeout=instance_ready_timeout)

        leader = kwargs.get('leader')
        self.leader = leader or ProvisionerLeader(self.store, self.core,
                                                  record_reaping_max_age=record_reaping_max_age)

        self.ready_event = threading.Event()

    def start(self):

        log.info("starting provisioner instance %s" % self)

        epu.dashiproc.link_dashi_exceptions(self.dashi)

        # Set up operations
        self.dashi.handle(self.provision)
        self.dashi.handle(self.terminate_all)
        self.dashi.handle(self.terminate_nodes)
        self.dashi.handle(self.dump_state)
        self.dashi.handle(self.describe_nodes)
        self.dashi.handle(self.enable)

        self.leader.initialize()

        self.ready_event.set()

        try:
            self.dashi.consume()
        except KeyboardInterrupt:
            log.warning("Provisioner caught terminate signal. Bye!")
        else:
            log.info("Provisioner exiting normally. Bye!")

    def stop(self):
        self.ready_event.clear()
        self.dashi.cancel()
        self.dashi.disconnect()
        self.store.shutdown()

    @property
    def default_user(self):
        if not self._default_user:
            msg = "Operation called for the default user, but none is defined."
            raise UserNotPermittedError(msg)
        else:
            return self._default_user

    @default_user.setter  # noqa
    def default_user(self, default_user):
        self._default_user = default_user

    def provision(self, launch_id, deployable_type, instance_ids,
                  site, allocation=None, vars=None, caller=None):
        """Service operation: provision new VM instances

        At this time, only single-instance launches are supported. This means
        the length of the instance_ids sequence must be 1.

        @param launch_id: unique launch ID
        @param deployable_type: name of the deployable type to provision
        @param instance_ids: sequence of unique instance IDs
        @param site: IaaS site to deploy instance to
        @param allocation: IaaS allocation to request
        @param vars: dictionary of key/value pairs to be fed to deployable type
        @param caller: name of user who owns this launch
        @return:
        """

        caller = caller or self.default_user

        launch, nodes = self.core.prepare_provision(launch_id, deployable_type,
            instance_ids, site, allocation, vars, caller=caller)

        if launch['state'] < InstanceState.FAILED:
            self.core.execute_provision(launch, nodes, caller=caller)
        else:
            log.warn("Launch %s couldn't be prepared, not executing",
                launch['launch_id'])

        return nodes

    def terminate_nodes(self, nodes, caller=None):
        """Service operation: Terminate one or more nodes

        @param nodes: sequence of node_ids to terminate
        """
        caller = caller or self.default_user
        return self.core.mark_nodes_terminating(nodes, caller)

    def terminate_all(self):
        """Service operation: terminate all running instances

        This operation disables the provisioner, ensuring that future provision
        requests result in REJECTED states. It then terminates all running
        instances. Because termination may be a lengthy operation, it is
        carried out in the background by the Provisioner leader process. This
        operation returns quickly with a boolean value indicating whether all
        instances have been terminated successfully. The client is expected to
        poll this operation until a True value is received.

        @return boolean value indicating whether all nodes are terminated
        """
        # TODO: this is probably only available to superuser
        if not self.store.is_disabled():
            log.critical('Terminate all initiated.')
            log.critical('Disabling provisioner, future requests will be rejected')
            self.store.disable_provisioning()

        return self.core.check_terminate_all()

    def enable(self):
        """Service operation: re-enable the provisioner after a terminate_all
        """
        if self.store.is_disabled():
            log.info("Re-enabling provisioner")
            self.store.enable_provisioning()

    def describe_nodes(self, nodes=None, caller=None):
        """Service operation: return state records for nodes managed by the provisioner

        @param nodes: sequence of node IDs. If empty or None, all nodes will be described.
        @return: list of node records
        """
        caller = caller or self.default_user
        return self.core.describe_nodes(nodes, caller)

    def dump_state(self, nodes=None):
        """Service operation: (re)send state information to subscribers
        """
        if not nodes:
            log.error("Got dump_state request without a nodes list")
        else:
            self.core.dump_state(nodes)

    def _get_context_client(self):
        if not self.CFG.get('context'):
            log.warning("No context configuration provided.")
            return None
        try:
            return ProvisionerContextClient(self.CFG.context.uri,
                                            self.CFG.context.key,
                                            self.CFG.context.secret)
        except AttributeError, e:
            raise AttributeError("Provisioner config missing: " + str(e))

    def _get_core(self):

        try:
            core_name = self.CFG.provisioner['core']
        except KeyError:
            return ProvisionerCore

        core = get_class(core_name)
        return core

    def _get_dtrs(self, dtrs_topic, statsd_cfg=None, client_name=None):

        dtrs = DTRSClient(self.dashi, topic=dtrs_topic, statsd_cfg=statsd_cfg, client_name=client_name)
        return dtrs


def statsd(func):
    def call(provisioner_client, *args, **kwargs):
        before = time.time()
        ret = func(provisioner_client, *args, **kwargs)
        after = time.time()
        if provisioner_client.statsd_client is not None:
            try:
                client_name = provisioner_client.client_name or "provisioner_client"
                provisioner_client.statsd_client.timing(
                    '%s.%s.timing' % (client_name, func.__name__), (after - before) * 1000)
                provisioner_client.statsd_client.incr('%s.%s.count' % (client_name, func.__name__))
            except:
                log.exception("Failed to submit metrics")
        return ret
    return call


class ProvisionerClient(object):

    def __init__(self, dashi, topic=None, statsd_cfg=None, client_name=None):
        self.dashi = dashi
        self.topic = topic or "provisioner"
        self.client_name = client_name
        self.statsd_client = None
        if statsd_cfg is not None:
            try:
                host = statsd_cfg["host"]
                port = statsd_cfg["port"]
                log.info("Setting up statsd client with host %s and port %d" % (host, port))
                self.statsd_client = StatsClient(host, port)
            except:
                log.exception("Failed to set up statsd client")

    @statsd
    def terminate_nodes(self, nodes, caller=None):
        """Service operation: Terminate one or more nodes
        """
        log.debug("op_terminate_nodes nodes:" + str(nodes))
        self.dashi.fire(self.topic, "terminate_nodes", nodes=nodes, caller=caller)

    @statsd
    def terminate_all(self, rpcwait=True):
        if rpcwait:
            return self.dashi.call(self.topic, "terminate_all")
        else:
            self.dashi.fire(self.topic, "terminate_all")

    @statsd
    def provision(self, launch_id, instance_ids, deployable_type,
                  site=None, allocation=None, vars=None, **extras):
        """Provisions a deployable type
        """
        if len(instance_ids) != 1:
            raise ValueError("only single-instance launches are supported now")

        self.dashi.fire(self.topic, "provision", launch_id=launch_id,
            deployable_type=deployable_type, instance_ids=instance_ids,
            site=site, allocation=allocation, vars=vars, **extras)

    @statsd
    def dump_state(self, nodes=None, force_subscribe=None):
        log.debug("Sending dump_state request to provisioner")
        self.dashi.fire(self.topic, 'dump_state', nodes=nodes)

    @statsd
    def describe_nodes(self, nodes=None, caller=None):
        """Query state records for nodes managed by the provisioner

        @param nodes: sequence of node IDs. If empty or None, all nodes will be described.
        @return: list of node records
        """
        return self.dashi.call(self.topic, 'describe_nodes', nodes=nodes, caller=caller)

    @statsd
    def enable(self):
        self.dashi.call(self.topic, 'enable')


class ProvisionerNotifier(object):
    """Abstraction for sending node updates to subscribers.
    """
    def __init__(self, process, subscribers, operation='instance_info'):
        self.process = process
        self.subscribers = list(subscribers)
        self.operation = operation

    def send_record(self, record):
        """Send a single node record to all subscribers.
        """
        record = record.copy()

        # somewhat hacky. store keeps some version metadata on records.
        # strip this off before we send it out to subscribers.
        sanitize_record(record)

        subscribers = self.subscribers
        log.debug("Sending state %s record for node %s to %s",
                record['state'], record['node_id'], repr(subscribers))
        if subscribers:
            for sub in subscribers:
                self.process.dashi.fire(sub, self.operation, record=record)

    def send_records(self, records):
        """Send a set of node records to all subscribers.
        """
        for rec in records:
            self.send_record(rec)


def main():
    epu.dashiproc.epu_register_signal_stack_debug()
    provisioner = ProvisionerService()
    provisioner.start()
