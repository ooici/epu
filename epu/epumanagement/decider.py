import uuid
import logging

from dashi.util import LoopingCall
from copy import deepcopy
from datetime import datetime, timedelta

from epu import cei_events
from epu.epumanagement.conf import *
from epu.epumanagement.forengine import Control
from epu.decisionengine import EngineLoader
from epu.states import InstanceState
from epu.sensors import MOCK_CLOUDWATCH_SENSOR_TYPE, OPENTSDB_SENSOR_TYPE, CLOUDWATCH_SENSOR_TYPE, TRAFFIC_SENTINEL_SENSOR_TYPE, Statistics
from epu.sensors.cloudwatch import CloudWatch
from epu.sensors.opentsdb import OpenTSDB
from epu.epumanagement.test.mocks import MockCloudWatch
from epu.decisionengine.impls.sensor import CONF_SENSOR_TYPE

from epu.domain_log import EpuLoggerThreadSpecific

log = logging.getLogger(__name__)

DEFAULT_ENGINE_CLASS = "epu.decisionengine.impls.simplest.SimplestEngine"
DEFAULT_SENSOR_SAMPLE_PERIOD = 90
DEFAULT_SENSOR_SAMPLE_FUNCTION = 'Average'

class EPUMDecider(object):
    """The decider handles critical sections related to running decision engine cycles.

    In the future it may farm out subtasks to the EPUM workers (EPUMReactor) but currently all
    decision engine activity happens directly via the decider role.

    The instance of the EPUManagementService process that hosts a particular EPUMDecider instance
    might not be the elected decider.  When it is the elected decider, its EPUMDecider instance
    handles that functionality.  When it is not the elected decider, its EPUMDecider instance
    handles being available in the election.

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    See: https://confluence.oceanobservatories.org/display/CIDev/EPUManagement+Refactor

    "I hear the voices [...] and I know the speculation.  But I'm the decider, and I decide what is best."
    """

    def __init__(self, epum_store, subscribers, provisioner_client, epum_client, dtrs_client,
                 disable_loop=False, base_provisioner_vars=None):
        """
        @param epum_store State abstraction for all domains
        @type epum_store EPUMStore
        @param subscribers A way to signal state changes
        @param provisioner_client A way to launch/destroy VMs
        @param epum_client A way to launch subtasks to EPUM workers (reactor roles)
        @param dtrs_client A way to get information from dtrs
        @param disable_loop For unit/integration tests, don't run a timed decision loop
        @param base_provisioner_vars base vars given to every launch
        """

        self.epum_store = epum_store
        self.subscribers = subscribers
        self.provisioner_client = provisioner_client
        self.epum_client = epum_client
        self.dtrs_client = dtrs_client

        self.control_loop = None
        self.enable_loop = not disable_loop
        self.is_leader = False

        # these are given to every launch after engine-provided vars are folded in
        self.base_provisioner_vars = base_provisioner_vars

        # The instances of Engine that make the control decisions for each domain
        self.engines = {}

        # the versions of the engine configs currently applied
        self.engine_config_versions = {}

        # The instances of Control (stateful) that are passed to each Engine to get info and execute cmds
        self.controls = {}


    def recover(self):
        """Called whenever the whole EPUManagement instance is instantiated.
        """
        # For callbacks: "now_leader()" and "not_leader()"
        self.epum_store.register_decider(self)

    def now_leader(self, block=False):
        """Called when this instance becomes the decider leader.

        When block is true, waits until leader dies or is cancelled
        """
        log.info("Elected as Decider leader")
        self._leader_initialize()
        self.is_leader = True
        if block:
            if self.control_loop:
                self.control_loop.thread.join()
            else:
                raise ValueError("cannot block without a control loop")

    def not_leader(self):
        """Called when this instance is known not to be the decider leader.
        """
        if self.control_loop:
            self.control_loop.stop()
            self.control_loop = None
        self.is_leader = False

    def _leader_initialize(self):
        """Performs initialization routines that may require async processing
        """

        # to make certain we have the latest records for instances, we request provisioner to dump state
        instance_ids = []
        for owner, domain_id in self.epum_store.list_domains():
            domain = self.epum_store.get_domain(owner, domain_id)

            with EpuLoggerThreadSpecific(domain=domain.domain_id, user=domain.owner):
                for instance in domain.get_instances():
                    if instance.state < InstanceState.TERMINATED:
                        instance_ids.append(instance.instance_id)

        if instance_ids:
            svc_name = self.epum_store.epum_service_name()
            self.provisioner_client.dump_state(nodes=instance_ids, force_subscribe=svc_name)

        # TODO: We need to make a decision about how an engine can be configured to fire vs. how the
        #       decider fires it's top-loop.  The decider's granularity controls minimums.
        # WARN: For now the engine-specific "pulse" configuration is ignored.
        if self.enable_loop:
            if not self.control_loop:
                self.control_loop = LoopingCall(self._loop_top)
            self.control_loop.start(5)

    def _loop_top(self):
        """Every iteration of the decider loop, the following happens:

        1. Refresh state.  The EPUM worker processes are constantly updating persistence about the
        state of instances.  We do not suffer from efficiency fears here (without evidence).

        2. In particular, refresh the master domain list.  Some may have been created/removed in the meantime.
        Or this could be the first time this decider is the leader and the engine instances need to be
        created.

        3. For each new domain, create an engine instance and initialize it.

        4. For each pre-existing domain that is not marked as removed:
           A. Check if it has been reconfigured in the meantime.  If so, call reconfigure on the engine.
           B. Run decision cycle.
        """

        domains = self.epum_store.get_all_domains()

        # Perhaps in the meantime, the leader connection failed, bail early
        if not self.is_leader:
            return

        # look for domains that are not active anymore
        active_domains = {}
        for domain in domains:
            with EpuLoggerThreadSpecific(domain=domain.domain_id, user=domain.owner):
                if domain.is_removed():
                    self._shutdown_domain(domain)
                else:
                    active_domains[domain.key] = domain

                    if domain.key not in self.engines:
                        # New engines (new to this decider instance, at least)
                            try:
                                self._new_engine(domain)
                            except Exception,e:
                                log.error("Error creating engine '%s' for user '%s': %s",
                                    domain.domain_id, domain.owner, str(e), exc_info=True)

        for key in self.engines:
            # Perhaps in the meantime, the leader connection failed, bail early
            if not self.is_leader:
                return

            domain = active_domains.get(key)
            if not domain:
                continue

            with EpuLoggerThreadSpecific(domain=domain.domain_id, user=domain.owner):
                engine_conf, version = domain.get_versioned_engine_config()
                if version > self.engine_config_versions[key]:
                    try:
                        self.engines[key].reconfigure(self.controls[key], engine_conf)
                        self.engine_config_versions[key] = version
                    except Exception,e:
                        log.error("Error in reconfigure call for user '%s' domain '%s': %s",
                              domain.owner, domain.domain_id, str(e), exc_info=True)

                self._get_engine_sensor_state(domain)
                engine_state = domain.get_engine_state()
                try:
                    self.engines[key].decide(self.controls[key], engine_state)

                except Exception,e:
                    # TODO: if failure, notify creator
                    # TODO: If initialization fails, the engine won't be added to the list and it will be
                    #       attempted over and over.  There could be a retry limit?  Or jut once is enough.
                    log.error("Error in decide call for user '%s' domain '%s': %s",
                        domain.owner, domain.domain_id, str(e), exc_info=True)

    def _get_engine_sensor_state(self, domain):
        config = domain.get_engine_config()
        if config is None:
            log.debug("No engine config for sensor available")
            return

        domain_id = domain.domain_id
        user = domain.owner
        sensor_type = config.get(CONF_SENSOR_TYPE)
        period = 60
        monitor_sensors = config.get('monitor_sensors', [])
        monitor_domain_sensors = config.get('monitor_domain_sensors', [])
        sample_period = config.get('sample_period', DEFAULT_SENSOR_SAMPLE_PERIOD)
        sample_function = config.get('sample_function', DEFAULT_SENSOR_SAMPLE_FUNCTION)

        sensor_aggregator = self._get_sensor_aggregator(config)
        if sensor_aggregator is None:
            return

        # Support only OpenTSDB sensors for now
        domain_sensor_state = {}
        if sensor_type in (OPENTSDB_SENSOR_TYPE, MOCK_CLOUDWATCH_SENSOR_TYPE):
            for metric in monitor_domain_sensors:
                start_time = None
                end_time = None
                dimensions = {}

                if sensor_type in (MOCK_CLOUDWATCH_SENSOR_TYPE):
                    # Only for testing. Won't work with real cloudwatch
                    end_time = datetime.utcnow()
                    start_time = end_time - timedelta(seconds=sample_period)
                    dimensions = {'DomainId': domain_id}
                elif sensor_type == OPENTSDB_SENSOR_TYPE:
                    # OpenTSDB requires local time
                    end_time = datetime.now()
                    start_time = end_time - timedelta(seconds=sample_period)
                    if not instance.hostname:
                        log.warning("No hostname for '%s'. skipping for now" % instance.iaas_id)
                        continue
                    dimensions = {'domain': domain_id, 'user': user}
                else:
                    log.warning("Not sure how to setup '%s' query, skipping" % sensor_type)
                    continue

                state = sensor_aggregator.get_metric_statistics(period, start_time,
                        end_time, metric, sample_function, dimensions)
                for index, metric_result in state.iteritems():
                    if index not in (domain_id,):
                        continue
                    series = metric_result.get(Statistics.SERIES)
                    if series is not None and series != []:
                        domain_sensor_state[metric] = metric_result

        if domain_sensor_state != {}:
            domain.add_domain_sensor_data(domain_sensor_state)


        instances = domain.get_instances()
        for instance in instances:
            sensor_state = {}
            for metric in monitor_sensors:
                if 'ec2' not in instance.site and sensor_type == CLOUDWATCH_SENSOR_TYPE:
                    # Don't support pulling sensor data from cloudwatch in non-ec2 clouds
                    continue

                if 'ec2' in instance.site and sensor_type == CLOUDWATCH_SENSOR_TYPE:
                    credentials = self.dtrs_client.describe_credentials(domain.owner, instance.site)
                    config['access_key'] = credentials.get('access_key')
                    config['secret_key'] = credentials.get('secret_key')

                start_time = None
                end_time = None
                dimensions = {}
                if sensor_type in (CLOUDWATCH_SENSOR_TYPE, MOCK_CLOUDWATCH_SENSOR_TYPE):
                    end_time = datetime.utcnow()
                    start_time = end_time - timedelta(seconds=sample_period)
                    dimensions = {'InstanceId': instance.iaas_id}
                elif sensor_type == OPENTSDB_SENSOR_TYPE:
                    # OpenTSDB requires local time
                    end_time = datetime.now()
                    start_time = end_time - timedelta(seconds=sample_period)
                    if not instance.hostname:
                        log.warning("No hostname for '%s'. skipping for now" % instance.iaas_id)
                        continue

                    dimensions = {'host': instance.hostname}
                else:
                    log.warning("Not sure how to setup '%s' query, skipping" % sensor_type)
                    continue

                state = sensor_aggregator.get_metric_statistics(period, start_time,
                        end_time, metric, sample_function, dimensions)
                for index, metric_result in state.iteritems():
                    if index not in (instance.iaas_id, instance.hostname):
                        continue
                    series = metric_result.get(Statistics.SERIES)
                    if series is not None and series != []:
                        if not sensor_state.get(instance.instance_id):
                            sensor_state[instance.instance_id] = {}
                        sensor_state[instance.instance_id][metric] = metric_result

            if sensor_state != {}:
                domain.new_instance_sensor(instance.instance_id, sensor_state)


    def _get_sensor_aggregator(self, config):
        sensor_type = config.get(CONF_SENSOR_TYPE)
        if sensor_type == CLOUDWATCH_SENSOR_TYPE:
            if not config.get('access_key') and not config.get('secret_key'):
                log.debug("No CloudWatch key and secret provided")
                return
            sensor_aggregator = CloudWatch(config.get('access_key'),
                    config.get('secret_key'))
            return sensor_aggregator
        elif sensor_type == MOCK_CLOUDWATCH_SENSOR_TYPE:
            sensor_data = config.get('sensor_data')
            sensor_aggregator = MockCloudWatch(sensor_data)
            return sensor_aggregator
        elif sensor_type == OPENTSDB_SENSOR_TYPE:
            if not config.get('opentsdb_host') and not config.get('opentsdb_port'):
                log.debug("No OpenTSDB host and port provided")
                return
            sensor_aggregator = OpenTSDB(config.get('opentsdb_host'), config.get('opentsdb_port'))
            return sensor_aggregator
        elif sensor_type is None:
            return
        else:
            log.warning("Unsupported sensor type '%s'" % sensor_type)
            return



    def _shutdown_domain(self, domain):
        """Terminates all nodes for a domain and removes it.

        Expected to be called in several iterations of the decider loop until
        all instances are terminated.
        """
        with EpuLoggerThreadSpecific(domain=domain.domain_id, user=domain.owner):

            instances = [i for i in domain.get_instances()
                     if i.state < InstanceState.TERMINATED]
            if instances:
                # if the decider died after a domain was marked for
                # destroy but before it was cleaned up it may not be in
                # the self.engines table
                if domain.key not in self.engines:
                    self._new_engine(domain)

                instance_id_s = [i['instance_id'] for i in instances]
                log.debug("terminating %s", instance_id_s)
                c = self.controls[domain.key]
                try:
                    c.destroy_instances(instance_id_s, caller=domain.owner)
                except Exception:
                    log.exception("Error destroying instances")
            else:
                log.debug("domain has no instances left, removing")
                try:
                    # Domain engine may not exist yet
                    if domain.key in self.engines:
                        try:
                            self.engines[domain.key].dying()
                        except Exception:
                            log.exception("Error calling engine.dying()")

                        del self.engines[domain.key]
                        del self.controls[domain.key]

                    self.epum_store.remove_domain(domain.owner, domain.domain_id)

                except Exception:
                    # these should all happen atomically... not sure what to do.
                    log.exception("cleaning up a removed domain did not go well")
                    raise

    def _new_engine(self, domain):

        with EpuLoggerThreadSpecific(domain=domain.domain_id, user=domain.owner):
            general_config = domain.get_general_config()
            engine_class = general_config.get(EPUM_CONF_ENGINE_CLASS, None)
            if not engine_class:
                engine_class = DEFAULT_ENGINE_CLASS

            engine_config, version = domain.get_versioned_engine_config()
            engine_prov_vars = engine_config.get(PROVISIONER_VARS_KEY, None)

            if self.base_provisioner_vars:
                prov_vars = deepcopy(self.base_provisioner_vars)
                if engine_prov_vars:
                    prov_vars.update(engine_prov_vars)
            else:
                prov_vars = engine_prov_vars

            engine = EngineLoader().load(engine_class)
            control = ControllerCoreControl(self.provisioner_client, domain, prov_vars,
                                        self.epum_store.epum_service_name())

            engine.initialize(control, domain, engine_config)
            self.engines[domain.key] = engine
            self.engine_config_versions[domain.key] = version
            self.controls[domain.key] = control


class ControllerCoreControl(Control):
    def __init__(self, provisioner_client, domain, prov_vars, controller_name, health_not_checked=True):
        super(ControllerCoreControl, self).__init__()
        self.sleep_seconds = 5.0 # TODO: ignored for now on a per-engine basis
        self.provisioner = provisioner_client
        self.domain = domain
        self.controller_name = controller_name
        if prov_vars:
            self.prov_vars = prov_vars
        else:
            self.prov_vars = {}
        self.health_not_checked = health_not_checked

    def configure(self, parameters):
        """
        Give the engine the opportunity to offer input about how often it
        should be called or what specific events it would always like to be
        triggered after.

        See the decision engine implementer's guide for specific configuration
        options.

        @retval None
        @exception Exception illegal/unrecognized input
        """
        if not parameters:
            log.info("ControllerCoreControl is configured, no parameters")
            return

        if parameters.has_key("timed-pulse-irregular"):
            sleep_ms = int(parameters["timed-pulse-irregular"])
            self.sleep_seconds = sleep_ms / 1000.0
            # TODO: ignored for now on a per-engine basis
            #log.info("Configured to pulse every %.2f seconds" % self.sleep_seconds)

        if parameters.has_key(PROVISIONER_VARS_KEY):
            self.prov_vars = parameters[PROVISIONER_VARS_KEY]
            log.info("Configured with new provisioner vars:\n%s", self.prov_vars)

    def launch(self, deployable_type_id, site, allocation, count=1, extravars=None, caller=None):
        """
        Choose instance IDs for each instance desired, a launch ID and send
        appropriate message to Provisioner.

        @param deployable_type_id string identifier of the DT to launch
        @param site IaaS site to launch on
        @param allocation IaaS allocation (size) to request
        @param count number of instances to launch
        @param extravars Optional, see engine implementer's guide
        @retval tuple (launch_id, instance_ids), see guide
        @exception Exception illegal input
        @exception Exception message not sent
        """

        # right now we are sending some node-specific data in provisioner vars
        # (node_id at least)
        if count != 1:
            raise NotImplementedError("Only single-node launches are supported")

        launch_id = str(uuid.uuid4())
        log.info("Request for DT '%s' is a new launch with id '%s'", deployable_type_id, launch_id)
        new_instance_id_list = []

        vars_send = self.prov_vars.copy()
        if extravars:
            extravars = deepcopy(extravars)
            vars_send.update(extravars)
        else:
            extravars = None

        for i in range(count):
            new_instance_id = str(uuid.uuid4())
            self.domain.new_instance_launch(deployable_type_id, new_instance_id, launch_id,
                                  site, allocation, extravars=extravars)
            new_instance_id_list.append(new_instance_id)

        # The node_id var is the reason only single-node launches are supported.
        # It could be instead added by the provisioner or something? It also
        # is complicated by the contextualization system.
        vars_send['node_id'] = new_instance_id_list[0]
        vars_send['heartbeat_dest'] = self.controller_name

        subscribers = (self.controller_name,)

        self.provisioner.provision(launch_id, new_instance_id_list,
            deployable_type_id, subscribers, site=site,
            allocation=allocation, vars=vars_send, caller=caller)
        extradict = {"launch_id":launch_id,
                     "new_instance_ids":new_instance_id_list,
                     "subscribers":subscribers}
        cei_events.event("controller", "new_launch", extra=extradict)
        return launch_id, new_instance_id_list

    def destroy_instances(self, instance_list, caller=None):
        """Terminate particular instances.

        Control API method, see the decision engine implementer's guide.

        @param instance_list list size >0 of instance IDs to terminate
        @retval None
        @exception Exception illegal input/unknown ID(s)
        @exception Exception message not sent
        """
        self.provisioner.terminate_nodes(instance_list, caller=caller)

    def destroy_all(self):
        self.provisioner.terminate_all()
