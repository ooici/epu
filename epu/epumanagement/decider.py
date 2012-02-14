import logging
from copy import deepcopy
import uuid

from dashi.util import LoopingCall

from epu import cei_events
from epu.decisionengine.impls.needy import CONF_PRESERVE_N, CONF_DEPLOYABLE_TYPE
from epu.epumanagement.conf import *
from epu.epumanagement.forengine import Control
from epu.decisionengine import EngineLoader
from epu.states import InstanceState

log = logging.getLogger(__name__)

DEFAULT_ENGINE_CLASS = "epu.decisionengine.impls.simplest.SimplestEngine"

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

    def __init__(self, epum_store, notifier, provisioner_client, epum_client,
                 disable_loop=False, base_provisioner_vars=None):
        """
        @param epum_store State abstraction for all EPUs
        @param notifier A way to signal state changes (TODO: don't think is needed)
        @param provisioner_client A way to launch/destroy VMs
        @param epum_client A way to launch subtasks to EPUM workers (reactor roles)
        @param disable_loop For unit/integration tests, don't run a timed decision loop
        @param base_provisioner_vars base vars given to every launch
        """

        self.epum_store = epum_store
        self.notifier = notifier
        self.provisioner_client = provisioner_client
        self.epum_client = epum_client

        self.control_loop = None
        self.enable_loop = not disable_loop

        # these are given to every launch after engine-provided vars are folded in
        self.base_provisioner_vars = base_provisioner_vars

        # The instances of Engine that make the control decisions for each EPU
        self.engines = {}

        # The instances of Control (stateful) that are passed to each Engine to get info and execute cmds
        self.controls = {}

        #NOTE (DGL) not replicating this busy semaphore deal from twisted. I don't see that it is
        # necessary as these calls don't seem to happen in parallel anyways.

        # There can only ever be one 'decide' engine call run at ANY time.  This could be expanded
        # to be a latch per EPU for concurrency, but keeping it simple, especially for prototype.
        #self.busy = defer.DeferredSemaphore(1)

    def recover(self):
        """Called whenever the whole EPUManagement instance is instantiated.
        """
        # For callbacks: "now_leader()" and "not_leader()"
        self.epum_store.register_decider(self)

    def now_leader(self):
        """Called when this instance becomes the decider leader.
        """
        self._leader_initialize()

    def not_leader(self):
        """Called when this instance is known not to be the decider leader.
        """
        if self.control_loop:
            self.control_loop.stop()
            self.control_loop = None

    def _leader_initialize(self):
        """Performs initialization routines that may require async processing
        """

        # TODO: needs real world testing with multiple workers.  Currently this is done in initialize which
        #       may mean the service can't receive messages (this issue may not be present in R2/dashi)

        # to make certain we have the latest records for instances, we request provisioner to dump state
        instance_ids = []
        epus = self.epum_store.all_active_epus()
        for epu_name in epus.keys():
            for instance in epus[epu_name].instances.itervalues():
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

    def _needs_sensors(self, active_epus):
        """See the NeedyEngine class notes for an explanation of the decider's role in needs-sensors.

        @param active_epus recent list of all the active EPUState instances
        """
        pending = self.epum_store.get_pending_needs()
        for key in pending.keys():
            (dt_id, iaas_site, iaas_allocation, num_needed) = pending[key]
            log.debug("New need for (%s, %s, %s) => %d" % (dt_id, iaas_site, iaas_allocation, num_needed))
            engine_config = {CONF_PRESERVE_N:num_needed,
                             CONF_IAAS_SITE: iaas_site,
                             CONF_IAAS_ALLOCATION: iaas_allocation,
                             CONF_DEPLOYABLE_TYPE: dt_id}
            log.debug("engine_config: %s" % engine_config)

            # TODO: Handling a removed EPU is currently not a practical issue since all NeedyEngine
            #       instances will remain active.  Just brought to preserve-zero when done.
            if key in active_epus.keys():
                epu = self.epum_store.get_epu_state(key)
                epu.add_engine_conf(engine_config)
            else:
                # TODO: defaults for needy-EPU health settings would come from initial conf
                engine_class = "epu.decisionengine.impls.needy.NeedyEngine"
                general = {EPUM_CONF_ENGINE_CLASS: engine_class}
                health = {EPUM_CONF_HEALTH_MONITOR: False}
                epu_config = {EPUM_CONF_GENERAL:general,
                              EPUM_CONF_ENGINE: engine_config,
                              EPUM_CONF_HEALTH: health}
                self.epum_store.create_new_epu(None, key, epu_config)

            # If another decider was elected in the meantime (and read those pending needs), that loop will
            # be redoable without changing the final effect.
            self.epum_store.clear_pending_need(key, dt_id, iaas_site, iaas_allocation, num_needed)

    def _loop_top(self):
        """Every iteration of the decider loop, the following happens:

        1. Refresh state.  The EPUM worker processes are constantly updating persistence about the
        state of instances.  We do not suffer from efficiency fears here (without evidence).

        2. Handle the needs-sensor queue, see self._needs_sensors()

        3. In particular, refresh the master EPU list.  Some may have been created/removed in the meantime.
        Or this could be the first time this decider is the leader and the engine instances need to be
        created.

        4. For each new EPU, create an engine instance and initialize it.

        5. For each pre-existing EPU that is not marked as removed:
           A. Check if it has been reconfigured in the meantime.  If so, call reconfigure on the engine.
           B. Run decision cycle.
        """

        # Perhaps in the meantime, the leader connection failed, bail early
        if not self.epum_store.currently_decider():
            return
        epus = self.epum_store.all_active_epus()

        self._needs_sensors(epus)

        # EPUs could have been just added
        epus = self.epum_store.all_active_epus()

        for epu_name in epus.keys():
            epus[epu_name].recover()

        # Perhaps in the meantime, the leader connection failed, bail early
        if not self.epum_store.currently_decider():
            return
            
        # Engines that are not active anymore
        all_epus = self.epum_store.all_epus()
        for epu_name in all_epus.keys():
            epu_state = self.epum_store.get_epu_state(epu_name)
            if epu_state.is_removed():
                # if the decider died after a epu was marked for destroy but before it was cleaned up
                # it may not be in the self.engines table
                if epu_name not in self.engines:
                    self._new_engine(epu_name)
                insts = epu_state.get_instance_dicts()
                c = self.controls[epu_name]
                c.destroy_instances(insts)
                self.engines[epu_name].dying()
                del self.engines[epu_name]
                del self.controls[epu_name]
            else:
                if epu_name not in epus.keys():
                    raise Exception("This should not be possible")

        # New engines (new to this decider instance, at least)
        for new_epu_name in filter(lambda x: x not in self.engines.keys(), epus.keys()):
            try:
                self._new_engine(new_epu_name)
            except Exception,e:
                log.error("Error creating engine '%s': %s", new_epu_name,
                          str(e), exc_info=True)

        for epu_name in self.engines.keys():
            # Perhaps in the meantime, the leader connection failed, bail early
            if not self.epum_store.currently_decider():
                return

            epu_state = self.epum_store.get_epu_state(epu_name)
            
            reconfigured = epu_state.has_been_reconfigured()
            if reconfigured:
                engine_conf = epu_state.get_engine_conf()
                try:
                    # NOTE: this used to be wrapped in a deferred semaphore. necessary?
                    self.engines[epu_name].reconfigure(self.controls[epu_name], engine_conf)
                except Exception,e:
                    log.error("Error in reconfigure call for '%s': %s",
                              epu_name, str(e), exc_info=True)
                epu_state.set_reconfigure_mark()

            engine_state = epu_state.get_engine_state()
            try:
                # NOTE: this used to be wrapped in a deferred semaphore. necessary?
                self.engines[epu_name].decide(self.controls[epu_name], engine_state)
            except Exception,e:
                # TODO: if failure, notify creator
                # TODO: If initialization fails, the engine won't be added to the list and it will be
                #       attempted over and over.  There could be a retry limit?  Or jut once is enough.
                log.error("Error in decide call for '%s': %s", epu_name,
                          str(e), exc_info=True)

    def _new_engine(self, epu_name):
        epu_state = self.epum_store.get_epu_state(epu_name)
        
        general_config = epu_state.get_general_conf()
        engine_class = general_config.get(EPUM_CONF_ENGINE_CLASS, None)
        if not engine_class:
            engine_class = DEFAULT_ENGINE_CLASS

        engine_config = epu_state.get_engine_conf()
        engine_prov_vars = engine_config.get(PROVISIONER_VARS_KEY, None)

        if self.base_provisioner_vars:
            prov_vars = deepcopy(self.base_provisioner_vars)
            if engine_prov_vars:
                prov_vars.update(engine_prov_vars)
        else:
            prov_vars = engine_prov_vars

        engine = EngineLoader().load(engine_class)
        control = ControllerCoreControl(self.provisioner_client, epu_state, prov_vars,
                                        self.epum_store.epum_service_name())

        engine.initialize(control, epu_state, engine_config)
        self.engines[epu_name] = engine
        self.controls[epu_name] = control


class ControllerCoreControl(Control):
    def __init__(self, provisioner_client, epu_state, prov_vars, controller_name, health_not_checked=True):
        super(ControllerCoreControl, self).__init__()
        self.sleep_seconds = 5.0 # TODO: ignored for now on a per-engine basis
        self.provisioner = provisioner_client
        self.epu_state = epu_state
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
            log.info("Configured with new provisioner vars:\n%s" % self.prov_vars)

    def launch(self, deployable_type_id, site, allocation, count=1, extravars=None):
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
        log.info("Request for DT '%s' is a new launch with id '%s'" % (deployable_type_id, launch_id))
        new_instance_id_list = []

        for i in range(count):
            new_instance_id = str(uuid.uuid4())
            self.epu_state.new_instance_launch(deployable_type_id, new_instance_id, launch_id,
                                  site, allocation)
            new_instance_id_list.append(new_instance_id)

        vars_send = self.prov_vars.copy()
        if extravars:
            vars_send.update(extravars)

        # The node_id var is the reason only single-node launches are supported.
        # It could be instead added by the provisioner or something? It also
        # is complicated by the contextualization system.
        vars_send['node_id'] = new_instance_id_list[0]
        vars_send['heartbeat_dest'] = self.controller_name

        # hide passwords from logging
        hide_password = deepcopy(vars_send)
        if 'cassandra_password' in hide_password:
            hide_password['cassandra_password'] = '*****'
        if 'broker_password' in hide_password:
            hide_password['broker_password'] = '*****'

        log.debug("Launching with parameters:\n%s" % str(hide_password))

        subscribers = (self.controller_name,)

        self.provisioner.provision(launch_id, new_instance_id_list,
            deployable_type_id, subscribers, site=site,
            allocation=allocation, vars=vars_send)
        extradict = {"launch_id":launch_id,
                     "new_instance_ids":new_instance_id_list,
                     "subscribers":subscribers}
        cei_events.event("controller", "new_launch", extra=extradict)
        return launch_id, new_instance_id_list

    def destroy_instances(self, instance_list):
        """Terminate particular instances.

        Control API method, see the decision engine implementer's guide.

        @param instance_list list size >0 of instance IDs to terminate
        @retval None
        @exception Exception illegal input/unknown ID(s)
        @exception Exception message not sent
        """
        self.provisioner.terminate_nodes(instance_list)
