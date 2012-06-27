import logging
import random

from epu.decisionengine import Engine
from epu.states import InstanceState

log = logging.getLogger(__name__)

HEALTHY_STATES = [InstanceState.REQUESTING, InstanceState.REQUESTED, InstanceState.PENDING, InstanceState.RUNNING, InstanceState.STARTED]
UNHEALTHY_STATES = [InstanceState.TERMINATING, InstanceState.TERMINATED, InstanceState.FAILED, InstanceState.RUNNING_FAILED]

CONF_PRESERVE_N = "preserve_n"
CONF_INSTANCE_TYPE = "instance_type"
CONF_SITES = "sites"
CONF_DEPLOYABLE_TYPE = "deployable_type_name"

class PhantomMultiNSite(object):

    def __init__(self, deployable_type_name, sitename, instance_type, count, error_delay=1, max_delay=60*10):
        self.sitename = sitename
        self.dt_name = deployable_type_name
        self.count = count
        self.current_delay = error_delay
        self.max_delay = max_delay
        self.last_failure_count = 0
        self.last_success_count = 0
        self.instance_type = instance_type

    def check_for_new_failure(self, unhealthy_instances, healthy_instances):
        if len(unhealthy_instances) > self.last_failure_count:
            self.current_delay = (self.current_delay * 2) + 1
            if self.current_delay > self.self.max_delay:
                self.current_delay = self.self.max_delay
        elif len(healthy_instances) > self.last_success_count:
            self.current_delay = 0
        self.last_failure_count = len(unhealthy_instances)
        self.last_success_count = len(healthy_instances)

    def decide(self, control, state):
        all_instances = state.instances.values()
        site_instances = [for a in all_instances if a.site == self.sitename]

        healthy_instances = [for i in site_instances if i.state in HEALTHY_STATES]
        unhealthy_instances = [for i in site_instances if i.state in UNHEALTHY_STATES]

        # sort the arrays by time
        unhealthy_instances.sort(key=lambda x: x.state_time, reverse=True)
        healthy_instances.sort(key=lambda x: x.state_time, reverse=True)

        # check if we need to kill one
        new_vms = self.count - len(healthy_instances)
        if new_vms < 0:
            new_vms = -new_vms
            log.info("PhantomMultiNSite killing off %d VMs on %s" % (new_vms, self.sitename))
            to_kill_array = healthy_instances[0:new_vms]
            instance_id_a = [for i in to_kill_array i.instance_id]
            log.info("Destroying an instances %s for %s" % (str(instance_id_a), control.owner))
            control.destroy_instances(instance_id_a, caller=control.owner)
            return

        if new_vms == 0:
            log.debug("PhantomMultiNSite no changes needed")
            return

        # if we are here we have new VMs to start
        self.check_for_new_failure(unhealthy_instances, healthy_instances)

        # check to see if we should back off for error
        if self._check_for_delay(unhealthy_instances):
            log.debug("PhantomMultiNSite delaying to run another instance")
            return 

        # run a new VM
        log.debug("PhantomMultiNSite start new VMs")
        for i in range(new_vms):
            (launch_ids, instance_ids) = control.launch(self.dt_name,
                self.sitename, self.instance_type,
                caller=control.owner)
            log.debug("PhantomMultiNSite launched %s %s" % (str(launch_ids), str(instance_ids)))

    def _check_for_delay(self, unhealthy_instances):
        if len(unhealthy_instances) == 0:
            return True

        td = datetime.datetime.now() - unhealthy_instances[0].state_time
        if td.total_seconds < self.current_delay:
            return True

        return False



class PhantomMultiNEngine(Engine):
    """A decision engine for distributing VMs across multiple clouds
    """

    def __init__(self):
        super(PhantomMultiNEngine, self).__init__()
        self.preserve_n = 0
        self.instance_type = None
        self.available_sites = []
        self.available_types = ["epu_work_consumer"]

        self.site_manager_dict = {}
        self.deployable_type_name = None

    def initialize(self, control, state, conf=None):
        """
        Give the engine a chance to initialize.  The current state of the
        system is given as well as a mechanism for the engine to offer the
        controller input about how often it should be called.

        @note Must be invoked and return before the 'decide' method can
        legally be invoked.

        @param control instance of Control, used to request changes to system
        @param state instance of State, used to obtain any known information
        @param conf None or dict of key/value pairs
        @exception Exception if engine cannot reach a sane state

        """
        if not conf:
            raise ValueError("requires engine conf")

        required_values = [CONF_PRESERVE_N, CONF_INSTANCE_TYPE, CONF_SITES,
                            CONF_DEPLOYABLE_TYPE,]

        for rk in required_values:
            if not conf.has_key(rk):
                raise ValueError("requires a value for %s" % (rk))

        self._configure(conf)
        log.info("%s initialized, preserve_n: %d" % (type(self), self.preserve_n))

    def dying(self):
        log.warn("%s does not implement dying" % (type(self)))

    def decide(self, control, state):

        for site_name in self.site_manager_dict:
            site_manager = self.site_manager_dict[site_name]
            site_manager.decide(control, state)
        
    def reconfigure(self, control, newconf):
        """
        Give the engine a new configuration.

        @note There must not be a decide call in progress when this is called,
        and there must not be a new decide call while this is in progress.

        @param control instance of Control, used to request changes to system
        @param newconf None or dict of key/value pairs
        @exception Exception if engine cannot reach a sane state
        @exception NotImplementedError if engine does not support this

        """
        if not newconf:
            raise ValueError("expected new engine conf")
        log.info("%s engine reconfigure, newconf: %s" % (type(self), newconf))

        self.__configure(newconf)


    def _configure(self, conf):

        if newconf.has_key(CONF_DEPLOYABLE_TYPE):
            self.deployable_type_name = conf[CONF_DEPLOYABLE_TYPE]

        if conf.has_key(CONF_PRESERVE_N):
            self.preserve_n = int(conf[CONF_PRESERVE_N])
            log.info("%s setting preserve_n to %d" % (type(self), self.preserve_n))
            if self.preserve_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_PRESERVE_N, self.preserve_n))

        if conf.has_key(CONF_INSTANCE_TYPE):
            self.instance_type = conf[CONF_INSTANCE_TYPE]
            log.info("%s setting instance type to %s" % (type(self), self.instance_type))

        new_sites = self.available_sites
        if conf.has_key(CONF_SITES):
            new_sites = conf[CONF_SITES]

        ndx = 0
        # make sure that any delete sites are cleaned up
        for s in self.available_sites:
            if s not in new_sites:
                # clean up all of s
                pass

        self.site_manager_dict = {}
        self.available_sites = new_sites
        each = self.preserve_n / len(self.available_sites)
        m = self.preserve_n % len(self.available_sites)
        for s in self.available_sites:
            c = each
            if ndx < m:
                c = c + 1
            phantom_site_object = PhantomMultiNSite(
                self.deployable_type_name,
                s,
                self.instance_type,
                c)
            self.site_manager_dict[s] = phantom_site_object

            ndx = ndx + 1






