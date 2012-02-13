import logging
import random

from epu.epumanagement.conf import CONF_IAAS_SITE, CONF_IAAS_ALLOCATION

from epu.decisionengine import Engine
from epu.states import InstanceState

log = logging.getLogger(__name__)

CONF_PRESERVE_N = "preserve_n"
CONF_DEPLOYABLE_TYPE = "deployable_type"
CONF_IAAS_IMAGE = "epuworker_image_id"

# Engine-conf key for a list of node IDs that the client would prefer be killed first
CONF_RETIRABLE_NODES = "retirable_nodes"

BAD_STATES = [InstanceState.TERMINATING, InstanceState.TERMINATED, InstanceState.FAILED]

class PhantomEngine(Engine):
    """
    A decision engine based on the needy engine for phantom.
    """
    def __init__(self):
        super(PhantomEngine, self).__init__()
        self.preserve_n = 0
        self.iaas_site = None
        self.iaas_allocation = None
        self.iaas_image = None
        self.deployable_type = None
        self.retirable_nodes = []

        # For tests.  This information could be logged, as well.
        self.initialize_count = 0
        self.initialize_conf = None
        self.decide_count = 0
        self.reconfigure_count = 0

    def _set_conf(self, newconf):
        if not newconf:
            raise ValueError("requires engine conf")
        if newconf.has_key(CONF_PRESERVE_N):
            new_n = int(newconf[CONF_PRESERVE_N])
            if new_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_PRESERVE_N, new_n))
            self.preserve_n = new_n
        if newconf.has_key(CONF_IAAS_SITE):
            self.iaas_site = newconf[CONF_IAAS_SITE]
        if newconf.has_key(CONF_IAAS_ALLOCATION):
            self.iaas_allocation = newconf[CONF_IAAS_ALLOCATION]
        if newconf.has_key(CONF_DEPLOYABLE_TYPE):
            self.deployable_type = newconf[CONF_DEPLOYABLE_TYPE]
        if newconf.has_key(CONF_RETIRABLE_NODES):
            self.retirable_nodes = newconf[CONF_RETIRABLE_NODES]
        if newconf.has_key(CONF_IAAS_IMAGE):
            self.iaas_image = newconf[CONF_IAAS_IMAGE]

    def initialize(self, control, state, conf=None):
        self._set_conf(conf)
        log.info("%s initialized" % __name__)
        self.initialize_count += 1
        self.initialize_conf = conf

    def decide(self, control, state):
        all_instances = state.instances.values()
        valid_set = set(i.instance_id for i in all_instances if not i.state in BAD_STATES)

        #check all nodes to see if some are unhealthy, and terminate them
        for instance in state.get_unhealthy_instances():
            log.warn("Terminating unhealthy node: %s", instance.instance_id)
            self._destroy_one(control, instance.instance_id)
            # some of our "valid" instances above may be unhealthy
            valid_set.discard(instance.instance_id)

        # How many instances are not terminated/ing or corrupted?
        valid_count = len(valid_set)

        force_pending = True
        if valid_count == self.preserve_n:
            log.debug("valid count (%d) = target (%d)" % (valid_count, self.preserve_n))
            force_pending = False
        elif valid_count < self.preserve_n:
            log.debug("valid count (%d) < target (%d)" % (valid_count, self.preserve_n))
            while valid_count < self.preserve_n:
                self._launch_one(control, uniquekv={CONF_IAAS_IMAGE: self.iaas_image})
                valid_count += 1
        elif valid_count > self.preserve_n:
            log.debug("valid count (%d) > target (%d)" % (valid_count, self.preserve_n))
            while valid_count > self.preserve_n:
                die_id = None
                for instance_id in valid_set:
                    # Client would prefer that one of these is terminated
                    if instance_id in self.retirable_nodes:
                        die_id = instance_id
                        break
                if not die_id:
                    die_id = random.sample(valid_set, 1)[0] # len(valid_set) is always > 0 here
                self._destroy_one(control, die_id)
                valid_set.discard(die_id)
                valid_count -= 1

        if force_pending:
            self._set_state_pending()
        else:
            self._set_state(all_instances, -1, health_not_checked=control.health_not_checked)

        self.decide_count += 1

    def _launch_one(self, control, uniquekv=None):
        if not self.iaas_site:
            raise Exception("No IaaS site configuration")
        if not self.iaas_allocation:
            raise Exception("No IaaS allocation configuration")
        if not self.iaas_image:
            raise Exception("No IaaS image configuration")
        if not self.deployable_type:
            raise Exception("No deployable type configuration")
        launch_id, instance_ids = control.launch(self.deployable_type,
            self.iaas_site, self.iaas_allocation, extravars=uniquekv)
        if len(instance_ids) != 1:
            raise Exception("Could not retrieve instance ID after launch")
        log.info("Launched an instance ('%s')", instance_ids[0])

    def _destroy_one(self, control, instanceid):
        control.destroy_instances([instanceid])
        log.info("Destroyed an instance ('%s')" % instanceid)

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
            raise ValueError("reconfigure expects new engine conf")
        self._set_conf(newconf)
        log.info("%s reconfigured" % __name__)
        self.reconfigure_count += 1
