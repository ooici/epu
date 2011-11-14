import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)

from epu.decisionengine import Engine
from epu.epumanagement.forengine import LaunchItem
import epu.states as InstanceStates

import random

BAD_STATES = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]

CONF_PRESERVE_N = "preserve_n"

class SimplestEngine(Engine):
    """A decision engine that maintains N instances of the compensating units.
    It's Npreserving policy (only) can be reconfigured.
    It has zero other features.  It's good for tests.
    """

    def __init__(self):
        super(SimplestEngine, self).__init__()
        self.preserve_n = 0
        self.available_allocations = ["small"]
        self.available_sites = ["ec2-east"]
        self.available_types = ["epu_work_consumer"]

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
        
        if conf.has_key("force_site"):
            self.available_sites = [conf["force_site"]]

        if conf.has_key("epuworker_type"):
            self.available_types = [conf["epuworker_type"]]

        if conf.has_key("epuworker_allocation"):
            self.available_allocations = [conf["epuworker_allocation"]]

        if conf.has_key(CONF_PRESERVE_N):
            self.preserve_n = int(conf[CONF_PRESERVE_N])
            if self.preserve_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_PRESERVE_N, self.preserve_n))
        else:
            raise ValueError("requires %s conf: %d" % (CONF_PRESERVE_N, self.preserve_n))

        log.info("Simplest-engine initialized, preserve_n: %d" % self.preserve_n)

    def decide(self, control, state):
        """
        Give the engine a chance to act on the current state of the system.

        @note May only be invoked once at a time.
        @note When it is invoked is up to EPU Controller policy and engine
        preferences, see the decision engine implementer's guide.

        @param control instance of Control, used to request changes to system
        @param state instance of State, used to obtain any known information
        @retval None
        @exception Exception if the engine has been irrevocably corrupted

        """
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
                self._launch_one(control)
                valid_count += 1
        elif valid_count > self.preserve_n:
            log.debug("valid count (%d) > target (%d)" % (valid_count, self.preserve_n))
            while valid_count > self.preserve_n:
                die_id = random.sample(valid_set, 1)[0] # len(valid_set) is always > 0 here
                self._destroy_one(control, die_id)
                valid_set.discard(die_id)
                valid_count -= 1

        if force_pending:
            self._set_state_pending()
        else:
            self._set_state(all_instances, -1, health_not_checked=control.health_not_checked)
            
    def _launch_one(self, control, uniquekv=None):
        launch_item = LaunchItem(1, self.available_allocations[0], self.available_sites[0], None)
        launch_description = {"work_consumer": launch_item}
        control.launch(self.available_types[0], launch_description, extravars=uniquekv)
        if len(launch_item.instance_ids) != 1:
            raise Exception("Could not retrieve instance ID after launch")
        log.info("Launched an instance ('%s')" % launch_item.instance_ids[0])

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
            raise ValueError("expected new engine conf")
        log.debug("engine reconfigure, newconf: %s" % newconf)
        if newconf.has_key(CONF_PRESERVE_N):
            new_n = int(newconf[CONF_PRESERVE_N])
            if new_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_PRESERVE_N, new_n))
            self.preserve_n = new_n
