import logging
import random

from epu.decisionengine import Engine
from epu.states import InstanceState

log = logging.getLogger(__name__)

BAD_STATES = [InstanceState.TERMINATING, InstanceState.TERMINATED, InstanceState.FAILED]

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

        if "force_site" in conf:
            self.available_sites = [conf["force_site"]]

        if "epuworker_type" in conf:
            self.available_types = [conf["epuworker_type"]]

        if "epuworker_allocation" in conf:
            self.available_allocations = [conf["epuworker_allocation"]]

        if CONF_PRESERVE_N in conf:
            self.preserve_n = int(conf[CONF_PRESERVE_N])
            if self.preserve_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_PRESERVE_N, self.preserve_n))
        else:
            raise ValueError("requires %s conf: %d" % (CONF_PRESERVE_N, self.preserve_n))

        log.info("Simplest-engine initialized, preserve_n: %d" % self.preserve_n)

    def dying(self):
        raise Exception("Dying not implemented on the simplest decision engine")

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

        # check all nodes to see if some are unhealthy, and terminate them
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
                die_id = random.sample(valid_set, 1)[0]  # len(valid_set) is always > 0 here
                self._destroy_one(control, die_id)
                valid_set.discard(die_id)
                valid_count -= 1

        if force_pending:
            self._set_state_pending()
        else:
            self._set_state(all_instances, -1, health_not_checked=control.health_not_checked)

    def _launch_one(self, control, uniquekv=None):
        launch_id, instance_ids = control.launch(self.available_types[0],
            self.available_sites[0], self.available_allocations[0],
            extravars=uniquekv)
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
            raise ValueError("expected new engine conf")
        log.debug("engine reconfigure, newconf: %s" % newconf)
        if CONF_PRESERVE_N in newconf:
            new_n = int(newconf[CONF_PRESERVE_N])
            if new_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_PRESERVE_N, new_n))
            self.preserve_n = new_n

    @classmethod
    def validate_config(cls, conf):
        if not conf:
            raise ValueError("requires engine conf")

        valid_keys = ["force_site", "epuworker_type", "epuworker_allocation", CONF_PRESERVE_N]

        for key in conf.keys():
            if key not in valid_keys:
                raise ValueError("key %s in conf is not accepted by engine" % key)

            if CONF_PRESERVE_N in conf:
                try:
                    preserve_n = int(conf[CONF_PRESERVE_N])
                except ValueError:
                    raise ValueError("%s conf must be a base-10 integer" % CONF_PRESERVE_N)

                if preserve_n < 0:
                    raise ValueError("cannot have negative %s conf: %d" % (CONF_PRESERVE_N, preserve_n))
            else:
                raise ValueError("requires %s conf" % CONF_PRESERVE_N)

    @classmethod
    def get_config_doc(cls):
        config_doc = """
Required config:

- %s: Number of instances to run continuously in the domain

Optional config:

- force_site: IaaS site to use (defaults to ec2-east)
- epuworker_type: DT to provision (defaults to epu_work_consumer)
- epuworker_allocation: IaaS allocation to use (defaults to small)
""" % CONF_PRESERVE_N

        return cls.__doc__ + config_doc
