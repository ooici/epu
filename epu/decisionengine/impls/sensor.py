import logging
import random
from itertools import ifilter
from datetime import datetime, timedelta

from epu.epumanagement.conf import CONF_IAAS_SITE, CONF_IAAS_ALLOCATION

from epu.decisionengine import Engine
from epu.states import InstanceState

log = logging.getLogger(__name__)

CONF_SENSOR_TYPE = "sensor_type"
CONF_METRIC = "metric"
CONF_SAMPLE_FUNCTION = "sample_function"
CONF_COOLDOWN = "cooldown_period"
CONF_SCALE_UP_THRESHOLD = "scale_up_threshold"
CONF_SCALE_UP_N_VMS = "scale_up_n_vms"
CONF_SCALE_DOWN_THRESHOLD = "scale_down_threshold"
CONF_SCALE_DOWN_N_VMS = "scale_down_n_vms"
CONF_MINIMUM_VMS = "minimum_vms"
CONF_MAXIMUM_VMS = "maximum_vms"
CONF_DEPLOYABLE_TYPE = "deployable_type"

# Engine-conf key for a list of node IDs that the client would prefer be killed first
CONF_RETIRABLE_NODES = "retirable_nodes"

# engine can optionally provide a unique value to each VM. For each VM, one of
# the values from the unique_values list is provided as a variable. If a VM
# dies or is terminated, its assigned variable will be reused. Values will be
# preferred in order, but this is not guaranteed. If your preserve_n value is
# larger than the number of unique values, additional VMs will get a None
# value.
CONF_UNIQUE_KEY = "unique_key"
CONF_UNIQUE_VALUES = "unique_values"

BAD_STATES = [InstanceState.TERMINATING, InstanceState.TERMINATED, InstanceState.FAILED]


class SensorEngine(Engine):
    """
    A decision engine that takes DT-based sensor requests into account (DT: deployable type).
    These "strongly typed" sensor inputs are new in R2.

    A client (e.g. the Process Dispatcher) can express its 'needs' as a sensor input.  These
    needs may be changing on a constant basis.  (It is not necessary to always fulfill the
    request, the EPUM works on a best effort basis.)

    The client may also have been informed of a DT started because of a registered need.
    When it's no longer of interest, the client may call "retire node" to indicate that it
    would prefer those are killed first.  The EPUM, however, is the ultimately arbiter of
    the resource acquisitions/attempts and it only tries to do the best job it can.

    There are a number of ways to internally implement the DT-based sensor request system.
    You could have one engine instance do everything.  Or you could have an engine instance
    per DT, per site, per-caller, or some permutation of these.  There are a number of ways
    to break up policy (quotas etc) between them or have one central place to implement all.

    The first prototype of the system uses one decision engine instance per DT, each an
    instance of this class.  Each engine instance is acting independently, balancing one
    permutation of {DT, IaaS, allocation} in the system.  As far as the engine instance
    is concerned, it has one current target that it must achieve (unless reconfigured).
    The decider will cause a reconfiguration to occur if necessary.

    This is a tactical decision that makes the new need-sensor system easier to implement.
    We can use the same initialize/reconfigure and EngineState patterns that were used in
    R1 (that the engine API was built for).

    These DT-based engines are not pre-configured in the EPUM configuration .  They are
    instead created by the decider leader in response to a "register need" sensor input if
    that particular permutation of {DT, allocation, IaaS} has not been seen before (in the
    future this list will likely include the caller or institution as well).

    The decider also translates those sensor inputs (both needs and "retire node" inputs) into
    engine reconfigurations.  Note that the EPUM workers handle the need/retire wire messages
    by making atomic insertions into the need queue.  The decider is what translates this queue
    of input into new Needy-engines (by using the epum_client to call msg_add_epu) or engine
    reconfigs (by using the epum_client to call msg_reconfigure_epu).

    The presence of these engines does not limit anyone's ability to call msg_add_epu directly
    and create other, non-need-driven EPUs.
    """
    def __init__(self):
        super(SensorEngine, self).__init__()
        self.minimum_vms = 0
        self.maximum_vms = 0
        self.metric = None
        self.sample_function = None
        self.cooldown_period = 0
        self.scale_up_threshold = 0
        self.scale_up_n_vms = 0
        self.scale_down_threshold = 0
        self.scale_down_n_vms = 0
        self.iaas_site = None
        self.iaas_allocation = None
        self.deployable_type = None
        self.retirable_nodes = []
        self.time_of_last_scale_action = datetime.min

        self.unique_key = None
        self.unique_values = None

        # For tests.  This information could be logged, as well.
        self.initialize_count = 0
        self.initialize_conf = None
        self.decide_count = 0
        self.reconfigure_count = 0

    def _set_conf(self, newconf):
        if not newconf:
            raise ValueError("requires engine conf")
        if CONF_MINIMUM_VMS in newconf:
            new_n = int(newconf[CONF_MINIMUM_VMS])
            if new_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_MINIMUM_VMS, new_n))
            self.minimum_vms = new_n
        if CONF_MAXIMUM_VMS in newconf:
            new_n = int(newconf[CONF_MAXIMUM_VMS])
            if new_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_MAXIMUM_VMS, new_n))
            self.maximum_vms = new_n
        if CONF_METRIC in newconf:
            self.metric = newconf[CONF_METRIC]
        if CONF_SAMPLE_FUNCTION in newconf:
            self.sample_function = newconf[CONF_SAMPLE_FUNCTION]
        if CONF_COOLDOWN in newconf:
            new_n = int(newconf[CONF_COOLDOWN])
            if new_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_COOLDOWN, new_n))
            self.cooldown_period = new_n
        if CONF_SCALE_UP_N_VMS in newconf:
            new_n = int(newconf[CONF_SCALE_UP_N_VMS])
            if new_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_SCALE_UP_N_VMS, new_n))
            self.scale_up_n_vms = new_n
        if CONF_SCALE_UP_THRESHOLD in newconf:
            new_n = float(newconf[CONF_SCALE_UP_THRESHOLD])
            self.scale_up_threshold = new_n
        if CONF_SCALE_DOWN_N_VMS in newconf:
            new_n = abs(int(newconf[CONF_SCALE_DOWN_N_VMS]))
            if new_n < 0:
                raise ValueError("cannot have negative %s conf: %d" % (CONF_SCALE_DOWN_N_VMS, new_n))
            self.scale_down_n_vms = new_n
        if CONF_SCALE_DOWN_THRESHOLD in newconf:
            new_n = float(newconf[CONF_SCALE_DOWN_THRESHOLD])
            self.scale_down_threshold = new_n
        if CONF_IAAS_SITE in newconf:
            self.iaas_site = newconf[CONF_IAAS_SITE]
        if CONF_IAAS_ALLOCATION in newconf:
            self.iaas_allocation = newconf[CONF_IAAS_ALLOCATION]
        if CONF_DEPLOYABLE_TYPE in newconf:
            self.deployable_type = newconf[CONF_DEPLOYABLE_TYPE]
        if CONF_RETIRABLE_NODES in newconf:
            self.retirable_nodes = newconf[CONF_RETIRABLE_NODES]
        if CONF_UNIQUE_KEY in newconf:
            if newconf.get(CONF_UNIQUE_VALUES):
                self.unique_key = newconf[CONF_UNIQUE_KEY]
                self.unique_values = newconf[CONF_UNIQUE_VALUES]

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
        self._set_conf(conf)
        log.info("%s initialized" % __name__)
        self.initialize_count += 1
        self.initialize_conf = conf

    def dying(self):
        raise Exception("Dying not implemented on the sensor decision engine")

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

        # if we are tracking unique values per VM, figure out the set currently
        # in use by valid nodes
        valid_uniques_set = set()
        if self.unique_key:
            for instance_id in valid_set:
                instance = state.instances[instance_id]
                if instance.extravars:
                    value = instance.extravars.get(self.unique_key)
                    if value:
                        valid_uniques_set.add(value)

        # How many instances are not terminated/ing or corrupted?
        valid_count = len(valid_set)

        # Check Sensors
        cooldown = timedelta(seconds=self.cooldown_period)
        time_since_last_action = datetime.now() - self.time_of_last_scale_action
        if time_since_last_action < cooldown:
            log.debug("No scaling action, in cooldown period")
            return
        elif self.metric is not None and self.sample_function is not None:
            values = []
            for instance_id in valid_set:
                instance = state.instances[instance_id]
                if (hasattr(instance, 'sensor_data') and instance.sensor_data and
                        instance.sensor_data.get(self.metric) and
                        instance.sensor_data[self.metric].get(self.sample_function)):
                    values.append(instance.sensor_data[self.metric].get(self.sample_function))
            try:
                divisor = max(len(values), valid_count)
                average_metric = float(sum(values)) / float(divisor)
            except ZeroDivisionError:
                average_metric = None

            if average_metric is None:
                scale_by = 0
            elif average_metric > self.scale_up_threshold:
                scale_by = self.scale_up_n_vms
            elif average_metric < self.scale_down_threshold:
                scale_by = - abs(self.scale_down_n_vms)
            else:
                scale_by = 0

            if scale_by != 0:
                self.time_of_last_scale_action = datetime.now()

        else:
            # No sensor metric or sample function specified. Not scaling
            scale_by = 0

        wanted_target = valid_count + scale_by
        target = max(min(self.maximum_vms, wanted_target), self.minimum_vms)

        force_pending = True
        if valid_count == target:
            log.debug("valid count (%d) = target (%d)" % (valid_count, target))
            force_pending = False
        elif valid_count < target:
            log.debug("valid count (%d) < target (%d)" % (valid_count, target))

            next_uniques = None
            extravars = None
            if self.unique_key:
                next_uniques = ifilter(lambda v: v not in valid_uniques_set, self.unique_values)
                extravars = {self.unique_key: None}

            while valid_count < target:
                # if we run out of uniques, we start using None
                if self.unique_key:
                    extravars[self.unique_key] = next(next_uniques, None)

                self._launch_one(control, extravars=extravars)
                valid_count += 1

        elif valid_count > target:
            log.debug("valid count (%d) > target (%d)" % (valid_count, target))
            while valid_count > target:
                die_id = None
                for instance_id in valid_set:
                    # Client would prefer that one of these is terminated
                    if instance_id in self.retirable_nodes:
                        die_id = instance_id
                        break
                if not die_id:
                    die_id = random.sample(valid_set, 1)[0]  # len(valid_set) is always > 0 here
                self._destroy_one(control, die_id)
                valid_set.discard(die_id)
                valid_count -= 1

        if force_pending:
            self._set_state_pending()
        else:
            self._set_state(all_instances, -1, health_not_checked=control.health_not_checked)

        self.decide_count += 1

    def _launch_one(self, control, extravars=None):

        if not self.iaas_site:
            raise Exception("No IaaS site configuration")
        if not self.iaas_allocation:
            raise Exception("No IaaS allocation configuration")
        if not self.deployable_type:
            raise Exception("No deployable type configuration")
        launch_id, instance_ids = control.launch(self.deployable_type,
            self.iaas_site, self.iaas_allocation, extravars=extravars)
        if len(instance_ids) != 1:
            raise Exception("Could not retrieve instance ID after launch")
        if extravars:
            log.info("Launched an instance ('%s') with vars: %s", instance_ids[0], extravars)
        else:
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
