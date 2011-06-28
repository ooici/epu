import ion.util.ionlog
from epu.epucontroller.forengine import State

log = ion.util.ionlog.getLogger(__name__)

import random

from epu.decisionengine import Engine
from epu.epucontroller import LaunchItem
import epu.states as InstanceStates

BAD_STATES = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]

CONF_HIGH_WATER = "queuelen_high_water"
CONF_LOW_WATER = "queuelen_low_water"
CONF_MIN_INSTANCES = "min_instances"

class QueueLengthBoundedEngine(Engine):
    """
    A decision engine that looks at queue length.  If there are more queued
    messages than the maximum, it will launch compensation.  If the queue
    falls below the given minimum, it will contract instances (unless there
    is only one instance left).
    """
    
    def __init__(self):
        super(QueueLengthBoundedEngine, self).__init__()
        self.high_water = 0
        self.low_water = 0
        self.min_instances = 0
        # todo: get all of this from conf:
        self.available_allocations = ["small"]
        self.available_sites = ["ec2-east"]
        self.available_types = ["epu_work_consumer"]
        
    def initialize(self, control, state, conf=None):
        """Engine API method"""
        # todo: need central constants for these key strings
        parameters = {"timed-pulse-irregular":5000}
        if conf and conf.has_key("force_site"):
            self.available_sites = [conf["force_site"]]
        if not conf:
            raise Exception("cannot initialize without external configuration")
        
        if conf and conf.has_key("epuworker_type"):
            self.available_types = [conf["epuworker_type"]]

        if conf and conf.has_key("epuworker_allocation"):
            self.available_allocations = [conf["epuworker_allocation"]]
        
        if not conf.has_key(CONF_HIGH_WATER):
            raise Exception("cannot initialize without %s" % CONF_HIGH_WATER)
        if not conf.has_key(CONF_LOW_WATER):
            raise Exception("cannot initialize without %s" % CONF_LOW_WATER)
        self.high_water = int(conf[CONF_HIGH_WATER])
        self.low_water = int(conf[CONF_LOW_WATER])
        
        if conf.has_key(CONF_MIN_INSTANCES):
            self.min_instances = int(conf[CONF_MIN_INSTANCES])
            if self.min_instances < 0:
                raise Exception("cannot have negative %s conf" % CONF_MIN_INSTANCES)
        else:
            self.min_instances = 0

        # This conf is not respected in the queuelen_bounded engine
        self.devmode_no_failure_compensation = False
        
        log.info("Bounded queue length engine initialized, high water mark is %d, low water %d" % (self.high_water, self.low_water))
        
        control.configure(parameters)

    def decide(self, control, state):
        """Engine API method"""

        all_instances = state.instances.values()
        valid_set = set(i.instance_id for i in all_instances if not i.state in BAD_STATES)

        #check all nodes to see if some are unhealthy, and terminate them
        for instance in state.get_unhealthy_instances():
            log.warn("Terminating unhealthy node: %s",instance.instance_id)
            self._destroy_one(control, instance.instance_id)

            # some of our "valid" instances above may be unhealthy
            valid_set.discard(instance.instance_id)

        valid_count = len(valid_set)

        # If there is an explicit minimum, always respect that.
        if valid_count < self.min_instances:
            log.info("Bringing instance count up to the explicit minimum")
            while valid_count < self.min_instances:
                self._launch_one(control)
                valid_count += 1
        
        # Won't make a decision if there are pending instances. This would
        # need to be a lot more elaborate (requiring a datastore) to get a
        # faster response time whilst not grossly overcompensating. 
        any_pending = bool(state.get_pending_instances())

        if any_pending:
            log.debug("Will not analyze with pending instances")
            self._set_state_pending()
            return
        
        heading = self._heading(state, valid_count)
        if heading > 0:
            self._launch_one(control)
            valid_count += 1
        elif heading < 0:
            instanceid = self._pick_instance_to_die(valid_set)
            if not instanceid:
                log.error("There are no valid instances to terminate")
            else:
                self._destroy_one(control, instanceid)
                valid_count -= 1

        if not heading:
            self._set_state(all_instances, -1)
        else:
            self._set_state_pending()
        
        txt = "instance"
        if valid_count != 1:
            txt += "s"
        log.debug("Aware of %d running/starting %s" % (valid_count, txt))
            
    def _heading(self, state, valid_count):

        queue_len_sensor = state.get_sensor("queue-length")
        # should only be one queue reading for now:
        if queue_len_sensor is None:
            log.debug("no queuelen readings to analyze")
            return 0

        queue_len = int(queue_len_sensor.value['queue_length'])
        
        msg = "most recent qlen reading is %d" % queue_len
        
        if queue_len == 0 and valid_count == 0:
            log.debug(msg + " (empty queue and no instances)")
            return 0
        
        # If there are zero started already and a non-zero qlen, start one
        # even if it is below the low water mark.  Work still needs to be
        # drained by one instance.
        if queue_len != 0 and valid_count == 0:
            log.debug(msg + " (non-empty queue and no instances yet)")
            return 1
        
        if queue_len > self.high_water:
            log.debug(msg + " (above high water)")
            return 1
        elif queue_len < self.low_water:
            log.debug(msg + " (below low water)")
            if valid_count == self.min_instances:
                if self.min_instances == 1:
                    txt = "1 instance"
                else:
                    txt = "%d instances" % self.min_instances
                log.info("Down to %s, cannot reduce" % txt)
                return 0
            else:
                return -1
        else:
            log.debug(msg + " (inside bounds)")
            return 0
            
    def _launch_one(self, control):
        log.info("Requesting instance")
        launch_description = {"work_consumer": LaunchItem(1, self._allocation(), self._site(), None)}
        control.launch(self._deployable_type(), launch_description)
    
    def _pick_instance_to_die(self, candidates):
        # filter out instances that are in terminating state or 'worse'
        
        if not candidates:
            return None
        return random.sample(candidates, 1)[0]
    
    def _destroy_one(self, control, instanceid):
        log.info("Destroying an instance ('%s')" % instanceid)
        instance_list = [instanceid]
        control.destroy_instances(instance_list)
        
    def _deployable_type(self):
        return self.available_types[0]
        
    def _allocation(self):
        return self.available_allocations[0]
        
    def _site(self):
        return self.available_sites[0]
