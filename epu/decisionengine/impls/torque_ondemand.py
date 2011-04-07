import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import random

from epu.decisionengine import Engine
from epu.epucontroller import LaunchItem
import epu.states as InstanceStates

BAD_STATES = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]

class TorqueOnDemandEngine(Engine):
    """
    A decision engine that looks at queue length.  If there are queued
    jobs, it will launch one instance per job. If there are idle nodes,
    it will terminate them.
    """
    
    def __init__(self):
        super(TorqueOnDemandEngine, self).__init__()
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
        
        log.info("Torque on demand engine initialized")
        
        control.configure(parameters)

    def decide(self, control, state):
        """Engine API method"""
        all_instance_lists = state.get_all("instance-state")
        all_instance_health = state.get_all("instance-health")

        if all_instance_health:
            health = dict((node.node_id, node) for node in all_instance_health)
        else:
            health = None

        valid_count = 0
        for instance_list in all_instance_lists:
            instance_id = None
            ok = True
            for state_item in instance_list:
                if not instance_id:
                    instance_id = state_item.key
                if state_item.value in BAD_STATES:
                    ok = False
                    break
            if ok and instance_id:
                if health and not health[instance_id].is_ok():
                    self._destroy_one(control, instance_id)
                else:
                    valid_count += 1
        
        
        # Won't make a decision if there are pending instances. This would
        # need to be a lot more elaborate (requiring a datastore) to get a
        # faster response time whilst not grossly overcompensating. 
        any_pending = False
        for instance_list in all_instance_lists:
            # "has it contextualized at some point in its life?"
            found_started = False
            for state_item in instance_list:
                if state_item.value == InstanceStates.RUNNING:
                    found_started = True
                    break
            if not found_started:
                any_pending = True
        
        if any_pending:
            log.debug("Will not analyze with pending instances")
            self._set_state_pending()
            return
        
        heading = self._heading(state, valid_count)
        if heading > 0:
            self._launch_one(control)
            valid_count += 1
        elif heading < 0:
            instanceid = self._pick_instance_to_die(all_instance_lists)
            if not instanceid:
                log.error("There are no valid instances to terminate")
            else:
                self._destroy_one(control, instanceid)
                valid_count -= 1

        if not heading:
            self._set_state(all_instance_lists, -1)
        else:
            self._set_state_pending()
        
        txt = "instance"
        if valid_count != 1:
            txt += "s"
        log.debug("Aware of %d running/starting %s" % (valid_count, txt))
            
    def _heading(self, state, valid_count):
        all_qlens = state.get_all("queue-length")
        # should only be one queue reading for now:
        if len(all_qlens) == 0:
            log.debug("no queuelen readings to analyze")
            return 0
        
        if len(all_qlens) != 1:
            raise Exception("multiple queuelen readings to analyze?")
        
        qlens = all_qlens[0]
        
        if len(qlens) == 0:
            log.debug("no queuelen readings to analyze")
            return 0
            
        recent = qlens[-1].value
        msg = "most recent qlen reading is %d" % recent
        
        if recent == 0 and valid_count == 0:
            log.debug(msg + " (empty queue and no instances)")
            return 0
        
        return 0
            
    def _launch_one(self, control):
        log.info("Requesting instance")
        launch_description = {}
        launch_description["work_consumer"] = \
                LaunchItem(1, self._allocation(), self._site(), None)
        control.launch(self._deployable_type(), launch_description)
    
    def _pick_instance_to_die(self, all_instance_lists):
        # filter out instances that are in terminating state or 'worse'
        
        candidates = []
        for instance_list in all_instance_lists:
            ok = True
            for state_item in instance_list:
                if state_item.value in BAD_STATES:
                    ok = False
                    break
            if ok:
                candidates.append(state_item.key)
        
        log.debug("Found %d instances that could be killed:\n%s" % (len(candidates), candidates))
        
        if len(candidates) == 0:
            return None
        elif len(candidates) == 1:
            return candidates[0]
        else:
            idx = random.randint(0, len(candidates)-1)
            return candidates[idx]
    
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
