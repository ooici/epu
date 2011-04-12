import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import random

from twisted.internet import defer

from epu.decisionengine import Engine
from epu.epucontroller import LaunchItem
from epu.ionproc.torque import TorqueManagerClient
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

        self.torque = None # setup in initialize()
        
    @defer.inlineCallbacks
    def initialize(self, control, state, conf=None):
        """Engine API method"""
        # todo: need central constants for these key strings
        parameters = {"timed-pulse-irregular":5000}
        if conf and conf.has_key("force_site"):
            self.available_sites = [conf["force_site"]]
        if not conf:
            raise Exception("cannot initialize without external configuration")

        # create a client for managing the torque headnode
        self.torque = TorqueManagerClient()
        yield self.torque.attach()

        # first thing to do is subscribe to the torque default queue
        yield self.torque.watch_queue(control.controller_name)

        log.info("Torque on demand engine initialized")
        
        control.configure(parameters)

    @defer.inlineCallbacks
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
        
        """
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
        """

        # TODO sample code:
        #    do this for adding/removing/offlining nodes
        #
        #   The yield is important.
        #
        #    yield self.torque.add_node("hostname")
        #    yield self.torque.remove_node("hostname")
        #    yield self.torque.offline_node("hostname")

        worker_status_str = state.get_all("worker-status")
        worker_status = self._get_worker_status(worker_status_str)
        log.debug("Got worker status: %s" % worker_status)

        num_pending_instances = self._get_num_pending_instances(all_instance_lists)
        log.debug("There are %s pending instances." % num_pending_instances)

        num_queued_jobs = state.get_all("queue-length")
        log.debug("There are %s queued jobs." % num_queued_jobs)

        num_instances_to_launch = num_queued_jobs - num_pending_instances
        if num_instances_to_launch > 0:
            log.debug("Attempting to launch %s instances." % num_instances_to_launch)
            for i in range(num_instances_to_launch):
                self._launch_one(control)
                valid_count += 1
        else:
            log.debug("Not launching instances. Offlining free nodes.")
            for host in worker_status.keys():
                if worker_status[host] == 'free':
                    log.debug("Offlining node: %s" % host)
                    yield self.torque.offline_node(host)

        new_running_workers = self._get_new_running_workers(state,
                                worker_status, all_instance_lists)
        log.debug("There are %s new running workers." % new_running_workers)
        for host in new_running_workers:
            log.debug("Adding node: %s" % host)
            yield self.torque.add_node(host)

        log.debug("Attempting to remove and terminate all offline nodes.")
        for host in worker_status.keys():
            if worker_status[host] == 'offline':
                log.debug("Removing node: %s" % host)
                yield self.torque.remove_node(host)
                instanceid = state.get_instance_from_ip(host)
                log.debug("Terminating node: %s (%s)" % (instanceid, host))
                self._destroy_one(control, instanceid)
                valid_count -= 1

        txt = "instance"
        if valid_count != 1:
            txt += "s"
        log.debug("Aware of %d running/starting %s" % (valid_count, txt))
            
    def _get_num_pending_instances(self, all_instances):
        pending_states = [InstanceStates.REQUESTING, InstanceStates.REQUESTED,
                          InstanceStates.PENDING, InstanceStates.STARTED,
                          InstanceStates.ERROR_RETRYING]
        num_pending_instances = 0
        for instance in all_instances:
            for state_item in instance:
                if state_item.value in pending_states:
                    num_pending_instances += 1
        return num_pending_instances

    def _get_new_running_workers(self, state, worker_status, all_instances):
        new_running_workers = []
        for instance in all_instances:
            for state_item in instance:
                if state_item.value == InstanceStates.RUNNING:
                    host = state.get_instance_public_ip(state_item.key)
                    if host not in worker_status.keys():
                        new_running_workers.append(host)
        return new_running_workers

    def _get_worker_status(self, worker_status_str):
        workersplit = worker_status_str.split(';')
        worker_status = {}
        for worker in workersplit:
            host = worker.split(':')[0].strip()
            status = worker.split(':')[1].strip()
            worker_status[host] = status
        return worker_status

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
