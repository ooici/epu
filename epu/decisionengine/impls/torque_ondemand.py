import ion.util.ionlog
from epu.epucontroller.controller_core import EngineState

log = ion.util.ionlog.getLogger(__name__)

import random
import time

from twisted.internet import defer

from epu.decisionengine import Engine
from epu.epucontroller import LaunchItem
from epu.ionproc.torque import TorqueManagerClient
import epu.states as InstanceStates

BAD_STATES = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]
TERMINATE_DELAY_SECS = 300

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

        self.free_worker_times = {}
        self.add_worker_times = {}
        self.num_torque_workers = 0
        self.workers = []
        
    @defer.inlineCallbacks
    def initialize(self, control, state, conf=None):
        """Engine API method"""
        # todo: need central constants for these key strings
        parameters = {"timed-pulse-irregular":5000}
        if conf and conf.has_key("force_site"):
            self.available_sites = [conf["force_site"]]

        if conf and conf.has_key("epuworker_type"):
            self.available_types = [conf["epuworker_type"]]

        if conf and conf.has_key("epuworker_allocation"):
            self.available_allocations = [conf["epuworker_allocation"]]

        if not conf:
            raise Exception("cannot initialize without external configuration")

        # create a client for managing the torque headnode
        if conf.has_key("torque"):
            self.torque = conf['torque']
        else:
            self.torque = None
        if not self.torque:
            self.torque = TorqueManagerClient()
            yield self.torque.attach()

        # first thing to do is subscribe to the torque default queue
        yield self.torque.watch_queue(control.controller_name)

        log.info("Torque on demand engine initialized")
        
        control.configure(parameters)

    @defer.inlineCallbacks
    def decide(self, control, state):
        """Engine API method"""

        self._set_state_stable()

        # get worker status (free, offline, etc.) info from torque
        worker_status_msg = state.get_sensor("worker-status")
        worker_status = self._get_worker_status(worker_status_msg)
        try:
            del worker_status['localhost']
            log.debug("Removed localhost from worker_status")
        except:
            log.debug("No localhost in worker_status, skipping.")
        log.debug("Got worker status message: %s" % worker_status)

        pending = state.get_pending_instances()
        num_pending_instances = len(pending)
        log.debug("There are %s pending instances." % num_pending_instances)

        num_queued_jobs = self._get_queuelen(state)
        log.debug("There are %s queued jobs." % num_queued_jobs)

        num_free_workers = self._get_num_workers_status(worker_status, 'free')
        log.debug("There are %s free workers." % num_free_workers)

        num_offline_workers = self._get_num_workers_status(worker_status, 'offline')
        log.debug("There are %s offline workers." % num_offline_workers)

        num_new_offline_workers = self._get_num_new_offline_workers(worker_status)
        log.debug("There are %s new offline workers." % num_new_offline_workers)

        new_workers = self._get_new_running_workers(state)
        num_new_workers = len(new_workers)
        log.debug("There are %s new running workers: %s" % (num_new_workers, new_workers))

        log.debug("There are %s total workers." % self.num_torque_workers)

        # determine the number of instances to launch
        if (num_pending_instances >= 0) and \
           (self.num_torque_workers >= 0) and \
           (num_new_workers >= 0) and \
           (num_queued_jobs >= 0) and \
           (num_new_offline_workers < 1) and \
           (num_free_workers >= 0):
            num_instances = num_pending_instances + \
                            num_free_workers + \
                            num_new_workers
            num_instances_to_launch = num_queued_jobs - num_instances
        else:
            val = "%s, %s, %s, %s" % (num_pending_instances, \
                                      self.num_torque_workers, \
                                      num_new_workers, \
                                      num_queued_jobs)
            log.debug("Bad value detected: (%s)" % val)
            num_instances_to_launch = 0
        if num_instances_to_launch > 0:
            log.debug("Attempting to launch %s instances." % num_instances_to_launch)
            for i in range(num_instances_to_launch):
                self._launch_one(control)
        else:
            log.debug("Not launching instances. Offlining free nodes.")
            cur_time = time.time()
            for host in worker_status.keys():
                try:
                    time_diff = cur_time - self.free_worker_times[host]
                except:
                    time_diff = 0
                if (worker_status[host] == 'free') and \
                   (time_diff > TERMINATE_DELAY_SECS):
                    log.debug("Offlining node: %s" % host)
                    yield self.torque.offline_node(host)

        # add new workers to torque
        for host in new_workers:
            self.workers.append(host)
            self.num_torque_workers += 1
            log.debug("Adding node: %s" % host)
            self.add_worker_times[host] = time.time()
            yield self.torque.add_node(host)

        # note first time nodes move out of the offline state
        for host in worker_status.keys():
            if 'offline' not in worker_status[host]:
                cur_time = time.time()
                if not self.free_worker_times.has_key(host):
                    log.debug('Host %s is no longer offline: %s' % (host, cur_time))
                    self.free_worker_times[host] = cur_time

        # terminate nodes
        log.debug("Attempting to remove and terminate nodes.")
        for host in worker_status.keys():
            cur_time = time.time()
            try:
                time_diff = cur_time - self.free_worker_times[host]
            except:
                time_diff = 0
            if (('offline' in worker_status[host]) or \
                (('down' in worker_status[host]) and \
                 (host != 'localhost'))) and \
               (time_diff > TERMINATE_DELAY_SECS):
                log.debug("Removing node: %s" % host)
                yield self.torque.remove_node(host)
                instance = self._get_instance_from_ip(state, host)
                if instance:
                    instanceid = instance.instance_id
                    log.debug("Terminating node: %s (%s)" % (instanceid, host))
                    self._destroy_one(control, instanceid)
                    if self.num_torque_workers > 0:
                        self.num_torque_workers -= 1
                    if self.add_worker_times.has_key(host):
                        del self.add_worker_times[host]
                    if self.free_worker_times.has_key(host):
                        del self.free_worker_times[host]
                    if host in self.workers:
                        self.workers.remove(host)
                else:
                    log.debug("Could not terminate node: %s" % host)

        # cleanup other nodes
        log.debug("Attempting to cleanup nodes.")
        for host in self.add_worker_times.keys():
            if not self.free_worker_times.has_key(host):
                add_time = self.add_worker_times[host]
                cur_time = time.time()
                kill_time = add_time + TERMINATE_DELAY_SECS
                if cur_time > kill_time:
                    log.debug("Removing node (cleanup): %s" % host)
                    yield self.torque.remove_node(host)
                    instance = self._get_instance_from_ip(state, host)
                    instanceid = instance.instance_id
                    log.debug("Terminating node (cleanup): %s (%s)" % (instanceid, host))
                    self._destroy_one(control, instanceid)
                    if self.num_torque_workers > 0:
                        self.num_torque_workers -= 1
                    if self.add_worker_times.has_key(host):
                        del self.add_worker_times[host]
                    if self.free_worker_times.has_key(host):
                        del self.free_worker_times[host]
                    if host in self.workers:
                        self.workers.remove(host)

        # remove from workers, free_worker_times and add_worker_times
        log.debug("Attempting final cleanup.")
        bad_instances = state.get_instances_by_state(InstanceStates.FAILED)
        for instance in bad_instances:
            if instance.private_hostname:
                host = instance.private_hostname
            elif instance.public_hostname:
                host = instance.public_hostname
            else:
                host = instance.public_ip
            if host in self.workers:
                log.debug("Performing final cleanup for %s" % host)
                if self.num_torque_workers > 0:
                    self.num_torque_workers -= 1
                if self.add_worker_times.has_key(host):
                    del self.add_worker_times[host]
                if self.free_worker_times.has_key(host):
                    del self.free_worker_times[host]
                if host in self.workers:
                    self.workers.remove(host)

        valid_count = num_pending_instances + self.num_torque_workers
        txt = "instance"
        if valid_count != 1:
            txt += "s"
        log.debug("Aware of %d running/pending %s" % (valid_count, txt))
            
    def _get_queuelen(self, state):
        qlen_item = state.get_sensor("queue-length")

        if not qlen_item:
            log.debug("no queuelen readings to analyze")
            return 0
        try:
            qlen = int(qlen_item.value['queue_length'])
        except (KeyError, ValueError):
            log.debug("Got invalid queuelen value: %s", qlen_item.value)
        
        return qlen

    def _get_num_new_offline_workers(self, worker_status):
        num_workers = 0
        for worker in worker_status.keys():
            if (not self.free_worker_times.has_key(worker)) and \
               (worker_status[worker] == 'offline'):
                num_workers += 1
        return num_workers

    def _get_num_workers_status(self, worker_status, status):
        num_workers = 0
        for worker in worker_status.keys():
            if worker_status[worker] == status:
                num_workers += 1
        return num_workers

    def _get_instance_from_ip(self, state, host):
        found = []
        for instance in state.instances.itervalues():
            if instance.private_hostname == host:
                found.append(instance)
            elif instance.public_hostname == host:
                found.append(instance)
            elif instance.public_ip == host:
                found.append(instance)

        if not found:
            return None

        if len(found) == 1:
            return found[0]

        # if there are multiple matches, grab the most recent one
        return max(found, key=lambda i: i.state_time)

    def _get_new_running_workers(self, state):
        new_running_workers = []
        for instance in state.get_instances_by_state(InstanceStates.RUNNING):
            if instance.private_hostname:
                host = instance.private_hostname
            elif instance.public_hostname:
                host = instance.public_hostname
            else:
                host = instance.public_ip
            if not host:
                log.warn('Instance %s is running but has no IP (??)', instance.instance_id)
                continue
            if host not in self.workers:
                log.debug('new running instance: %s (%s)', host, instance.instance_id)
                new_running_workers.append(host)
        return new_running_workers

    def _get_worker_status(self, worker_status_msg):
        if not worker_status_msg:
            log.debug("no worker status message")
            return {}

        # worker status sensors look like:
        #   {"queue_name": "blahblah", "worker_status": "thestring"}
        
        worker_status = worker_status_msg.value
        if not worker_status or not 'worker_status' in worker_status:
            log.warn("Got invalid worker status sensor item: %s", worker_status)
            return {}

        worker_status_str = worker_status['worker_status']
        log.debug("worker status string: %s" % worker_status_str)

        if not worker_status_str:
            log.debug("empty worker status string")
            return {}

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
