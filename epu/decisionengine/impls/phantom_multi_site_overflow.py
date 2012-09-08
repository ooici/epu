import logging
import random
import datetime
from epu.decisionengine import Engine
from epu.states import InstanceState

log = logging.getLogger(__name__)

CONF_CLOUD_KEY = 'clouds'
CONF_N_PRESERVE_KEY = 'n_preserve'
CONF_SITE_KEY = 'site_name'
CONF_SIZE_KEY = 'size'
CONF_RANK_KEY = 'rank'
CONF_DTNAME_KEY = 'dtname'
CONF_INSTANCE_TYPE_KEY = 'instance_type'

HEALTHY_STATES = [InstanceState.REQUESTING, InstanceState.REQUESTED, InstanceState.PENDING, InstanceState.RUNNING, InstanceState.STARTED]
UNHEALTHY_STATES = [InstanceState.TERMINATING, InstanceState.TERMINATED, InstanceState.FAILED, InstanceState.RUNNING_FAILED]


class _PhantomOverflowSiteBase(object):

    def __init__(self, site_name, max_vms, dt_name, instance_type):
        self.site_name = site_name
        self.max_vms = max_vms
        self.target_count = -1
        self.determined_capacity = -1

        self.dt_name = dt_name
        self.instance_type = instance_type

        self.last_max_vms = 0
        self.last_healthy_vms = 0
        self.delta_vms = 0
        self.last_failure_count = 0
        self.last_healthy_count = 0

    def update_max(self, new_max):
        self.max_vms = new_max

    def update_state(self, state):
        all_instances = state.instances.values()
        self.site_instances = [a for a in all_instances if a.site == self.site_name]
        self.healthy_instances = [i for i in self.site_instances if i.state in HEALTHY_STATES]
        self.unhealthy_instances = [i for i in self.site_instances if i.state in UNHEALTHY_STATES]
        failed_array = set(i.instance_id for i in self.site_instances if i.state == InstanceState.FAILED)

        # if the failure count is increasing we may be at capacity
        if self.last_failure_count > len(failed_array):
            self.determined_capacity = len(self.healthy_instances)
            # if the health count has increased then we are not at capacity
            if len(self.healthy_instances) > self.last_healthy_count:
                self.determined_capacity = -1
            # if the total failure count is not at least N, then keep trying
            if len(failed_array) < self._failure_threshold:
                self.determined_capacity = -1

            # something based on the last time we tried

        self.last_failure_count = len(failed_array)

    def kill_vms(self, n):
        self.delta_vms = self.delta_vms - n

    def create_vms(self, n):
        self.delta_vms = self.delta_vms + n

    def commit_vms(self, control):
        # This is called once all of the adds and removes have happened
        # this is the number of VMs to increase of decrease by.  Here 
        # we also need to take into account any lost VMs
        lost_vm_count = self.last_healthy_count - len(self.healthy_instances)
        if lost_vm_count > 0:
            self.delta_vms = self.delta_vms + lost_vm_count 

        # use control object and delta to create of kill
        if self.delta_vms < 0:
            self.last_healthy_count = len(self.healthy_instances) + self.delta_vms
            # kill
        elif self.delta_vms > 0:
            self._launch_vms(control, self.delta_vms)
        self.delta_vms = 0

    def get_max_count(self):
        if self.determined_capacity == -1:
            return self.max_vms
        return min(self.max_vms, self.determined_capacity)

    def get_healthy_count(self):
        return len(self.healthy_instances)

    def _launch_vms(self, control, n):
        owner = control.domain.owner

        for i in range(n):
            launch_id, instance_ids = control.launch(self.dt_name,
                self.site_name,
                self.instance_type,
                caller=owner)
            if len(instance_ids) != 1:
                raise Exception("Could not retrieve instance ID after launch")

    def _destroy_vms(self, control, n):
        instanceids = []
        his = self.healthy_instances[:]
        for i in range(n):
            x = random.choice(his)
            his.remove(x)
            instanceids.append(x.instance_id)
        owner = control.domain.owner
        control.destroy_instances(instanceids, caller=owner)


class PhantomMultiSiteOverflowEngine(Engine):
    """A decision engine for allowing VMs to overflow to another cloud
        when the preferred cloud experiences an error
    """

    def __init__(self):
        super(PhantomMultiSiteOverflowEngine, self).__init__()

        self._site_list = []
        self.max_last_delta = 0

    def _conf_validate(self, conf):
        if not conf:
            raise ValueError("requires engine conf")

        required_fields = [CONF_CLOUD_KEY, CONF_N_PRESERVE_KEY]
        for f in required_fields:
            if f not in conf:
                raise ValueError("The configuration requires the key %s" % (f))
        self._cloud_list_validate(conf[CONF_CLOUD_KEY])

    def _cloud_list_validate(self, cloud_list):
        required_fields = [(CONF_SITE_KEY, str), 
                            (CONF_SIZE_KEY, int), 
                            (CONF_RANK_KEY, int)]
        for cloud in cloud_list:
            for f in required_fields:
                if f[0] not in cloud.keys():
                    raise ValueError("The configuration needs the key %s for all clouds" % (f))
                try:
                    cloud[f[0]] = f[1](cloud[f[0]])
                except ValueError:
                    raise ValueError("The value for %s must be a %s" % (str(f[0]), str(f[1])))


    def initialize(self, control, state, conf=None):
        try:
            self._conf_validate(conf)
            self.dt_name = conf[CONF_DTNAME_KEY]
            self.instance_type = conf[CONF_INSTANCE_TYPE_KEY]
            self._npreserve = conf[CONF_N_PRESERVE_KEY]
            clouds_list = sorted(conf[CONF_CLOUD_KEY], 
                                 key=lambda cloud: cloud[CONF_RANK_KEY])
            # more validation
            i = 1
            for c in clouds_list:
                if c[CONF_RANK_KEY] != i:
                    raise ValueError(
                        "The cloud rank is out of order.  No %d found" % (i))

                site_obj = _PhantomOverflowSiteBase(c[CONF_SITE_KEY], c[CONF_SIZE_KEY], self.dt_name, self.instance_type)
                self._site_list.append(site_obj)

                i = i + 1

        except Exception, ex:
            # cleanup anything created
            log.info("%s failed to initialized, error %s" % (type(self), ex))
            raise
        else:
            log.info("%s initialized: configuration is: %s" % (type(self), str(conf)))

    def dying(self):
        log.warn("%s does not implement dying" % (type(self)))

    def _kill_loop(self):
        for site in self._site_list:
            # check to see if the site vm max has been lowered
            c = site.get_healthy_count()
            m = site.get_max_count()
            if c > m:
                site.kill_vms(c - m)

    def _reduce_big_n_loop(self, delta):
        ndx = -(len(self._site_list))
        while delta > 0 and ndx < 0:
            site = self._site_list[ndx]
            c = site.get_healthy_count()
            if c <= delta:
                to_kill_count = c
            else:
                to_kill_count = delta

            delta = delta - to_kill_count
            site.kill_vms(to_kill_count)
            ndx = ndx + 1

    def _increse_big_n_loop(self, delta):
        ndx = 0
        while delta > 0 and ndx < len(self._site_list):
            site = self._site_list[ndx]
            c = site.get_healthy_count()
            m = site.get_max_count()
            available = m - c
            if available > 0:
                if available <= delta:
                    to_add = available
                else:
                    to_add = delta
                site.create_vms(to_add)
                delta = delta - to_add
            ndx = ndx + 1

    def decide(self, control, state):
        for site in self._site_list:
            site.update_state(state)
        self._kill_loop()

        delta = self._npreserve - self.max_last_delta
        if delta > 0:
            self._increse_big_n_loop(delta)
        else:
            self._reduce_big_n_loop(-delta)

        for site in self._site_list:
            site.commit_vms(control)
        self.max_last_delta = self._npreserve
        
    def reconfigure(self, control, newconf):
        if not newconf:
            raise ValueError("expected new engine conf")
        log.info("%s engine reconfigure, newconf: %s" % (type(self), newconf))


        try:
            self._cloud_list_validate(newconf[CONF_CLOUD_KEY])
            if newconf.has_key(npreserve_key):
                self._npreserve = newconf[npreserve_key]
            if newconf.has_key(CONF_CLOUD_KEY):
                self._merge_cloud_lists(newconf(CONF_CLOUD_KEY))

        except Exception, ex:
            log.info("%s failed to initialized, error %s" % (type(self), ex))
            raise
        else:
            log.info("%s initialized: configuration is: %s" % (type(self), str(conf)))

    

