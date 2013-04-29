import sys
import logging
import random

from datetime import datetime, timedelta

from epu.decisionengine import Engine
from epu.states import InstanceState

log = logging.getLogger(__name__)

CONF_CLOUD_KEY = 'clouds'
CONF_N_PRESERVE_KEY = 'domain_desired_size'
CONF_SITE_KEY = 'site_name'
CONF_SIZE_KEY = 'size'
CONF_RANK_KEY = 'rank'
CONF_DTNAME_KEY = 'dtname'
CONF_N_TERMINATE_KEY = 'terminate'

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

HEALTHY_STATES = [InstanceState.REQUESTING, InstanceState.REQUESTED,
    InstanceState.PENDING, InstanceState.RUNNING, InstanceState.STARTED]
UNHEALTHY_STATES = [InstanceState.TERMINATING, InstanceState.TERMINATED,
    InstanceState.FAILED, InstanceState.RUNNING_FAILED]


class _PhantomOverflowSiteBase(object):

    def __init__(self, site_name, max_vms, dt_name, rank):
        self.site_name = site_name
        self.max_vms = max_vms
        self.target_count = -1
        self.determined_capacity = -1
        self.rank = rank
        self.healthy_instances = []

        self.dt_name = dt_name
        self.instance_type = None

        self.last_max_vms = 0
        self.last_healthy_vms = 0
        self.delta_vms = 0
        self.last_failure_count = 0
        self.last_healthy_count = 0
        self._failure_threshold = 32

    def update_max(self, new_max):
        self.max_vms = new_max

    def update_state(self, state):
        all_instances = state.instances.values()
        self.site_instances = [a for a in all_instances if a.site == self.site_name]
        self.healthy_instances = [i for i in self.site_instances if i.state in HEALTHY_STATES]
        self.unhealthy_instances = [i for i in self.site_instances if i.state in UNHEALTHY_STATES]
        failed_array = set(i.instance_id for i in self.site_instances if i.state == InstanceState.FAILED)

        # if the failure count is increasing we may be at capacity
        if self.last_failure_count < len(failed_array):
            self.determined_capacity = len(self.healthy_instances)
            # if the health count has increased then we are not at capacity
            if len(self.healthy_instances) > self.last_healthy_count:
                self.determined_capacity = -1
            # if the total failure count is not at least N, then keep trying
            if len(failed_array) < self._failure_threshold:
                self.determined_capacity = -1

            # something based on the last time we tried

        self.last_healthy_count = len(self.healthy_instances)
        self.last_failure_count = len(failed_array)

    def kill_vms(self, n):
        self.delta_vms = self.delta_vms - n

    def create_vms(self, n):
        self.delta_vms = self.delta_vms + n

    def commit_vms(self, control):
        # This is called once all of the adds and removes have happened
        # this is the number of VMs to increase of decrease by.  Here
        if self.delta_vms < 0:
            self._destroy_vms(control, -self.delta_vms)
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
        for i in range(n):
            log.info("calling launch for %s" % (self.site_name))
            launch_id, instance_ids = control.launch(self.dt_name,
                self.site_name,
                self.instance_type)
            if len(instance_ids) != 1:
                raise Exception("Could not retrieve instance ID after launch")

    def _destroy_vms(self, control, n):
        instanceids = []
        his = self.healthy_instances[:]
        if n > len(his):
            raise Exception("Trying to kill %d VMs but there are only %d" % (n, len(his)))
        for i in range(n):
            x = random.choice(his)
            his.remove(x)
            instanceids.append(x.instance_id)
        control.destroy_instances(instanceids)


class PhantomMultiSiteOverflowEngine(Engine):
    """A decision engine for allowing VMs to overflow to another cloud
        when the preferred cloud experiences an error
    """

    def __init__(self):
        super(PhantomMultiSiteOverflowEngine, self).__init__()

        self.dying_ctr = 0
        self.dying_ttl = 0
        self._site_list = []
        self.logprefix = ""

        self.cooldown_period = 0
        self.metric = None
        self.minimum_vms = 0
        self.maximum_vms = sys.maxint

    def _get_logprefix(self, control):
        try:
            logprefix = "%s:%s: " % (control.domain.owner, control.domain.domain_id)
            return logprefix
        except AttributeError:
            return ""

    def _conf_validate(self, conf):
        if not conf:
            raise ValueError("requires engine conf")
        if CONF_N_PRESERVE_KEY in conf:
            raise ValueError("'%s' should no longer be used. Please use '%s'" % (CONF_N_PRESERVE_KEY, CONF_MINIMUM_VMS))
        required_fields = [CONF_CLOUD_KEY, CONF_MINIMUM_VMS, CONF_DTNAME_KEY, ]
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
            self.logprefix = self._get_logprefix(control)
            self.time_of_last_scale_action = datetime.min
            clouds_list = sorted(conf[CONF_CLOUD_KEY],
                                 key=lambda cloud: cloud[CONF_RANK_KEY])
            # more validation
            i = 1
            for c in clouds_list:
                if c[CONF_RANK_KEY] != i:
                    raise ValueError(
                        "The cloud rank is out of order.  No %d found" % (i))

                site_obj = _PhantomOverflowSiteBase(c[CONF_SITE_KEY], c[CONF_SIZE_KEY], self.dt_name, i)
                self._site_list.append(site_obj)

                i = i + 1

            self.reconfigure(control, conf)

        except Exception, ex:
            # cleanup anything created
            log.info("%s failed to initialized, error %s" % (type(self), ex))
            raise
        else:
            log.info("%s initialized: configuration is: %s" % (type(self), str(conf)))

    def dying(self):
        log.warn("%s does not implement dying" % (type(self)))

    def _get_excess_vms(self):
        """_get_excess_vms

        return the number of excess VMs on any of the sites we
        have started VMs on. This can happen when a user lowers the limit
        of the maximum number of VMs on a site.
        """
        total_to_kill = 0
        for site in self._site_list:
            # check to see if the site vm max has been lowered
            c = site.get_healthy_count()
            m = site.get_max_count()
            if c > m and m >= 0:
                k = c - m
                site.kill_vms(k)
                total_to_kill = total_to_kill + k
        return total_to_kill

    def _reduce_big_n_loop(self, delta):
        ndx = len(self._site_list) - 1
        while delta > 0 and ndx >= 0:
            site = self._site_list[ndx]
            c = site.get_healthy_count()
            if c <= delta:
                to_kill_count = c
            else:
                to_kill_count = delta

            delta = delta - to_kill_count
            site.kill_vms(to_kill_count)
            ndx = ndx - 1

    def _increase_big_n_loop(self, delta):
        ndx = 0
        while delta > 0 and ndx < len(self._site_list):
            site = self._site_list[ndx]
            c = site.get_healthy_count()
            m = site.get_max_count()
            if m == -1:
                available = delta
            else:
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

        total_healthy_vms = 0
        healthy_instances = []
        for site in self._site_list:
            site.update_state(state)
            total_healthy_vms = total_healthy_vms + site.get_healthy_count()
            healthy_instances = healthy_instances + site.healthy_instances

        # ugly
        # the ugly works like this:  we count the number of terminates
        # that come in and we keep a TTL.
        # and the time of terminate we cache how many healthy VMs there
        # are.  Then at decide time we check the healthy count.  If
        # the healthy count did not go down by the dying count then
        # we delay making any decisions.  We decrement the TTL.  If
        # the TTL gets to 0 we assume something went wrong here and
        # we continue to the normal logic.
        if self.dying_ctr > 0:
            log.info("XXXXXXXXXXXXXX doing dying logic")
            some_died = self._total_healthy_vms - total_healthy_vms
            self._set_last_healthy_count()

            self.dying_ctr = self.dying_ctr - some_died
            if self.dying_ctr > 0:
                self.dying_ttl = self.dying_ttl - 1
                if self.dying_ttl > 0:
                    return
                self.dying_ttl = 0
                self.dying_ctr = 0
        # end ugly

        # Here we check if there are any excess VMs on any of the sites we
        # have started VMs on. This can happen when a user lowers the limit
        # of the maximum number of VMs on a site. In this case, rather than
        # the regular sensor/minimum/maximum calculation, we reduce the number
        # running VMs by the number of excess VMs
        #
        # The sensor calculation will be done on the next loop.
        excess_vms = self._get_excess_vms()
        if excess_vms > 0:
            x = total_healthy_vms - excess_vms
            delta = self.minimum_vms - x
        else:

            cooldown = timedelta(seconds=self.cooldown_period)
            time_since_last_action = datetime.now() - self.time_of_last_scale_action
            if time_since_last_action < cooldown:
                log.debug(self.logprefix + "No scaling action, in cooldown period")
                return
            sensor_delta = self._calculate_needed_vms(state, healthy_instances)

            wanted = total_healthy_vms + sensor_delta
            wanted = min(max(wanted, self.minimum_vms), self.maximum_vms)
            delta = wanted - total_healthy_vms

        log.info(self.logprefix + "multi site decide VM delta = %d; minimum = %d; maximum = %s; current = %d",
                 delta, self.minimum_vms, self.maximum_vms, total_healthy_vms)

        if delta >= 0:
            self._increase_big_n_loop(delta)
        else:
            self._reduce_big_n_loop(-delta)

        if delta != 0:
            self.time_of_last_scale_action = datetime.now()

        for site in self._site_list:
            site.commit_vms(control)

    def _calculate_needed_vms(self, state, healthy_instances):

        cooldown = timedelta(seconds=self.cooldown_period)
        time_since_last_action = datetime.now() - self.time_of_last_scale_action
        if time_since_last_action < cooldown:
            log.debug(self.logprefix + "No scaling action, in cooldown period")
            return 0
        elif self.metric is not None and self.sample_function is not None:
            values = []
            if (hasattr(state, 'sensors') and state.sensors and
                    state.sensors.get(self.metric) and
                    state.sensors[self.metric].get(self.sample_function)):
                values.append(state.sensors[self.metric].get(self.sample_function))

            # TODO: domain sensor values could preempt instance values, but they
            # are averaged for now (until someone complains)

            for instance in healthy_instances:

                if (hasattr(instance, 'sensor_data') and instance.sensor_data and
                        instance.sensor_data.get(self.metric) and
                        instance.sensor_data[self.metric].get(self.sample_function)):
                    values.append(instance.sensor_data[self.metric].get(self.sample_function))
            try:
                divisor = max(len(values), len(healthy_instances))
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

        else:
            scale_by = 0

        return scale_by

    def _set_last_healthy_count(self):
        total_healthy_vms = 0
        for site in self._site_list:
            total_healthy_vms = total_healthy_vms + site.get_healthy_count()
        self._total_healthy_vms = total_healthy_vms

    def reconfigure(self, control, newconf):
        if not newconf:
            raise ValueError("expected new engine conf")
        log.info(self.logprefix + "%s engine reconfigure, newconf: %s" % (type(self), newconf))

        try:
            self._cloud_list_validate(newconf[CONF_CLOUD_KEY])

            if CONF_CLOUD_KEY in newconf:
                self._merge_cloud_lists(newconf[CONF_CLOUD_KEY])

            if CONF_N_TERMINATE_KEY in newconf:
                terminate_id = newconf[CONF_N_TERMINATE_KEY]
                log.info(self.logprefix + "terminating %s" % (terminate_id))
                control.destroy_instances([terminate_id])

                # ugly things start here.  we need to know that some are
                # dying so we dont thrash with decide
                if self.dying_ctr == 0:
                    # we do not want to get the base count more than once
                    self._set_last_healthy_count()
                log.info("XXXXXXXXXXXXXX setting the dying counter")
                self.dying_ctr = self.dying_ctr + 1
                self.dying_ttl = self.dying_ttl + 5
                # end ugly

            if CONF_DTNAME_KEY in newconf:
                new_dt = newconf.get(CONF_DTNAME_KEY)
                if not new_dt:
                    raise ValueError("cannot have empty %s conf: %d" % (CONF_DTNAME_KEY, new_dt))
                self.dt_name = new_dt

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

        except Exception, ex:
            log.info(self.logprefix + "%s failed to initialized, error %s" % (type(self), ex))
            raise
        else:
            log.info(self.logprefix + "%s initialized: configuration is: %s" % (type(self), str(newconf)))

    def _merge_cloud_lists(self, clouds_list):

        # existing clouds
        clouds_dict = {}

        for c in self._site_list:
            clouds_dict[c.site_name] = (c, c.rank, c.max_vms)

        for c in clouds_list:
            site_name = c[CONF_SITE_KEY]
            size = c[CONF_SIZE_KEY]
            rank = c[CONF_RANK_KEY]

            if site_name in clouds_dict:
                clouds_dict[site_name] = (clouds_dict[site_name][0], rank, size)
            else:
                if rank in [i[1] for i in clouds_dict.values()]:
                    raise Exception("There is already a site at rank %d" % (rank))
                site_obj = _PhantomOverflowSiteBase(site_name, size, self.dt_name, rank)
                clouds_dict[site_name] = (site_obj, rank, size)

        # now make sure the order is preserved
        i = 1
        order_list = sorted(clouds_dict.values(), key=lambda cloud: cloud[1])
        for ct in order_list:
            if ct[1] != i:
                raise Exception("The order is not preserved in the multi site engine update")
            i = i + 1

        # at this point everything is good for adjustment
        for c in clouds_dict:
            (site_obj, rank, size) = clouds_dict[c]
            site_obj.rank = rank
            site_obj.update_max(size)
        # turn it back into an ordered list
        site_list = [i[0] for i in clouds_dict.values()]
        self._site_list = sorted(site_list, key=lambda cloud: cloud.rank)
