import logging
import datetime

from epu.sensors import Statistics
from epu.sensors.trafficsentinel import TrafficSentinel
from epu.states import ProcessState, HAState

log = logging.getLogger(__name__)


def dummy_schedule_process_callback(*args, **kwargs):
    log.debug("dummy_schedule_process_callback(%s, %s) called" % (args, kwargs))


def dummy_terminate_process_callback(*args, **kwargs):
    log.debug("dummy_terminate_process_callback(%s, %s) called" % (args, kwargs))


def dummy_process_state_callback(*args, **kwargs):
    log.debug("dummy_process_state_callback(%s, %s) called" % (args, kwargs))


class IPolicy(object):
    """Interface for HA Policies
    """

    _status = None

    def apply_policy(self, all_procs, managed_upids):
        raise NotImplementedError("'apply_policy' is not implemented")

    def status(self):
        raise NotImplementedError("'status' is not implemented")

    def _get_least_used_pd(self, all_procs):
        smallest_n = None
        smallest_pd = None
        for pd_name, procs in all_procs.iteritems():
            if smallest_n == None or smallest_n > len(procs):
                smallest_n = len(procs)
                smallest_pd = pd_name
        return smallest_pd

    def _extract_upids_from_all_procs(self, all_procs):
        all_upids = []
        for pd, procs in all_procs.iteritems():
            for proc in procs:
                all_upids.append(proc['upid'])
        return all_upids

    def _process_state(self, all_procs, upid):
        for pd, procs in all_procs.iteritems():
            for proc in procs:
                if proc.get('upid') == upid:
                    return proc.get('state')

    def _flatten_all_procs(self, all_procs):
        flat = {}
        for pd, procs in all_procs.iteritems():
            for proc in procs:
                if proc.get('upid') is not None:
                    flat[proc['upid']] = proc
        return flat

    def _filter_invalid_processes(self, all_procs, managed_upids):
        """_filter_invalid_processes
        Takes a list of processes and filters processes that will never reach
            a running state. This includes TERMINATING, TERMINATED, EXITED,
            FAILED, REJECTED
        """

        all_upids = self._extract_upids_from_all_procs(all_procs)
        # Check for missing upids (From a dead pd for example)
        for upid in managed_upids:
            if upid not in all_upids:
                # Process is missing! Remove from managed_upids
                managed_upids.remove(upid)

        for pd, procs in all_procs.iteritems():
            for proc in procs:

                if proc['upid'] not in managed_upids:
                    continue

                if proc.get('state') is None:
                    # Pyon procs may have no state
                    continue

                state = proc['state']
                if state > ProcessState.RUNNING:  # if terminating or exited, etc
                    managed_upids.remove(proc['upid'])

        return managed_upids


class NPreservingPolicy(IPolicy):
    """
    The NPreservingPolicy is a simple example HA Policy that is intended to be
    called periodically with the state of the processes in the PDs. Callbacks
    (see __init__) are called to terminate or start VMs.
    """

    def __init__(self, parameters=None, process_definition_id=None,
            process_configuration=None, schedule_process_callback=None,
            terminate_process_callback=None, process_state_callback=None, **kwargs):
        """Set up the Policy

        @param parameters: The parameters used by this policy to determine the
        distribution and number of VMs. This policy expects a dictionary with
        one key/val, like: {'preserve_n': n}

        @param process_definition_id: The process definition id to send to the PD on
        launch

        @param process_configuration: The process configuration to send to the PD on
        launch

        @param schedule_process_callback: A callback to schedule a process to a
        PD. Must have signature: schedule(pd_name, process_def_id), and return a
        upid as a string

        @param terminate_process_callback: A callback to terminate a process on
        a PD. Must have signature: terminate(upid)

        @param process_state_callback: A callback to get a process state from
        a PD. Must have signature: process_state(upid)
        """

        self.schedule_process = schedule_process_callback or dummy_schedule_process_callback
        self.terminate_process = terminate_process_callback or dummy_terminate_process_callback
        self.process_state = process_state_callback or dummy_process_state_callback

        self._status = HAState.PENDING

        if parameters:
            self.parameters = parameters
        else:
            self._parameters = None
            self._schedule_kwargs = {}

        self.process_definition_id = process_definition_id
        self.process_configuration = process_configuration
        self.previous_all_procs = {}

        self.minimum_n = 1  # Minimum number of instances running to be considered READY

    @property
    def parameters(self):
        """parameters

        a dictionary with the number of processes to maintain with the following
        schema:

        {
            'preserve_n': n,
            'execution_engine_id': 'someengineid', #OPTIONAL
            'node_exclusive': 'unique', #OPTIONAL
            'constraints': { ... }, #OPTIONAL
        }
        """
        return self._parameters

    @parameters.setter
    def parameters(self, new_parameters):
        try:
            new_parameters['preserve_n']
        except TypeError:
            raise HAPolicyException('parameters must be a dictionary')
        except KeyError:
            raise HAPolicyException('parameters must have a preserve_n value')

        if self._status in (HAState.READY, HAState.STEADY):
            self._status = HAState.READY

        self._parameters = new_parameters
        self._schedule_kwargs = get_schedule_process_kwargs(new_parameters)

    def apply_policy(self, all_procs, managed_upids):
        """Apply the policy.

        This method is intended to be called periodically to maintain the
        parameters of the policy. Returns a list of the upids that the HA is
        maintaining, and may start or terminate processes

        @param all_procs: a dictionary of PDs, each with a list of processes
        running on that PD
        @param managed_upids: a list of upids that the HA Service is maintaining
        """
        if not self.parameters:
            log.debug("No policy parameters set. Not applying policy.")
            return []

        managed_upids = self._filter_invalid_processes(all_procs, managed_upids)

        # Apply npreserving policy
        to_rebalance = self.parameters['preserve_n'] - len(managed_upids)
        if to_rebalance < 0:  # remove excess
            to_rebalance = -1 * to_rebalance
            for to_rebalance in range(0, to_rebalance):
                upid = managed_upids[0]
                terminated = self.terminate_process(upid)
        elif to_rebalance > 0:
            for to_rebalance in range(0, to_rebalance):
                pd_name = self._get_least_used_pd(all_procs)
                new_upid = self.schedule_process(pd_name, self.process_definition_id,
                    configuration=self.process_configuration,
                    **self._schedule_kwargs)

        self._set_status(to_rebalance, managed_upids, all_procs)

        self.previous_all_procs = all_procs

        return managed_upids

    def _set_status(self, to_rebalance, managed_upids, all_procs):

        running_upids = []
        for upid in managed_upids:
            if self._process_state(all_procs, upid) == ProcessState.RUNNING:
                running_upids.append(upid)

        if self._status == HAState.FAILED:
            # If already in FAILED state, keep this state.
            # Requires human intervention
            self._status = HAState.FAILED
        elif to_rebalance == 0 and len(running_upids) >= self.minimum_n:
            self._status = HAState.STEADY
        elif len(running_upids) >= self.minimum_n and self.parameters['preserve_n'] > 0:
            self._status = HAState.READY
        else:
            self._status = HAState.PENDING

    def status(self):
        return self._status


class SensorPolicy(IPolicy):

    def __init__(self, parameters=None, process_definition_id=None,
            schedule_process_callback=None, terminate_process_callback=None,
            process_state_callback=None,
            process_configuration=None, aggregator_config=None, *args, **kwargs):
        """Set up the Policy

        @param parameters: The parameters used by this policy to determine the
        distribution and number of VMs. This policy expects a dictionary with
        TODO

        @param process_definition_id: The process definition id to send to the
        PD on launch

        @param schedule_process_callback: A callback to schedule a process to a
        PD. Must have signature: schedule(pd_name, process_definition_id), and
        return a upid as a string

        @param terminate_process_callback: A callback to terminate a process on
        a PD. Must have signature: terminate(upid)

        @param process_state_callback: A callback to get a process state from
        a PD. Must have signature: process_state(upid)

        @param aggregator_config: configuration dict of aggregator. For traffic
        sentinel, this should look like:
          config = {
              'type': 'trafficsentinel',
              'host': 'host.name.tld',
              'port': 1235,
              'username': 'user',
              'password': 'pw'
          }
        """

        self.schedule_process = schedule_process_callback or dummy_schedule_process_callback
        self.terminate_process = terminate_process_callback or dummy_terminate_process_callback
        self.process_state = process_state_callback or dummy_process_state_callback

        if parameters:
            self.parameters = parameters
        else:
            self._parameters = None
            self._schedule_kwargs = {}

        self.process_definition_id = process_definition_id
        self.previous_all_procs = {}
        self._status = HAState.PENDING
        self.minimum_n = 1
        self.last_scale_action = datetime.datetime.min

        if aggregator_config is None:
            raise Exception("Must provide an aggregator config")

        aggregator_type = aggregator_config.get('type', '').lower()
        if aggregator_type == 'trafficsentinel':
            host = aggregator_config.get('host')
            username = aggregator_config.get('username')
            password = aggregator_config.get('password')
            port = aggregator_config.get('port', 443)
            protocol = aggregator_config.get('protocol', 'https')
            self._sensor_aggregator = TrafficSentinel(host, username, password, port=port, protocol=protocol)
            self.app_metrics = self._sensor_aggregator.app_metrics
            self.host_metrics = self._sensor_aggregator.app_metrics
        else:
            raise Exception("Don't know what to do with %s aggregator type" % aggregator_type)

    @property
    def parameters(self):
        """parameters

        a dictionary of parameters that looks like:

        metric: Name of Sensor Aggregator Metric to use for scaling decisions
        sample_period: Number of seconds of sample data to use (eg. if 3600, use sample data from 1 hour ago until present time
        sample_function: Statistical function to apply to sampled data. Choose from Average, Sum, SampleCount, Maximum, Minimum
        cooldown_period: Minimum time in seconds between scale up or scale down actions
        scale_up_threshhold: If the sampled metric is above this value, scale up the number of processes
        scale_up_n_processes: Number of processes to scale up by
        scale_down_threshhold: If the sampled metric is below this value, scale down the number of processes
        scale_down_n_processes: Number of processes to scale down by
        minimum_processes: Minimum number of processes to maintain
        maximum_processes: Maximum number of processes to maintain

        """
        return self._parameters

    @parameters.setter
    def parameters(self, new_parameters):

        if new_parameters.get('metric') is None:
            log.error("metric_name cannot be None")
            return

        try:
            sample = int(new_parameters.get('sample_period'))
            if sample < 0:
                raise ValueError()
        except ValueError:
            log.error("sample_period '%s' is not a positive integer" % (
                new_parameters.get('sample_period')))

        if new_parameters.get('sample_function') not in Statistics.ALL:
            log.error("'%s' is not a known sample_function. Choose from %s" % (
                new_parameters.get('sample_function'), Statistics.ALL))
            return

        try:
            cool = int(new_parameters.get('cooldown_period'))
            if cool < 0:
                raise ValueError()
        except ValueError:
            log.error("cooldown_period '%s' is not a positive integer" % (
                new_parameters.get('cooldown_period')))
            return

        try:
            float(new_parameters.get('scale_up_threshold'))
        except ValueError:
            log.error("scale_up_threshold '%s' is not a floating point number" % (
                new_parameters.get('scale_up_threshold')))
            return

        try:
            int(new_parameters.get('scale_up_n_processes'))
        except ValueError:
            log.error("scale_up_n_processes '%s' is not an integer" % (
                new_parameters.get('scale_up_n_processes')))
            return

        try:
            float(new_parameters.get('scale_down_threshold'))
        except ValueError:
            log.error("scale_down_threshold '%s' is not a floating point number" % (
                new_parameters.get('scale_down_threshold')))
            return

        try:
            int(new_parameters.get('scale_down_n_processes'))
        except ValueError:
            log.error("scale_down_n_processes '%s' is not an integer" % (
                new_parameters.get('scale_up_n_processes')))
            return

        try:
            minimum_processes = int(new_parameters.get('minimum_processes'))
            if minimum_processes < 0:
                raise ValueError()
        except ValueError:
            log.error("minimum_processes '%s' is not a positive integer" % (
                new_parameters.get('minimum_processes')))
            return

        try:
            maximum_processes = int(new_parameters.get('maximum_processes'))
            if maximum_processes < 0:
                raise ValueError()
        except ValueError:
            log.error("maximum_processes '%s' is not a positive integer" % (
                new_parameters.get('maximum_processes')))
            return

        # phew!
        self._parameters = new_parameters

        self._schedule_kwargs = get_schedule_process_kwargs(new_parameters)

    def status(self):
        return self._status

    def apply_policy(self, all_procs, managed_upids):

        if self._parameters is None:
            log.debug("No parameters set, unable to apply policy")
            return []

        time_since_last_scale = datetime.datetime.now() - self.last_scale_action
        if time_since_last_scale.seconds < self._parameters['cooldown_period']:
            log.debug("Returning early from apply policy because we're in cooldown")
            self._set_status(0, managed_upids)
            return managed_upids

        # Check for missing upids (From a dead pd for example)
        all_upids = self._extract_upids_from_all_procs(all_procs)
        for upid in managed_upids:
            if upid not in all_upids:
                # Process is missing! Remove from managed_upids
                managed_upids.remove(upid)

        # Check for terminated procs
        for pd, procs in all_procs.iteritems():
            for proc in procs:

                if proc['upid'] not in managed_upids:
                    continue

                if proc.get('state') is None:
                    # Pyon procs may have no state
                    continue

                state = proc['state']
                if state > ProcessState.RUNNING:  # if terminating or exited, etc
                    managed_upids.remove(proc['upid'])

        # Get numbers from metric
        hostnames = self._get_hostnames(all_procs, managed_upids)
        period = 60
        end_time = datetime.datetime.now() # TODO: what TZ does TS use?
        seconds = self._parameters['sample_period']
        start_time = end_time - datetime.timedelta(seconds=seconds)
        metric_name = self._parameters['metric']
        sample_function = self._parameters['sample_function']
        statistics = [sample_function, ]

        if metric_name in self.app_metrics or 'app_attributes' in metric_name:
            dimensions = {'pid': managed_upids}
        else:
            dimensions = {'hostname': hostnames}
        try:
            metric_per_host = self._sensor_aggregator.get_metric_statistics(
                    period, start_time, end_time, metric_name, statistics, dimensions)
        except Exception as e:
            log.exception("Problem getting metrics from sensor aggregator")
            return

        values = []
        for host, metric_value in metric_per_host.iteritems():
            values.append(metric_value[sample_function])

        log.debug("got metrics %s for %s" % (metric_per_host, dimensions))

        try:
            average_metric = sum(values) / len(values)
        except ZeroDivisionError:
            average_metric = 0
        if average_metric > self._parameters['scale_up_threshold']:
            scale_by = self._parameters['scale_up_n_processes']

            if len(managed_upids) - scale_by > self._parameters['maximum_processes']:
                scale_by = self._parameters['maximum_processes'] - len(managed_upids)

        elif average_metric < self._parameters['scale_down_threshold']:
            scale_by = - abs(self._parameters['scale_down_n_processes'])

            if len(managed_upids) + scale_by < self._parameters['minimum_processes']:
                scale_by = self._parameters['minimum_processes'] - len(managed_upids)
        else:
            scale_by = 0

        if scale_by == 0:
            if len(managed_upids) < self._parameters['minimum_processes']:
                scale_by = self._parameters['scale_up_n_processes']
            elif len(managed_upids) > self._parameters['maximum_processes']:
                scale_by = - abs(self._parameters['scale_down_n_processes'])

        if scale_by < 0:  # remove excess
            log.debug("Sensor policy scaling down by %s" % scale_by)
            scale_by = -1 * scale_by
            for to_scale in range(0, scale_by):
                upid = managed_upids[0]
                terminated = self.terminate_process(upid)
        elif scale_by > 0:  # Add processes
            log.debug("Sensor policy scaling up by %s" % scale_by)
            for to_rebalance in range(0, scale_by):
                pd_name = self._get_least_used_pd(all_procs)
                new_upid = self.schedule_process(pd_name, self.process_definition_id,
                    **self._schedule_kwargs)

        if scale_by != 0:
            self.last_scale_action = datetime.datetime.now()

        self._set_status(scale_by, managed_upids)

        self.previous_all_procs = all_procs

        return managed_upids

    def _set_status(self, to_rebalance, managed_upids):
        if self._status == HAState.FAILED:
            # If already in FAILED state, keep this state.
            # Requires human intervention
            self._status = HAState.FAILED
        elif to_rebalance == 0:
            self._status = HAState.STEADY
        elif len(managed_upids) >= self.minimum_n and self._parameters['minimum_processes'] > 0:
            self._status = HAState.READY
        else:
            self._status = HAState.PENDING

    def _get_hostnames(self, all_procs, upids):
        """get hostnames of eeagents that have managed processes
        """

        hostnames = []

        for pd, procs in all_procs.iteritems():
            for proc in procs:

                if proc['upid'] not in upids:
                    continue

                hostname = proc.get('hostname')
                if hostname is None:
                    continue

                hostnames.append(hostname)

        return list(set(hostnames))

policy_map = {
        'npreserving': NPreservingPolicy,
        'sensor': SensorPolicy,
}

_SCHEDULE_PROCESS_KWARGS = ('node_exclusive', 'execution_engine_id',
                            'constraints', 'queueing_mode', 'restart_mode')
def get_schedule_process_kwargs(parameters):
    kwargs = {}
    for k in _SCHEDULE_PROCESS_KWARGS:
        if k in parameters:
            kwargs[k] = parameters[k]
    return kwargs


class HAPolicyException(BaseException):
    pass
