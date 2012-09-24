import logging
import datetime

from epu.sensors.trafficsentinel import TrafficSentinel
from epu.states import ProcessState, HAState
log = logging.getLogger(__name__)


def dummy_schedule_process_callback(*args, **kwargs):
    log.debug("dummy_schedule_process_callback(%s, %s) called" % args, kwargs)


def dummy_terminate_process_callback(*args, **kwargs):
    log.debug("dummy_terminate_process_callback(%s, %s) called" % args, kwargs)


class IPolicy(object):
    """Interface for HA Policies
    """

    def __init__(self, parameters=None, process_definition_id=None,
            schedule_process_callback=None, terminate_process_callback=None):
        raise NotImplementedError("'__init__' is not implemented")

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


class NPreservingPolicy(IPolicy):
    """
    The NPreservingPolicy is a simple example HA Policy that is intended to be
    called periodically with the state of the processes in the PDs. Callbacks
    (see __init__) are called to terminate or start VMs.
    """

    def __init__(self, parameters=None, process_definition_id=None,
            process_configuration=None, schedule_process_callback=None,
            terminate_process_callback=None):
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
        """

        self.schedule_process = schedule_process_callback or dummy_schedule_process_callback
        self.terminate_process = terminate_process_callback or dummy_terminate_process_callback

        if parameters:
            self.parameters = parameters
        else:
            self._parameters = None

        self.process_id = process_definition_id
        self.process_configuration = process_configuration
        self.previous_all_procs = {}

        self._status = HAState.PENDING

        self.minimum_n = 1  # Minimum number of instances running to be considered READY

    @property
    def parameters(self):
        """parameters

        a dictionary with the number of processes to maintain with the following
        schema:

        {
            'preserve_n': n
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

        self._parameters = new_parameters

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
                state_code, state_name = state.split('-')
                running_code, running_name = ProcessState.RUNNING.split('-')
                if state_code > running_code:  # if terminating or exited, etc
                    managed_upids.remove(proc['upid'])

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
                new_upid = self.schedule_process(pd_name, self.process_id,
                        configuration=self.process_configuration)

        self._set_status(to_rebalance, managed_upids)

        self.previous_all_procs = all_procs

        return managed_upids

    def _set_status(self, to_rebalance, managed_upids):
        if self._status == HAState.FAILED:
            # If already in FAILED state, keep this state.
            # Requires human intervention
            self._status == HAState.FAILED
        elif to_rebalance == 0:
            self._status = HAState.STEADY
        elif len(managed_upids) >= self.minimum_n and self.parameters['preserve_n'] > 0:
            self._status = HAState.READY
        else:
            self._status = HAState.PENDING

    def status(self):
        return self._status



class SensorPolicy(IPolicy):

    def __init__(self, parameters=None, process_definition_id=None,
            schedule_process_callback=None, terminate_process_callback=None,
            aggregator_config=None):
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

        @param aggregator_config: configuration dict of aggregator. For traffic
        sentinel, this should look like:
          config = {
              'type': 'trafficsentinel',
              'host': 'host.name.tld',
              'username': 'user',
              'password': 'pw'
          }
        """

        self.schedule_process = schedule_process_callback or dummy_schedule_process_callback
        self.terminate_process = terminate_process_callback or dummy_terminate_process_callback

        if parameters:
            self.parameters = parameters
        else:
            self._parameters = None

        self.process_id = process_definition_id
        self.previous_all_procs = {}
        self._status = HAState.PENDING
        self.minimum_n = 1

        if aggregator_config is None:
            raise Exception("Must provide an aggregator config")

        aggregator_type = aggregator_config.get('type', '').lower()
        if aggregator_type == 'trafficsentinel':
            host = aggregator_config.get('host')
            username = aggregator_config.get('username')
            password = aggregator_config.get('password')
            self._sensor_aggregator = TrafficSentinel(host, username, password)
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
        scaleupthreshhold: If the sampled metric is above this value, scale up the number of processes
        scaleupnprocesses: Number of processes to scale up by
        scaledownthreshhold: If the sampled metric is below this value, scale down the number of processes
        scaledownnprocesses: Number of processes to scale down by
        minimum_processes: Minimum number of processes to maintain
        maximum_processes: Maximum number of processes to maintain

        """
        return self._parameters

    @parameters.setter
    def parameters(self, new_parameters):
        # TODO: validate parameters
        self._parameters = new_parameters

    def status(self):
        return self._status

    def apply_policy(self, all_procs, managed_upids):

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
                state_code, state_name = state.split('-')
                running_code, running_name = ProcessState.RUNNING.split('-')
                if state_code > running_code:  # if terminating or exited, etc
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
        dimensions = {'hostname': hostnames}
        metric_per_host = self._sensor_aggregator.get_metric_statistics(
                period, start_time, end_time, metric_name, statistics, dimensions)

        values = []
        for host, metric_value in metric_per_host.iteritems():
            values.append(metric_value[sample_function])
        
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

        if scale_by < 0:  # remove excess
            scale_by = -1 * scale_by
            for to_scale in range(0, scale_by):
                upid = managed_upids[0]
                terminated = self.terminate_process(upid)
        elif scale_by > 0:
            for to_rebalance in range(0, scale_by):
                pd_name = self._get_least_used_pd(all_procs)
                new_upid = self.schedule_process(pd_name, self.process_id)
        
        self._set_status(scale_by, managed_upids)

        self.previous_all_procs = all_procs

        return managed_upids

    def _set_status(self, to_rebalance, managed_upids):
        if self._status == HAState.FAILED:
            # If already in FAILED state, keep this state.
            # Requires human intervention
            self._status == HAState.FAILED
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


class HAPolicyException(BaseException):
    pass
