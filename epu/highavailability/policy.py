
import logging
from epu.states import ProcessState, HAState
log = logging.getLogger(__name__)


def dummy_dispatch_process_callback(*args, **kwargs):
    log.debug("dummy_dispatch_process_callback(%s, %s) called" % args, kwargs)


def dummy_terminate_process_callback(*args, **kwargs):
    log.debug("dummy_terminate_process_callback(%s, %s) called" % args, kwargs)


class IPolicy(object):
    """Interface for HA Policies
    """

    def __init__(self, parameters=None, process_spec=None,
            dispatch_process_callback=None, terminate_process_callback=None):
        raise NotImplementedError("'__init__' is not implemented")

    def apply_policy(self, all_procs, managed_upids):
        raise NotImplementedError("'apply_policy' is not implemented")

    def status(self):
        raise NotImplementedError("'status' is not implemented")


class NPreservingPolicy(IPolicy):
    """
    The NPreservingPolicy is a simple example HA Policy that is intended to be
    called periodically with the state of the processes in the PDs. Callbacks
    (see __init__) are called to terminate or start VMs.
    """

    def __init__(self, parameters=None, process_spec=None,
            dispatch_process_callback=None, terminate_process_callback=None):
        """Set up the Policy

        @param parameters: The parameters used by this policy to determine the
        distribution and number of VMs. This policy expects a dictionary with
        one key/val, like: {'preserve_n': n}

        @param process_spec: The process specification to send to the PD on
        launch

        @param dispatch_process_callback: A callback to dispatch a process to a
        PD. Must have signature: dispatch(pd_name, process_spec), and return a
        upid as a string

        @param terminate_process_callback: A callback to terminate a process on
        a PD. Must have signature: terminate(upid)
        """

        self.dispatch_process = dispatch_process_callback or dummy_dispatch_process_callback
        self.terminate_process = terminate_process_callback or dummy_terminate_process_callback

        if parameters:
            self.parameters = parameters
        else:
            self._parameters = None

        self.process_spec = process_spec
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
                new_upid = self.dispatch_process(pd_name, self.process_spec)

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


class HSflowPolicy(IPolicy):

    def __init__(self, parameters=None, process_spec=None,
            dispatch_process_callback=None, terminate_process_callback=None,
            ganglia_hostname=None, ganglia_port=None):
        """Set up the Policy

        @param parameters: The parameters used by this policy to determine the
        distribution and number of VMs. This policy expects a dictionary with
        TODO

        @param process_spec: The process specification to send to the PD on
        launch

        @param dispatch_process_callback: A callback to dispatch a process to a
        PD. Must have signature: dispatch(pd_name, process_spec), and return a
        upid as a string

        @param terminate_process_callback: A callback to terminate a process on
        a PD. Must have signature: terminate(upid)

        @param ganglia_hostname: hostname of Ganglia server to connect to

        @param ganglia_port: port of Ganglia server to connect to
        """

        self.dispatch_process = dispatch_process_callback or dummy_dispatch_process_callback
        self.terminate_process = terminate_process_callback or dummy_terminate_process_callback

        if parameters:
            self.parameters = parameters
        else:
            self._parameters = None

        self.process_spec = process_spec
        self.previous_all_procs = {}

        self._status = HAState.PENDING

        self._ganglia = GangliaClient(hostname=ganglia_hostname, port=ganglia_port)

    @property
    def parameters(self):
        """parameters

        a dictionary with TODO
        """
        return self._parameters

    @parameters.setter
    def parameters(self, new_parameters):
        # TODO: validate parameters
        self._parameters = new_parameters

    def status(self):
        return self._status

    def apply_policy(self, all_procs, managed_upids):

        # Query Ganglia
        ganglia_info = self._ganglia.query()




policy_map = {
        'npreserving': NPreservingPolicy,
        'hsflow': HSflowPolicy,
}


class HAPolicyException(BaseException):
    pass
