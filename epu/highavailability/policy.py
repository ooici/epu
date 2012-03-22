
import logging
from epu.states import ProcessState
log = logging.getLogger(__name__)


def dummy_dispatch_process_callback(*args, **kwargs):
    log.debug("dummy_dispatch_process_callback(%s, %s) called" % args, kwargs)

def dummy_terminate_process_callback(*args, **kwargs):
    log.debug("dummy_terminate_process_callback(%s, %s) called" % args, kwargs)

class NPreservingPolicy(object):

    def __init__(self, parameters=None, process_spec=None,
            dispatch_process_callback=None, terminate_process_callback=None):

        self.dispatch_process = dispatch_process_callback or dummy_dispatch_process_callback
        self.terminate_process = terminate_process_callback or dummy_terminate_process_callback

        if parameters:
            self.parameters = parameters
        else:
            self._parameters = None

        self.process_spec = process_spec
        self.previous_all_procs = {}

    @property
    def parameters(self):
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
        if not self.parameters:
            log.debug("No policy parameters set. Not applying policy.")
            return

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

                state = proc['state']
                state_code, state_name = state.split('-')
                running_code, running_name = ProcessState.RUNNING.split('-')
                if state_code > running_code: # if terminating or exited, etc
                    managed_upids.remove(proc['upid'])

        # Apply npreserving policy
        to_rebalance = self.parameters['preserve_n'] - len(managed_upids)
        if to_rebalance < 0: # remove excess
            to_rebalance = -1 * to_rebalance
            for to_rebalance in range(0, to_rebalance):
                upid = managed_upids[0]
                terminated = self.terminate_process(upid)
        elif to_rebalance > 0:
            for to_rebalance in range(0, to_rebalance):
                pd_name = self._get_least_used_pd(all_procs)
                new_upid = self.dispatch_process(pd_name, self.process_spec)

        self.previous_all_procs = all_procs

        return managed_upids


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


class HAPolicyException(BaseException):
    pass
