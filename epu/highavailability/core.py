
import logging
import uuid

from socket import timeout

log = logging.getLogger(__name__)  

class HighAvailabilityCore(object):
    """Core of High Availability Service
    """

    def __init__(self, CFG, pd_client_kls, process_dispatchers, process_spec, Policy):
        """Create HighAvailabilityCore

        @param CFG - config dictionary for highavailabilty
        @param pd_client_kls - a constructor method for creating a 
               ProcessDispatcherClient that takes one argument, the topic
        @param process_dispatchers - list of process dispatchers
        """

        self.CFG = CFG
        self.provisioner_client_kls = pd_client_kls
        self.process_dispatchers = process_dispatchers
        self.process_spec = process_spec
        self.policy_params = None
        self.policy = Policy(parameters=self.policy_params,
                dispatch_process_callback=self._dispatch_pd_spec,
                terminate_process_callback=self._terminate_upid, 
                process_spec=self.process_spec)
        self.managed_upids = []

    def apply_policy(self):
        """Should be run periodically by dashi/pyon proc container to check 
        status of services, and balance to compensate for changes
        """
        log.debug("applying policy")

        all_procs = self._query_process_dispatchers()
        self.managed_upids = list(self.policy.apply_policy(all_procs, self.managed_upids))


    def _query_process_dispatchers(self):
        """Get list of processes from each pd, and return a dictionary
        indexed by the pd name
        """
        all_procs = {}

        for pd_name in self.process_dispatchers:
            pd_client = self._get_pd_client(pd_name)
            try:
                procs = pd_client.describe_processes()
                all_procs[pd_name] = procs
            except timeout:
                log.warning("%s timed out when calling describe_processes" % pd_name)
            except:
                log.exception("Problem querying %s" % pd_name)

        return all_procs

    def _get_pd_client(self, name):
        """Returns a process dispatcher client with the topic/name
        provided, using the process dispatcher client class provided
        in the constructor
        """
        return self.provisioner_client_kls(name)

    def _dispatch_pd_spec(self, pd_name, spec):
        """Dispatches a process to the provided pd, and returns the upid used
        to do so
        """
        pd_client = self._get_pd_client(pd_name)
        upid = uuid.uuid4().hex
        
        pd_client.dispatch_process(upid, spec, None, None)
        self.managed_upids.append(upid)

        return upid

    def _terminate_upid(self, upid):
        """Finds a upid among available PDs, and terminates it
        """
        all_procs = self._query_process_dispatchers()
        for pd_name, procs in all_procs.iteritems():
            for proc in procs:
                if proc.get('upid') == upid:
                    pd_client = self._get_pd_client(pd_name)
                    pd_client.terminate_process(upid)
                    self.managed_upids.remove(upid)
                    return upid

        return None


    def reconfigure_policy(self, new_policy):
        """Change the number of needed instances of service
        """
        self.policy_params = new_policy
        self.policy.parameters = new_policy

    def dump(self):

        state = {}
        state['policy'] = self.policy_params
        state['managed_upids'] = self.managed_upids

        return state
