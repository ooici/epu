
import logging
import uuid

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
        self.policy_params = None
        self.policy = Policy(parameters=self.policy_params,
                dispatch_process_callback=self._dispatch_pd_spec,
                terminate_process_callback=self._terminate_upid)
        self.process_spec = process_spec
        self.managed_upids = []

    def apply_policy(self):
        """Should be run periodically by dashi/pyon proc container to check 
        status of services, and balance to compensate for changes
        """
        log.info("apply policy")

        all_procs = self._query_process_dispatchers()
        #self.policy.apply_policy(all_procs)

        #TODO: move to nPreserving module
        to_rebalance = self.policy_params.get('preserve_n', 0) - len(self.managed_upids)
        if to_rebalance < 0: # remove excess
            to_rebalance = -1 * to_rebalance
            for to_rebalance in range(0, to_rebalance):
                upid = self.managed_upids.pop()
                terminated = self._terminate_upid(upid)
                log.error("Couldn't terminate %s" % upid) 
        elif to_rebalance > 0:
            for to_rebalance in range(0, to_rebalance):
                pd_name = self._get_least_used_pd()
                upid = self._dispatch_pd_spec(pd_name, self.process_spec)
                self.managed_upids.append(upid)


    def _query_process_dispatchers(self):
        """Get list of processes from each pd, and return a dictionary
        indexed by the pd name
        """
        all_procs = {}

        for pd_name in self.process_dispatchers:
            pd_client = self._get_pd_client(pd_name)
            procs = pd_client.describe_processes()
            all_procs[pd_name] = procs

        return all_procs

    def _get_least_used_pd(self):
        all_procs = self._query_process_dispatchers()
        smallest_n = None
        smallest_pd = None
        for pd_name, procs in all_procs.iteritems():
            if smallest_n == None or smallest_n > len(procs):
                smallest_n = len(procs)
                smallest_pd = pd_name
        return smallest_pd

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
    
        return upid

    def _terminate_upid(self, upid):
        """Finds a upid among available PDs, and terminates it
        """
        all_procs = self._query_process_dispatchers()
        for pd_name, procs in all_procs.iteritems():
            for proc in procs:
                if proc.get('upid') == upid:
                    pd_client = self._get_pd_client(pd_name)
                    return pd_client.terminate_process(upid)

        return None


    def reconfigure_policy(self, new_policy):
        """Change the number of needed instances of service
        """
        self.policy_params = new_policy
