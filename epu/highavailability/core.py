
import logging

log = logging.getLogger(__name__)  


class HighAvailabilityCore(object):
    """Core of High Availability Service
    """

    def __init__(self, CFG, pd_client_kls, process_dispatchers):
        """Create HighAvailabilityCore

        @param CFG - config dictionary for highavailabilty
        @param pd_client_kls - a constructor method for creating a 
               ProcessDispatcherClient that takes one argument, the topic
        @param process_dispatchers - list of process dispatchers
        """

        self.CFG = CFG
        self.provisioner_client_kls = pd_client_kls
        self.process_dispatchers = process_dispatchers
        self.policy_params = {}

    def rebalance(self):
        """Should be run periodically by dashi/pyon proc container to check 
        status of services, and balance to compensate for changes
        """
        log.info("Rebalance")

        all_procs = self._query_process_dispatchers()
        log.debug("Got procs: %s" % all_procs)

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

    def _get_pd_client(self, name):
        """Returns a process dispatcher client with the topic/name
        provided, using the process dispatcher client class provided
        in the constructor
        """
        return self.provisioner_client_kls(name)


    def reconfigure_policy(self, new_policy):
        """Change the number of needed instances of service
        """
        self.policy_params = new_policy
