
import logging

from epu.exceptions import ProgrammingError, PolicyError

log = logging.getLogger(__name__)


class IProcessControl(object):

    def schedule_process(self, pd_name, process_definition_id, **kwargs):
        """Launches a new process on the specified process dispatcher

        Returns upid of process
        """

    def terminate_process(self, upid):
        """Terminates a process in the system
        """

    def get_all_processes(self):
        """Gets a dictionary of lists of {"upid": "XXXX", "state": "XXXXX"} dicts
        """


class HighAvailabilityCore(object):
    """Core of High Availability Service
    """

    def __init__(self, CFG, control, process_dispatchers, Policy,
            process_definition_id=None, process_configuration=None,
            parameters=None, aggregator_config=None, name=None):
        """Create HighAvailabilityCore

        @param CFG - config dictionary for highavailabilty
        @param control - process control object. interface of IPolicyControl
        @param process_dispatchers - list of process dispatchers
        """

        self.CFG = CFG
        self.control = control
        self.process_dispatchers = process_dispatchers
        self.process_configuration = process_configuration
        self.policy_params = parameters
        self.aggregator_config = aggregator_config
        if name:
            self.logprefix = "HA Agent (%s): " % name
        else:
            self.logprefix = ""

        if not process_definition_id:
            raise ProgrammingError("You must have a process_definition_id")
        self.process_definition_id = process_definition_id

        self.policy = Policy(parameters=self.policy_params,
                schedule_process_callback=self._schedule,
                terminate_process_callback=self._terminate_upid,
                process_state_callback=self._process_state,
                process_definition_id=self.process_definition_id,
                process_configuration=self.process_configuration,
                aggregator_config=self.aggregator_config, name=name)
        self.managed_upids = []

    def apply_policy(self):
        """Should be run periodically by dashi/pyon proc container to check
        status of services, and balance to compensate for changes
        """
        log.debug("%sapplying policy", self.logprefix)

        all_procs = self.control.get_all_processes()
        try:
            managed_upids = self.policy.apply_policy(all_procs, self.managed_upids)
            if isinstance(managed_upids, (tuple, list)):
                self.managed_upids = managed_upids
        except PolicyError:
            log.exception("Couldn't apply policy because of an error")

    def set_managed_upids(self, upids):
        """Called to override the managed process set, for HAAgent restart
        """
        self.managed_upids = list(upids)

    def _schedule(self, pd_name, pd_id, configuration=None, constraints=None,
                  queueing_mode=None, restart_mode=None,
                  execution_engine_id=None, node_exclusive=None):
        """Dispatches a process to the provided pd, and returns the upid used
        to do so
        """
        try:

            upid = self.control.schedule_process(pd_name, pd_id,
                configuration=configuration, constraints=constraints,
                queueing_mode=queueing_mode, restart_mode=restart_mode,
                execution_engine_id=execution_engine_id,
                node_exclusive=node_exclusive)

        except Exception:
            log.exception("%sProblem scheduling proc on '%s'. Will try again later", self.logprefix, pd_name)
            return None
        self.managed_upids.append(upid)
        return upid

    def _terminate_upid(self, upid):
        """Finds a upid among available PDs, and terminates it
        """
        try:
            self.control.terminate_process(upid)
            self.managed_upids.remove(upid)
            return upid
        except Exception:
            log.exception("%sProblem terminating process '%s'. Will try again later", self.logprefix, upid)

        return None

    def _process_state(self, upid):
        """Finds a upid among available PDs, and gets its status
        """
        all_procs = self.control.get_all_processes()
        for pd_name, procs in all_procs.iteritems():
            for proc in procs:
                if proc.get('upid') == upid:
                    return proc.get('state')

        return None

    def status(self):
        """Returns a single status for the current state of the service
        """
        return self.policy.status()

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
