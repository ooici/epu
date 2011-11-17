from twisted.internet.task import LoopingCall
from twisted.internet import defer
from epu.epumanagement.conf import *
from epu.epumanagement.health import HealthMonitor, TESTCONF_HEALTH_INIT_TIME

import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)

class EPUMDoctor(object):
    """The doctor handles critical sections related to 'pronouncing' a VM instance unhealthy.

    In the future it may farm out subtasks to the EPUM workers (EPUMReactor) but currently all
    health-check activity happens directly via the doctor role.

    The instance of the EPUManagementService process that hosts a particular EPUMDoctor instance
    might not be the elected doctor.  When it is the elected doctor, this EPUMDoctor instance
    handles that functionality.  When it is not the elected doctor, this EPUMDoctor instance
    handles being available in the election.

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    See: https://confluence.oceanobservatories.org/display/CIDev/EPUManagement+Refactor
    """
    
    def __init__(self, epum_store, notifier, provisioner_client, epum_client, ouagent_client, disable_loop=False):
        """
        @param epum_store State abstraction for all EPUs
        @param notifier A way to signal state changes
        @param provisioner_client A way to destroy VMs
        @param epum_client A way to launch subtasks to EPUM workers (reactor roles) (TODO: not sure if needed)
        @param ouagent_client See OUAgent dump_state() in architecture documentation
        @param disable_loop For unit/integration tests, don't run a timed decision loop
        """
        self.epum_store = epum_store
        self.notifier = notifier
        self.provisioner_client = provisioner_client
        self.epum_client = epum_client
        self.ouagent_client = ouagent_client

        self.control_loop = None
        self.enable_loop = not disable_loop

        # The instances of HealthMonitor that make the health decisions for each EPU
        self.monitors = {}

        # There can only ever be one health call run at ANY time.  This could be expanded to be
        # a latch per EPU for better concurrency, but keeping it simple, especially for prototype.
        self.busy = defer.DeferredSemaphore(1)

    def recover(self):
        """Called whenever the whole EPUManagement instance is instantiated.
        """
        # For callbacks: "now_leader()" and "not_leader()"
        self.epum_store.register_doctor(self)

    def now_leader(self):
        """Called when this instance becomes the doctor leader.
        """
        self._leader_initialize()

    def not_leader(self):
        """Called when this instance is known not to be the doctor leader.
        """
        if self.control_loop:
            self.control_loop.stop()
            self.control_loop = None

    def _leader_initialize(self):
        """Performs initialization routines that may require async processing
        """
        if self.enable_loop:
            if not self.control_loop:
                self.control_loop = LoopingCall(self._loop_top)
            self.control_loop.start(10)

    @defer.inlineCallbacks
    def _loop_top(self, timestamp=None):
        """
        Run the doctor decider loop.

        Every time this runs, each EPU's health monitor is loaded and
        """
        # Perhaps in the meantime, the leader connection failed, bail early
        if not self.epum_store.currently_doctor():
            defer.returnValue(None)

        epus = yield self.epum_store.all_active_epus()
        for epu_name in epus.keys():
            yield epus[epu_name].recover()
        
        # Perhaps in the meantime, the leader connection failed, bail early
        if not self.epum_store.currently_doctor():
            defer.returnValue(None)

        # Monitors that are not active anymore
        for epu_name in self.monitors.keys():
            if epu_name not in epus.keys():
                del self.monitors[epu_name]

        # New health monitors (new to this doctor instance, at least)
        for new_epu_name in filter(lambda x: x not in self.monitors.keys(), epus.keys()):
            try:
                yield self._new_monitor(new_epu_name)
            except Exception,e:
                log.error("Error creating health monitor for '%s': %s", new_epu_name, str(e), exc_info=True)

        for epu_name in self.monitors.keys():
            # Perhaps in the meantime, the leader connection failed, bail early
            if not self.epum_store.currently_doctor():
                defer.returnValue(None)
            try:
                yield self.busy.run(self.monitors[epu_name].update, timestamp)
            except Exception,e:
                log.error("Error in doctor's update call for '%s': %s", epu_name, str(e), exc_info=True)
    
    @defer.inlineCallbacks
    def _new_monitor(self, epu_name):
        epu_state = yield self.epum_store.get_epu_state(epu_name)
        if not epu_state.is_health_enabled():
            defer.succeed(None)
        health_conf = yield epu_state.get_health_conf()
        health_kwargs = {}
        if health_conf.has_key(EPUM_CONF_HEALTH_BOOT):
            health_kwargs['boot_seconds'] = health_conf[EPUM_CONF_HEALTH_BOOT]
        if health_conf.has_key(EPUM_CONF_HEALTH_MISSING):
            health_kwargs['missing_seconds'] = health_conf[EPUM_CONF_HEALTH_MISSING]
        if health_conf.has_key(EPUM_CONF_HEALTH_ZOMBIE):
            health_kwargs['zombie_seconds'] = health_conf[EPUM_CONF_HEALTH_ZOMBIE]
        if health_conf.has_key(TESTCONF_HEALTH_INIT_TIME):
            health_kwargs['init_time'] = health_conf[TESTCONF_HEALTH_INIT_TIME]
        self.monitors[epu_name] = HealthMonitor(epu_state, **health_kwargs)

