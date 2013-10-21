# Copyright 2013 University of Chicago

import logging

from dashi.util import LoopingCall
from epu.epumanagement.conf import *  # noqa
from epu.epumanagement.health import HealthMonitor, TESTCONF_HEALTH_INIT_TIME
from epu.domain_log import EpuLoggerThreadSpecific

log = logging.getLogger(__name__)


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

    def __init__(self, epum_store, notifier, provisioner_client, epum_client,
                 ouagent_client, disable_loop=False):
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
        self.is_leader = False

        # The instances of HealthMonitor that make the health decisions for each domain
        self.monitors = {}

    def recover(self):
        """Called whenever the whole EPUManagement instance is instantiated.
        """
        # For callbacks: "now_leader()" and "not_leader()"
        self.epum_store.register_doctor(self)

    def now_leader(self, block=False):
        """Called when this instance becomes the doctor leader.
        """
        log.info("Elected as Doctor leader")
        self._leader_initialize()
        self.is_leader = True
        if block:
            if self.control_loop:
                self.control_loop.thread.join()
            else:
                raise ValueError("cannot block without a control loop")

    def not_leader(self):
        """Called when this instance is known not to be the doctor leader.
        """
        if self.control_loop:
            self.control_loop.stop()
            self.control_loop = None
        self.is_leader = False

    def _leader_initialize(self):
        """Performs initialization routines that may require async processing
        """
        if self.enable_loop:
            if not self.control_loop:
                self.control_loop = LoopingCall(self._loop_top)
            self.control_loop.start(10)

    def _loop_top(self, timestamp=None):
        """
        Run the doctor decider loop.

        Every time this runs, each domain's health monitor is loaded and
        """
        # Perhaps in the meantime, the leader connection failed, bail early
        if not self.is_leader:
            return

        domains = self.epum_store.get_all_domains()
        active_domains = {}
        for domain in domains:
            with EpuLoggerThreadSpecific(domain=domain.domain_id, user=domain.owner):

                if not domain.is_removed():
                    active_domains[domain.key] = domain

        # Perhaps in the meantime, the leader connection failed, bail early
        if not self.is_leader:
            return

        # Monitors that are not active anymore
        for key in self.monitors.keys():
            if key not in active_domains:
                del self.monitors[key]

        # New health monitors (new to this doctor instance, at least)
        for domain_key in filter(lambda x: x not in self.monitors,
                active_domains.iterkeys()):
            try:
                self._new_monitor(active_domains[domain_key])
            except Exception, e:
                log.error("Error creating health monitor for '%s': %s",
                          domain_key, str(e), exc_info=True)

        for domain_key in self.monitors.keys():
            # Perhaps in the meantime, the leader connection failed, bail early
            if not self.is_leader:
                return
            try:
                self.monitors[domain_key].update(timestamp)
            except Exception, e:
                log.error("Error in doctor's update call for '%s': %s",
                          domain_key, str(e), exc_info=True)

    def _new_monitor(self, domain):
        with EpuLoggerThreadSpecific(domain=domain.domain_id, user=domain.owner):

            if not domain.is_health_enabled():
                return
            health_conf = domain.get_health_config()
            health_kwargs = {}
            if EPUM_CONF_HEALTH_BOOT in health_conf:
                health_kwargs['boot_seconds'] = health_conf[EPUM_CONF_HEALTH_BOOT]
            if EPUM_CONF_HEALTH_MISSING in health_conf:
                health_kwargs['missing_seconds'] = health_conf[EPUM_CONF_HEALTH_MISSING]
            if EPUM_CONF_HEALTH_REALLY_MISSING in health_conf:
                health_kwargs['really_missing_seconds'] = health_conf[EPUM_CONF_HEALTH_REALLY_MISSING]
            if EPUM_CONF_HEALTH_ZOMBIE in health_conf:
                health_kwargs['zombie_seconds'] = health_conf[EPUM_CONF_HEALTH_ZOMBIE]
            if TESTCONF_HEALTH_INIT_TIME in health_conf:
                health_kwargs['init_time'] = health_conf[TESTCONF_HEALTH_INIT_TIME]
            self.monitors[domain.key] = HealthMonitor(domain, self.ouagent_client, **health_kwargs)
