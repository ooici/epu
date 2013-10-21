# Copyright 2013 University of Chicago

import logging

from epu.epumanagement.reactor import EPUMReactor
from epu.epumanagement.doctor import EPUMDoctor
from epu.epumanagement.decider import EPUMDecider
from epu.epumanagement.reaper import EPUMReaper
from epu.epumanagement.core import DomainSubscribers
from epu.epumanagement.conf import EPUM_INITIALCONF_EXTERNAL_DECIDE,\
    EPUM_INITIALCONF_DEFAULT_NEEDY_IAAS,\
    EPUM_INITIALCONF_DEFAULT_NEEDY_IAAS_ALLOC,\
    PROVISIONER_VARS_KEY, EPUM_RECORD_REAPING_DEFAULT_MAX_AGE,\
    EPUM_CONF_DECIDER_LOOP_INTERVAL, EPUM_DECIDER_DEFAULT_LOOP_INTERVAL

log = logging.getLogger(__name__)


class EPUManagement(object):
    """
    Message-layer independent EPU Management Service
    See: https://confluence.oceanobservatories.org/display/CIDev/EPUManagement+Refactor
    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing

    This has the same method signatures as IEpuManagementClient -- this fact is exploited
    in test/dev situations to bypass the messaging layer altogether.
    """

    def __init__(self, initial_conf, notifier, provisioner_client, ouagent_client, dtrs_client, epum_client=None,
                 store=None, statsd_cfg=None):
        """Given a configuration, instantiate all EPUM roles and objects

        INITIAL_CONF dict:
           "service_name": If present, override the default service name "epu_management_service"
           "persistence_type": Only valid settings (strings) are currently: "memory" or "zookeeper"
           "persistence_url": If non-memory, a connection string.
           "persistence_user": If non-memory, a connection username.
           "persistence_pw": If non-memory, a connection password.
           "_external_decide_invocations": For unit and integration tests only. See use below.
           "needy_default_iaas_site": If register-need does not include IaaS site
           "needy_default_iaas_allocation": If register-need does not include IaaS allocation


        @param initial_conf All configurations as dict.
        @param notifier Subscriber notifier (See clients.py)
        @param provisioner_client ProvisionerClient instance (See clients.py)
        @param ouagent_client OUAgentClient instance (See clients.py)
        @param dtrs_client DTRSClient
        @param epum_client EPUManagement client (See clients.py). If None, uses self (in-memory).
        @param store EPUMStore implementation, or None
        @param statsd_cfg statsd configuration dict, or None
        """

        self.initialized = False

        if not epum_client:
            # If no EPUM client is supplied, it's assumed this is in test/dev mode.  The "messages"
            # are actually direct invocations of the service methods.  (See clients.py)
            epum_client = self

        if not notifier:
            raise ValueError("Subscriber notifier is required")

        if not provisioner_client:
            raise ValueError("Provisioner client is required")

        if not dtrs_client:
            raise ValueError("DTRS client is required")

        # See self.msg_register_need()
        self.needy_default_iaas_site = initial_conf.get(EPUM_INITIALCONF_DEFAULT_NEEDY_IAAS, None)
        self.needy_default_iaas_alloc = initial_conf.get(EPUM_INITIALCONF_DEFAULT_NEEDY_IAAS_ALLOC, None)

        # For unit and integration tests only.  Eliminates decider/doctor timed loop and makes the
        # decider/doctor wait for an external invocation.
        # See self._run_decisions() and self._doctor_appt()
        self._external_decide_mode = initial_conf.get(EPUM_INITIALCONF_EXTERNAL_DECIDE, False)

        base_provisioner_vars = initial_conf.get(PROVISIONER_VARS_KEY)

        self.domain_subscribers = DomainSubscribers(notifier)

        self.epum_store = store

        # The instance of the EPUManagementService process that hosts a particular EPUMReactor instance
        # might not be configured to receive messages.  But when it is receiving messages, they all go
        # to the EPUMReactor instance.
        self.reactor = EPUMReactor(self.epum_store, self.domain_subscribers, provisioner_client, epum_client)

        # The instance of the EPUManagementService process that hosts a particular EPUMDecider instance
        # might not be the elected decider.  When it is the elected decider, its EPUMDecider instance
        # handles that functionality.  When it is not the elected decider, its EPUMDecider instance
        # handles being available in the election.
        decider_loop_interval = initial_conf.get(EPUM_CONF_DECIDER_LOOP_INTERVAL,
            EPUM_DECIDER_DEFAULT_LOOP_INTERVAL)
        self.decider = EPUMDecider(self.epum_store, self.domain_subscribers, provisioner_client, epum_client,
                dtrs_client, disable_loop=self._external_decide_mode, base_provisioner_vars=base_provisioner_vars,
                loop_interval=decider_loop_interval, statsd_cfg=statsd_cfg)

        # The instance of the EPUManagementService process that hosts a particular EPUMDoctor instance
        # might not be the elected leader.  When it is the elected leader, this EPUMDoctor handles that
        # functionality.  When it is not the elected leader, this EPUMDoctor handles the constant
        # participation in the election.
        self.doctor = EPUMDoctor(self.epum_store, notifier, provisioner_client, epum_client,
                                 ouagent_client, disable_loop=self._external_decide_mode)

        # The instance of the EPUManagementService process that hosts a particular EPUMReaper instance
        # might not be the elected leader.  When it is the elected leader, this EPUMReaper handles that
        # functionality.  When it is not the elected leader, this EPUMReaper handles the constant
        # participation in the election.

        record_reaping_max_age = initial_conf.get('record_reaping_max_age',
                                                  EPUM_RECORD_REAPING_DEFAULT_MAX_AGE)
        self.reaper = EPUMReaper(self.epum_store, record_reaping_max_age,
                                 disable_loop=self._external_decide_mode)

    def initialize(self):
        """
        WARNING: Initialize should be called before any messages arrive to this worker instance
        """

        # EPUMReactor has no recover(), it is completely stateless and does not participate in any leader
        # elections. recover() needs to run before any messages start arriving. It pulls information
        # from persistence and refreshes local caches.
        self.doctor.recover()
        self.decider.recover()
        self.reaper.recover()

        self.initialized = True

    def _run_decisions(self):
        """For unit and integration tests only
        """
        if not self.initialized:
            raise Exception("Not initialized")
        if not self._external_decide_mode:
            raise Exception("Not configured to accept external decision invocations")
        self.decider._loop_top()

    def _doctor_appt(self, timestamp=None):
        """For unit and integration tests only
        """
        if not self.initialized:
            raise Exception("Not initialized")
        if not self._external_decide_mode:
            raise Exception("Not configured to accept external doctor check invocations")
        self.doctor._loop_top(timestamp=timestamp)

    def _run_reaper_loop(self):
        """For unit and integration tests only
        """
        if not self.initialized:
            raise Exception("Not initialized")
        if not self._external_decide_mode:
            raise Exception("Not configured to accept external decision invocations")
        self.reaper._loop_top()

    # -------------------------------------------
    # External Messages: Sent by other components
    # -------------------------------------------

    def msg_subscribe_domain(self, caller, domain_id, subscriber_name, subscriber_op):
        """Subscribe to asynchronous state updates for instances of a domain

        @param caller Caller, if available
        @param domain_id The domain of interest
        @param subscriber_name Interested party
        @param subscriber_op What to call; required if subscription requested
        """
        return self.reactor.subscribe_domain(caller, domain_id,
            subscriber_name, subscriber_op)

    def msg_unsubscribe_domain(self, caller, domain_id, subscriber_name):
        """ New in R2: Unsubscribe to state updates about a particular domain

        @param caller Caller, if available
        @param domain_id The domain of interest
        @param subscriber_name Uninterested party
        """
        return self.reactor.unsubscribe_domain(caller, domain_id, subscriber_name)

    def msg_list_domains(self, caller):
        """Return a list of domains in the system
        """
        return self.reactor.list_domains(caller)

    def msg_describe_domain(self, caller, domain_id):
        """Return a state structure for a domain, or None
        """
        return self.reactor.describe_domain(caller, domain_id)

    def msg_add_domain(self, caller, domain_id, definition_id, config, subscriber_name=None,
                    subscriber_op=None):
        """Add a new Domain (logically separate Decision Engine).

        SEE: msg_reconfigure_domain() documentation below

        @param caller Caller, if available
        @param domain_id domain name/ID
        @param definition_id domain definition name/ID
        @param config Initial configuration, see msg_reconfigure_domain for config doc
        """
        if not self.initialized:
            raise Exception("Not initialized")
        self.reactor.add_domain(caller, domain_id, definition_id, config,
            subscriber_name=subscriber_name, subscriber_op=subscriber_op)

    def msg_remove_domain(self, caller, domain_id):
        """ New in R2: Remove a domain entirely.  All running instances of that domain will be terminated.

        @param caller Caller, if available
        @param domain_id domain name/ID
        """
        if not self.initialized:
            raise Exception("Not initialized")
        self.reactor.remove_domain(caller, domain_id)
        # TODO: the engine API supports this via dying(), preserve_n is an internal thing (even though common)

    def msg_reconfigure_domain(self, caller, domain_id, config):
        """ From R1: op_reconfigure

        @param caller Caller, if available
        @param domain_id domain name/ID
        @param config New configuration

        =============
        DOMAIN_CONFIG
        =============

        The expectations for the "domain_config" parameter follow.  This will cover both the initial
        configuration (that is passed to add_domain) as well as any rules about reconfiguration of
        an existing domain.

        What is a Domain?  It is technically the VM instances out there running that make up a logical
        group of entities doing "something" together.

        To "reconfigure" a domain is technically to change the configuration here in the EPUM service
        (the control plane) which may have ramifications on what a particular domain's constituent
        parts end up being.

        When we refer to a domain vs. another, in most systems the main distinguishing thing is
        the *type* of VM instances that are being launched.  A "deployable type" is a VM image that
        is launched with specific configuration values and 'recipes' to instantiate it.

        In more advanced systems, the distinguishing feature is also the particular client (whose
        identity is verified in the messaging layer appropriate way).  There may be many different
        clients using the same EPUM service to launch many applications across many IaaS clouds.
        TODO: This is not currently implemented.

        A domain's configuration is broken down into the following key sections:

        * GENERAL
        * ENGINE CONF
        * HEALTH

        The entire configuration is a dict.  Each section has a corresponding key.  Which is a
        reference to a dict.  Each of which are discussed below.

        The string values mentioned below are constants defined in "epu.epumanagement.conf"

        GENERAL
        =======

        KEY: "general"

        DICT:
          *  "engine_class": Fully qualified name to the decision engine to use, which is what
             controls compensation behavior at a fine grained level.  If this key is missing, the
             decision engine will be the default one: epu.decisionengine.impls.simplest.SimplestEngine

        An engine class cannot currently be reconfigured.  It is fairly doable though, consider
        that the entire engine and configuration needs to be reconstituted after a decider leader
        change anyhow.


        ENGINE CONF
        ===========

        KEY: "engine_conf"

        This entire structure is passed into the decision engine implementation class.
        The configuration expectations are listed per decision engine, see the default one:
        epu.decisionengine.impls.simplest.SimplestEngine

        An engine reconfiguration does not happen immediately, it happens when the decider
        role becomes aware of a configuration change.


        HEALTH
        ======

        KEY: "health"

        DICT:
          *  "monitor_health": True or False
             If this is false, health monitoring is disabled and none of the other configurations
             in the health section are relevant.
          *  "boot_timeout": integer
             If present, number of seconds to wait for a "started" VM to become contextualized
             before declaring it hung.  Sometimes VMs start but have problems even contacting
             the context broker to report a contextualization error.
             If configuration is not present, the default is 300 seconds.
          *  "missing_timeout": integer
             If present, number of seconds to allow to elapse between VM heartbeats before
             declaring it corrupted.
             If configuration is not present, the default is 120 seconds.
          *  "zombie_seconds": integer
             If present, number of seconds to allow termination to occur.  VM instances may be
             in the TERMINATING state as far as the EPUM is concerned but may still be sending
             heartbeats.  After this period of time elapses and we are still receiving heartbeats,
             another termination request is sent.
             If configuration is not present, the default is 120 seconds.

        """
        if not self.initialized:
            raise Exception("Not initialized")
        self.reactor.reconfigure_domain(caller, domain_id, config)

    def msg_add_domain_definition(self, definition_id, definition):
        """Add a new Domain Definition

        @param definition_id domain definition name/ID
        @param definition Domain definition of the domain
        """
        if not self.initialized:
            raise Exception("Not initialized")
        self.reactor.add_domain_definition(definition_id, definition)

    def msg_remove_domain_definition(self, definition_id):
        """ Remove a domain definition

        @param definition_id domain definition name/ID
        """
        if not self.initialized:
            raise Exception("Not initialized")
        self.reactor.remove_domain_definition(definition_id)

    def msg_describe_domain_definition(self, definition_id):
        """Return a state structure for a domain definition, or None

        @param definition_id domain definition name/ID
        """
        return self.reactor.describe_domain_definition(definition_id)

    def msg_list_domain_definitions(self):
        """Return a list of domain definitions in the system
        """
        return self.reactor.list_domain_definitions()

    def msg_update_domain_definition(self, definition_id, definition):
        """Update the domain definition with a new definition

        @param definition_id domain definition name/ID
        @param definition New domain definition of the domain
        """
        if not self.initialized:
            raise Exception("Not initialized")
        return self.reactor.update_domain_definition(definition_id, definition)

    def msg_heartbeat(self, caller, content, timestamp=None):
        """ From R1: op_heartbeat
        Reactor parses content.
        """
        if not self.initialized:
            raise Exception("Not initialized")
        log.debug("Got node heartbeat: %s", content)
        self.reactor.new_heartbeat(caller, content, timestamp=timestamp)

    def msg_instance_info(self, caller, content):
        """ From R1: op_instance_state
        Reactor parses content.
        """
        if not self.initialized:
            raise Exception("Not initialized")
        return self.reactor.new_instance_state(content)
