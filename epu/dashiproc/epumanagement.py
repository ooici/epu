# Copyright 2013 University of Chicago

import logging

from dashi import bootstrap
import dashi.exceptions

from epu.epumanagement.test.mocks import MockOUAgentClient, MockProvisionerClient
from epu.epumanagement import EPUManagement
from epu.epumanagement.conf import EPUM_INITIALCONF_SERVICE_NAME, \
    EPUM_DEFAULT_SERVICE_NAME, EPUM_INITIALCONF_PROC_NAME
from epu.epumanagement.store import get_epum_store
from epu.dashiproc.provisioner import ProvisionerClient
from epu.dashiproc.dtrs import DTRSClient
from epu.util import get_config_paths
from epu.exceptions import UserNotPermittedError, NotFoundError, WriteConflictError
import epu.dashiproc

log = logging.getLogger(__name__)


class EPUManagementService(object):
    """EPU management service interface

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    """

    def __init__(self):
        configs = ["service", "epumanagement"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        self.dashi = bootstrap.dashi_connect(self.CFG.epumanagement.service_name, self.CFG)

        self.default_user = self.CFG.epumanagement.get('default_user')

        # TODO: create ION class here or depend on epuagent repo as a dep
        ou_client = MockOUAgentClient()

        statsd_cfg = self.CFG.get('statsd')

        if 'mock_provisioner' in self.CFG.epumanagement and \
           self.CFG.epumanagement['mock_provisioner']:
            prov_client = MockProvisionerClient()
        else:
            provisioner_topic = self.CFG.epumanagement.provisioner_service_name
            prov_client = ProvisionerClient(self.dashi, topic=provisioner_topic, statsd_cfg=statsd_cfg,
                                            client_name="epumanagement")

        self.service_name = self.CFG.epumanagement.get(EPUM_INITIALCONF_SERVICE_NAME, EPUM_DEFAULT_SERVICE_NAME)
        self.proc_name = self.CFG.epumanagement.get(EPUM_INITIALCONF_PROC_NAME, None)

        self.store = get_epum_store(self.CFG, service_name=self.service_name,
            proc_name=self.proc_name)
        self.store.initialize()

        dtrs_client = DTRSClient(self.dashi, statsd_cfg=statsd_cfg, client_name=self.CFG.epumanagement.service_name)

        self.epumanagement = EPUManagement(self.CFG.epumanagement, SubscriberNotifier(self.dashi), prov_client,
                                           ou_client, dtrs_client, store=self.store, statsd_cfg=statsd_cfg)

        # hack to inject epum reference for mock prov client
        if isinstance(prov_client, MockProvisionerClient):
            prov_client._set_epum(self.epumanagement)

    def start(self):

        epu.dashiproc.link_dashi_exceptions(self.dashi)

        self.dashi.handle(self.subscribe_domain)
        self.dashi.handle(self.unsubscribe_domain)
        self.dashi.handle(self.add_domain)
        self.dashi.handle(self.remove_domain)
        self.dashi.handle(self.list_domains)
        self.dashi.handle(self.describe_domain)
        self.dashi.handle(self.reconfigure_domain)
        self.dashi.handle(self.add_domain_definition)
        self.dashi.handle(self.remove_domain_definition)
        self.dashi.handle(self.list_domain_definitions)
        self.dashi.handle(self.describe_domain_definition)
        self.dashi.handle(self.update_domain_definition)
        self.dashi.handle(self.ou_heartbeat)
        self.dashi.handle(self.instance_info)

        # this may spawn some background threads
        self.epumanagement.initialize()

        # hack to load some domain definitions at boot. later this should be client driven.
        initial_definitions = self.CFG.epumanagement.initial_definitions
        for definition_id, definition in initial_definitions.iteritems():
            log.info("Loading Domain Definition %s", definition_id)
            try:
                self.epumanagement.msg_add_domain_definition(definition_id, definition)
            except WriteConflictError:
                log.warn("Conflict while loading domain definition. It probably exists.", exc_info=True)
            except Exception:
                log.exception("Failed to load Domain Definition %s", definition_id)

        # hack to load some domains at boot. later this should be client driven.
        initial_domains = self.CFG.epumanagement.initial_domains
        for domain_id, params in initial_domains.iteritems():
            log.info("Loading Domain %s", domain_id)
            definition_id = params['definition']
            config = params['config']
            try:
                self.epumanagement.msg_add_domain(self.default_user, domain_id, definition_id, config)
            except WriteConflictError:
                log.warn("Conflict while loading domain definition. It probably exists.", exc_info=True)
            except Exception:
                log.exception("Failed to load Domain %s", domain_id)

        # blocks til dashi.cancel() is called
        self.dashi.consume()

    @property
    def default_user(self):
        if not self._default_user:
            msg = "Operation called for the default user, but none is defined."
            raise UserNotPermittedError(msg)
        else:
            return self._default_user

    @default_user.setter  # noqa
    def default_user(self, default_user):
        self._default_user = default_user

    def subscribe_domain(self, domain_id, subscriber_name, subscriber_op, caller=None):
        caller = caller or self.default_user

        self.epumanagement.msg_subscribe_domain(caller, domain_id,
            subscriber_name, subscriber_op)

    def unsubscribe_domain(self, domain_id, subscriber_name, caller=None):
        caller = caller or self.default_user

        self.epumanagement.msg_unsubscribe_domain(caller, domain_id, subscriber_name)

    def list_domains(self, caller=None):
        """Return a list of domains in the system
        """
        caller = caller or self.default_user
        return self.epumanagement.msg_list_domains(caller=caller)

    def describe_domain(self, domain_id, caller=None):
        """Return a state structure for a domain, or None
        """
        caller = caller or self.default_user
        return self.epumanagement.msg_describe_domain(caller, domain_id)

    def add_domain(self, domain_id, definition_id, config, subscriber_name=None,
                subscriber_op=None, caller=None):
        caller = caller or self.default_user
        self.epumanagement.msg_add_domain(caller, domain_id, definition_id, config,
            subscriber_name=subscriber_name, subscriber_op=subscriber_op)

    def remove_domain(self, domain_id, caller=None):
        caller = caller or self.default_user
        self.epumanagement.msg_remove_domain(caller, domain_id)

    def reconfigure_domain(self, domain_id, config, caller=None):
        caller = caller or self.default_user
        self.epumanagement.msg_reconfigure_domain(caller, domain_id, config)

    def list_domain_definitions(self):
        return self.epumanagement.msg_list_domain_definitions()

    def describe_domain_definition(self, definition_id):
        return self.epumanagement.msg_describe_domain_definition(definition_id)

    def add_domain_definition(self, definition_id, definition):
        self.epumanagement.msg_add_domain_definition(definition_id, definition)

    def remove_domain_definition(self, definition_id):
        self.epumanagement.msg_remove_domain_definition(definition_id)

    def update_domain_definition(self, definition_id, definition):
        self.epumanagement.msg_update_domain_definition(definition_id, definition)

    def ou_heartbeat(self, heartbeat):
        self.epumanagement.msg_heartbeat(None, heartbeat)  # epum parses

    def instance_info(self, record):
        self.epumanagement.msg_instance_info(None, record)  # epum parses


class SubscriberNotifier(object):
    """See: ISubscriberNotifier
    """
    def __init__(self, dashi):
        self.dashi = dashi

    def notify_by_name(self, receiver_name, operation, message):
        """The name is translated into the appropriate messaging-layer object.
        @param receiver_name Message layer name
        @param operation The operation to call on that name
        @param message dict to send
        """
        self.dashi.fire(receiver_name, operation, args=message)


class EPUManagementClient(object):
    """See: IEpuManagementClient
    """
    def __init__(self, dashi, topic):
        self.dashi = dashi
        self.topic = topic

    def subscribe_domain(self, domain_id, subscriber_name, subscriber_op, caller=None):
        self.dashi.fire(self.topic, "subscribe_domain", domain_id=domain_id,
                        subscriber_name=subscriber_name,
                        subscriber_op=subscriber_op, caller=caller)

    def unsubscribe_domain(self, domain_id, subscriber_name, caller=None):
        self.dashi.fire(self.topic, "unsubscribe_domain", domain_id=domain_id,
                        subscriber_name=subscriber_name, caller=caller)

    def list_domains(self, caller=None):
        return self.dashi.call(self.topic, "list_domains")

    def describe_domain(self, domain_id, caller=None):
        try:
            return self.dashi.call(self.topic, "describe_domain", domain_id=domain_id, caller=caller)
        except dashi.exceptions.NotFoundError:
            raise NotFoundError("Unknown domain: %s" % domain_id)

    def add_domain(self, domain_id, definition_id, config, subscriber_name=None,
                subscriber_op=None, caller=None):
        self.dashi.call(self.topic, "add_domain", domain_id=domain_id,
            definition_id=definition_id, config=config,
            subscriber_name=subscriber_name, subscriber_op=subscriber_op,
            caller=caller)

    def remove_domain(self, domain_id, caller=None):
        self.dashi.call(self.topic, "remove_domain", domain_id=domain_id, caller=caller)

    def reconfigure_domain(self, domain_id, config, caller=None):
        self.dashi.call(self.topic, "reconfigure_domain", domain_id=domain_id,
                        config=config, caller=caller)

    def list_domain_definitions(self):
        self.dashi.call(self.topic, "list_domain_definitions")

    def describe_domain_definition(self, definition_id):
        self.dashi.call(self.topic, "describe_domain_definition",
            definition_id=definition_id)

    def add_domain_definition(self, definition_id, definition):
        self.dashi.call(self.topic, "add_domain_definition",
            definition_id=definition_id, definition=definition)

    def remove_domain_definition(self, definition_id):
        self.dashi.call(self.topic, "remove_domain_definition",
            definition_id=definition_id)

    def update_domain_definition(self, definition_id, definition):
        self.dashi.call(self.topic, "update_domain_definition",
            definition_id=definition_id, definition=definition)

    def ou_heartbeat(self, heartbeat):
        self.dashi.fire(self.topic, "ou_heartbeat", heartbeat=heartbeat)

    def instance_info(self, record):
        self.dashi.fire(self.topic, "instance_info", record=record)


def main():
    logging.basicConfig(level=logging.DEBUG)
    epu.dashiproc.epu_register_signal_stack_debug()
    epum = EPUManagementService()
    epum.start()
