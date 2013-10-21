# Copyright 2013 University of Chicago

import copy
import logging

from epu import cei_events
from epu.epumanagement.conf import *  # noqa
from epu.epumanagement.decider import DEFAULT_ENGINE_CLASS
from epu.states import InstanceState, InstanceHealthState
from epu.domain_log import EpuLoggerThreadSpecific
from epu.exceptions import NotFoundError, WriteConflictError
from epu.util import get_class

log = logging.getLogger(__name__)


class EPUMReactor(object):
    """Handles message-driven sub tasks that do not require locks for critical sections.

    The instance of the EPUManagementService process that hosts a particular EPUMReactor instance
    might not be configured to receive messages.  But when it is receiving messages, they all go
    to the EPUMReactor instance.

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    See: https://confluence.oceanobservatories.org/display/CIDev/EPUManagement+Refactor
    """

    def __init__(self, store, subscribers, provisioner_client, epum_client):
        self.store = store
        self.subscribers = subscribers
        self.provisioner_client = provisioner_client
        self.epum_client = epum_client

    def add_domain(self, caller, domain_id, definition_id, config,
                   subscriber_name=None, subscriber_op=None):
        """See: EPUManagement.msg_add_domain()
        """
        # TODO: parameters are from messages, do legality checks here
        # assert that engine_conf['epuworker_type']['sleeper'] is owned by caller
        log.debug("ADD Domain: %s", config)

        # Make a copy of the definition and merge the config inside.
        # If a definition is changed or deleted, it has no impact over any of
        # the domains.
        try:
            definition = self.store.get_domain_definition(definition_id)
        except NotFoundError:
            raise ValueError("Domain definition does not exist: %s" % definition_id)

        merged_config = copy.copy(definition.get_definition())

        # Hand-made deep merge of config into definition
        if EPUM_CONF_GENERAL in config:
            if EPUM_CONF_GENERAL in merged_config:
                merged_config[EPUM_CONF_GENERAL].update(config[EPUM_CONF_GENERAL])
            else:
                merged_config[EPUM_CONF_GENERAL] = config[EPUM_CONF_GENERAL]

        if EPUM_CONF_HEALTH in config:
            if EPUM_CONF_HEALTH in merged_config:
                merged_config[EPUM_CONF_HEALTH].update(config[EPUM_CONF_HEALTH])
            else:
                merged_config[EPUM_CONF_HEALTH] = config[EPUM_CONF_HEALTH]

        if EPUM_CONF_ENGINE in config:
            if EPUM_CONF_ENGINE in merged_config:
                merged_config[EPUM_CONF_ENGINE].update(config[EPUM_CONF_ENGINE])
            else:
                merged_config[EPUM_CONF_ENGINE] = config[EPUM_CONF_ENGINE]

        self._validate_engine_config(merged_config)

        with EpuLoggerThreadSpecific(domain=domain_id, user=caller):
            domain = self.store.add_domain(caller, domain_id, merged_config)

            if subscriber_name and subscriber_op:
                domain.add_subscriber(subscriber_name, subscriber_op)

        extradict = {'user': caller, 'domain_id': domain_id,
                'definition_id': definition_id}
        cei_events.event("epumanagement", "new_domain", extra=extradict)

    def _validate_engine_config(self, config):
        engine_class = DEFAULT_ENGINE_CLASS
        if EPUM_CONF_GENERAL in config:
            general_config = config[EPUM_CONF_GENERAL]
            if EPUM_CONF_ENGINE_CLASS in general_config:
                engine_class = general_config[EPUM_CONF_ENGINE_CLASS]

        log.debug("attempting to load Decision Engine '%s'" % engine_class)
        kls = get_class(engine_class)
        if not kls:
            raise Exception("Cannot find decision engine implementation: '%s'" % engine_class)

        if hasattr(kls, 'validate_config') and callable(getattr(kls, 'validate_config')):
            kls.validate_config(config.get(EPUM_CONF_ENGINE, {}))

    def remove_domain(self, caller, domain_id):
        with EpuLoggerThreadSpecific(domain=domain_id, user=caller):
            try:
                domain = self.store.get_domain(caller, domain_id)
            except ValueError:
                return None
            if not domain:
                return None

            # mark a domain removed, instances will be terminated in the background
            domain.remove()

    def list_domains(self, caller):
        with EpuLoggerThreadSpecific(user=caller):
            return self.store.list_domains_by_owner(caller)

    def describe_domain(self, caller, domain_id):
        with EpuLoggerThreadSpecific(domain=domain_id, user=caller):
            try:
                domain = self.store.get_domain(caller, domain_id)
            except ValueError:
                return None
            if not domain:
                return None
            domain_desc = dict(name=domain.domain_id,
                config=domain.get_all_config(),
                sensor_data=domain.get_domain_sensor_data(),
                instances=[i.to_dict() for i in domain.get_instances()])
            return domain_desc

    def reconfigure_domain(self, caller, domain_id, config):
        """See: EPUManagement.msg_reconfigure_domain()
        """
        # TODO: parameters are from messages, do legality checks here
        with EpuLoggerThreadSpecific(domain=domain_id, user=caller):
            domain = self.store.get_domain(caller, domain_id)
            if not domain:
                raise ValueError("Domain does not exist: %s" % domain_id)

            if EPUM_CONF_GENERAL in config:
                domain.add_general_config(config[EPUM_CONF_GENERAL])
            if EPUM_CONF_ENGINE in config:
                domain.add_engine_config(config[EPUM_CONF_ENGINE])
            if EPUM_CONF_HEALTH in config:
                domain.add_health_config(config[EPUM_CONF_HEALTH])

    def subscribe_domain(self, caller, domain_id, subscriber_name, subscriber_op):
        """Subscribe to asynchronous state updates for instances of a domain
        """
        with EpuLoggerThreadSpecific(domain=domain_id, user=caller):
            domain = self.store.get_domain(caller, domain_id)
            if not domain:
                raise ValueError("Domain does not exist: %s" % domain_id)

            domain.add_subscriber(subscriber_name, subscriber_op)

    def unsubscribe_domain(self, caller, domain_id, subscriber_name):
        """Subscribe to asynchronous state updates for instances of a domain
        """
        with EpuLoggerThreadSpecific(domain=domain_id, user=caller):
            domain = self.store.get_domain(caller, domain_id)
            if not domain:
                raise ValueError("Domain does not exist: %s" % domain_id)

            domain.remove_subscriber(subscriber_name)

    def add_domain_definition(self, definition_id, definition):
        """See: EPUManagement.msg_add_domain_definition()
        """
        log.debug("ADD Domain Definition: %s", definition)

        with EpuLoggerThreadSpecific(definition=definition_id):
            definition = self.store.add_domain_definition(definition_id, definition)

    def list_domain_definitions(self):
        with EpuLoggerThreadSpecific():
            return self.store.list_domain_definitions()

    def describe_domain_definition(self, definition_id):
        with EpuLoggerThreadSpecific(definition=definition_id):
            try:
                definition = self.store.get_domain_definition(definition_id)
            except ValueError:
                return None
            if not definition:
                return None
            definition_config = definition.get_definition()
            doc = self._get_engine_doc(definition_config)
            definition_desc = dict(name=definition.definition_id,
                    definition=definition_config)
            if doc:
                definition_desc['documentation'] = doc
            return definition_desc

    def _get_engine_doc(self, config):
        engine_class = DEFAULT_ENGINE_CLASS
        if EPUM_CONF_GENERAL in config:
            general_config = config[EPUM_CONF_GENERAL]
            if EPUM_CONF_ENGINE_CLASS in general_config:
                engine_class = general_config[EPUM_CONF_ENGINE_CLASS]

        log.debug("attempting to load Decision Engine '%s'" % engine_class)
        kls = get_class(engine_class)
        if not kls:
            raise Exception("Cannot find decision engine implementation: '%s'" % engine_class)

        if hasattr(kls, 'get_config_doc') and callable(getattr(kls, 'get_config_doc')):
            doc = kls.get_config_doc()
        else:
            doc = kls.__doc__

        return doc

    def remove_domain_definition(self, definition_id):
        with EpuLoggerThreadSpecific(definition=definition_id):
            self.store.remove_domain_definition(definition_id)

    def update_domain_definition(self, definition_id, definition):
        with EpuLoggerThreadSpecific(definition=definition_id):
            domain_definition = self.store.get_domain_definition(definition_id)
            if not domain_definition:
                raise ValueError("Domain definition does not exist: %s" % definition_id)

            self.store.update_domain_definition(definition_id, definition)

    def new_instance_state(self, content):
        """Handle an incoming instance state message

        @param content Raw instance state content
        """
        try:
            instance_id = content['node_id']
            state = content['state']
        except KeyError:
            log.warn("Got invalid state message: %s", content)
            return

        if instance_id:
            domain = self.store.get_domain_for_instance_id(instance_id)
            if domain:

                # retry update in case of write conflict
                instance, updated = self._maybe_update_domain_instance(
                    domain, instance_id, content)
                if updated:
                    log.debug("Got state %s for instance '%s'", state, instance_id)

                    # The higher level clients of EPUM only see RUNNING or FAILED (or nothing)
                    if content['state'] < InstanceState.RUNNING:
                        return
                    elif content['state'] == InstanceState.RUNNING:
                        notify_state = InstanceState.RUNNING
                    else:
                        notify_state = InstanceState.FAILED
                    try:
                        self.subscribers.notify_subscribers(instance, domain, notify_state)
                    except Exception, e:
                        log.error("Error notifying subscribers '%s': %s",
                            instance_id, str(e), exc_info=True)
                else:
                    log.warn("Already received a more recent state message for instance '%s'" % instance_id)
            else:
                log.warn("Unknown Domain for state message for instance '%s'" % instance_id)
        else:
            log.error("Could not parse instance ID from state message: '%s'" % content)

    def _maybe_update_domain_instance(self, domain, instance_id, msg):
        while True:
            instance = domain.get_instance(instance_id)
            content = copy.deepcopy(msg)
            try:
                updated = domain.new_instance_state(content, previous=instance)
                return instance, updated
            except WriteConflictError:
                pass
            except NotFoundError:
                return instance, False

    def new_heartbeat(self, caller, content, timestamp=None):
        """Handle an incoming heartbeat message

        @param caller Name of heartbeat sender (used for responses via ouagent client). If None, uses node_id
        @param content Raw heartbeat content
        @param timestamp For unit tests
        """

        try:
            instance_id = content['node_id']
            state = content['state']
        except KeyError:
            log.error("Got invalid heartbeat message from '%s': %s", caller, content)
            return

        domain = self.store.get_domain_for_instance_id(instance_id)
        if not domain:
            log.error("Unknown Domain for health message for instance '%s'" % instance_id)
            return

        if not domain.is_health_enabled():
            # The instance should not be sending heartbeats if health is disabled
            log.warn("Ignored health message for instance '%s'" % instance_id)
            return

        instance = domain.get_instance(instance_id)
        if not instance:
            log.error("Could not retrieve instance information for '%s'" % instance_id)
            return

        if state == InstanceHealthState.OK:

            if instance.health not in (InstanceHealthState.OK,
                                       InstanceHealthState.ZOMBIE) and \
               instance.state < InstanceState.TERMINATED:

                # Only updated when we receive an OK heartbeat and instance health turned out to
                # be wrong (e.g. it was missing and now we finally hear from it)
                domain.new_instance_health(instance_id, state, caller=caller)

        else:

            # TODO: We've been talking about having an error report that will only say
            #       "x failed" and then OU agent would have an RPC op that allows doctor
            #       to trigger a "get_error_info()" retrieval before killing it
            # But for now we want OU agent to send full error information.
            # The EPUMStore should key error storage off {node_id + error_time}

            if state != instance.health:
                errors = []
                error_time = content.get('error_time')
                err = content.get('error')
                if err:
                    errors.append(err)
                procs = content.get('failed_processes')
                if procs:
                    errors.extend(p.copy() for p in procs)

                domain.new_instance_health(instance_id, state, error_time, errors, caller)

        # Only update this "last heard" timestamp when the other work is committed.  In situations
        # where a heartbeat is re-queued or never ACK'd and the message is picked up by another
        # EPUM worker, the lack of a timestamp update will give the doctor a better chance to
        # catch health issues.
        domain.set_instance_heartbeat_time(instance_id, timestamp)
