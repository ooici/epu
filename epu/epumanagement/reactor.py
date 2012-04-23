import logging

from epu.epumanagement.conf import *
from epu.states import InstanceState, InstanceHealthState
from epu.util import check_user
from epu.exceptions import UserNotPermittedError

log = logging.getLogger(__name__)

class EPUMReactor(object):
    """Handles message-driven sub tasks that do not require locks for critical sections.

    The instance of the EPUManagementService process that hosts a particular EPUMReactor instance
    might not be configured to receive messages.  But when it is receiving messages, they all go
    to the EPUMReactor instance.

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    See: https://confluence.oceanobservatories.org/display/CIDev/EPUManagement+Refactor
    """

    def __init__(self, epum_store, subscribers, provisioner_client, epum_client):
        self.epum_store = epum_store
        self.subscribers = subscribers
        self.provisioner_client = provisioner_client
        self.epum_client = epum_client

    def add_epu(self, caller, epu_name, epu_config):
        """See: EPUManagement.msg_add_epu()
        """
        # TODO: parameters are from messages, do legality checks here
        # assert that engine_conf['epuworker_type']['sleeper'] is owned by caller
        log.debug("ADD EPU: %s" % epu_config)
        worker_type = epu_config.get('engine_conf', {}).get('epuworker_type', None)

        self.epum_store.add_domain(caller, epu_name, epu_config)

    def remove_epu(self, caller, domain_id):
        try:
            domain = self.epum_store.get_domain(caller, domain_id)
        except ValueError:
            return None
        if not domain:
            return None

        self.epum_store.remove_domain(caller, domain_id)

    def list_epus(self, caller):
        return self.epum_store.list_domains_by_owner(caller)

    def describe_epu(self, caller, domain_id):
        try:
            domain = self.epum_store.get_domain(caller, domain_id)
        except ValueError:
            return None
        if not domain:
            return None
        domain_desc = dict(name=domain.domain_id,
            config=domain.get_all_config(),
            instances=[i.to_dict() for i in domain.get_instances()])
        return domain_desc

    def reconfigure_epu(self, caller, domain_id, epu_config):
        """See: EPUManagement.msg_reconfigure_epu()
        """
        # TODO: parameters are from messages, do legality checks here
        epu_state = self.epum_store.get_domain(caller, domain_id)
        if not epu_state:
            raise ValueError("EPU does not exist: %s" % domain_id)

        worker_type = epu_config.get('engine_conf', {}).get('epuworker_type', None)

        if epu_config.has_key(EPUM_CONF_GENERAL):
            epu_state.add_general_config(epu_config[EPUM_CONF_GENERAL])
        if epu_config.has_key(EPUM_CONF_ENGINE):
            epu_state.add_engine_config(epu_config[EPUM_CONF_ENGINE])
        if epu_config.has_key(EPUM_CONF_HEALTH):
            epu_state.add_health_config(epu_config[EPUM_CONF_HEALTH])

    def subscribe_domain(self, caller, domain_id, subscriber_name, subscriber_op):
        """Subscribe to asynchronous state updates for instances of a domain
        """
        epu_state = self.epum_store.get_domain(caller, domain_id)
        if not epu_state:
            raise ValueError("EPU does not exist: %s" % domain_id)

        epu_state.add_subscriber(subscriber_name, subscriber_op)

    def unsubscribe_domain(self, caller, domain_id, subscriber_name):
        """Subscribe to asynchronous state updates for instances of a domain
        """
        epu_state = self.epum_store.get_domain(caller, domain_id)
        if not epu_state:
            raise ValueError("EPU does not exist: %s" % domain_id)

        epu_state.remove_subscriber(subscriber_name)

    def new_sensor_info(self, content):
        """Handle an incoming sensor message

        @param content Raw sensor content
        """

        # TODO: need a new sensor abstraction; have no way of knowing which epu_state to associate this with
        # TODO: sensor API will change, should include a mandatory field for epu (vs. a general sensor)
        raise NotImplementedError
        #epu_state.new_sensor_item(content)

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
            epu_state = self.epum_store.get_domain_for_instance_id(instance_id)
            if epu_state:
                log.debug("Got state %s for instance '%s'", state, instance_id)

                instance = epu_state.get_instance(instance_id)
                epu_state.new_instance_state(content, previous=instance)

                # The higher level clients of EPUM only see RUNNING or FAILED (or nothing)
                if content['state'] < InstanceState.RUNNING:
                    return
                elif content['state'] == InstanceState.RUNNING:
                    notify_state = InstanceState.RUNNING
                else:
                    notify_state = InstanceState.FAILED
                try:
                    self.subscribers.notify_subscribers(instance, epu_state, notify_state)
                except Exception, e:
                    log.error("Error notifying subscribers '%s': %s",
                        instance_id, str(e), exc_info=True)

            else:
                log.warn("Unknown EPU for state message for instance '%s'" % instance_id)
        else:
            log.error("Could not parse instance ID from state message: '%s'" % content)

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

        epu_state = self.epum_store.get_domain_for_instance_id(instance_id)
        if not epu_state:
            log.error("Unknown EPU for health message for instance '%s'" % instance_id)
            return

        if not epu_state.is_health_enabled():
            # The instance should not be sending heartbeats if health is disabled
            log.warn("Ignored health message for instance '%s'" % instance_id)
            return

        instance = epu_state.get_instance(instance_id)
        if not instance:
            log.error("Could not retrieve instance information for '%s'" % instance_id)
            return

        if state == InstanceHealthState.OK:

            if instance.health not in (InstanceHealthState.OK,
                                       InstanceHealthState.ZOMBIE) and \
               instance.state < InstanceState.TERMINATED:

                # Only updated when we receive an OK heartbeat and instance health turned out to
                # be wrong (e.g. it was missing and now we finally hear from it)
                epu_state.new_instance_health(instance_id, state, caller=caller)

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

                epu_state.new_instance_health(instance_id, state, error_time, errors, caller)

        # Only update this "last heard" timestamp when the other work is committed.  In situations
        # where a heartbeat is re-queued or never ACK'd and the message is picked up by another
        # EPUM worker, the lack of a timestamp update will give the doctor a better chance to
        # catch health issues.
        epu_state.set_instance_heartbeat_time(instance_id, timestamp)
