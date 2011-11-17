from twisted.internet import defer

from epu.epumanagement.conf import *
from epu.epumanagement.health import InstanceHealthState
import epu.states as InstanceStates

import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)

class EPUMReactor(object):
    """Handles message-driven sub tasks that do not require locks for critical sections.

    The instance of the EPUManagementService process that hosts a particular EPUMReactor instance
    might not be configured to receive messages.  But when it is receiving messages, they all go
    to the EPUMReactor instance.

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    See: https://confluence.oceanobservatories.org/display/CIDev/EPUManagement+Refactor
    """

    def __init__(self, epum_store, notifier, provisioner_client, epum_client):
        self.epum_store = epum_store
        self.notifier = notifier
        self.provisioner_client = provisioner_client
        self.epum_client = epum_client

    @defer.inlineCallbacks
    def add_epu(self, caller, epu_name, epu_config):
        """See: EPUManagement.msg_add_epu()
        """
        # TODO: parameters are from messages, do legality checks here
        yield self.epum_store.create_new_epu(caller, epu_name, epu_config)

    @defer.inlineCallbacks
    def reconfigure_epu(self, caller, epu_name, epu_config):
        """See: EPUManagement.msg_reconfigure_epu()
        """
        # TODO: parameters are from messages, do legality checks here
        epu_state = yield self.epum_store.get_epu_state(epu_name)
        if not epu_state:
            raise ValueError("EPU does not exist: %s" % epu_name)
        if epu_config.has_key(EPUM_CONF_GENERAL):
            yield epu_state.add_general_conf(epu_config[EPUM_CONF_GENERAL])
        if epu_config.has_key(EPUM_CONF_ENGINE):
            yield epu_state.add_engine_conf(epu_config[EPUM_CONF_ENGINE])
        if epu_config.has_key(EPUM_CONF_HEALTH):
            yield epu_state.add_health_conf(epu_config[EPUM_CONF_HEALTH])

    def new_sensor_info(self, content):
        """Handle an incoming sensor message

        @param content Raw sensor content
        @retval Deferred
        """

        # TODO: need a new sensor abstraction; have no way of knowing which epu_state to associate this with
        # TODO: sensor API will change, should include a mandatory field for epu (vs. a general sensor)
        raise NotImplementedError
        #epu_state.new_sensor_item(content)

    @defer.inlineCallbacks
    def new_instance_state(self, content):
        """Handle an incoming instance state message

        @param content Raw instance state content
        @retval Deferred
        """
        instance_id = None # for IDE
        try:
            instance_id = content['node_id']
        except KeyError:
            log.warn("Got invalid state message: %s", content)
            defer.returnValue(None)
        if instance_id:
            epu_state = yield self.epum_store.get_epu_state_by_instance_id(instance_id)
            if epu_state:
                log.debug("New instance state msg for '%s'" % instance_id)
                yield epu_state.new_instance_state(content)
            else:
                log.warn("Unknown EPU for state message for instance '%s'" % instance_id)
        else:
            log.error("Could not parse instance ID from state message: '%s'" % content)

    @defer.inlineCallbacks
    def new_heartbeat(self, content, timestamp=None):
        """Handle an incoming heartbeat message

        @param content Raw heartbeat content
        @param timestamp For unit tests
        """

        try:
            instance_id = content['node_id']
            state = content['state']
        except KeyError:
            log.error("Got invalid heartbeat message: %s", content)
            defer.returnValue(None)

        epu_state = yield self.epum_store.get_epu_state_by_instance_id(instance_id)
        if not epu_state:
            log.error("Unknown EPU for health message for instance '%s'" % instance_id)
            defer.returnValue(None)

        if not epu_state.is_health_enabled():
            # The instance should not be sending heartbeats if health is disabled
            log.warn("Ignored health message for instance '%s'" % instance_id)
            defer.returnValue(None)

        instance = epu_state.instances.get(instance_id)
        if not instance:
            log.error("Could not retrieve instance information for '%s'" % instance_id)
            defer.returnValue(None)

        if state == InstanceHealthState.OK:

            if instance.health not in (InstanceHealthState.OK,
                                       InstanceHealthState.ZOMBIE) and \
               instance.state < InstanceStates.TERMINATED:

                # Only updated when we receive an OK heartbeat and instance health turned out to
                # be wrong (e.g. it was missing and now we finally hear from it)
                yield epu_state.new_instance_health(instance_id, state)

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

                yield epu_state.new_instance_health(instance_id, state, error_time, errors)

        # Only update this "last heard" timestamp when the other work is committed.  In situations
        # where a heartbeat is re-queued or never ACK'd and the message is picked up by another
        # EPUM worker, the lack of a timestamp update will give the doctor a better chance to
        # catch health issues.
        yield epu_state.new_instance_heartbeat(instance_id,  timestamp=timestamp)
