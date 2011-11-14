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

    def new_heartbeat(self, content):
        """Handle an incoming heartbeat message

        @param content Raw heartbeat content
        @retval Deferred
        """
        # TODO: In R1, the controller ignored heartbeats if health monitoring was disabled.

        # TODO: How to handle "last heartbeat" timestamp for the doctor?
        #       In R2, the reactor is handling these messages separately.

        instance_id, state = None # for IDE which does not understand Twisted

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

        instance = epu_state.instances.get(instance_id)
        if not instance:
            log.error("Could not retrieve instance information for '%s'" % instance_id)
            defer.returnValue(None)

        if state == InstanceHealthState.OK:
            if instance.health not in (InstanceHealthState.OK,
                                       InstanceHealthState.ZOMBIE) and \
               instance.state < InstanceStates.TERMINATED:
                yield epu_state.new_instance_health(instance_id, state)

        else:

            # TODO: Also need to know how to handle "self.error_time[instance_id]"
            #       (And what it's purpose was in R1)
            #error_time = content.get('error_time')
            if state != instance.health:
                # in R1 also:  (or error_time != self.error_time.get(instance_id))
                #               self.error_time[instance_id] = error_time
                errors = []
                err = content.get('error')
                if err:
                    errors.append(err)
                procs = content.get('failed_processes')
                if procs:
                    errors.extend(p.copy() for p in procs)

                yield epu_state.new_instance_health(instance_id, state, errors)
