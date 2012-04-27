import logging
import time
import simplejson as json

from epu.epumanagement.core import EngineState, SensorItemParser, InstanceParser, CoreInstance
from epu.states import InstanceState, InstanceHealthState
from epu.exceptions import NotFoundError, WriteConflictError
from epu.epumanagement.conf import *


log = logging.getLogger(__name__)


#############################################################################
# STORAGE INTERFACES
#############################################################################

class EPUMStore(object):
    """Interface for accessing storage and synchronization.

    This class cannot be used directly, you must use a subclass.
    """

    def __init__(self):
        self.memory_mode_decider = True
        self.memory_mode_doctor = True

        self.local_decider_ref = None
        self.local_doctor_ref = None

    # --------------
    # Leader related
    # --------------

    def currently_decider(self):
        """Return True if this instance is still the leader. This is used to check on
        leader status just before a critical section update.  It is possible that the
        synchronization service (or the loss of our connection to it) triggered a callback
        that could not interrupt a thread of control in progress.  Expecting this will
        be reworked/matured after adding ZK and after the eventing system is decided on
        for all deployments and containers.
        """
        return self.memory_mode_decider

    def _change_decider(self, make_leader):
        """For internal use by EPUMStore
        @param make_leader True/False
        """
        self.memory_mode_decider = make_leader
        if self.local_decider_ref:
            if make_leader:
                self.local_decider_ref.now_leader()
            else:
                self.local_decider_ref.not_leader()

    def register_decider(self, decider):
        """For callbacks: now_leader() and not_leader()
        """
        self.local_decider_ref = decider

    def currently_doctor(self):
        """See currently_decider()
        """
        return self.memory_mode_doctor

    def _change_doctor(self, make_leader):
        """For internal use by EPUMStore
        @param make_leader True/False
        """
        self.memory_mode_doctor = True
        if self.local_doctor_ref:
            if make_leader:
                self.local_doctor_ref.now_leader()
            else:
                self.local_doctor_ref.not_leader()

    def register_doctor(self, doctor):
        """For callbacks: now_leader() and not_leader()
        """
        self.local_doctor_ref = doctor

    def epum_service_name(self):
        """Return the service name (to use for heartbeat/IaaS subscriptions, launches, etc.)

        It is a configuration error to configure many instances of EPUM with the same ZK coordinates
        but different service names.  TODO: in the future, check for this inconsistency, probably by
        putting the epum_service_name in persistence.
        """

    def add_domain(self, owner, domain_id, config, subscriber_name=None,
                   subscriber_op=None):
        """Add a new domain

        Returns the new DomainStore
        Raises a WriteConflictError if a domain already exists with this name
        and owner.
        """

    def remove_domain(self, owner, domain_id):
        """Remove a domain

        This will only work when there are no running instances for the domain
        """

    def list_domains_by_owner(self, owner):
        """Retrieve a list of domains owned by a particular user
        """

    def list_domains(self):
        """Retrieve a list of (owner, domain) pairs
        """

    def get_domain(self, owner, domain_id):
        """Retrieve the store for a particular domain

        Raises NotFoundError if domain does not exist

        @rtype DomainStore
        """

    def get_all_domains(self):
        """Retrieve a list of all domain stores
        """

    def get_domain_for_instance_id(self, instance_id):
        """Retrieve the domain associated with an instance

        Returns a DomainStore, or None if not found
        """


class DomainStore(object):
    """Interface for accessing storage and synchronization for a single domain.

    This class cannot be used directly, you must use a subclass.
    """

    def __init__(self):
        self.instance_parser = InstanceParser()
        self.sensor_parser = SensorItemParser()

    def is_removed(self):
        """Whether this domain has been removed
        """

    def remove(self):
        """Mark this instance for removal
        """

    def get_engine_config(self, keys=None):
        """Retrieve the engine config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """

    def get_versioned_engine_config(self):
        """Retrieve the engine config dictionary and a version

        Returns a (config, version) tuple. The version is used to tell when
        a new config is available and an engine reconfigure is needed.
        """

    def add_engine_config(self, conf):
        """Store a dictionary of new engine conf values.

        These are folded into the existing configuration map. So for example
        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
        the result from get_config() will be {'a' : 1, 'b' : 2}.

        @param conf dictionary mapping strings to JSON-serializable objects
        """

    def get_health_config(self, keys=None):
        """Retrieve the health config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """

    def add_health_config(self, conf):
        """Store a dictionary of new health conf values.

        These are folded into the existing configuration map. So for example
        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
        the result from get_health_config() will be {'a' : 1, 'b' : 2}.

        @param conf dictionary mapping strings to JSON-serializable objects
        """

    def is_health_enabled(self):
        """Return True if the EPUM_CONF_HEALTH_MONITOR setting is True
        """

    def get_general_config(self, keys=None):
        """Retrieve the general config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """

    def add_general_config(self, conf):
        """Store a dictionary of new general conf values.

        These are folded into the existing configuration map. So for example
        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
        the result from get_general_config() will be {'a' : 1, 'b' : 2}.

        @param conf dictionary mapping strings to JSON-serializable objects
        """

    def get_subscribers(self):
        """Retrieve a list of current subscribers
        """

    def add_subscriber(self, name, op):
        """Add a new subscriber to instance state changes for this domain
        """

    def remove_subscriber(self, name):
        """Remove a subscriber of instance state changes for this domain
        """

    def add_instance(self, instance):
        """Add a new instance record

        Raises a WriteConflictError if the instance already exists
        """

    def update_instance(self, instance, previous=None):
        """Update an existing instance record

        Raises a WriteConflictError if a previous record is specified and does
        not match what is in datastore

        Raise a NotFoundError if the instance is unknown
        """

    def get_instance(self, instance_id):
        """Retrieve an instance record

        Returns the instance record, or None if not found
        """

    def set_instance_heartbeat_time(self, instance_id, time):
        """Store a new instance heartbeat
        """

    def get_instance_heartbeat_time(self, instance_id):
        """Retrieve the timestamp of the last heartbeat from this instance
        """

    def get_instances(self):
        """Retrieve a list of instance records
        """

    def get_instance_ids(self):
        """Retrieve a list of known instance IDs
        """

    def get_engine_state(self):
        """Get an object to provide to engine decide() and reset pending state

        Beware that the object provided may be changed and reused by the
        next invocation of this method.
        """

    def new_instance_state(self, content, timestamp=None, previous=None):
        """Introduce a new instance state from an incoming message
        """
        instance_id = self.instance_parser.parse_instance_id(content)
        if instance_id:
            if not previous:
                previous = self.get_instance(instance_id)
            instance = self.instance_parser.parse(content, previous,
                                                  timestamp=timestamp)
            if instance:
                self.update_instance(instance, previous=previous)

    def new_instance_launch(self, deployable_type_id, instance_id, launch_id, site, allocation,
                            extravars=None, timestamp=None):
        """Record a new instance launch

        @param deployable_type_id string identifier of the DT to launch
        @param instance_id Unique id for the new instance
        @param launch_id Unique id for the new launch group
        @param site Site instance is being launched at
        @param allocation Size of new instance
        @param extravars optional dictionary of variables sent to the instance
        """
        now = time.time() if timestamp is None else timestamp

        instance = CoreInstance(instance_id=instance_id, launch_id=launch_id,
                            site=site, allocation=allocation,
                            state=InstanceState.REQUESTING,
                            state_time=now,
                            health=InstanceHealthState.UNKNOWN,
                            deployable_type=deployable_type_id,
                            extravars=extravars)
        self.add_instance(instance)

    def new_instance_health(self, instance_id, health_state, error_time=None, errors=None, caller=None):
        """Record instance health change

        @param instance_id Id of instance
        @param health_state The state
        @param error_time Time of the instance errors, if applicable
        @param errors Instance errors provided in the heartbeat
        @param caller Name of heartbeat sender (used for responses via ouagent client). If None, uses node_id
        """
        instance = self.get_instance(instance_id)
        if not instance:
            log.error("Got health state change for unknown instance %s: %s",
                instance_id, health_state)

        d = dict(instance.iteritems())
        d['health'] = health_state
        d['errors'] = errors
        d['error_time'] = error_time
        if not caller:
            caller = instance_id
        d['caller'] = caller

        if errors:
            log.error("Got error heartbeat from instance %s. State: %s. "+
                      "Health: %s. Errors: %s", instance_id, instance.state,
                      health_state, errors)

        else:
            log.info("Instance %s (%s) entering health state %s", instance_id,
                     instance.state, health_state)

        newinstance = CoreInstance(**d)
        self.update_instance(newinstance, previous=instance)

    def ouagent_address(self, instance_id):
        """Return address to send messages to a particular OU Agent, or None"""
        instance = self.get_instance(instance_id)
        if not instance:
            return None
        return instance.caller

    def get_all_config(self):
        """Retrieve a dictionary of all config
        """
        return {EPUM_CONF_GENERAL: self.get_general_config(),
                EPUM_CONF_HEALTH: self.get_health_config(),
                EPUM_CONF_ENGINE: self.get_engine_config()}

#############################################################################
# IN-MEMORY STORAGE IMPLEMENTATION
#############################################################################

class LocalEPUMStore(EPUMStore):
    """EPUM store that uses local memory only
    """

    def __init__(self, service_name):
        super(LocalEPUMStore, self).__init__()

        self.domains = {}
        self.service_name = service_name

    def epum_service_name(self):
        """Return the service name (to use for heartbeat/IaaS subscriptions, launches, etc.)

        It is a configuration error to configure many instances of EPUM with the same ZK coordinates
        but different service names.  TODO: in the future, check for this inconsistency, probably by
        putting the epum_service_name in persistence.
        """
        return self.service_name

    def add_domain(self, owner, domain_id, config, subscriber_name=None,
                   subscriber_op=None):
        """Add a new domain

        Raises a WriteConflictError if a domain already exists with this name
        and owner.
        """
        key = (owner, domain_id)
        if key in self.domains:
            raise WriteConflictError()

        domain = LocalDomainStore(owner, domain_id, config)
        if subscriber_name and subscriber_op:
            domain.add_subscriber(subscriber_name, subscriber_op)
        self.domains[key] = domain
        return domain

    def remove_domain(self, owner, domain_id):
        """Remove a domain

        TODO this should only work when there are no running instances for the domain

        Raises a NotFoundError if the domain is unknown
        """
        key = (owner, domain_id)
        if key not in self.domains:
            raise NotFoundError()
        del self.domains[key]

    def list_domains_by_owner(self, owner):
        """Retrieve a list of domains owned by a particular user
        """
        return [domain_id for domain_owner, domain_id in self.domains.keys()
                if owner == domain_owner]

    def list_domains(self):
        """Retrieve a list of (owner, domain) pairs
        """
        return self.domains.keys()

    def get_domain(self, owner, domain_id):
        """Retrieve the store for a particular domain

        Raises NotFoundError if domain does not exist

        @rtype DomainStore
        """
        try:
            return self.domains[(owner, domain_id)]
        except KeyError:
            raise NotFoundError()

    def get_all_domains(self):
        """Retrieve a list of all domain stores
        """
        return self.domains.values()

    def get_domain_for_instance_id(self, instance_id):
        """Retrieve the domain associated with an instance

        Returns a DomainStore, or None if not found
        """
        for domain in self.domains.itervalues():
            if domain.get_instance(instance_id):
                return domain

class LocalDomainStore(DomainStore):

    def __init__(self, owner, domain_id, config):
        super(LocalDomainStore, self).__init__()

        self.owner = owner
        self.domain_id = domain_id
        self.removed = False
        self.engine_config_version = 0
        self.engine_config = {}
        self.health_config = {}
        self.general_config = {}
        if config:
            if config.has_key(EPUM_CONF_GENERAL):
                self.add_general_config(config[EPUM_CONF_GENERAL])

            if config.has_key(EPUM_CONF_ENGINE):
                self.add_engine_config(config[EPUM_CONF_ENGINE])

            if config.has_key(EPUM_CONF_HEALTH):
                self.add_health_config(config[EPUM_CONF_HEALTH])
        self.engine_state = EngineState()

        self.subscribers = set()

        self.instances = {}
        self.instance_heartbeats = {}

    @property
    def key(self):
        return (self.owner, self.domain_id)

    def is_removed(self):
        """Whether this domain has been marked for removal
        """
        return self.removed

    def remove(self):
        """Mark this instance for removal
        """
        self.removed = True

    def get_engine_config(self, keys=None):
        """Retrieve the engine config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """

        if keys is None:
            d = dict((k, json.loads(v)) for k,v in self.engine_config.iteritems())
        else:
            d = dict((k, json.loads(self.engine_config[k]))
                for k in keys if k in self.engine_config)
        return d

    def get_versioned_engine_config(self):
        """Retrieve the engine config dictionary and a version

        Returns a (config, version) tuple. The version is used to tell when
        a new config is available and an engine reconfigure is needed.
        """
        return self.get_engine_config(), self.engine_config_version

    def add_engine_config(self, conf):
        """Store a dictionary of new engine conf values.

        These are folded into the existing configuration map. So for example
        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
        the result from get_config() will be {'a' : 1, 'b' : 2}.

        @param conf dictionary mapping strings to JSON-serializable objects
        """
        for k,v in conf.iteritems():
            self.engine_config[k] = json.dumps(v)
        self.engine_config_version += 1

    def get_health_config(self, keys=None):
        """Retrieve the health config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """
        if keys is None:
            d = dict((k, json.loads(v)) for k,v in self.health_config.iteritems())
        else:
            d = dict((k, json.loads(self.health_config[k]))
                for k in keys if k in self.health_config)
        return d

    def add_health_config(self, conf):
        """Store a dictionary of new health conf values.

        These are folded into the existing configuration map. So for example
        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
        the result from get_health_config() will be {'a' : 1, 'b' : 2}.

        @param conf dictionary mapping strings to JSON-serializable objects
        """
        for k,v in conf.iteritems():
            self.health_config[k] = json.dumps(v)

    def is_health_enabled(self):
        """Return True if the EPUM_CONF_HEALTH_MONITOR setting is True
        """
        health_conf = self.get_health_config()
        if not health_conf.has_key(EPUM_CONF_HEALTH_MONITOR):
            return False
        else:
            return bool(health_conf[EPUM_CONF_HEALTH_MONITOR])

    def get_general_config(self, keys=None):
        """Retrieve the general config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """
        if keys is None:
            d = dict((k, json.loads(v)) for k,v in self.general_config.iteritems())
        else:
            d = dict((k, json.loads(self.general_config[k]))
                for k in keys if k in self.general_config)
        return d

    def add_general_config(self, conf):
        """Store a dictionary of new general conf values.

        These are folded into the existing configuration map. So for example
        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
        the result from get_general_config() will be {'a' : 1, 'b' : 2}.

        @param conf dictionary mapping strings to JSON-serializable objects
        """
        for k,v in conf.iteritems():
            self.general_config[k] = json.dumps(v)

    def get_subscribers(self):
        """Retrieve a list of current subscribers
        """
        return list(self.subscribers)

    def add_subscriber(self, name, op):
        """Add a new subscriber to instance state changes for this domain
        """

        self.subscribers.add((name, op))

    def remove_subscriber(self, name):
        """Remove a subscriber of instance state changes for this domain
        """
        for subscriber in list(self.subscribers):
            if subscriber[0] == name:
                subscriber.remove(subscriber)

    def add_instance(self, instance):
        """Add a new instance record

        Raises a WriteConflictError if the instance already exists
        """
        instance_id = instance.instance_id
        if instance_id in self.instances:
            raise WriteConflictError()

        self.instances[instance_id] = instance

    def update_instance(self, instance, previous=None):
        """Update an existing instance record

        Raises a WriteConflictError if a previous record is specified and does
        not match what is in datastore

        Raise a NotFoundError if the instance is unknown
        """
        instance_id = instance.instance_id
        existing = self.instances.get(instance_id)
        if not existing:
            raise NotFoundError()

        if previous and previous != existing:
            raise WriteConflictError()

        self.instances[instance_id] = instance

    def get_instance(self, instance_id):
        """Retrieve an instance record

        Returns the instance record, or None if not found
        """
        return self.instances.get(instance_id)

    def set_instance_heartbeat_time(self, instance_id, time):
        """Store a new instance heartbeat
        """
        self.instance_heartbeats[instance_id] = time

    def get_instance_heartbeat_time(self, instance_id):
        """Retrieve the timestamp of the last heartbeat from this instance

        Returns the heartbeat time, or None if not found
        """
        return self.instance_heartbeats.get(instance_id)

    def get_instances(self):
        """Retrieve a list of instance records
        """
        return self.instances.values()

    def get_instance_ids(self):
        """Retrieve a list of known instance IDs
        """
        return self.instances.keys()

    def get_engine_state(self):
        """Get an object to provide to engine decide() and reset pending state

        Beware that the object provided may be changed and reused by the
        next invocation of this method.
        """
        s = self.engine_state
        #TODO not yet dealing with sensors or change lists
        s.instances = dict((i.instance_id, i) for i in self.get_instances())
        return s

#
#class EPUMStore(object):
#
#    def __init__(self, initial_conf, dt_subscribers=None):
#        """
#        See EPUManagement.__init__() for an explanation of the initial_conf contents.
#
#        During initialization, this object loads the appropriate state and leader election
#        backends.
#
#        During operation, this object is how you look up a particular EPUState instance to
#        do work.
#
#        NOTE: there are no initial EPU requests in the initial config.  EPUs are either
#              added by operations or tended to because of the recovery procedure.
#        """
#        if not initial_conf.has_key(EPUM_INITIALCONF_PERSISTENCE):
#            raise ValueError("%s configuration is required" % EPUM_INITIALCONF_PERSISTENCE)
#
#        if initial_conf[EPUM_INITIALCONF_PERSISTENCE] != "memory":
#            raise ValueError("The only persistence_type handled right now is 'memory'")
#        self.memory_mode = True
#        self.memory_mode_decider = True
#        self.memory_mode_doctor = True
#
#        self.local_decider_ref = None
#        self.local_doctor_ref = None
#
#        self.service_name = initial_conf.get(EPUM_INITIALCONF_SERVICE_NAME, EPUM_DEFAULT_SERVICE_NAME)
#
#        # TODO: when using zookeeper, here is where the initial 'schema' of znodes will be
#        #       set up (if they do not exist already).  The schema for memory is a collection
#        #       of dict instances.
#
#        # Key: string EPU name
#        # Value: EPUState instance
#        self.epus = {}
#
#        # Key: DT id+IaaS+allocation name, see self.derive_needy_name()
#        # Value: tuple (DT id, IaaS, allocation, pending integer num_needed request)
#        self.needy_dts = {}
#
#        # Key: DT id+IaaS+allocation name, see self.derive_needy_name()
#        # Value: list of node IDs that client would prefer be terminated first
#        self.needy_retirable = {}
#
#        self.dt_subscribers = dt_subscribers
#
#    def epum_service_name(self):
#        """Return the service name (to use for heartbeat/IaaS subscriptions, launches, etc.)
#        It is a configuration error to configure many instances of EPUM with the same ZK coordinates
#        but different service names.  TODO: in the future, check for this inconsistency, probably by
#        putting the epum_service_name in persistence.
#        """
#        return self.service_name
#
#
#    # ---------------------------
#    # EPU lookup/creation methods
#    # ---------------------------
#
#    def create_new_epu(self, creator, epu_name, epu_config):
#        """
#        See EPUManagement.msg_reconfigure_epu() for a long message about the epu_config parameter
#        """
#        exists = self.get_epu_state(epu_name)
#        if exists:
#            raise ValueError("The epu_name is already in use: " + epu_name)
#        else:
#            self.epus[epu_name] = EPUState(creator, epu_name, epu_config, dt_subscribers=self.dt_subscribers)
#
#    def all_epus(self):
#        """Return dict of EPUState instances for all that are not removed
#        """
#        alles = {}
#        for epu_name in self.epus.keys():
#            alles[epu_name] = self.epus[epu_name]
#        return alles
#
#
#    def all_active_epus(self):
#        """Return dict of EPUState instances for all that are not removed
#        """
#        active = {}
#        for epu_name in self.epus.keys():
#            if not self.epus[epu_name].is_removed():
#                active[epu_name] = self.epus[epu_name]
#        return active
#
#    def all_active_epu_names(self):
#        """Return list of EPUState names for all that are not removed
#        """
#        active = []
#        for epu_name in self.epus.keys():
#            if not self.epus[epu_name].is_removed():
#                active.append(epu_name)
#        return active
#
#    def remove_epu_state(self, epu_name):
#        exists = self.get_epu_state(epu_name)
#        if not exists:
#            raise ValueError("The epu_name is unknown: " + epu_name)
#
#        del self.epus[epu_name]
#
#
#    def get_epu_state(self, epu_name):
#        """Return the EPUState instance for this particular EPU or None if it does not exist.
#        """
#
#        # Applies to all persistence schemes, the epu name must be a non-empty string.
#        if not isinstance(epu_name, basestring):
#            raise ValueError("The epu_name must be a string (got %s)", epu_name)
#        if not epu_name or not epu_name.strip():
#            raise ValueError("The epu_name must be a non-empty string")
#
#        if self.memory_mode:
#            if self.epus.has_key(epu_name):
#                return self.epus[epu_name]
#            else:
#                return None
#        else:
#            raise NotImplementedError()
#
#    def get_epu_state_by_instance_id(self, instance_id):
#        """Return the EPUState instance that launched an instance ID, or None.
#
#        In the future some efficient lookup mechanism might be used (reverse map under a znode?).
#        """
#        for epu_name in self.epus.keys():
#            if self.epus[epu_name]._has_instance_id(instance_id):
#                return self.epus[epu_name]
#        return None
#
#
#    # -------------------
#    # Need-sensor related
#    # -------------------
#
#    def derive_needy_name(self, dt_id, iaas_site, iaas_allocation):
#        """Must be prefixed by '_'  (see the security TODO @ EPUManagement.msg_add_epu())
#        """
#        return "_" + dt_id + "_" + iaas_site + "_" + iaas_allocation
#
#    def new_need(self, num_needed, dt_id, iaas_site, iaas_allocation):
#        """Register a new need from the client.
#
#        See the NeedyEngine class notes for the best explanation of what is happening here.
#
#        Old values will be overwritten.  If the PD service says num_needed = 10 and then
#        quickly sends another message num_needed = 12 before the engine has been reconfigured,
#        there will never be an engine cycle run with 10.
#
#        TODO: deal with out of order messages?
#        """
#        num_needed = int(num_needed)
#        if num_needed < 0:
#            raise ValueError("num instance needed must be zero or larger")
#        if not dt_id:
#            raise ValueError("no deployable type ID was provided")
#        if not iaas_site:
#            raise ValueError("no IaaS site was provided")
#        if not iaas_allocation:
#            raise ValueError("no IaaS allocation was provided")
#        needy_name = self.derive_needy_name(dt_id, iaas_site, iaas_allocation)
#        self.needy_dts[needy_name] = (dt_id, iaas_site, iaas_allocation, num_needed)
#
#    def get_pending_needs(self):
#        """The decider has insight into this dict ... for now.
#        """
#        return self.needy_dts
#
#    def clear_pending_need(self, key, dt_id, iaas_site, iaas_allocation, num_needed):
#        """The decider is signalling that all pending EPU changes are dealt with.
#        """
#        # There is not a race condition if new_need() is called between get_pending_needs() and
#        # clear_pending_need().  If something in the num_needed changed in the meantime, it will
#        # not get cleared here.  If there was an "A-B-A" issue in this (very short) time window,
#        # it doesn't matter since the caller of clear_pending_need() just made the A take effect,
#        # the B value is not needed.
#        if self.needy_dts.has_key(key):
#            # The whole tuple needs to match exactly:
#            toclear = (dt_id, iaas_site, iaas_allocation, num_needed)
#            in_storage = self.needy_dts[key]
#            if toclear == in_storage:
#                del self.needy_dts[key]
#
#    def new_retirable(self, node_id):
#        epu_state = self.get_epu_state_by_instance_id(node_id)
#        if not epu_state:
#            raise Exception("Cannot find engine that control node ID '%s', so could not add retirable" % node_id)
#        if self.needy_retirable.has_key(epu_state.epu_name):
#            self.needy_retirable[epu_state.epu_name].append(node_id)
#        else:
#            self.needy_retirable[epu_state.epu_name] = [node_id]
#        to_engine = copy.copy(self.needy_retirable[epu_state.epu_name])
#        engine_conf = {CONF_RETIRABLE_NODES: to_engine}
#        epu_state.add_engine_conf(engine_conf)
#        log.debug("Added retirable: %s" % node_id)
#
#    def needy_subscriber(self, dt_id, subscriber_name, subscriber_op):
#        if self.dt_subscribers:
#            self.dt_subscribers.needy_subscriber(dt_id, subscriber_name, subscriber_op)
#
#    def needy_unsubscriber(self, dt_id, subscriber_name):
#        if self.dt_subscribers:
#            self.dt_subscribers.needy_unsubscriber(dt_id, subscriber_name)
#
#
#    # --------------
#    # Leader related
#    # --------------
#
#    def currently_decider(self):
#        """Return True if this instance is still the leader. This is used to check on
#        leader status just before a critical section update.  It is possible that the
#        synchronization service (or the loss of our connection to it) triggered a callback
#        that could not interrupt a thread of control in progress.  Expecting this will
#        be reworked/matured after adding ZK and after the eventing system is decided on
#        for all deployments and containers.
#        """
#        return self.memory_mode_decider
#
#    def _change_decider(self, make_leader):
#        """For internal use by EPUMStore
#        @param make_leader True/False
#        """
#        self.memory_mode_decider = make_leader
#        if self.local_decider_ref:
#            if make_leader:
#                self.local_decider_ref.now_leader()
#            else:
#                self.local_decider_ref.not_leader()
#
#    def register_decider(self, decider):
#        """For callbacks: now_leader() and not_leader()
#        """
#        self.local_decider_ref = decider
#
#    def currently_doctor(self):
#        """See currently_decider()
#        """
#        return self.memory_mode_doctor
#
#    def _change_doctor(self, make_leader):
#        """For internal use by EPUMStore
#        @param make_leader True/False
#        """
#        self.memory_mode_doctor = True
#        if self.local_doctor_ref:
#            if make_leader:
#                self.local_doctor_ref.now_leader()
#            else:
#                self.local_doctor_ref.not_leader()
#
#    def register_doctor(self, doctor):
#        """For callbacks: now_leader() and not_leader()
#        """
#        self.local_doctor_ref = doctor
#
#class EPUState(object):
#    """Provides state and persistence management facilities for one EPU
#
#    Note that this is no longer the object given to Decision Engine decide().
#
#    In memory version. The same interface will be used for real ZK persistence.
#
#    See EPUManagement.msg_reconfigure_epu() for a long message about the epu_config parameter
#    """
#
#    def __init__(self, creator, epu_name, epu_config, backing_store=None, dt_subscribers=None):
#        self.creator = creator
#        self.epu_name = epu_name
#        self.removed = False
#
#        if not backing_store:
#            self.store = ControllerStore()
#        else:
#            self.store = backing_store
#
#        self.dt_subscribers = dt_subscribers
#
#        if epu_config.has_key(EPUM_CONF_GENERAL):
#            self.add_general_conf(epu_config[EPUM_CONF_GENERAL])
#
#        if epu_config.has_key(EPUM_CONF_ENGINE):
#            self.add_engine_conf(epu_config[EPUM_CONF_ENGINE])
#
#        if epu_config.has_key(EPUM_CONF_HEALTH):
#            self.add_health_conf(epu_config[EPUM_CONF_HEALTH])
#
#        # See self.set_reconfigure_mark() and self.has_been_reconfigured()
#        self.was_reconfigured = False
#
#        self.engine_state = EngineState()
#
#        self.instance_parser = InstanceParser()
#        self.sensor_parser = SensorItemParser()
#
#        self.instances = {}
#        self.sensors = {}
#        self.pending_instances = defaultdict(list)
#        self.pending_sensors = defaultdict(list)
#
#    def is_removed(self):
#        """Return True if the EPU was removed.
#        We can't just delete this EPU state instance, it is still being used during
#        EPU removal for terminations etc.
#        """
#        return self.removed
#
#    def is_health_enabled(self):
#        """Return True if the EPUM_CONF_HEALTH_MONITOR setting is True
#        """
#        health_conf = self.get_health_conf()
#        if not health_conf.has_key(EPUM_CONF_HEALTH_MONITOR):
#            return False
#        else:
#            return bool(health_conf[EPUM_CONF_HEALTH_MONITOR])
#
#    def recover(self):
#        log.debug("Attempting recovery of controller state")
#        instance_ids = self.store.get_instance_ids()
#        for instance_id in instance_ids:
#            instance = self.store.get_instance(instance_id)
#            if instance:
#                #log.info("Recovering instance %s: state=%s health=%s iaas_id=%s",
#                #         instance_id, instance.state, instance.health,
#                #         instance.iaas_id)
#                self.instances[instance_id] = instance
#
#        sensor_ids = self.store.get_sensor_ids()
#        for sensor_id in sensor_ids:
#            sensor = self.store.get_sensor(sensor_id)
#            if sensor:
#                #log.info("Recovering sensor %s with value %s", sensor_id,
#                #         sensor.value)
#                self.sensors[sensor_id] = sensor
#
#    def new_instance_state(self, content, timestamp=None):
#        """Introduce a new instance state from an incoming message
#        """
#        instance_id = self.instance_parser.parse_instance_id(content)
#        if instance_id:
#            previous = self.instances.get(instance_id)
#            instance = self.instance_parser.parse(content, previous,
#                                                  timestamp=timestamp)
#            if instance:
#                self._add_instance(instance)
#                if self.dt_subscribers:
#                    # The higher level clients of EPUM only see RUNNING or FAILED (or nothing)
#                    if content['state'] < InstanceState.RUNNING:
#                        return
#                    elif content['state'] == InstanceState.RUNNING:
#                        notify_state = InstanceState.RUNNING
#                    else:
#                        notify_state = InstanceState.FAILED
#                    try:
#                        self.dt_subscribers.notify_subscribers(instance_id, notify_state)
#                    except Exception, e:
#                        log.error("Error notifying subscribers '%s': %s", instance_id, str(e), exc_info=True)
#
#    def new_instance_launch(self, deployable_type_id, instance_id, launch_id, site, allocation,
#                            extravars=None, timestamp=None):
#        """Record a new instance launch
#
#        @param deployable_type_id string identifier of the DT to launch
#        @param instance_id Unique id for the new instance
#        @param launch_id Unique id for the new launch group
#        @param site Site instance is being launched at
#        @param allocation Size of new instance
#        @param extravars optional dictionary of variables sent to the instance
#        @retval Deferred
#        """
#        now = time.time() if timestamp is None else timestamp
#
#        if instance_id in self.instances:
#            raise KeyError("instance %s already exists" % instance_id)
#
#        instance = CoreInstance(instance_id=instance_id, launch_id=launch_id,
#                            site=site, allocation=allocation,
#                            state=InstanceState.REQUESTING,
#                            state_time=now,
#                            health=InstanceHealthState.UNKNOWN,
#                            extravars=extravars)
#        self._add_instance(instance)
#        if self.dt_subscribers and deployable_type_id and instance_id:
#            try:
#                self.dt_subscribers.correlate_instance_id(deployable_type_id, instance_id)
#            except Exception, e:
#                log.error("Error correlating '%s' with '%s': %s",
#                          deployable_type_id, instance_id, str(e), exc_info=True)
#
#    def new_instance_health(self, instance_id, health_state, error_time=None, errors=None, caller=None):
#        """Record instance health change
#
#        @param instance_id Id of instance
#        @param health_state The state
#        @param error_time Time of the instance errors, if applicable
#        @param errors Instance errors provided in the heartbeat
#        @param caller Name of heartbeat sender (used for responses via ouagent client). If None, uses node_id
#        @retval Deferred
#        """
#        instance = self.instances[instance_id]
#        d = dict(instance.iteritems())
#        d['health'] = health_state
#        d['errors'] = errors
#        d['error_time'] = error_time
#        if not caller:
#            caller = instance_id
#        d['caller'] = caller
#
#        if errors:
#            log.error("Got error heartbeat from instance %s. State: %s. "+
#                      "Health: %s. Errors: %s", instance_id, instance.state,
#                      health_state, errors)
#
#        else:
#            log.info("Instance %s (%s) entering health state %s", instance_id,
#                     instance.state, health_state)
#
#        newinstance = CoreInstance(**d)
#        return self._add_instance(newinstance)
#
#    def ouagent_address(self, instance_id):
#        """Return address to send messages to a particular OU Agent, or None"""
#        instance = self.store.get_instance(instance_id)
#        if not instance:
#            return None
#        return instance.caller
#
#    def new_instance_heartbeat(self, instance_id, timestamp=None):
#        """Record that a heartbeat happened
#        @param instance_id ID of instance to retrieve
#        @param timestamp integer timestamp or None to clear record
#        """
#        now = time.time() if timestamp is None else timestamp
#        return self.store.add_heartbeat(instance_id, now)
#
#    def last_heartbeat_time(self, instance_id):
#        """Return time (seconds since epoch) of last heartbeat for a node, or None
#        @param instance_id ID of instance heartbeat to retrieve
#        """
#        return self.store.get_heartbeat(instance_id)
#
#    def clear_heartbeat_time(self, instance_id):
#        """Ignore that a heartbeat happened
#        @param instance_id ID of instance to clear
#        """
#        return self.store.add_heartbeat(instance_id, None)
#
#    def new_sensor_item(self, content):
#        """Introduce new sensor item from an incoming message
#
#        @retval Deferred
#        """
#        item = self.sensor_parser.parse(content)
#        if item:
#            return self._add_sensor(item)
#        return False
#
#    def get_engine_state(self):
#        """Get an object to provide to engine decide() and reset pending state
#
#        Beware that the object provided may be changed and reused by the
#        next invocation of this method.
#        """
#        s = self.engine_state
#        s.sensors = dict(self.sensors.iteritems())
#        s.sensor_changes = dict(self.pending_sensors.iteritems())
#        s.instances = dict(self.instances.iteritems())
#        s.instance_changes = dict(self.pending_instances.iteritems())
#
#        self._reset_pending()
#        return s
#
#    def set_reconfigure_mark(self):
#        """Signal that any configuration changes to this EPU will be judged a reconfigure
#        starting now.
#        """
#        # TODO: this impl only works for in-memory
#        self.was_reconfigured = False
#
#    def has_been_reconfigured(self):
#        # TODO: this impl only works for in-memory
#        return self.was_reconfigured
#
#    def add_engine_conf(self, config):
#        """Add new engine config values
#
#        @param config dictionary of configuration key/value pairs.
#            Value can be any JSON-serializable object.
#        """
#        self.was_reconfigured = True
#        self.store.add_config(config)
#
#    def get_engine_conf(self):
#        """Retrieve engine configuration key/value pairs
#
#        @retval config dictionary
#        """
#        return self.store.get_config()
#
#    def add_health_conf(self, config):
#        """Add new health config values
#
#        @param config dictionary of configuration key/value pairs.
#            Value can be any JSON-serializable object.
#        """
#        self.store.add_health_config(config)
#
#    def get_health_conf(self):
#        """Retrieve health configuration key/value pairs
#
#        @retval config dictionary
#        """
#        return self.store.get_health_config()
#
#    def add_general_conf(self, config):
#        """Add new general config values
#
#        @param config dictionary of configuration key/value pairs.
#            Value can be any JSON-serializable object.
#        """
#        self.store.add_general_config(config)
#
#    def get_general_conf(self):
#        """Retrieve general configuration key/value pairs
#
#        @retval config dictionary
#        """
#        return self.store.get_general_config()
#
#    def get_all_conf(self):
#        """Retrieve a dictionary of all config
#        """
#        return {EPUM_CONF_GENERAL: self.store.get_general_config(),
#                EPUM_CONF_HEALTH: self.store.get_health_config(),
#                EPUM_CONF_ENGINE: self.store.get_config()}
#
#    def get_instance_dicts(self):
#        """Retrieve a list of dictionaries describing each instance in the EPU
#        """
#        return [instance.to_dict() for instance in self.instances.itervalues()]
#
#    def _add_instance(self, instance):
#        instance_id = instance.instance_id
#        self.instances[instance_id] = instance
#        self.pending_instances[instance_id].append(instance)
#        self.store.add_instance(instance)
#
#    def _update_instance(self, instance):
#        instance_id = instance.instance_id
#        self.instances[instance_id] = instance
#        self.pending_instances[instance_id].append(instance)
#        self.store.update_instance(instance)
#
#    def _has_instance_id(self, instance_id):
#        return self.instances.has_key(instance_id)
#
#    def _add_sensor(self, sensor):
#        sensor_id = sensor.sensor_id
#        previous = self.sensors.get(sensor_id)
#
#        # we only update the current sensor value if the timestamp is newer.
#        # But we can still add out-of-order items to the store and the
#        # pending list.
#        if previous and sensor.time < previous.time:
#            log.warn("Received out of order %s sensor item!", sensor_id)
#        else:
#            self.sensors[sensor_id] = sensor
#
#        self.pending_sensors[sensor_id].append(sensor)
#        return self.store.add_sensor(sensor)
#
#    def _reset_pending(self):
#        self.pending_instances.clear()
#        self.pending_sensors.clear()
#
#    def set_removed(self):
#        self.removed = True
#
#class DTSubscribers(object):
#    """In memory persistence for DT subscribers.
#    Shared reference:
#    1. The EPUStore instance updates this
#    2. Each EPUState instance potentially signals to notify
#    """
#
#    def __init__(self, notifier):
#
#        self.notifier = notifier
#
#        # Key: Instance ID
#        # Value: DT ID
#        self.instance_dt = {}
#
#        # Key: DT id
#        # Value: list of subscriber+operation tuples e.g. [(client01, dt_info), (client02, dt_info), ...]
#        self.needy_subscribers = {}
#
#    def needy_subscriber(self, dt_id, subscriber_name, subscriber_op):
#        if not self.notifier:
#            return
#        tup = (subscriber_name, subscriber_op)
#        if not self.needy_subscribers.has_key(dt_id):
#            self.needy_subscribers[dt_id] = [tup]
#            return
#
#        # handling op name changes (probably unecessary)
#        for name,op in self.needy_subscribers[dt_id]:
#            if name == subscriber_name:
#                rm_tup = (name,op)
#                self.needy_subscribers[dt_id].remove(rm_tup)
#                break
#        self.needy_subscribers[dt_id].append(tup)
#
#    def needy_unsubscriber(self, dt_id, subscriber_name):
#        if not self.notifier:
#            return
#        if not self.needy_subscribers.has_key(dt_id):
#            return
#        for name,op in self.needy_subscribers[dt_id]:
#            if name == subscriber_name:
#                rm_tup = (name,op)
#                self.needy_subscribers[dt_id].remove(rm_tup)
#
#    def notify_subscribers(self, instance_id, state):
#        """Notify all dt-id subscribers of this state change.
#
#        @param instance_id The instance_id whose state changed
#        @param state The state to deliver
#        """
#        if not self.notifier:
#            return
#        dt_id = self.instance_dt.get(instance_id)
#        if not dt_id:
#            return
#        tups = self._current_dt_subscribers(dt_id)
#        for subscriber_name, subscriber_op in tups:
#            content = {'node_id': instance_id, 'state': state,
#                       'deployable_type' : dt_id}
#            self.notifier.notify_by_name(subscriber_name, subscriber_op, content)
#
#    def correlate_instance_id(self, dt_id, instance_id):
#        """Create a correlation between dt id and instance id.
#        TODO: There may be a much better way to structure all of this when not using
#        memory persistence. Notifier leader?
#
#        @param dt_id The DT that subscribers registered for
#        @param instance_id The instance_id
#        """
#        self.instance_dt[instance_id] = dt_id
#
#    def _current_dt_subscribers(self, dt_id):
#        """Return list of subscription targets for a given DT id.
#        Only considers DTs running via the "register need" strongly typed sensor mechanism.
#        Does not consider allocation or site differences.
#
#        @param dt_id The DT of interest
#        @retval list of tuples: (subscriber_name, subscriber_op)
#        """
#        if not self.notifier:
#            return []
#        if not self.needy_subscribers.has_key(dt_id):
#            return []
#        return copy.copy(self.needy_subscribers[dt_id])
#
#
#class ControllerStore(object):
#    """In memory "persistence" for EPU Controller state
#
#    The same interface wille be used for real ZK persistence.
#    """
#
#    def __init__(self):
#        self.instances = defaultdict(list)
#        self.sensors = defaultdict(list)
#        self.config = {}
#        self.health_config = {}
#        self.general_config = {}
#        self.heartbeats = {}
#
#    def add_instance(self, instance):
#        """Adds a new instance object to persistence
#        @param instance Instance to add
#        """
#        instance_id = instance.instance_id
#        self.instances[instance_id].append(instance)
#
#    def get_instance_ids(self):
#        """Retrieves a list of known instances
#
#        @retval list of instance IDs
#        """
#        return self.instances.keys()
#
#    def get_instance(self, instance_id):
#        """Retrieves the latest instance object for the specified id
#        @param instance_id ID of instance to retrieve
#        @retval Instance object or None
#        """
#        if instance_id in self.instances:
#            instance_list = self.instances[instance_id]
#            if instance_list:
#                instance = instance_list[-1]
#            else:
#                instance = None
#        else:
#            instance = None
#        return instance
#
#    def add_heartbeat(self, instance_id, timestamp):
#        """Adds a new heartbeat time, replaces any old value
#        @param instance_id ID of instance to retrieve
#        @param timestamp integer timestamp or None to clear record
#        """
#        self.heartbeats[instance_id] = timestamp
#
#    def get_heartbeat(self, instance_id):
#        """Retrieves last known heartbeat
#        @param instance_id ID of instance heartbeat to retrieve
#        @retval timestamp integer or None
#        """
#        return self.heartbeats.get(instance_id)
#
#    def add_sensor(self, sensor):
#        """Adds a new sensor object to persistence
#        @param sensor Sensor to add
#        """
#        sensor_id = sensor.sensor_id
#        sensor_list = self.sensors[sensor_id]
#        sensor_list.append(sensor)
#
#        # this isn't efficient but not a big deal because this is only used
#        # in tests
#        # if a sensor item has an earlier timestamp, store it but sort it into
#        # the appropriate place. Would be faster to use bisect here
#        if len(sensor_list) > 1 and sensor_list[-2].time > sensor.time:
#            sensor_list.sort(key=lambda s: s.time)
#
#    def get_sensor_ids(self):
#        """Retrieves a list of known sensors
#
#        @retval list of sensor IDs
#        """
#        return self.sensors.keys()
#
#    def get_sensor(self, sensor_id):
#        """Retrieve the latest sensor item for the specified sensor
#
#        @param sensor_id ID of the sensor item to retrieve
#        @retval SensorItem object or None
#        """
#        if sensor_id in self.sensors:
#            sensor_list = self.sensors[sensor_id]
#            if sensor_list:
#                sensor = sensor_list[-1]
#            else:
#                sensor = None
#        else:
#            sensor = None
#        return sensor
#
#    def get_config(self, keys=None):
#        """Retrieve the engine config dictionary.
#
#        @param keys optional list of keys to retrieve
#        @retval config dictionary object
#        """
#        if keys is None:
#            d = dict((k, json.loads(v)) for k,v in self.config.iteritems())
#        else:
#            d = dict((k, json.loads(self.config[k]))
#                    for k in keys if k in self.config)
#        return d
#
#    def add_config(self, conf):
#        """Store a dictionary of new engine conf values.
#
#        These are folded into the existing configuration map. So for example
#        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
#        the result from get_config() will be {'a' : 1, 'b' : 2}.
#
#        @param conf dictionary mapping strings to JSON-serializable objects
#        """
#        for k,v in conf.iteritems():
#            self.config[k] = json.dumps(v)
#
#    def get_health_config(self, keys=None):
#        """Retrieve the health config dictionary.
#
#        @param keys optional list of keys to retrieve
#        @retval config dictionary object
#        """
#        if keys is None:
#            d = dict((k, json.loads(v)) for k,v in self.health_config.iteritems())
#        else:
#            d = dict((k, json.loads(self.health_config[k]))
#                    for k in keys if k in self.health_config)
#        return d
#
#    def add_health_config(self, conf):
#        """Store a dictionary of new health conf values.
#
#        These are folded into the existing configuration map. So for example
#        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
#        the result from get_health_config() will be {'a' : 1, 'b' : 2}.
#
#        @param conf dictionary mapping strings to JSON-serializable objects
#        """
#        for k,v in conf.iteritems():
#            self.health_config[k] = json.dumps(v)
#
#    def get_general_config(self, keys=None):
#        """Retrieve the general config dictionary.
#
#        @param keys optional list of keys to retrieve
#        @retval config dictionary object
#        """
#        if keys is None:
#            d = dict((k, json.loads(v)) for k,v in self.general_config.iteritems())
#        else:
#            d = dict((k, json.loads(self.general_config[k]))
#                    for k in keys if k in self.general_config)
#        return d
#
#    def add_general_config(self, conf):
#        """Store a dictionary of new general conf values.
#
#        These are folded into the existing configuration map. So for example
#        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
#        the result from get_general_config() will be {'a' : 1, 'b' : 2}.
#
#        @param conf dictionary mapping strings to JSON-serializable objects
#        """
#        for k,v in conf.iteritems():
#            self.general_config[k] = json.dumps(v)
#
