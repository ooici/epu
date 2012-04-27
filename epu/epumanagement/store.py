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

