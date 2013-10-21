# Copyright 2013 University of Chicago

import logging
import time
import simplejson as json
import threading
import re
import socket
import os

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NodeExistsException, BadVersionException,\
    NoNodeException

import epu.tevent as tevent
from epu.epumanagement.core import EngineState, InstanceParser, CoreInstance
from epu.states import InstanceState, InstanceHealthState
from epu.exceptions import NotFoundError, WriteConflictError
from epu import zkutil
from epu.epumanagement.conf import *  # noqa


log = logging.getLogger(__name__)


def get_epum_store(config, service_name, use_gevent=False, proc_name=None):
    """Instantiate EPUM store object for the given configuration
    """
    if zkutil.is_zookeeper_enabled(config):
        zookeeper = zkutil.get_zookeeper_config(config)

        log.info("Using ZooKeeper EPUM store")

        store = ZooKeeperEPUMStore(service_name, zookeeper['hosts'],
            zookeeper['path'], username=zookeeper.get('username'),
            password=zookeeper.get('password'), use_gevent=use_gevent,
            timeout=zookeeper.get('timeout'), proc_name=proc_name)

    else:
        log.info("Using in-memory EPUM store")
        store = LocalEPUMStore(service_name)

    return store


#############################################################################
# STORAGE INTERFACES
#############################################################################

class EPUMStore(object):
    """Interface for accessing storage and synchronization.

    This class cannot be used directly, you must use a subclass.
    """

    def currently_decider(self):
        """Return True if this instance is still the leader. This is used to check on
        leader status just before a critical section update.  It is possible that the
        synchronization service (or the loss of our connection to it) triggered a callback
        that could not interrupt a thread of control in progress.  Expecting this will
        be reworked/matured after adding ZK and after the eventing system is decided on
        for all deployments and containers.
        """

    def register_decider(self, decider):
        """For callbacks: now_leader() and not_leader()
        """

    def currently_doctor(self):
        """See currently_decider()
        """

    def register_doctor(self, doctor):
        """For callbacks: now_leader() and not_leader()
        """

    def currently_reaper(self):
        """See currently_decider()
        """

    def register_reaper(self, reaper):
        """For callbacks: now_leader() and not_leader()
        """

    def epum_service_name(self):
        """Return the service name (to use for heartbeat/IaaS subscriptions, launches, etc.)

        It is a configuration error to configure many instances of EPUM with the same ZK coordinates
        but different service names.  TODO: in the future, check for this inconsistency, probably by
        putting the epum_service_name in persistence.
        """

    def add_domain(self, owner, domain_id, config):
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

    def add_domain_definition(self, definition_id, definition):
        """Add a new domain definition

        Returns the new DomainDefinitionStore
        Raises a WriteConflictError if a domain definition already exists with
        this name.
        """

    def get_domain_definition(self, definition_id):
        """Retrieve a domain definition

        Raises NotFoundError if domain definition does not exist
        @rtype DomainDefinitionStore
        """

    def remove_domain_definition(self, definition_id):
        """Remove a domain definition
        """

    def list_domain_definitions(self):
        """Retrieve a list of domain definitions ids
        """

    def update_domain_definition(self, definition_id, definition):
        """Update domain definition
        """


class DomainStore(object):
    """Interface for accessing storage and synchronization for a single domain.

    This class cannot be used directly, you must use a subclass.
    """

    def __init__(self, owner, domain_id):
        self.instance_parser = InstanceParser()
        self.owner = owner
        self.domain_id = domain_id

    @property
    def key(self):
        return (self.owner, self.domain_id)

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

    def add_domain_sensor_data(self, sensor_data):
        """Store a dictionary of domain sensor data.

        This operation replaces previous sensor data

        data is in the format:
        {
          'metric':{
            'Average': 5
          }
        }

        @param sensor_data dictionary mapping strings to JSON-serializable objects
        """

    def get_domain_sensor_data(self):
        """Retrieve a dictionary of sensor data from the store
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
        health_conf = self.get_health_config()
        if EPUM_CONF_HEALTH_MONITOR not in health_conf:
            return False
        else:
            return bool(health_conf[EPUM_CONF_HEALTH_MONITOR])

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

    def remove_instance(self, instance_id):
        """Remove an instance record

        Raise a NotFoundError if the instance is unknown
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

        returns True/False indicating whether instance state was accepted
        """
        instance_id = self.instance_parser.parse_instance_id(content)
        if instance_id:
            if not previous:
                previous = self.get_instance(instance_id)
            instance = self.instance_parser.parse(content, previous,
                                                  timestamp=timestamp)
            if instance:
                self.update_instance(instance, previous=previous)
                return True
            # instance was probably a duplicate
            return False
        return False

    def mark_instance_terminating(self, instance_id):
        """Mark an instance for termination

        returns True/False indicating where instance was updated
        """
        while 1:
            instance = self.get_instance(instance_id)
            if not instance or instance.state >= InstanceState.TERMINATING:
                return False

            d = dict(instance.iteritems())
            d['state'] = InstanceState.TERMINATING
            newinstance = CoreInstance(**d)

            try:
                self.update_instance(newinstance, previous=instance)
                return True
            except WriteConflictError:
                pass

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
        return instance

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
            log.error("Got error heartbeat from instance %s. State: %s. " +
                      "Health: %s. Errors: %s", instance_id, instance.state,
                      health_state, errors)

        else:
            log.info("Instance %s (%s) entering health state %s", instance_id,
                     instance.state, health_state)

        newinstance = CoreInstance(**d)
        self.update_instance(newinstance, previous=instance)

    def new_instance_sensor(self, instance_id, sensor_data):
        """Record instance sensor change

        @param instance_id Id of instance
        @param sensor_data The state
        """
        instance = self.get_instance(instance_id)
        if not instance:
            log.error("Got sensor data for unknown instance %s: %s",
                instance_id, sensor_data)

        d = dict(instance.iteritems())
        if sensor_data.get(instance.instance_id):
            sensor_data = sensor_data.get(instance.instance_id)
        if not d.get('sensor_data'):
            d['sensor_data'] = {}
        for key, val in sensor_data.iteritems():
            d['sensor_data'][key] = val

        log.info("Instance %s (%s) got sensor data %s", instance_id,
                 instance.state, sensor_data)

        newinstance = CoreInstance(**d)
        self.update_instance(newinstance, previous=instance)

    def new_domain_sensor(self, sensor_data):
        """Record domain sensor change

        @param sensor_data The state
        """

        log.info("Domain %s got sensor data %s", self.domain_id, sensor_data)

        previous_sensor_data = self.get_sensor_data()
        self.update_sensor_data(sensor_data, previous=previous_sensor_data)

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


class DomainDefinitionStore(object):
    """Interface for accessing storage and synchronization for a single domain
    definition.

    This class cannot be used directly, you must use a subclass.
    """
    def __init__(self, definition_id):
        self.definition_id = definition_id


#############################################################################
# IN-MEMORY STORAGE IMPLEMENTATION
#############################################################################

class LocalEPUMStore(EPUMStore):
    """EPUM store that uses local memory only
    """

    def __init__(self, service_name):
        super(LocalEPUMStore, self).__init__()

        self.domains = {}
        self.domain_definitions = {}
        self.service_name = service_name

        self.local_decider_ref = None
        self.local_doctor_ref = None
        self.local_reaper_ref = None

    def initialize(self):
        pass

    def shutdown(self):
        pass

    def _change_decider(self, make_leader):
        """For internal use by EPUMStore
        @param make_leader True/False
        """
        if self.local_decider_ref:
            if make_leader:
                self.local_decider_ref.now_leader()
            else:
                self.local_decider_ref.not_leader()

    def register_decider(self, decider):
        """For callbacks: now_leader() and not_leader()
        """
        self.local_decider_ref = decider
        self._change_decider(True)

    def _change_doctor(self, make_leader):
        """For internal use by EPUMStore
        @param make_leader True/False
        """
        if self.local_doctor_ref:
            if make_leader:
                self.local_doctor_ref.now_leader()
            else:
                self.local_doctor_ref.not_leader()

    def register_doctor(self, doctor):
        """For callbacks: now_leader() and not_leader()
        """
        self.local_doctor_ref = doctor
        self._change_doctor(True)

    def _change_reaper(self, make_leader):
        """For internal use by EPUMStore
        @param make_leader True/False
        """
        if self.local_reaper_ref:
            if make_leader:
                self.local_reaper_ref.now_leader()
            else:
                self.local_reaper_ref.not_leader()

    def register_reaper(self, reaper):
        """For callbacks: now_leader() and not_leader()
        """
        self.local_reaper_ref = reaper
        self._change_reaper(True)

    def epum_service_name(self):
        """Return the service name (to use for heartbeat/IaaS subscriptions, launches, etc.)

        It is a configuration error to configure many instances of EPUM with the same ZK coordinates
        but different service names.  TODO: in the future, check for this inconsistency, probably by
        putting the epum_service_name in persistence.
        """
        return self.service_name

    def add_domain(self, owner, domain_id, config):
        """Add a new domain

        Returns the new DomainStore
        Raises a WriteConflictError if a domain already exists with this name
        and owner.
        """
        validate_entity_name(owner)
        validate_entity_name(domain_id)

        key = (owner, domain_id)
        if key in self.domains:
            raise WriteConflictError()

        domain = LocalDomainStore(owner, domain_id, config)
        self.domains[key] = domain
        return domain

    def remove_domain(self, owner, domain_id):
        """Remove a domain

        TODO this should only work when there are no running instances for the domain

        Raises a NotFoundError if the domain is unknown
        """
        validate_entity_name(owner)
        validate_entity_name(domain_id)

        key = (owner, domain_id)
        if key not in self.domains:
            raise NotFoundError()
        del self.domains[key]

    def list_domains_by_owner(self, owner):
        """Retrieve a list of domains owned by a particular user
        """
        validate_entity_name(owner)
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
        validate_entity_name(owner)
        validate_entity_name(domain_id)
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
        validate_entity_name(instance_id)
        for domain in self.domains.itervalues():
            if domain.get_instance(instance_id):
                return domain

    def add_domain_definition(self, definition_id, definition):
        """Add a new domain definition

        Returns the new DomainDefinitionStore
        Raises a WriteConflictError if a domain definition already exists with
        this name.
        """
        validate_entity_name(definition_id)

        if definition_id in self.domain_definitions:
            raise WriteConflictError()

        domain_definition = LocalDomainDefinitionStore(definition_id, definition)
        self.domain_definitions[definition_id] = domain_definition
        return domain_definition

    def list_domain_definitions(self):
        """Retrieve a list of domain definitions ids
        """
        return self.domain_definitions.keys()

    def get_domain_definition(self, definition_id):
        """Retrieve the store for a particular domain definition

        Raises NotFoundError if domain definition does not exist

        @rtype DomainDefinitionStore
        """
        validate_entity_name(definition_id)
        try:
            return self.domain_definitions[definition_id]
        except KeyError:
            raise NotFoundError()

    def remove_domain_definition(self, definition_id):
        """Remove a domain definition

        Raises a NotFoundError if the domain definition is unknown
        """
        validate_entity_name(definition_id)

        if definition_id not in self.domain_definitions:
            raise NotFoundError()
        del self.domain_definitions[definition_id]

    def update_domain_definition(self, definition_id, definition):
        """Update domain definition
        """
        validate_entity_name(definition_id)

        if definition_id not in self.domain_definitions:
            raise NotFoundError("Domain definition %s not found" % definition_id)

        domain_definition = LocalDomainDefinitionStore(definition_id, definition)
        self.domain_definitions[definition_id] = domain_definition


class LocalDomainStore(DomainStore):

    def __init__(self, owner, domain_id, config):
        super(LocalDomainStore, self).__init__(owner, domain_id)

        self.removed = False
        self.engine_config_version = 0
        self.engine_config = {}
        self.health_config = {}
        self.general_config = {}
        if config:
            if EPUM_CONF_GENERAL in config:
                self.add_general_config(config[EPUM_CONF_GENERAL])

            if EPUM_CONF_ENGINE in config:
                self.add_engine_config(config[EPUM_CONF_ENGINE])

            if EPUM_CONF_HEALTH in config:
                self.add_health_config(config[EPUM_CONF_HEALTH])
        self.engine_state = EngineState()

        self.subscribers = set()

        self.instances = {}
        self.instance_heartbeats = {}

        self.domain_sensor_data = {}

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
            d = dict((k, json.loads(v)) for k, v in self.engine_config.iteritems())
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
        for k, v in conf.iteritems():
            self.engine_config[k] = json.dumps(v)
        self.engine_config_version += 1

    def get_domain_sensor_data(self):
        """Retrieve a dictionary of sensor data from the store
        """
        return self.domain_sensor_data

    def add_domain_sensor_data(self, sensor_data):
        """Store a dictionary of domain sensor data.

        This operation replaces previous sensor data

        data is in the format:
        {
          'metric':{
            'Average': 5
          }
        }

        @param sensor_data dictionary mapping strings to JSON-serializable objects
        """
        self.domain_sensor_data = sensor_data

    def get_health_config(self, keys=None):
        """Retrieve the health config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """
        if keys is None:
            d = dict((k, json.loads(v)) for k, v in self.health_config.iteritems())
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
        for k, v in conf.iteritems():
            self.health_config[k] = json.dumps(v)

    def get_general_config(self, keys=None):
        """Retrieve the general config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """
        if keys is None:
            d = dict((k, json.loads(v)) for k, v in self.general_config.iteritems())
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
        for k, v in conf.iteritems():
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

    def remove_instance(self, instance_id):
        """Remove an instance record

        Raise a NotFoundError if the instance is unknown
        """
        instance = self.instances.get(instance_id)
        if instance:
            del self.instances[instance_id]
        else:
            raise NotFoundError()

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
        # TODO not yet dealing with sensors or change lists
        s.sensors = self.get_domain_sensor_data()
        s.instances = dict((i.instance_id, i) for i in self.get_instances())
        return s


class LocalDomainDefinitionStore(DomainDefinitionStore):

    def __init__(self, definition_id, definition):
        super(LocalDomainDefinitionStore, self).__init__(definition_id)

        self.definition = definition

    def get_definition(self):
        return self.definition


#############################################################################
# ZOOKEEPER STORAGE IMPLEMENTATION
#############################################################################

class ZooKeeperEPUMStore(EPUMStore):
    """EPUM store that uses ZooKeeper
    """

    DECIDER_ELECTION_PATH = "/elections/decider"
    DOCTOR_ELECTION_PATH = "/elections/doctor"
    REAPER_ELECTION_PATH = "/elections/reaper"

    DOMAINS_PATH = "/domains"
    DEFINITIONS_PATH = "/definitions"

    def __init__(self, service_name, hosts, base_path, username=None, password=None,
                 timeout=None, use_gevent=False, proc_name=None):
        super(ZooKeeperEPUMStore, self).__init__()

        self.service_name = service_name

        kwargs = zkutil.get_kazoo_kwargs(username=username, password=password,
            timeout=timeout, use_gevent=use_gevent)
        self.kazoo = KazooClient(hosts + base_path, **kwargs)

        self.retry = zkutil.get_kazoo_retry()

        if not proc_name:
            proc_name = ""
        zk_id = "%s:%s:%d" % (proc_name, socket.gethostname(), os.getpid())

        self.decider_election = self.kazoo.Election(self.DECIDER_ELECTION_PATH, identifier=zk_id)
        self.doctor_election = self.kazoo.Election(self.DOCTOR_ELECTION_PATH, identifier=zk_id)
        self.reaper_election = self.kazoo.Election(self.REAPER_ELECTION_PATH, identifier=zk_id)

        #  callback fired when the connection state changes
        self.kazoo.add_listener(self._connection_state_listener)

        self._election_enabled = False
        self._election_condition = threading.Condition()
        self._decider_election_thread = None
        self._doctor_election_thread = None
        self._reaper_election_thread = None

        self._decider_leader = None
        self._doctor_leader = None
        self._reaper_leader = None

        # cache domain stores locally. Note that this is not necessarily the
        # complete set of domains, just the ones that this worker has seen.
        # (owner, domain_id) -> ZooKeeperDomainStore
        self._domain_cache_lock = threading.RLock()
        self._domain_cache = {}

    def initialize(self):

        self.kazoo.start()

        for path in (self.DOMAINS_PATH, self.DEFINITIONS_PATH):
            self.kazoo.ensure_path(path)

    def shutdown(self):
        self.kazoo.stop()
        try:
            self.kazoo.close()
        except Exception:
            log.exception("Problem cleaning up kazoo")

    def _connection_state_listener(self, state):
        # called by kazoo when the connection state changes.
        # handle in background
        tevent.spawn(self._handle_connection_state, state)

    def _handle_connection_state(self, state):

        if state in (KazooState.LOST, KazooState.SUSPENDED):
            with self._election_condition:
                self._election_enabled = False
                self._election_condition.notify_all()

            # depose the leaders and cancel the elections just in case
            try:
                self._decider_leader.not_leader()
            except Exception, e:
                log.exception("Error deposing decider leader: %s", e)

            try:
                self._doctor_leader.not_leader()
            except Exception, e:
                log.exception("Error deposing doctor leader: %s", e)

            try:
                self._reaper_leader.not_leader()
            except Exception, e:
                log.exception("Error deposing reaper leader: %s", e)

            self.decider_election.cancel()
            self.doctor_election.cancel()
            self.reaper_election.cancel()

        elif state == KazooState.CONNECTED:
            with self._election_condition:
                self._election_enabled = True
                self._election_condition.notify_all()

    def _run_election(self, election, leader, name):
        """Election thread function
        """
        while True:
            with self._election_condition:
                while not self._election_enabled:
                    self._election_condition.wait()

            try:
                election.run(leader.now_leader, block=True)
            except Exception, e:
                log.exception("Error in %s election: %s", name, e)

    def register_decider(self, decider):
        """For callbacks: now_leader() and not_leader()
        """
        if self._decider_leader:
            raise Exception("decider already registered")
        self._decider_leader = decider
        self._decider_election_thread = tevent.spawn(self._run_election,
            self.decider_election, decider, "decider")

    def register_doctor(self, doctor):
        """For callbacks: now_leader() and not_leader()
        """
        if self._doctor_leader:
            raise Exception("doctor already registered")
        self._doctor_leader = doctor
        self._doctor_election_thread = tevent.spawn(self._run_election,
            self.doctor_election, doctor, "doctor")

    def register_reaper(self, reaper):
        """For callbacks: now_leader() and not_leader()
        """
        if self._reaper_leader:
            raise Exception("reaper already registered")
        self._reaper_leader = reaper
        self._reaper_election_thread = tevent.spawn(self._run_election,
            self.reaper_election, reaper, "reaper")

    def epum_service_name(self):
        """Return the service name (to use for heartbeat/IaaS subscriptions, launches, etc.)

        It is a configuration error to configure many instances of EPUM with the same ZK coordinates
        but different service names.  TODO: in the future, check for this inconsistency, probably by
        putting the epum_service_name in persistence.
        """
        return self.service_name

    def _get_domain_path(self, owner, domain_id):
        validate_entity_name(owner)
        validate_entity_name(domain_id)
        return self.DOMAINS_PATH + "/" + owner + "/" + domain_id

    def _get_owner_path(self, owner):
        validate_entity_name(owner)
        return self.DOMAINS_PATH + "/" + owner

    def _get_domain_store(self, owner, domain_id):
        validate_entity_name(owner)
        validate_entity_name(domain_id)
        key = (owner, domain_id)

        with self._domain_cache_lock:
            domain = self._domain_cache.get(key)
            if not domain:
                path = self._get_domain_path(owner, domain_id)
                domain = ZooKeeperDomainStore(owner, domain_id, self.kazoo, self.retry, path)
                self._domain_cache[key] = domain
            return domain

    def _get_definition_store(self, definition_id):
        validate_entity_name(definition_id)

        path = self._get_definition_path(definition_id)
        definition = ZooKeeperDomainDefinitionStore(definition_id, self.kazoo, self.retry, path)
        return definition

    def _get_definition_path(self, definition_id):
        validate_entity_name(definition_id)
        return self.DEFINITIONS_PATH + "/" + definition_id

    def add_domain(self, owner, domain_id, config):
        """Add a new domain

        Returns the new DomainStore
        Raises a WriteConflictError if a domain already exists with this name
        and owner.
        """

        # store the entire domain config as a single ZNode, to ensure creation is atomic

        path = self._get_domain_path(owner, domain_id)
        data = json.dumps(config)

        try:
            self.retry(self.kazoo.create, path, data, makepath=True)
        except NodeExistsException:
            raise WriteConflictError("domain %s already exists for owner %s" %
                                     (domain_id, owner))

        return self._get_domain_store(owner, domain_id)

    def remove_domain(self, owner, domain_id):
        """Remove a domain

        Warning: this should only be used when there are no running instances
        for the domain.
        """
        path = self._get_domain_path(owner, domain_id)
        self.retry(self.kazoo.delete, path, recursive=True)

        with self._domain_cache_lock:
            if (owner, domain_id) in self._domain_cache:
                del self._domain_cache[(owner, domain_id)]

    def list_domains_by_owner(self, owner):
        """Retrieve a list of domains owned by a particular user
        """
        path = self._get_owner_path(owner)
        try:
            return self.retry(self.kazoo.get_children, path)
        except NoNodeException:
            # if the owner ZNode doesn't exist, that user isn't necessarily
            # invalid as those buckets are lazily-created. Return the empty
            # list instead.
            return []

    def list_domains(self):
        """Retrieve a list of (owner, domain) pairs
        """
        # parallelize this?

        owners = self.retry(self.kazoo.get_children, self.DOMAINS_PATH)

        found = []
        for owner in owners:
            try:
                domains = self.retry(self.kazoo.get_children, self._get_owner_path(owner))
                found.extend((owner, domain_id) for domain_id in domains)

            except NoNodeException:
                pass
        return found

    def get_domain(self, owner, domain_id):
        """Retrieve the store for a particular domain

        Raises NotFoundError if domain does not exist

        @rtype DomainStore
        """

        stat = self.retry(self.kazoo.exists, self._get_domain_path(owner, domain_id))

        if stat:
            return self._get_domain_store(owner, domain_id)
        else:
            raise NotFoundError()

    def get_all_domains(self):
        """Retrieve a list of all domain stores
        """
        domains = []
        for owner, domain_id in self.list_domains():
            domains.append(self._get_domain_store(owner, domain_id))
        return domains

    def get_domain_for_instance_id(self, instance_id):
        """Retrieve the domain associated with an instance

        Returns a DomainStore, or None if not found
        """

        validate_entity_name(instance_id)

        # TODO speed this up with a lookup table from instance ID to domainid/owner
        # at the same time, we can centralize the ID generating and even switch to
        # more legible IDs. DI-XXXXXXX and DL-XXXXXXX (Domain Instance and Domain
        # Launch)

        for owner, domain_id in self.list_domains():
            domain = self._get_domain_store(owner, domain_id)
            if domain.get_instance(instance_id):
                return domain
        return None

    def add_domain_definition(self, definition_id, definition):
        """Add a new domain definition

        Returns the new DomainDefinitionStore
        Raises a WriteConflictError if a domain definition already exists with
        this name.
        """

        path = self._get_definition_path(definition_id)
        data = json.dumps(definition)

        try:
            self.retry(self.kazoo.create, path, data, makepath=True)
        except NodeExistsException:
            raise WriteConflictError("domain definition %s already exists" %
                                     definition_id)

        return self._get_definition_store(definition_id)

    def list_domain_definitions(self):
        """Retrieve a list of domain definitions ids
        """
        definitions = self.retry(self.kazoo.get_children, self.DEFINITIONS_PATH)
        return definitions

    def get_domain_definition(self, definition_id):
        """Retrieve the store for a particular domain definition

        Raises NotFoundError if domain definition does not exist

        @rtype DomainDefinitionStore
        """

        stat = self.retry(self.kazoo.exists, self._get_definition_path(definition_id))

        if stat:
            return self._get_definition_store(definition_id)
        else:
            raise NotFoundError()

    def remove_domain_definition(self, definition_id):
        """Remove a domain definition
        """
        path = self._get_definition_path(definition_id)
        self.retry(self.kazoo.delete, path)

    def update_domain_definition(self, definition_id, definition):
        """Update a domain definition
        """
        path = self._get_definition_path(definition_id)
        data = json.dumps(definition)

        try:
            self.retry(self.kazoo.set, path, data, -1)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()


class ZooKeeperDomainStore(DomainStore):

    REMOVED_PATH = "removed"
    SUBSCRIBERS_PATH = "subscribers"
    INSTANCES_PATH = "instances"
    INSTANCE_HEARTBEAT_PATH = "heartbeat"
    DOMAIN_SENSOR_PATH = "domainsensor"

    def __init__(self, owner, domain_id, kazoo, retry, path):
        super(ZooKeeperDomainStore, self).__init__(owner, domain_id)

        self.kazoo = kazoo
        self.retry = retry
        self.path = path

        self.removed_path = self.path + "/" + self.REMOVED_PATH
        self.subscribers_path = self.path + "/" + self.SUBSCRIBERS_PATH
        self.instances_path = self.path + "/" + self.INSTANCES_PATH
        self.domain_sensor_path = self.path + "/" + self.DOMAIN_SENSOR_PATH

        self.engine_state = EngineState()

    def is_removed(self):
        """Whether this domain has been marked for removal
        """
        return bool(self.retry(self.kazoo.exists, self.removed_path))

    def remove(self):
        """Mark this instance for removal
        """
        try:
            self.retry(self.kazoo.create, self.removed_path, "")
        except NodeExistsException:
            pass

    def _get_config_and_version(self, section, keys=None):
        domain_config, stat = self.retry(self.kazoo.get, self.path)

        domain_config = json.loads(domain_config)
        version = stat.version

        section_config = domain_config.get(section)
        if section_config is None:
            return {}, version

        if keys is not None:
            filtered = dict((k, section_config[k]) for k in keys
                if k in section_config)
            return filtered, version
        return section_config, version

    def _add_config(self, section, conf):

        updated = False
        while not updated:
            domain_config, stat = self.retry(self.kazoo.get, self.path)
            domain_config = json.loads(domain_config)
            section_conf = domain_config.get(section)
            if section_conf is None:
                domain_config[section] = conf
            else:
                section_conf.update(conf)

            data = json.dumps(domain_config)
            try:
                self.retry(self.kazoo.set, self.path, data, stat.version)
                updated = True
            except BadVersionException:
                pass

    def get_engine_config(self, keys=None):
        """Retrieve the engine config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """
        conf, _ = self._get_config_and_version(EPUM_CONF_ENGINE, keys)
        return conf

    def get_versioned_engine_config(self):
        """Retrieve the engine config dictionary and a version

        Returns a (config, version) tuple. The version is used to tell when
        a new config is available and an engine reconfigure is needed.
        """
        return self._get_config_and_version(EPUM_CONF_ENGINE)

    def add_engine_config(self, conf):
        """Store a dictionary of new engine conf values.

        These are folded into the existing configuration map. So for example
        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
        the result from get_config() will be {'a' : 1, 'b' : 2}.

        @param conf dictionary mapping strings to JSON-serializable objects
        """
        self._add_config(EPUM_CONF_ENGINE, conf)

    def get_domain_sensor_data(self):
        """Retrieve a dictionary of sensor data from the store
        """
        path = self.domain_sensor_path
        try:
            sensor_data = self.retry(self.kazoo.get, path)
        except NoNodeException:
            sensor_data = {}
        return sensor_data

    def add_domain_sensor_data(self, sensor_data):
        """Store a dictionary of domain sensor data.

        This operation replaces previous sensor data

        data is in the format:
        {
          'metric':{
            'Average': 5
          }
        }

        @param sensor_data dictionary mapping strings to JSON-serializable objects

        """

        try:
            sensor_json = json.dumps(sensor_data)
        except Exception:
            log.exception("Could not convert sensor data to JSON")
            return

        path = self.domain_sensor_path
        version = -1

        try:
            self.retry(self.kazoo.get, path)
        except NoNodeException:
            try:
                self.retry(self.kazoo.create, path, sensor_json, makepath=True)
            except BadVersionException:
                raise WriteConflictError()
            except NoNodeException:
                raise NotFoundError()
        else:
            try:
                self.retry(self.kazoo.set, path, sensor_json, version)
            except BadVersionException:
                raise WriteConflictError()
            except NoNodeException:
                raise NotFoundError()

    def get_health_config(self, keys=None):
        """Retrieve the health config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """
        conf, _ = self._get_config_and_version(EPUM_CONF_HEALTH, keys)
        return conf

    def add_health_config(self, conf):
        """Store a dictionary of new health conf values.

        These are folded into the existing configuration map. So for example
        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
        the result from get_health_config() will be {'a' : 1, 'b' : 2}.

        @param conf dictionary mapping strings to JSON-serializable objects
        """
        self._add_config(EPUM_CONF_HEALTH, conf)

    def get_general_config(self, keys=None):
        """Retrieve the general config dictionary.

        @param keys optional list of keys to retrieve
        @retval config dictionary object
        """
        conf, _ = self._get_config_and_version(EPUM_CONF_GENERAL, keys)
        return conf

    def add_general_config(self, conf):
        """Store a dictionary of new general conf values.

        These are folded into the existing configuration map. So for example
        if you first store {'a' : 1, 'b' : 1} and then store {'b' : 2},
        the result from get_general_config() will be {'a' : 1, 'b' : 2}.

        @param conf dictionary mapping strings to JSON-serializable objects
        """
        self._add_config(EPUM_CONF_GENERAL, conf)

    def get_subscribers(self):
        """Retrieve a list of current subscribers
        """
        try:
            subscribers_json, _ = self.retry(self.kazoo.get, self.subscribers_path)
        except NoNodeException:
            return []

        subscribers = json.loads(subscribers_json)
        # discard invalid subscribers
        return [(s[0], s[1]) for s in subscribers if len(s) == 2]

    def add_subscriber(self, name, op):
        """Add a new subscriber to instance state changes for this domain
        """
        # explicit returns seems the cleanest for this one
        while True:
            try:
                subscribers_json, stat = self.retry(self.kazoo.get, self.subscribers_path)
            except NoNodeException:

                # there are no subscribers so far. create the ZNode, while
                # allowing for a race with someone else doing the same.

                subscribers = [[name, op]]
                subscribers_json = json.dumps(subscribers)

                try:
                    self.retry(self.kazoo.create, self.subscribers_path, subscribers_json)

                    # **** EXPLICIT RETURN ****
                    return

                except NodeExistsException:
                    # someone created in the meantime. loop back and start over
                    continue

            # A subscriber set exists, add ours if it isn't present
            subscribers = json.loads(subscribers_json)
            found = False
            for subscriber in subscribers:
                if len(subscriber) != 2:
                    continue
                if subscriber[0] == name:
                    subscriber[1] = op
                    found = True
            if not found:
                subscribers.append([name, op])

            subscribers_json = json.dumps(subscribers)
            try:
                self.retry(self.kazoo.set, self.subscribers_path, subscribers_json,
                    stat.version)
                # **** EXPLICIT RETURN ****
                return

            except BadVersionException:
                # someone else has updated in the meantime, go try again
                continue
            except NoNodeException:
                # someone else has deleted in the meantime, go try again
                continue

    def remove_subscriber(self, name):
        """Remove a subscriber of instance state changes for this domain
        """
        while True:
            try:
                subscribers_json, stat = self.retry(self.kazoo.get, self.subscribers_path)
            except NoNodeException:
                # **** EXPLICIT RETURN ****
                return

            subscribers = json.loads(subscribers_json)
            new_subscribers = []
            for subscriber in subscribers:
                if len(subscriber) != 2:
                    continue
                if subscriber[0] != name:
                    new_subscribers.append(subscriber)

            subscribers_json = json.dumps(subscribers)
            try:
                self.retry(self.kazoo.set, self.subscribers_path, subscribers_json,
                    stat.version)

                # **** EXPLICIT RETURN ****
                return

            except BadVersionException:
                # someone else has updated in the meantime, go try again
                continue
            except NoNodeException:
                return

    def _get_instance_path(self, instance_id):
        return self.instances_path + "/" + instance_id

    def _get_instance_heartbeat_path(self, instance_id):
        return (self._get_instance_path(instance_id) + "/" +
                self.INSTANCE_HEARTBEAT_PATH)

    def add_instance(self, instance):
        """Add a new instance record

        Raises a WriteConflictError if the instance already exists
        """
        instance_id = instance.get('instance_id')
        if not instance_id:
            raise ValueError("instance has no instance_id")

        instance_json = json.dumps(instance.to_dict())
        path = self._get_instance_path(instance_id)

        try:
            self.retry(self.kazoo.create, path, instance_json, makepath=True)
            instance.set_version(0)
        except NodeExistsException:
            raise WriteConflictError()

    def update_instance(self, instance, previous=None):
        """Update an existing instance record

        Raises a WriteConflictError if a previous record is specified and does
        not match what is in datastore

        Raise a NotFoundError if the instance is unknown
        """

        instance_id = instance.get('instance_id')
        if not instance_id:
            raise ValueError("instance has no instance_id")

        if previous:
            if previous._version is None:
                raise ValueError("previous record has no version")
            version = previous._version
            if previous.get('instance_id') != instance_id:
                raise ValueError("previous record has different instance ID")
        else:
            version = -1

        instance_json = json.dumps(instance.to_dict())
        path = self._get_instance_path(instance_id)

        try:
            self.retry(self.kazoo.set, path, instance_json, version)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

    def get_instance(self, instance_id):
        """Retrieve an instance record

        Returns the instance record, or None if not found
        """
        path = self._get_instance_path(instance_id)
        try:
            instance_json, stat = self.retry(self.kazoo.get, path)
        except NoNodeException:
            return None

        instance_dict = json.loads(instance_json)

        instance = CoreInstance.from_dict(instance_dict)
        instance.set_version(stat.version)

        return instance

    def remove_instance(self, instance_id):
        """Remove an instance record

        Raise a NotFoundError if the instance is unknown
        """

        path = self._get_instance_path(instance_id)
        try:
            instance_json, stat = self.retry(self.kazoo.get, path)
        except NoNodeException:
            raise NotFoundError()

        self.retry(self.kazoo.delete, path)

    def set_instance_heartbeat_time(self, instance_id, time):
        """Store a new instance heartbeat
        """
        while True:
            path = self._get_instance_heartbeat_path(instance_id)
            time_json = json.dumps(time)
            try:
                beat_time_json, stat = self.retry(self.kazoo.get, path)
            except NoNodeException:

                # there is no heartbeat node yet

                try:
                    self.retry(self.kazoo.create, path, time_json)

                    # **** EXPLICIT RETURN ****
                    return

                except NodeExistsException:
                    # someone created it in the meantime. start over.
                    continue
                except NoNodeException:
                    # the instance record itself doesn't exist! error out
                    raise NotFoundError()

            beat_time = json.loads(beat_time_json)

            # only update if the last beat time is older
            if beat_time < time:
                try:
                    self.retry(self.kazoo.set, path, time_json, stat.version)

                    # **** EXPLICIT RETURN ****
                    return

                except NoNodeException:
                    # someone deleted in the meantime. start over
                    continue
                except BadVersionException:
                    # someone updated in the meantime. start over
                    continue

    def get_instance_heartbeat_time(self, instance_id):
        """Retrieve the timestamp of the last heartbeat from this instance

        Returns the heartbeat time, or None if not found
        """
        path = self._get_instance_heartbeat_path(instance_id)
        try:
            beat_time_json = self.retry(self.kazoo.get, path)
        except NoNodeException:
            return None

        return json.loads(beat_time_json)

    def get_instances(self):
        """Retrieve a list of instance records
        """
        result = []
        for instance_id in self.get_instance_ids():
            instance = self.get_instance(instance_id)
            if instance:
                result.append(instance)
        return result

    def get_instance_ids(self):
        """Retrieve a list of known instance IDs
        """
        try:
            return self.retry(self.kazoo.get_children, self.instances_path)
        except NoNodeException:
            return []

    def get_engine_state(self):
        """Get an object to provide to engine decide() and reset pending state

        Beware that the object provided may be changed and reused by the
        next invocation of this method.
        """
        s = self.engine_state
        # TODO not yet dealing with sensors or change lists
        s.instances = dict((i.instance_id, i) for i in self.get_instances())
        return s


class ZooKeeperDomainDefinitionStore(DomainDefinitionStore):

    def __init__(self, definition_id, kazoo, retry, path):
        super(ZooKeeperDomainDefinitionStore, self).__init__(definition_id)

        self.kazoo = kazoo
        self.retry = retry
        self.path = path

        definition, stat = self.retry(self.kazoo.get, self.path)
        definition = json.loads(definition)
        self.definition = definition

    def get_definition(self):
        return self.definition


_INVALID_NAMES = ("..", ".", "zookeeper")


def validate_entity_name(name):
    """validation for owner and domain_id strings
    """
    if (not name or re.match('[^a-zA-Z0-9_\-.@]', name)
            or name in _INVALID_NAMES):
        raise ValueError("invalid name: %s" % name)
