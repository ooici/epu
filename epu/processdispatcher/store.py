# Copyright 2013 University of Chicago

from functools import partial
import simplejson as json
import logging
import time
import re
import threading
import copy

from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NodeExistsException, BadVersionException, \
    NoNodeException

import epu.tevent as tevent
from epu.exceptions import NotFoundError, WriteConflictError
from epu import zkutil
from epu.states import ProcessDispatcherState, ExecutionResourceState
from epu.util import parse_datetime

log = logging.getLogger(__name__)


def get_processdispatcher_store(config, use_gevent=False):
    """Instantiate PD store object for the given configuration
    """
    if zkutil.is_zookeeper_enabled(config):
        zookeeper = zkutil.get_zookeeper_config(config)

        log.info("Using ZooKeeper ProcessDispatcher store")
        store = ProcessDispatcherZooKeeperStore(zookeeper['hosts'],
                                                zookeeper['path'],
                                                zookeeper.get('timeout'),
                                                use_gevent=use_gevent)

    else:
        log.info("Using in-memory ProcessDispatcher store")
        store = ProcessDispatcherStore()

    return store


class ProcessDispatcherStore(object):
    """
    This store is responsible for persistence of several types of records.
    It also supports providing certain notifications about changes to stored
    records.

    This is an in-memory only version.
    """

    def __init__(self, system_boot=False):
        self.lock = threading.RLock()

        self._is_initialized = threading.Event()
        self._pd_state = None
        self._pd_state_watches = []
        self._is_system_boot = bool(system_boot)
        self._system_boot_watches = []

        self.definitions = {}

        self.processes = {}
        self.process_watches = {}

        self.queued_processes = []
        self.queued_process_set_watches = []

        self.resources = {}
        self.resource_set_watches = []
        self.resource_watches = {}

        self.nodes = {}
        self.node_set_watches = []
        self.node_watches = {}

        self._matchmaker = None
        self._matchmaker_thread = None
        self.is_matchmaker = False

        self._doctor = None
        self._doctor_thread = None
        self.is_doctor = False

    def initialize(self):
        pass

    def contend_matchmaker(self, matchmaker):
        """Provide a matchmaker object to participate in an election
        """
        assert self._matchmaker is None
        self._matchmaker = matchmaker

        # since this is in-memory store, we are the only possible matchmaker
        self._make_matchmaker()

    def _make_matchmaker(self):
        assert self._matchmaker
        assert not self.is_matchmaker
        self.is_matchmaker = True

        self._matchmaker_thread = tevent.spawn(self._matchmaker.inaugurate)

    def contend_doctor(self, doctor):
        """Provide a doctor object to participate in an election
        """
        assert self._doctor is None
        self._doctor = doctor

        # since this is in-memory store, we are the only possible doctor
        self._make_doctor()

    def _make_doctor(self):
        assert self._doctor
        assert not self.is_doctor
        self.is_doctor = True

        self._doctor_thread = tevent.spawn(self._doctor.inaugurate)

    def shutdown(self):
        # In-memory store, only stop the leaders
        try:
            if self._matchmaker:
                self._matchmaker.cancel()
        except Exception, e:
            log.exception("Error cancelling matchmaker: %s", e)
        try:
            if self._doctor:
                self._doctor.cancel()
        except Exception, e:
            log.exception("Error cancelling matchmaker: %s", e)
        if self._doctor_thread:
            self._doctor_thread.join()
        if self._matchmaker_thread:
            self._matchmaker_thread.join()

        self._doctor = None
        self.is_doctor = False
        self._matchmaker = None
        self.is_matchmaker = False

        self._is_initialized.clear()

    #########################################################################
    # PROCESS DISPATCHER STATE
    #########################################################################

    def set_system_boot(self, system_boot):
        """
        called by launch plan with system_boot=True at start of launch
        called by launch plan with system_boot=False at end of launch
        """
        with self.lock:
            system_boot = bool(system_boot)
            self._is_system_boot = system_boot
            if self._system_boot_watches:
                for watcher in self._system_boot_watches:
                    watcher()
                self._system_boot_watches[:] = []

    def is_system_boot(self, watcher=None):
        """
        called by doctor during init to decide what PD state to move to
        """
        with self.lock:
            if watcher is not None:
                if not callable(watcher):
                    raise ValueError("watcher is not callable")
                self._system_boot_watches.append(watcher)
            return self._is_system_boot

    def wait_initialized(self, timeout=None):
        """Wait for the Process Dispatcher to be initialized by the doctor
        """
        return self._is_initialized.wait(timeout)

    def set_initialized(self):
        """called by doctor after initialization is complete
        """
        with self.lock:
            assert not self._is_initialized.is_set()
            self._is_initialized.set()

    def get_pd_state(self):
        """Get the current state of the Process Dispatcher. One of ProcessDispatcherState
        """
        with self.lock:
            if not self._is_initialized.is_set():
                return ProcessDispatcherState.UNINITIALIZED
            return self._pd_state

    def set_pd_state(self, state):
        """called by doctor to change PD state after init
        """
        if state not in ProcessDispatcherState.VALID_STATES:
            raise ValueError("bad state '%s'" % state)
        with self.lock:
            self._pd_state = state

    #########################################################################
    # PROCESS DEFINITIONS
    #########################################################################

    def add_definition(self, definition):
        """Adds a new process definition

        Raises WriteConflictError if the definition already exists
        """
        log.debug("creating definition %s" % definition)
        with self.lock:
            definition_id = definition.definition_id
            if definition_id in self.definitions:
                raise WriteConflictError("definition %s already exists" %
                                         str(definition_id))

            data = json.dumps(definition)
            self.definitions[definition_id] = data

    def get_definition(self, definition_id):
        """Retrieve definition record or None if not found
        """
        found = self.definitions.get(definition_id)
        if found is None:
            return None

        raw_dict = json.loads(found)
        definition = ProcessDefinitionRecord(raw_dict)

        return definition

    def update_definition(self, definition):
        """Update existing definition

        A NotFoundError is raised if the definition doesn't exist
        """
        with self.lock:
            definition_id = definition.definition_id

            found = self.get_definition(definition_id)
            if found is None:
                raise NotFoundError()

            data = json.dumps(definition)
            self.definitions[definition_id] = data

    def remove_definition(self, definition_id):
        """Remove definition record

        A NotFoundError is raised if the definition doesn't exist
        """
        with self.lock:
            if definition_id not in self.definitions:
                raise NotFoundError()
            del self.definitions[definition_id]

    def list_definition_ids(self):
        """Retrieve list of known definition IDs
        """
        return self.definitions.keys()

    #########################################################################
    # PROCESSES
    #########################################################################

    def add_process(self, process):
        """Adds a new process record to the store

        If the process record already exists, a WriteConflictError exception
        is raised.
        """
        with self.lock:
            key = (process.owner, process.upid)
            if key in self.processes:
                raise WriteConflictError("process %s already exists" % str(key))

            data = json.dumps(process)
            self.processes[key] = data, 0
            process.metadata['version'] = 0

    def update_process(self, process, force=False):
        """Updates an existing process record

        Process records are versioned and if the version in the store does not
        match the version of the process record being updated, the write will
        fail with a WriteConflictError. If the force flag is True, the write
        will occur regardless of version.

        If the process does not exist in the store, a NotFoundError will be
        raised.

        @param process: process record
        @return:
        """
        with self.lock:
            key = (process.owner, process.upid)

            found = self.processes.get(key)
            if found is None:
                raise NotFoundError()

            version = process.metadata.get('version')
            if version is None and not force:
                raise ValueError("process has no version and force=False")

            if version != found[1]:
                raise WriteConflictError("version mismatch. " +
                                         "current=%s, attempted to write %s" %
                                         (version, found[1]))

            # pushing to JSON to prevent side effects of shared objects
            data = json.dumps(process)
            self.processes[key] = data, version + 1
            process.metadata['version'] = version + 1

            self._fire_process_watchers(process.owner, process.upid)

    def get_process(self, owner, upid, watcher=None):
        """Retrieve process record
        """
        with self.lock:
            key = (owner, upid)

            found = self.processes.get(key)
            if found is None:
                return None

            rawdict = json.loads(found[0])
            process = ProcessRecord(rawdict)
            process.metadata['version'] = found[1]

            if watcher:
                if not callable(watcher):
                    raise ValueError("watcher is not callable")

                watches = self.process_watches.get(key)
                if watches is None:
                    self.process_watches[key] = [watcher]
                else:
                    watches.append(watcher)

            return process

    def remove_process(self, owner, upid):
        """Remove process record from store
        """
        with self.lock:
            key = (owner, upid)
            if key not in self.processes:
                raise NotFoundError()
            del self.processes[key]

    def get_process_ids(self):
        """Retrieve available process IDs
        """
        with self.lock:
            return self.processes.keys()

    def _fire_process_watchers(self, owner, upid):
        # expected to be called under lock
        watchers = self.process_watches.get((owner, upid))
        if watchers:
            for watcher in watchers:
                watcher(owner, upid)
            watchers[:] = []

    #########################################################################
    # QUEUED PROCESSES
    #########################################################################

    def enqueue_process(self, owner, upid, round):
        """Mark a process as runnable, to be inspected by the matchmaker

        @param owner:
        @param upid:
        @param round:
        @return:
        """
        with self.lock:
            key = (owner, upid, round)
            self.queued_processes.append(key)
            self._fire_queued_process_set_watchers()

    def _fire_queued_process_set_watchers(self):
    # expected to be called under lock
        if self.queued_process_set_watches:
            for watcher in self.queued_process_set_watches:
                watcher()
            self.queued_process_set_watches[:] = []

    def get_queued_processes(self, watcher=None):
        """Get the queued processes and optionally set a watcher for changes

        @param watcher: callable to be called ONCE when the queued process set changes
        @return list of (owner, upid, round) tuples
        """
        with self.lock:
            if watcher:
                if not callable(watcher):
                    raise ValueError("watcher is not callable")

                self.queued_process_set_watches.append(watcher)
            return list(self.queued_processes)

    def remove_queued_process(self, owner, upid, round):
        """Remove a process from the runnable queue
        """
        with self.lock:
            key = (owner, upid, round)
            try:
                self.queued_processes.remove(key)
            except ValueError:
                raise NotFoundError("queued process not found: %s" % (key,))

            self._fire_queued_process_set_watchers()

    def clear_queued_processes(self):
        """Reset the process queue
        """
        with self.lock:
            self.queued_processes[:] = []
            self._fire_queued_process_set_watchers()

    #########################################################################
    # NODES
    #########################################################################

    def _fire_node_set_watchers(self):
        # expected to be called under lock
            if self.node_set_watches:
                for watcher in self.node_set_watches:
                    watcher()
                self.node_set_watches[:] = []

    def _fire_node_watchers(self, node_id):
        # expected to be called under lock
        watchers = self.node_watches.get(node_id)
        if watchers:
            for watcher in watchers:
                watcher(node_id)
            watchers[:] = []

    def add_node(self, node):
        """Add a new node record
        """
        with self.lock:
            node_id = node.node_id
            if node_id in self.nodes:
                raise WriteConflictError("node %s already exists" % node_id)

            data = json.dumps(node)
            self.nodes[node_id] = data, 0
            node.metadata['version'] = 0

            self._fire_node_set_watchers()

    def update_node(self, node, force=False):
        """Update a node record
        """
        with self.lock:
            node_id = node.node_id

            found = self.nodes.get(node_id)
            if found is None:
                raise NotFoundError()

            version = node.metadata.get('version')
            if version is None and not force:
                raise ValueError("node has no version and force=False")

            if version != found[1]:
                raise WriteConflictError("version mismatch. " +
                                         "current=%s, attempted to write %s" %
                                         (version, found[1]))

            # pushing to JSON to prevent side effects of shared objects
            data = json.dumps(node)
            self.nodes[node_id] = data, version + 1
            node.metadata['version'] = version + 1

            self._fire_node_watchers(node_id)

    def get_node(self, node_id, watcher=None):
        """Retrieve a node record
        """
        with self.lock:
            found = self.nodes.get(node_id)
            if found is None:
                return None

            rawdict = json.loads(found[0])
            node = NodeRecord(rawdict)
            node.metadata['version'] = found[1]

            if watcher:
                if not callable(watcher):
                    raise ValueError("watcher is not callable")

                watches = self.node_watches.get(node_id)
                if watches is None:
                    self.node_watches[node_id] = [watcher]
                else:
                    watches.append(watcher)

            return node

    def remove_node(self, node_id):
        """Remove a node record
        """
        with self.lock:
            if node_id not in self.nodes:
                raise NotFoundError()
            del self.nodes[node_id]

            self._fire_node_set_watchers()

    def get_node_ids(self, watcher=None):
        """Retrieve available node IDs and optionally watch for changes
        """
        with self.lock:
            if watcher:
                if not callable(watcher):
                    raise ValueError("watcher is not callable")

                self.node_set_watches.append(watcher)
            return self.nodes.keys()

    #########################################################################
    # EXECUTION RESOURCES
    #########################################################################

    def _fire_resource_set_watchers(self):
        # expected to be called under lock
        if self.resource_set_watches:
            for watcher in self.resource_set_watches:
                watcher()
            self.resource_set_watches[:] = []

    def _fire_resource_watchers(self, resource_id):
        # expected to be called under lock
        watchers = self.resource_watches.get(resource_id)
        if watchers:
            for watcher in watchers:
                watcher(resource_id)
            watchers[:] = []

    def add_resource(self, resource):
        """Add an execution resource record to the store
        """
        with self.lock:
            resource_id = resource.resource_id
            if resource_id in self.resources:
                raise WriteConflictError("resource %s already exists" % resource_id)

            data = json.dumps(resource)
            self.resources[resource_id] = data, 0
            resource.metadata['version'] = 0

            self._fire_resource_set_watchers()

    def update_resource(self, resource, force=False):
        """Update an existing resource record
        """
        with self.lock:
            resource_id = resource.resource_id

            found = self.resources.get(resource_id)
            if found is None:
                raise NotFoundError()

            version = resource.metadata.get('version')
            if version is None and not force:
                raise ValueError("resource has no version and force=False")

            if version != found[1]:
                raise WriteConflictError("version mismatch. " +
                                         "current=%s, attempted to write %s" %
                                         (version, found[1]))

            # pushing to JSON to prevent side effects of shared objects
            data = json.dumps(resource)
            self.resources[resource_id] = data, version + 1
            resource.metadata['version'] = version + 1

            self._fire_resource_watchers(resource_id)

    def get_resource(self, resource_id, watcher=None):
        """Retrieve a resource record
        """
        with self.lock:
            found = self.resources.get(resource_id)
            if found is None:
                return None

            rawdict = json.loads(found[0])
            resource = ResourceRecord(rawdict)
            resource.metadata['version'] = found[1]

            if watcher:
                if not callable(watcher):
                    raise ValueError("watcher is not callable")

                watches = self.resource_watches.get(resource_id)
                if watches is None:
                    self.resource_watches[resource_id] = [watcher]
                else:
                    watches.append(watcher)

            return resource

    def remove_resource(self, resource_id):
        """Remove a resource from the store
        """
        with self.lock:

            if resource_id not in self.resources:
                raise NotFoundError()
            del self.resources[resource_id]

            self._fire_resource_set_watchers()

    def get_resource_ids(self, watcher=None):
        """Retrieve available resource IDs and optionally watch for changes
        """
        with self.lock:
            if watcher:
                if not callable(watcher):
                    raise ValueError("watcher is not callable")

                self.resource_set_watches.append(watcher)
            return self.resources.keys()


class ProcessDispatcherZooKeeperStore(object):
    """
    This store is responsible for persistence of several types of records.
    It also supports providing certain notifications about changes to stored
    records.

    This is a ZooKeeper-backed version.
    """

    # the existence of this path indicates whether the system is currently
    # booting
    SYSTEM_BOOT_PATH = "/system_boot"

    # this path hosts an ephemeral node created by the doctor after it inits
    INITIALIZED_PATH = "/initialized"

    # contains the current Process Dispatcher state
    PD_STATE_PATH = "/pd_state"

    PARTY_PATH = "/party"

    NODES_PATH = "/nodes"

    PROCESSES_PATH = "/processes"

    DEFINITIONS_PATH = "/definitions"

    QUEUED_PROCESSES_PATH = "/requested"

    RESOURCES_PATH = "/resources"

    # these paths is used for leader election. PD workers line up for
    # an exclusive lock on leadership.
    MATCHMAKER_ELECTION_PATH = "/elections/matchmaker"
    DOCTOR_ELECTION_PATH = "/elections/doctor"

    def __init__(self, hosts, base_path, username=None, password=None,
                 timeout=None, use_gevent=False):

        kwargs = zkutil.get_kazoo_kwargs(username=username, password=password,
                                         timeout=timeout, use_gevent=use_gevent)
        self.kazoo = KazooClient(hosts + base_path, **kwargs)
        self.retry = zkutil.get_kazoo_retry()
        self.matchmaker_election = self.kazoo.Election(self.MATCHMAKER_ELECTION_PATH)
        self.doctor_election = self.kazoo.Election(self.DOCTOR_ELECTION_PATH)

        # callback fired when the connection state changes
        self.kazoo.add_listener(self._connection_state_listener)

        self._is_initialized = threading.Event()
        self._party = self.kazoo.Party(self.PARTY_PATH)

        self._shutdown = False
        self._election_enabled = False
        self._election_condition = threading.Condition()
        self._matchmaker_election_thread = None
        self._doctor_election_thread = None

        self._matchmaker = None
        self._doctor = None

    def initialize(self):
        self._shutdown = False
        self.kazoo.start()

        for path in (self.NODES_PATH, self.PROCESSES_PATH,
                     self.DEFINITIONS_PATH, self.QUEUED_PROCESSES_PATH,
                     self.RESOURCES_PATH, self.MATCHMAKER_ELECTION_PATH,
                     self.DOCTOR_ELECTION_PATH, self.PARTY_PATH):
            self.retry(self.kazoo.ensure_path, path)

        # the Process Dispatcher is in the UNINITIALIZED state until
        # one or both of the following conditions is true:
        # 1. The /initialized flag is set (ephemeral node exists). This
        #    flag is set once the Doctor decides that the system is healthy.
        # 2. There is at least one participant in the party. Workers join
        #    the party when they see #1 above come true. However they can
        #    remain in the party even after #1 ceases to be true, such as
        #    if/when the doctor dies.
        #
        # The net effect is that once the initialized flag is set and all
        # processes join the party, we will never become UNINITIALIZED again
        # as long as at least one worker is alive, even if the doctor dies
        # and takes the initialized flag with it. However, if in fact all
        # workers die, when they resume the Doctor gets a chance to inspect
        # and repair the system state before any work is done.

        if len(self._party) > 0:
            self._is_initialized.set()
            self._party.join()
        else:
            self.kazoo.DataWatch(
                self.INITIALIZED_PATH, self._initialized_watcher,
                allow_missing_node=True)

    def _initialized_watcher(self, data, stat):
        if not (data is None and stat is None):
            # initialized node exists! set our event and join the party.
            self._is_initialized.set()
            self._party.join()

            # return False to indicate that we don't want any more callbacks
            return False

    def _connection_state_listener(self, state):
        # called by kazoo when the connection state changes.
        # handle in background
        tevent.spawn(self._handle_connection_state, state)

    def _handle_connection_state(self, state):

        if state in (KazooState.LOST, KazooState.SUSPENDED):
            log.debug("disabling elections and leaders")
            with self._election_condition:
                self._election_enabled = False
                self._election_condition.notify_all()

            # depose the leaders and cancel the elections just in case
            try:
                if self._matchmaker:
                    self._matchmaker.cancel()
            except Exception, e:
                log.exception("Error deposing matchmaker: %s", e)
            try:
                if self._doctor:
                    self._doctor.cancel()
            except Exception, e:
                log.exception("Error deposing doctor: %s", e)
            self.matchmaker_election.cancel()
            self.doctor_election.cancel()

        elif state == KazooState.CONNECTED:
            log.debug("enabling elections")
            with self._election_condition:
                self._election_enabled = True
                self._election_condition.notify_all()

    def contend_matchmaker(self, matchmaker):
        """Provide a matchmaker object to participate in an election
        """
        assert self._matchmaker is None
        self._matchmaker = matchmaker
        self._matchmaker_election_thread = tevent.spawn(self._run_election,
            self.matchmaker_election, matchmaker, "matchmaker")

    def contend_doctor(self, doctor):
        """Provide a doctor object to participate in an election
        """
        assert self._doctor is None
        self._doctor = doctor
        self._doctor_election_thread = tevent.spawn(self._run_election,
            self.doctor_election, doctor, "doctor")

    def _run_election(self, election, leader, name):
        """Election thread function
        """
        while True:
            with self._election_condition:
                while not self._election_enabled:
                    if self._shutdown:
                        return
                    log.debug("%s election waiting for to be enabled", name)
                    self._election_condition.wait()
                if self._shutdown:
                    return
            try:
                election.run(leader.inaugurate)
            except Exception, e:
                log.exception("Error in %s election: %s", name, e)
            except:
                log.exception("Unhandled error in election")
                raise

    def shutdown(self):
        with self._election_condition:
            self._shutdown = True
            self._election_enabled = False
            self._election_condition.notify_all()
        try:
            if self._matchmaker:
                self._matchmaker.cancel()
        except Exception, e:
            log.exception("Error deposing matchmaker: %s", e)
        try:
            if self._doctor:
                self._doctor.cancel()
        except Exception, e:
            log.exception("Error deposing doctor: %s", e)
        self.matchmaker_election.cancel()
        self.doctor_election.cancel()
        if self._matchmaker_election_thread:
            self._matchmaker_election_thread.join()
        if self._doctor_election_thread:
            self._doctor_election_thread.join()
        self._doctor = None
        self._matchmaker = None

        self.kazoo.stop()
        try:
            self.kazoo.close()
        except Exception:
            log.exception("Problem cleaning up kazoo")
        self._is_initialized.clear()

    #########################################################################
    # PROCESS DISPATCHER STATE
    #########################################################################

    def set_system_boot(self, system_boot):
        """
        called by launch plan with system_boot=True at start of launch
        called by launch plan with system_boot=False at end of launch
        """
        if system_boot is not False and system_boot is not True:
            raise ValueError("expected a boolean value. got %s" % system_boot)
        if system_boot:
            try:
                self.retry(self.kazoo.create, self.SYSTEM_BOOT_PATH, "")
            except NodeExistsException:
                pass  # ok if node already exists
        else:
            try:
                self.retry(self.kazoo.delete, self.SYSTEM_BOOT_PATH)
            except NoNodeException:
                pass  # ok if node doesn't exist

    def is_system_boot(self, watcher=None):
        """
        called by doctor during init to decide what PD state to move to
        """
        if watcher:
            if not callable(watcher):
                raise ValueError("watcher is not callable")
        return self.retry(self.kazoo.exists, self.SYSTEM_BOOT_PATH, watch=watcher) is not None

    def wait_initialized(self, timeout=None):
        """Wait for the Process Dispatcher to be initialized by the doctor
        """
        return self._is_initialized.wait(timeout)

    def set_initialized(self):
        """called by doctor after initialization is complete
        """
        try:
            self.retry(self.kazoo.create, self.INITIALIZED_PATH, "", ephemeral=True)
        except NodeExistsException:
            # ok if the node already exists
            pass

        # shortcut event setting so doctor doesn't have to wait for watch
        self._is_initialized.set()

    def get_pd_state(self):
        """Get the current state of the Process Dispatcher. One of ProcessDispatcherState
        """
        if not self._is_initialized.is_set():
            return ProcessDispatcherState.UNINITIALIZED
        data, _ = self.retry(self.kazoo.get, self.PD_STATE_PATH)
        data = str(data)
        if data in ProcessDispatcherState.VALID_STATES:
            return data
        else:
            log.error("Process dispatcher is in invalid state: %s!", data)
            return None

    def set_pd_state(self, state):
        """called by doctor to change PD state after init
        """
        state = str(state)
        if state not in ProcessDispatcherState.VALID_STATES:
            raise ValueError("Invalid state: %s" % state)

        try:
            self.retry(self.kazoo.create, self.PD_STATE_PATH, state)
        except NodeExistsException:
            self.retry(self.kazoo.set, self.PD_STATE_PATH, state, version=-1)

    #########################################################################
    # PROCESS DEFINITIONS
    #########################################################################

    def _make_definition_path(self, definition_id):
        if definition_id is None:
            raise ValueError('invalid definition id')

        return self.DEFINITIONS_PATH + "/" + definition_id

    def add_definition(self, definition):
        """Adds a new process definition

        Raises WriteConflictError if the definition already exists
        """
        definition_id = definition.definition_id
        data = json.dumps(definition)
        zkutil.check_data(data)

        try:
            self.retry(self.kazoo.create, self._make_definition_path(definition_id), data)
        except NodeExistsException:
            raise WriteConflictError("definition %s already exists" % definition_id)

    def get_definition(self, definition_id):
        """Retrieve definition record or None if not found
        """

        try:
            data, stat = self.retry(self.kazoo.get,
                self._make_definition_path(definition_id))
        except NoNodeException:
            return None

        rawdict = json.loads(data)
        return ProcessDefinitionRecord(rawdict)

    def update_definition(self, definition):
        """Update existing definition

        A NotFoundError is raised if the definition doesn't exist
        """
        definition_id = definition.definition_id
        data = json.dumps(definition)
        zkutil.check_data(data)
        try:
            self.retry(self.kazoo.set,
                self._make_definition_path(definition_id), data, -1)
        except NoNodeException:
            raise NotFoundError()

    def remove_definition(self, definition_id):
        """Remove definition record

        A NotFoundError is raised if the definition doesn't exist
        """
        try:
            self.retry(self.kazoo.delete,
                self._make_definition_path(definition_id))
        except NoNodeException:
            raise NotFoundError()

    def list_definition_ids(self):
        """Retrieve list of known definition IDs
        """
        definition_ids = self.retry(self.kazoo.get_children,
            self.DEFINITIONS_PATH)
        return definition_ids

    #########################################################################
    # PROCESSES
    #########################################################################

    def _make_process_path(self, owner=None, upid=None):
        if upid is None:
            raise ValueError('invalid process upid')

        path = self.PROCESSES_PATH + "/"
        if owner is not None:
            path += "owner=" + owner + "&"
        path += "upid=" + upid

        return path

    def add_process(self, process):
        """Adds a new process record to the store

        If the process record already exists, a WriteConflictError exception
        is raised.
        """
        data = json.dumps(process)
        zkutil.check_data(data)

        try:
            self.retry(self.kazoo.create,
                self._make_process_path(owner=process.owner, upid=process.upid),
                data)
        except NodeExistsException:
            raise WriteConflictError("process %s for user %s already exists" % (process.upid, process.owner))

        process.metadata['version'] = 0

    def update_process(self, process, force=False):
        """Updates an existing process record

        Process records are versioned and if the version in the store does not
        match the version of the process record being updated, the write will
        fail with a WriteConflictError. If the force flag is True, the write
        will occur regardless of version.

        If the process does not exist in the store, a NotFoundError will be
        raised.

        @param process: process record
        @return:
        """
        path = self._make_process_path(owner=process.owner, upid=process.upid)
        data = json.dumps(process)
        zkutil.check_data(data)
        version = process.metadata.get('version')

        if version is None and not force:
            raise ValueError("process has no version and force=False")

        try:
            if force:
                set_version = -1
            else:
                set_version = version
            self.retry(self.kazoo.set, path, data, set_version)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

        process.metadata['version'] = version + 1

    def get_process(self, owner, upid, watcher=None):
        """Retrieve process record
        """
        path = self._make_process_path(owner=owner, upid=upid)

        if watcher:
            if not callable(watcher):
                raise ValueError("watcher is not callable")

        try:
            data, stat = self.retry(self.kazoo.get,
                path, watch=watcher)
        except NoNodeException:
            return None

        rawdict = json.loads(data)
        process = ProcessRecord(rawdict)
        process.metadata['version'] = stat.version

        return process

    def remove_process(self, owner, upid):
        """Remove process record from store
        """
        path = self._make_process_path(owner=owner, upid=upid)

        try:
            self.retry(self.kazoo.delete, path)
        except NoNodeException:
            raise NotFoundError()

    def get_process_ids(self):
        """Retrieve available node IDs
        """
        process_ids = []
        processes = self.kazoo.get_children(self.PROCESSES_PATH)

        for p in processes:
            owner = None

            match = re.match(r'^owner=(.+)&upid=(.+)$', p)
            if match is not None:
                owner = match.group(1)
                upid = match.group(2)
            else:
                match = re.match(r'^upid=(.+)$', p)
                if match is None:
                    raise ValueError("queued process %s could not be parsed" % p)
                upid = match.group(1)

            process_ids.append((owner, upid))

        return process_ids

    #########################################################################
    # QUEUED PROCESSES
    #########################################################################

    def _make_requested_path(self, owner=None, upid=None, round=None, seq=None):
        if upid is None:
            raise ValueError('invalid process upid')
        if round is None:
            raise ValueError('invalid process round')

        path = self.QUEUED_PROCESSES_PATH + "/"
        if owner is not None:
            path += "owner=" + owner + "&"

        path += "upid=" + upid + "&round=" + str(round) + "+"

        if seq is not None:
            path += str(seq)

        return path

    def enqueue_process(self, owner, upid, round):
        """Mark a process as runnable, to be inspected by the matchmaker

        @param owner:
        @param upid:
        @param round:
        @return:
        """
        try:
            path = self._make_requested_path(owner=owner, upid=upid, round=round)
            self.retry(self.kazoo.create, path, "", sequence=True)
        except NodeExistsException:
            raise WriteConflictError("process %s for user %s already in queue" % (upid, owner))

    def get_queued_processes(self, watcher=None):
        """Get the queued processes and optionally set a watcher for changes

        @param watcher: callable to be called ONCE when the queued process set changes
        @return list of (owner, upid, round) tuples
        """
        queued_processes = []

        if watcher:
            if not callable(watcher):
                raise ValueError("watcher is not callable")
        processes = self.retry(self.kazoo.get_children,
            self.QUEUED_PROCESSES_PATH, watch=watcher)

        for p in processes:
            owner = None
            match = re.match(r'^owner=(.+)&upid=(.+)&round=([0-9]+)\+([0-9]+)$', p)
            if match is not None:
                owner = match.group(1)
                upid = match.group(2)
                round = int(match.group(3))
                seq = match.group(4)
            else:
                match = re.match(r'^upid=(.+)&round=([0-9]+)\+([0-9]+)$', p)
                if match is None:
                    raise ValueError("queued process %s could not be parsed" % p)
                upid = match.group(1)
                round = int(match.group(2))
                seq = match.group(3)

            queued_processes.append((seq, (owner, upid, round)))

        return map(lambda x: x[1], sorted(queued_processes))

    def remove_queued_process(self, owner, upid, round):
        """Remove a process from the runnable queue
        """
        processes = self.retry(self.kazoo.get_children,
            self.QUEUED_PROCESSES_PATH)

        seq = None
        # Find a zknode that matches this process
        for p in processes:
            p_owner = None
            match = re.match(r'^owner=(.+)&upid=(.+)&round=([0-9]+)\+([0-9]+)$', p)
            if match is not None:
                p_owner = match.group(1)
                p_upid = match.group(2)
                p_round = int(match.group(3))
                p_seq = match.group(4)
            else:
                match = re.match(r'^upid=(.+)&round=([0-9]+)\+([0-9]+)$', p)
                if match is None:
                    raise ValueError("queued process %s could not be parsed" % p)
                p_upid = match.group(1)
                p_round = int(match.group(2))
                p_seq = match.group(3)
            if owner == p_owner and upid == p_upid and round == p_round:
                seq = p_seq
                break

        if seq is None:
            raise NotFoundError("queue process (%s, %s, %s) could not be found" % (owner, upid, str(round)))

        path = self._make_requested_path(owner=owner, upid=upid, round=round, seq=seq)
        try:
            self.retry(self.kazoo.delete, path)
        except NoNodeException:
            raise NotFoundError()

    def clear_queued_processes(self):
        """Reset the process queue
        """
        processes = self.retry(self.kazoo.get_children,
            self.QUEUED_PROCESSES_PATH)
        for process in processes:
            self.retry(self.kazoo.delete,
                "%s/%s" % (self.QUEUED_PROCESSES_PATH, process))

    #########################################################################
    # NODES
    #########################################################################

    def _make_node_path(self, node_id):
        if node_id is None:
            raise ValueError('invalid node_id')

        return self.NODES_PATH + "/" + node_id

    def add_node(self, node):
        """Add a new node record
        """
        node_id = node.node_id
        data = json.dumps(node)
        zkutil.check_data(data)

        try:
            self.retry(self.kazoo.create,
                self._make_node_path(node_id), data)
        except NodeExistsException:
            raise WriteConflictError("node %s already exists" % node_id)

        node.metadata['version'] = 0

    def update_node(self, node, force=False):
        """Update a node record
        """
        node_id = node.node_id
        path = self._make_node_path(node_id)
        data = json.dumps(node)
        zkutil.check_data(data)
        version = node.metadata.get('version')

        if version is None and not force:
            raise ValueError("node has no version and force=False")

        try:
            if force:
                set_version = -1
            else:
                set_version = version
            self.retry(self.kazoo.set, path, data, set_version)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

        node.metadata['version'] = version + 1

    def get_node(self, node_id, watcher=None):
        """Retrieve a node record
        """
        path = self._make_node_path(node_id)
        if watcher:
            if not callable(watcher):
                raise ValueError("watcher is not callable")

        try:
            data, stat = self.retry(self.kazoo.get, path, watch=watcher)
        except NoNodeException:
            return None

        rawdict = json.loads(data)
        node = NodeRecord(rawdict)
        node.metadata['version'] = stat.version

        return node

    def remove_node(self, node_id):
        """Remove a node record
        """
        path = self._make_node_path(node_id)

        try:
            self.retry(self.kazoo.delete, path)
        except NoNodeException:
            raise NotFoundError()

    def get_node_ids(self, watcher=None):
        """Retrieve available node IDs and optionally watch for changes
        """
        if watcher:
            if not callable(watcher):
                raise ValueError("watcher is not callable")

        node_ids = self.retry(self.kazoo.get_children, self.NODES_PATH)
        return node_ids

    #########################################################################
    # EXECUTION RESOURCES
    #########################################################################

    def _make_resource_path(self, resource_id):
        if resource_id is None:
            raise ValueError('invalid resource_id')

        return self.RESOURCES_PATH + "/" + resource_id

    def add_resource(self, resource):
        """Add an execution resource record to the store
        """
        resource_id = resource.resource_id
        data = json.dumps(resource)
        zkutil.check_data(data)

        try:
            self.retry(self.kazoo.create,
                self._make_resource_path(resource_id), data)
        except NodeExistsException:
                raise WriteConflictError("resource %s already exists" % resource_id)

        resource.metadata['version'] = 0

    def update_resource(self, resource, force=False):
        """Update an existing resource record
        """
        resource_id = resource.resource_id
        path = self._make_resource_path(resource_id)
        data = json.dumps(resource)
        zkutil.check_data(data)
        version = resource.metadata.get('version')

        if version is None and not force:
            raise ValueError("resource has no version and force=False")

        try:
            if force:
                set_version = -1
            else:
                set_version = version
            self.retry(self.kazoo.set, path, data, version=set_version)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

        resource.metadata['version'] = version + 1

    def watcher_wrapper(self, watched_event, watcher=None):
        # Extract resource name from the watched_event object
        match = re.match(r'^%s/(.*)$' % self.RESOURCES_PATH, watched_event.path)
        if match is not None:
            resource_id = match.group(1)
        else:
            raise AttributeError("could not parse watched_event %s" % str(watched_event))

        if watcher is not None:
            watcher(resource_id)

    def get_resource(self, resource_id, watcher=None):
        """Retrieve a resource record
        """
        path = self._make_resource_path(resource_id)

        if watcher:
            if not callable(watcher):
                raise ValueError("watcher is not callable")

        try:
            watch = partial(self.watcher_wrapper, watcher=watcher)
            data, stat = self.retry(self.kazoo.get, path, watch=watch)
        except NoNodeException:
            return None

        rawdict = json.loads(data)
        resource = ResourceRecord(rawdict)
        resource.metadata['version'] = stat.version

        return resource

    def remove_resource(self, resource_id):
        """Remove a resource from the store
        """
        path = self._make_resource_path(resource_id)

        try:
            self.retry(self.kazoo.delete, path)
        except NoNodeException:
            raise NotFoundError()

    def get_resource_ids(self, watcher=None):
        """Retrieve available resource IDs and optionally watch for changes
        """
        resource_ids = []
        if watcher:
            if not callable(watcher):
                raise ValueError("watcher is not callable")

        resource_ids = self.retry(self.kazoo.get_children,
            self.RESOURCES_PATH, watch=watcher)
        return resource_ids


class Record(dict):
    __slots__ = ['metadata']

    def __init__(self, *args, **kwargs):
        object.__setattr__(self, 'metadata', {})
        super(Record, self).__init__(*args, **kwargs)

    def __getattr__(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)


class ProcessDefinitionRecord(Record):
    @classmethod
    def new(cls, definition_id, definition_type, executable, name=None,
            description=None, version=None):
        d = dict(definition_id=definition_id, definition_type=definition_type,
            executable=executable, name=name, description=description,
            version=version)
        return cls(d)


class ProcessRecord(Record):
    @classmethod
    def new(cls, owner, upid, definition, state, configuration=None,
            constraints=None, subscribers=None, round=0, assigned=None,
            hostname=None, queueing_mode=None, restart_mode=None,
            node_exclusive=None, name=None):

        definition = copy.deepcopy(definition)

        if constraints:
            const = copy.deepcopy(constraints)
        else:
            const = {}

        if configuration:
            conf = copy.deepcopy(configuration)
        else:
            conf = {}

        starts = 0
        start_times = []
        dispatches = 0
        dispatch_times = []
        d = dict(owner=owner, upid=upid, subscribers=subscribers, state=state,
                 round=int(round), definition=definition, configuration=conf,
                 constraints=const, assigned=assigned, hostname=hostname,
                 queueing_mode=queueing_mode, restart_mode=restart_mode,
                 starts=starts, node_exclusive=node_exclusive, name=name,
                 start_times=start_times, dispatches=dispatches,
                 dispatch_times=dispatch_times)
        return cls(d)

    def increment_starts(self):
        self.starts += 1
        self.start_times.append(time.time())

    def increment_dispatches(self):
        self.dispatches += 1
        self.dispatch_times.append(time.time())

    def get_key(self):
        return self.owner, self.upid, self.round

    @property
    def key(self):
        return self.owner, self.upid, self.round

    def __hash__(self):
        return hash(self.get_key())


class ResourceRecord(Record):
    @classmethod
    def new(cls, resource_id, node_id, slot_count, properties=None,
            state=ExecutionResourceState.OK, last_heartbeat=None):
        if properties:
            props = properties.copy()
        else:
            props = {}

        # Special case to allow matching against resource_id
        props['resource_id'] = resource_id

        d = dict(resource_id=resource_id, node_id=node_id, state=state,
                 slot_count=int(slot_count), properties=props, assigned=[],
                 last_heartbeat=last_heartbeat)
        return cls(d)

    @property
    def last_heartbeat_datetime(self):
        if self.last_heartbeat is None:
            return None
        return parse_datetime(self.last_heartbeat)

    def new_last_heartbeat_datetime(self, d):
        self.last_heartbeat = d.isoformat()

    @property
    def available_slots(self):
        return max(0, self.slot_count - len(self.assigned))

    def is_assigned(self, owner, upid, round):
        t = (owner, upid, round)
        for assignment in self.assigned:
            if t == tuple(assignment):
                return True
        return False


class NodeRecord(Record):
    @classmethod
    def new(cls, node_id, domain_id, properties=None, resources=None, state_time=None):
        if properties:
            props = properties.copy()
        else:
            props = {}

        if resources:
            res = resources.copy()
        else:
            res = []

        d = dict(node_id=node_id, domain_id=domain_id, properties=props,
            resources=res, node_exclusive=[], state_time=time.time())
        return cls(d)

    def node_exclusive_available(self, attr):
        if attr is None:
            return True
        elif attr not in self.node_exclusive:
            return True
        else:
            return False
