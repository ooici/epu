import simplejson as json
import logging
import threading

import epu.tevent as tevent
from epu.exceptions import NotFoundError, WriteConflictError
from epu.states import ProcessDispatcherState
from epu.processdispatcher.astore import ProcessRecord, ProcessDefinitionRecord,\
    ResourceRecord, ShadowResourceRecord, NodeRecord

log = logging.getLogger(__name__)


class MemoryProcessDispatcherSync(object):
    """Coordination activities between Process Dispatcher workers
    """

    def __init__(self, system_boot=False):
        self.lock = threading.RLock()

        self._is_initialized = threading.Event()
        self._pd_state = None
        self._pd_state_watches = []
        self._is_system_boot = bool(system_boot)
        self._system_boot_watches = []

        self.queued_processes = []
        self.queued_process_set_watches = []

        self._matchmaker = None
        self._matchmaker_thread = None
        self.is_matchmaker = False

        self._doctor = None
        self._doctor_thread = None
        self.is_doctor = False

        self.resources = {}
        self.resource_set_watches = []
        self.resource_watches = {}

    def initialize(self):
        """Initialize the store
        """

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
    #  SHADOW RESOURCES
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

    def add_shadow_resource(self, resource):
        """Add an execution shadow resource record
        """
        with self.lock:
            resource_id = resource.resource_id
            if resource_id in self.resources:
                raise WriteConflictError("resource %s already exists" % resource_id)

            data = json.dumps(resource)
            self.resources[resource_id] = data, 0
            resource.metadata['version'] = 0

            self._fire_resource_set_watchers()

    def update_shadow_resource(self, resource, force=False):
        """Update an existing shadow resource record
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

    def get_shadow_resource(self, resource_id, watcher=None):
        """Retrieve a shadow resource record
        """
        with self.lock:
            found = self.resources.get(resource_id)
            if found is None:
                return None

            rawdict = json.loads(found[0])
            resource = ShadowResourceRecord(rawdict)
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

    def remove_shadow_resource(self, resource_id):
        """Remove a shadow resource
        """
        with self.lock:

            if resource_id not in self.resources:
                raise NotFoundError()
            del self.resources[resource_id]

            self._fire_resource_set_watchers()

    def get_shadow_resource_ids(self, watcher=None):
        """Retrieve available shadow resource IDs and optionally watch for changes
        """
        with self.lock:
            if watcher:
                if not callable(watcher):
                    raise ValueError("watcher is not callable")

                self.resource_set_watches.append(watcher)
            return self.resources.keys()


class MemoryProcessDispatcherStore(object):
    """
    This store is responsible for persistence of several types of records.
    It also supports providing certain notifications about changes to stored
    records.

    This is an in-memory only version.
    """

    def __init__(self, system_boot=False):
        self.lock = threading.RLock()

        self.definitions = {}
        self.processes = {}
        self.resources = {}
        self.nodes = {}
        self.assignments = set()

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

    def get_process(self, owner, upid):
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

    #########################################################################
    # PROCESS ASSIGNMENTS
    #########################################################################

    def create_process_assignment(self, process, resource):
        """Assign a process to a resource
        """
        t = (process.owner, process.upid, resource.resource_id)
        with self.lock:
            self.assignments.add(t)

    def remove_process_assignment(self, process, resource):
        """Remove any assignment of process
        """
        t = (process.owner, process.upid, resource.resource_id)
        with self.lock:
            self.assignments.discard(t)

    def get_process_assignments(self, process):
        """Retrieve the resource IDs a process is assigned to

        This returns a list because it is up to the caller to enforce
        consistency.
        """
        with self.lock:
            return [resource_id for owner, upid, resource_id in self.assignments
                    if owner == process.owner and upid == process.upid]

    def get_resource_assignments(self, resource):
        """Retrieve a list of upids assigned to a resource
        """
        with self.lock:
            return [(owner, upid) for owner, upid, resource_id in self.assignments
                    if resource_id == resource.resource_id]

    #########################################################################
    # NODES
    #########################################################################

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

    def get_node(self, node_id):
        """Retrieve a node record
        """
        with self.lock:
            found = self.nodes.get(node_id)
            if found is None:
                return None

            rawdict = json.loads(found[0])
            node = NodeRecord(rawdict)
            node.metadata['version'] = found[1]

            return node

    def remove_node(self, node_id):
        """Remove a node record
        """
        with self.lock:
            if node_id not in self.nodes:
                raise NotFoundError()
            del self.nodes[node_id]

    def get_node_ids(self):
        """Retrieve available node IDs and optionally watch for changes
        """
        with self.lock:
            return self.nodes.keys()

    #########################################################################
    # EXECUTION RESOURCES
    #########################################################################

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

    def get_resource(self, resource_id):
        """Retrieve a resource record
        """
        with self.lock:
            found = self.resources.get(resource_id)
            if found is None:
                return None

            rawdict = json.loads(found[0])
            resource = ResourceRecord(rawdict)
            resource.metadata['version'] = found[1]

            return resource

    def remove_resource(self, resource_id):
        """Remove a resource from the store
        """
        with self.lock:

            if resource_id not in self.resources:
                raise NotFoundError()
            del self.resources[resource_id]

    def get_resource_ids(self):
        """Retrieve available resource IDs and optionally watch for changes
        """
        with self.lock:
            return self.resources.keys()

    def enable_resource(self, resource):
        """Enable resource for scheduling
        """
        with self.lock:
            resource = self.get_resource(resource.resource_id)
            if resource is None:
                raise NotFoundError()
            resource.enabled = True
            self.update_resource(resource)

    def disable_resource(self, resource):
        """Disable resource for scheduling
        """
        with self.lock:
            resource = self.get_resource(resource.resource_id)
            if resource is None:
                raise NotFoundError()
            resource.enabled = False
            self.update_resource(resource)
