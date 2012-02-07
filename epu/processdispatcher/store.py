import threading
import json
import logging

from epu.exceptions import NotFoundError, WriteConflictError

log = logging.getLogger(__name__)

class ProcessDispatcherStore(object):
    """
    This store is responsible for persistence of several types of records.
    It also supports providing certain notifications about changes to stored
    records.

    This is an in-memory only version.
    """

    def __init__(self):
        self.lock = threading.RLock()
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
                raise WriteConflictError("version mismatch. "+
                                         "current=%s, attempted to write %s" %
                                         (version, found[1]))

            # pushing to JSON to prevent side effects of shared objects
            data = json.dumps(process)
            self.processes[key] = data,version+1
            process.metadata['version'] = version+1

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
        """Retrieve available node IDs and optionally watch for changes
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
                raise WriteConflictError("version mismatch. "+
                                         "current=%s, attempted to write %s" %
                                         (version, found[1]))

            # pushing to JSON to prevent side effects of shared objects
            data = json.dumps(node)
            self.nodes[node_id] = data,version+1
            node.metadata['version'] = version+1

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
                raise WriteConflictError("version mismatch. "+
                                         "current=%s, attempted to write %s" %
                                         (version, found[1]))

            # pushing to JSON to prevent side effects of shared objects
            data = json.dumps(resource)
            self.resources[resource_id] = data,version+1
            resource.metadata['version'] = version+1

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


class Record(dict):
    __slots__ = ['metadata']
    def __init__(self, *args, **kwargs):
        self.metadata = {}
        super(Record, self).__init__(*args, **kwargs)

    def __getattr__(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)


class ProcessRecord(Record):
    @classmethod
    def new(cls, owner, upid, spec, state, constraints=None, subscribers=None,
            round=0, immediate=False, assigned=None):
        d = dict(owner=owner, upid=upid, spec=spec, subscribers=subscribers,
                 state=state, round=int(round), immediate=bool(immediate),
                 constraints=constraints, assigned=assigned)
        return cls(d)

    def get_key(self):
        return self.owner, self.upid, self.round

    @property
    def key(self):
        return self.owner, self.upid, self.round


class ResourceRecord(Record):
    @classmethod
    def new(cls, resource_id, node_id, slot_count, properties=None,
            enabled=True):
        if properties:
            props = properties.copy()
        else:
            props = {}

        d = dict(resource_id=resource_id, node_id=node_id, enabled=enabled,
                 slot_count=int(slot_count), properties=props, assigned=[])
        return cls(d)

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
    def new(cls, node_id, deployable_type, properties=None):
        if properties:
            props = properties.copy()
        else:
            props = {}

        d = dict(node_id=node_id, deployable_type=deployable_type,
                 properties=props)
        return cls(d)
