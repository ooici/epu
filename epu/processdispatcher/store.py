from functools import partial
import json
import logging
import re
import threading

import gevent

# conditionally import these so we can use the in-memory store without ZK
try:
    from kazoo.client import KazooClient, KazooState, EventType
    from kazoo.exceptions import NodeExistsException, BadVersionException, \
        NoNodeException
    from kazoo.recipe.leader import LeaderElection

except ImportError:
    KazooClient = None
    KazooState = None
    EventType = None
    LeaderElection = None
    NodeExistsException = None
    BadVersionException = None
    NoNodeException = None

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
        self.is_leading = False

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
        assert not self.is_leading
        self.is_leading = True

        self._matchmaker_thread = gevent.spawn(self._matchmaker.inaugurate)

    def shutdown(self):
        # In-memory store, only stop the matchmaker thread
        try:
            self._matchmaker.cancel()
        except Exception, e:
            log.exception("Error cancelling matchmaker: %s", e)

    #########################################################################
    # PROCESS DEFINITIONS
    #########################################################################

    def add_definition(self, definition):
        """Adds a new process definition

        Raises WriteConflictError if the definition already exists
        """
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
        """Retrieve available node IDs
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

    NODES_PATH = "/nodes"

    PROCESSES_PATH = "/processes"

    DEFINITIONS_PATH = "/definitions"

    QUEUED_PROCESSES_PATH = "/requested"

    RESOURCES_PATH = "/resources"

    # this path is used for leader election. PD workers line up for
    # an exclusive lock on leadership.
    ELECTION_PATH = "/election"

    def __init__(self, hosts, base_path, timeout=None):
        self.kazoo = KazooClient(hosts, timeout=timeout, namespace=base_path)
        self.election = LeaderElection(self.kazoo, self.ELECTION_PATH)

        # callback fired when the connection state changes
        self.kazoo.add_listener(self._connection_state_listener)

        self._election_enabled = False
        self._election_condition = threading.Condition()
        self._election_thread = None

        self._matchmaker = None

    def initialize(self):

        self.kazoo.connect()

        for path in (self.NODES_PATH, self.PROCESSES_PATH,
                     self.DEFINITIONS_PATH, self.QUEUED_PROCESSES_PATH,
                     self.RESOURCES_PATH, self.ELECTION_PATH):
            self.kazoo.ensure_path(path)

    def _connection_state_listener(self, state):
        # called by kazoo when the connection state changes.
        # handle in background
        gevent.spawn(self._handle_connection_state, state)

    def _handle_connection_state(self, state):

        if state in (KazooState.LOST, KazooState.SUSPENDED):
            with self._election_condition:
                self._election_enabled = False
                self._election_condition.notify_all()

            # depose the matchmaker and cancel the elections just in case
            try:
                self._matchmaker.depose()
            except Exception, e:
                log.exception("Error deposing matchmaker: %s", e)

            self.election.cancel()

        elif state == KazooState.CONNECTED:
            with self._election_condition:
                self._election_enabled = True
                self._election_condition.notify_all()

    def contend_matchmaker(self, matchmaker):
        """Provide a matchmaker object to participate in an election
        """
        assert self._matchmaker is None
        self._matchmaker = matchmaker
        self._election_thread = gevent.spawn(self._run_election)

    def _run_election(self):
        """Election thread function
        """
        while True:
            with self._election_condition:
                while not self._election_enabled:
                    self._election_condition.wait()

                try:
                    self.election.run(self._matchmaker.inaugurate)
                except Exception, e:
                    log.exception("Error in matchmaker election: %s", e)

    def shutdown(self):
        # depose the leader and cancel the election just in case
        try:
            self._matchmaker.cancel()
        except Exception, e:
            log.exception("Error deposing leader: %s", e)

        self.election.cancel()
        self._election_thread.kill()
        self.kazoo.close()

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
        try:
            self.kazoo.create(self._make_definition_path(definition_id), data)
        except NodeExistsException:
            raise WriteConflictError("definition %s already exists" % definition_id)

    def get_definition(self, definition_id):
        """Retrieve definition record or None if not found
        """

        try:
            data, stat = self.kazoo.get(self._make_definition_path(definition_id))
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
        try:
            self.kazoo.set(self._make_definition_path(definition_id), data, -1)
        except NoNodeException:
            raise NotFoundError()

    def remove_definition(self, definition_id):
        """Remove definition record

        A NotFoundError is raised if the definition doesn't exist
        """
        try:
            self.kazoo.delete(self._make_definition_path(definition_id))
        except NoNodeException:
            raise NotFoundError()

    def list_definition_ids(self):
        """Retrieve list of known definition IDs
        """
        definition_ids = self.kazoo.get_children(self.DEFINITIONS_PATH)
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

        try:
            self.kazoo.create(self._make_process_path(owner=process.owner, upid=process.upid), data)
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
        version = process.metadata.get('version')

        if version is None and not force:
            raise ValueError("process has no version and force=False")

        try:
            if force:
                set_version = -1
            else:
                set_version = version
            stat = self.kazoo.set(path, data, set_version)
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
            data, stat = self.kazoo.get(path, watch=watcher)
        except NoNodeException:
            return None

        rawdict = json.loads(data)
        process = ProcessRecord(rawdict)
        process.metadata['version'] = stat['version']

        return process

    def remove_process(self, owner, upid):
        """Remove process record from store
        """
        path = self._make_process_path(owner=owner, upid=upid)

        try:
            self.kazoo.delete(path)
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
            self.kazoo.create(path, "", sequence=True)
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
        processes = self.kazoo.get_children(self.QUEUED_PROCESSES_PATH, watch=watcher)

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
        processes = self.kazoo.get_children(self.QUEUED_PROCESSES_PATH)

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
            self.kazoo.delete(path)
        except NoNodeException:
            raise NotFoundError()

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

        try:
            self.kazoo.create(self._make_node_path(node_id), data)
        except NodeExistsException:
            raise WriteConflictError("node %s already exists" % node_id)

        node.metadata['version'] = 0

    def update_node(self, node, force=False):
        """Update a node record
        """
        node_id = node.node_id
        path = self._make_node_path(node_id)
        data = json.dumps(node)
        version = node.metadata.get('version')

        if version is None and not force:
            raise ValueError("node has no version and force=False")

        try:
            if force:
                set_version = -1
            else:
                set_version = version
            stat = self.kazoo.set(path, data, set_version)
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
            data, stat = self.kazoo.get(path, watch=watcher)
        except NoNodeException:
            return None

        rawdict = json.loads(data)
        node = NodeRecord(rawdict)
        node.metadata['version'] = stat['version']

        return node

    def remove_node(self, node_id):
        """Remove a node record
        """
        path = self._make_node_path(node_id)

        try:
            self.kazoo.delete(path)
        except NoNodeException:
            raise NotFoundError()

    def get_node_ids(self, watcher=None):
        """Retrieve available node IDs and optionally watch for changes
        """
        if watcher:
            if not callable(watcher):
                raise ValueError("watcher is not callable")

        node_ids = self.kazoo.get_children(self.NODES_PATH)
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

        try:
            self.kazoo.create(self._make_resource_path(resource_id), data)
        except NodeExistsException:
                raise WriteConflictError("resource %s already exists" % resource_id)

        resource.metadata['version'] = 0

    def update_resource(self, resource, force=False):
        """Update an existing resource record
        """
        resource_id = resource.resource_id
        path = self._make_resource_path(resource_id)
        data = json.dumps(resource)
        version = resource.metadata.get('version')

        if version is None and not force:
            raise ValueError("resource has no version and force=False")

        try:
            if force:
                set_version = -1
            else:
                set_version = version
            stat = self.kazoo.set(path, data, version=set_version)
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
            data, stat = self.kazoo.get(path, watch=watch)
        except NoNodeException:
            return None

        rawdict = json.loads(data)
        resource = ResourceRecord(rawdict)
        resource.metadata['version'] = stat['version']

        return resource

    def remove_resource(self, resource_id):
        """Remove a resource from the store
        """
        path = self._make_resource_path(resource_id)

        try:
            self.kazoo.delete(path)
        except NoNodeException:
            raise NotFoundError()

    def get_resource_ids(self, watcher=None):
        """Retrieve available resource IDs and optionally watch for changes
        """
        resource_ids = []
        if watcher:
            if not callable(watcher):
                raise ValueError("watcher is not callable")

        resource_ids = self.kazoo.get_children(self.RESOURCES_PATH, watch=watcher)
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
    def new(cls, owner, upid, spec, state, constraints=None,
            subscribers=None, round=0, immediate=False, assigned=None,
            hostname=None, queueing_mode=None, restart_mode=None):
        if constraints:
            const = constraints.copy()
        else:
            const = {}
        starts = 0
        d = dict(owner=owner, upid=upid, spec=spec, subscribers=subscribers,
                 state=state, round=int(round), immediate=bool(immediate),
                 constraints=const, assigned=assigned, hostname=hostname,
                 queueing_mode=queueing_mode, restart_mode=restart_mode,
                 starts=starts)
        return cls(d)

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
            enabled=True):
        if properties:
            props = properties.copy()
        else:
            props = {}

        # Special case to allow matching against resource_id
        props['resource_id'] = resource_id

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
