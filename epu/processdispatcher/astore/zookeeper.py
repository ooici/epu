import simplejson as json
import logging
import re
import threading
from functools import partial

from kazoo.exceptions import NodeExistsException, BadVersionException, \
    NoNodeException

from epu.exceptions import NotFoundError, WriteConflictError
from epu.states import ProcessDispatcherState
from epu.processdispatcher.astore import ProcessRecord, ProcessDefinitionRecord,\
    ResourceRecord, NodeRecord

log = logging.getLogger(__name__)


class ZKProcessDispatcherSync(object):
    """Coordination activities between Process Dispatcher workers
    """

    # the existence of this path indicates whether the system is currently
    # booting
    SYSTEM_BOOT_PATH = "/system_boot"

    # this path hosts an ephemeral node created by the doctor after it inits
    INITIALIZED_PATH = "/initialized"

    # contains the current Process Dispatcher state
    PD_STATE_PATH = "/pd_state"

    PARTY_PATH = "/party"

    QUEUED_PROCESSES_PATH = "/requested"

    RESOURCE_SENTINELS_PATH = "/resource_sentinels"

    # these paths is used for leader election. PD workers line up for
    # an exclusive lock on leadership.
    MATCHMAKER_ELECTION_PATH = "/elections/matchmaker"
    DOCTOR_ELECTION_PATH = "/elections/doctor"

    ENSURE_PATHS = (QUEUED_PROCESSES_PATH, MATCHMAKER_ELECTION_PATH,
                    DOCTOR_ELECTION_PATH, PARTY_PATH, RESOURCE_SENTINELS_PATH)

    def __init__(self, kazoo_base):

        self.kazoo_base = kazoo_base
        self.kazoo = kazoo_base.kazoo
        self.retry = kazoo_base.retry

        self.kazoo_base.add_paths(self.ENSURE_PATHS)
        self.kazoo_base.add_election("matchmaker", self.MATCHMAKER_ELECTION_PATH)
        self.kazoo_base.add_election("doctor", self.DOCTOR_ELECTION_PATH)

        self._is_initialized = threading.Event()
        self._party = self.kazoo.Party(self.PARTY_PATH)

    def initialize(self):
        self.kazoo_base.start()

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

    def contend_matchmaker(self, matchmaker):
        """Provide a matchmaker object to participate in an election
        """
        self.kazoo_base.contend_election("matchmaker", matchmaker)

    def contend_doctor(self, doctor):
        """Provide a doctor object to participate in an election
        """
        self.kazoo_base.contend_election("doctor", doctor)

    def shutdown(self):

        self.kazoo_base.stop()
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
    # EXECUTION RESOURCE NOTIFICATIONS
    #########################################################################

    def _make_resource_path(self, resource_id):
        if resource_id is None:
            raise ValueError('invalid resource id')

        return self.RESOURCE_SENTINELS_PATH + "/" + resource_id

    def notify_resource_added(self, resource_id):
        """Notify observers of resource creation
        """
        path = self._make_resource_path(resource_id)
        try:
            self.retry(self.kazoo.create, path, "")
        except NodeExistsException:
            pass

    def notify_resource_removed(self, resource_id):
        """Notify observers of resource removal
        """
        path = self._make_resource_path(resource_id)
        try:
            self.retry(self.kazoo.delete, path)
        except NoNodeException:
            pass

    def notify_resource_changed(self, resource_id):
        """Notify observers of resource update
        """
        path = self._make_resource_path(resource_id)
        try:
            self.retry(self.kazoo.update, path, "")
        except NoNodeException:
            raise NotFoundError()

    def watch_resource_set(self, watcher):
        """Watch for resource set changes

        Watch will be continually re-called until it returns False
        """
        if not callable(watcher):
            raise ValueError("watcher is not callable")
        self.kazoo.ChildrenWatch(self.RESOURCES_PATH, watcher)

    def watch_resource(self, resource_id, watcher):
        """Watch for resource updates
        """
        path = self._make_resource_path(resource_id)

        if not callable(watcher):
            raise ValueError("watcher is not callable")

        watcher = partial(watcher, resource_id)
        self.kazoo.DataWatch(path, watcher)


class ZKProcessDispatcherStore(object):
    """
    This store is responsible for persistence of several types of records.
    It also supports providing certain notifications about changes to stored
    records.
    """

    NODES_PATH = "/nodes"

    PROCESSES_PATH = "/processes"

    ASSIGNMENTS_PATH = "/assignments"

    DEFINITIONS_PATH = "/definitions"

    RESOURCES_PATH = "/resources"

    ENSURE_PATHS = (NODES_PATH, PROCESSES_PATH, ASSIGNMENTS_PATH,
                    DEFINITIONS_PATH, RESOURCES_PATH)

    def __init__(self, kazoo_base):
        self.kazoo_base = kazoo_base
        self.kazoo = kazoo_base.kazoo
        self.retry = kazoo_base.retry

        self.kazoo_base.add_paths(self.ENSURE_PATHS)

    def initialize(self):
        self.kazoo_base.start()

    def shutdown(self):
        self.kazoo_base.stop()

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

    def get_process(self, owner, upid):
        """Retrieve process record
        """
        path = self._make_process_path(owner=owner, upid=upid)

        try:
            data, stat = self.retry(self.kazoo.get, path)
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
    # PROCESS ASSIGNMENTS
    #########################################################################

    def _make_process_assignment_path(self, owner, upid, resource_id):
        if upid is None:
            raise ValueError('invalid process upid')
        if resource_id is None:
            raise ValueError('invalid resource_id')

        path = self.ASSIGNMENTS_PATH + "/"
        if owner is not None:
            path += "owner=" + owner + "&"
        path += "upid=" + upid + "&res=" + resource_id

        return path

    def create_process_assignment(self, process, resource):
        """Assign a process to a resource
        """
        path = self._make_process_assignment_path(process.owner, process.upid,
            resource.resource_id)
        try:
            self.retry(self.kazoo.create, path, "")
        except NodeExistsException:
            pass

    def remove_process_assignment(self, process, resource):
        """Remove any assignment of process
        """
        path = self._make_process_assignment_path(process.owner, process.upid,
            resource.resource_id)
        try:
            self.retry(self.kazoo.delete, path)
        except NoNodeException:
            pass

    def _get_process_assignments(self):
        assignments = self.kazoo.get_children(self.ASSIGNMENTS_PATH)
        result = []

        for a in assignments:
            owner = None

            match = re.match(r'^owner=(.+)&upid=(.+)&res=(.+)$', a)
            if match is not None:
                owner = match.group(1)
                upid = match.group(2)
                resource_id = match.group(3)

            else:
                match = re.match(r'^upid=(.+)&res=(.+)$', a)
                if match is None:
                    raise ValueError("queued process %s could not be parsed" % a)
                upid = match.group(1)
                resource_id = match.group(2)

            result.append((owner, upid, resource_id))

        return result

    def get_process_assignments(self, process):
        """Retrieve the resource IDs a process is assigned to

        This returns a list because it is up to the caller to enforce
        consistency.
        """
        assignments = self._get_process_assignments()
        return [resource_id for (owner, upid, resource_id) in assignments
                if owner == process.owner and upid == process.upid]

    def get_resource_assignments(self, resource):
        """Retrieve a list of upids assigned to a resource
        """
        assignments = self._get_process_assignments()
        return [(owner, upid) for (owner, upid, resource_id) in assignments
                if resource_id == resource.resource_id]

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

    def get_node(self, node_id):
        """Retrieve a node record
        """
        path = self._make_node_path(node_id)
        try:
            data, stat = self.retry(self.kazoo.get, path)
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

    def get_node_ids(self):
        """Retrieve available node IDs and optionally watch for changes
        """
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

    def get_resource(self, resource_id):
        """Retrieve a resource record
        """
        path = self._make_resource_path(resource_id)

        try:
            data, stat = self.retry(self.kazoo.get, path)
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

    def get_resource_ids(self):
        """Retrieve available resource IDs
        """
        resource_ids = self.retry(self.kazoo.get_children,
            self.RESOURCES_PATH)
        return resource_ids
