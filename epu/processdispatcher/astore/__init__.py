import copy
import time


class IProcessDispatcherSync(object):
    """Coordination activities between Process Dispatcher workers
    """

    def initialize(self):
        """Initialize the store
        """

    def shutdown(self):
        """Shutdown the store
        """

    def contend_matchmaker(self, matchmaker):
        """Provide a matchmaker object to participate in an election
        """

    def contend_doctor(self, doctor):
        """Provide a doctor object to participate in an election
        """

    #########################################################################
    # PROCESS DISPATCHER STATE
    #########################################################################

    def set_system_boot(self, system_boot):
        """
        called by launch plan with system_boot=True at start of launch
        called by launch plan with system_boot=False at end of launch
        """

    def is_system_boot(self, watcher=None):
        """
        called by doctor during init to decide what PD state to move to
        """

    def wait_initialized(self, timeout=None):
        """Wait for the Process Dispatcher to be initialized by the doctor
        """

    def set_initialized(self):
        """called by doctor after initialization is complete
        """

    def get_pd_state(self):
        """Get the current state of the Process Dispatcher. One of ProcessDispatcherState
        """

    def set_pd_state(self, state):
        """called by doctor to change PD state after init
        """

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

    def get_queued_processes(self, watcher=None):
        """Get the queued processes and optionally set a watcher for changes

        @param watcher: callable to be called ONCE when the queued process set changes
        @return list of (owner, upid, round) tuples
        """

    def remove_queued_process(self, owner, upid, round):
        """Remove a process from the runnable queue
        """

    def clear_queued_processes(self):
        """Reset the process queue
        """

    #########################################################################
    # EXECUTION RESOURCE NOTIFICATIONS
    #########################################################################

    def notify_resource_added(self, resource_id):
        """Notify observers of resource creation
        """

    def notify_resource_removed(self, resource_id):
        """Notify observers of resource removal
        """

    def notify_resource_changed(self, resource_id):
        """Notify observers of resource update
        """

    def watch_resource_set(self, watcher):
        """Watch for resource set changes
        """

    def watch_resource(self, resource_id, watcher):
        """Watch for resource updates
        """


class IProcessDispatcherStore(object):
    """
    This store is responsible for persistence of several types of records.
    It also supports providing certain notifications about changes to stored
    records.
    """

    #########################################################################
    # PROCESS DEFINITIONS
    #########################################################################

    def add_definition(self, definition):
        """Adds a new process definition

        Raises WriteConflictError if the definition already exists
        """

    def get_definition(self, definition_id):
        """Retrieve definition record or None if not found
        """

    def update_definition(self, definition):
        """Update existing definition

        A NotFoundError is raised if the definition doesn't exist
        """

    def remove_definition(self, definition_id):
        """Remove definition record

        A NotFoundError is raised if the definition doesn't exist
        """

    def list_definition_ids(self):
        """Retrieve list of known definition IDs
        """

    #########################################################################
    # PROCESSES
    #########################################################################

    def add_process(self, process):
        """Adds a new process record to the store

        If the process record already exists, a WriteConflictError exception
        is raised.
        """

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

    def get_process(self, owner, upid):
        """Retrieve process record
        """

    def remove_process(self, owner, upid):
        """Remove process record from store
        """

    def get_process_ids(self):
        """Retrieve available process IDs
        """

    #########################################################################
    # PROCESS ASSIGNMENTS
    #########################################################################

    def create_process_assignment(self, process, resource):
        """Assign a process to a resource
        """

    def remove_process_assignment(self, process, resource):
        """Remove any assignment of process
        """

    def get_process_assignments(self, process):
        """Retrieve the resource ID a process is assigned to, or None
        """

    def get_resource_assignments(self, resource):
        """Retrieve a list of upids assigned to a resource
        """

    #########################################################################
    # NODES
    #########################################################################

    def add_node(self, node):
        """Add a new node record
        """

    def update_node(self, node, force=False):
        """Update a node record
        """

    def get_node(self, node_id):
        """Retrieve a node record
        """

    def remove_node(self, node_id):
        """Remove a node record
        """

    def get_node_ids(self):
        """Retrieve available node IDs and optionally watch for changes
        """

    #########################################################################
    # EXECUTION RESOURCES
    #########################################################################

    def add_resource(self, resource, node):
        """Add an execution resource record to the store
        """

    def update_resource(self, resource, force=False):
        """Update an existing resource record
        """

    def get_resource(self, resource_id):
        """Retrieve a resource record
        """

    def remove_resource(self, resource_id):
        """Remove a resource from the store
        """

    def get_resource_ids(self):
        """Retrieve available resource IDs and optionally watch for changes
        """

    def enable_resource(resource):
        """Enable resource for scheduling
        """

    def disable_resource(resource):
        """Disable resource for scheduling
        """


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
        d = dict(owner=owner, upid=upid, subscribers=subscribers, state=state,
                 round=int(round), definition=definition, configuration=conf,
                 constraints=const, assigned=assigned, hostname=hostname,
                 queueing_mode=queueing_mode, restart_mode=restart_mode,
                 starts=starts, node_exclusive=node_exclusive, name=name)
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
