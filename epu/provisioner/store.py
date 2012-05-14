#!/usr/bin/env python

"""
@file epu/provisioner/store.py
@author David LaBissoniere
@brief Provisioner storage abstraction
"""
from itertools import groupby
import logging
import threading

import gevent
import simplejson as json

# conditionally import these so we can use the in-memory store without ZK
try:
    from kazoo.client import KazooClient, KazooState, EventType
    from kazoo.exceptions import NodeExistsException, BadVersionException, \
        NoNodeException
    from kazoo.recipe.leader import LeaderElection
    from kazoo.recipe.party import ZooParty

except ImportError:
    KazooClient = None
    KazooState = None
    EventType = None
    LeaderElection = None
    NodeExistsException = None
    BadVersionException = None
    NoNodeException = None
    ZooParty = None


from epu.exceptions import WriteConflictError, NotFoundError


log = logging.getLogger(__name__)

VERSION_KEY = "__version"

class ProvisionerStore(object):
    """In-memory version of Provisioner storage
    """
    def __init__(self):
        self.nodes = {}
        self.launches = {}
        self.terminating = {}

        self.leader = None
        self.leader_thread = None
        self.is_leading = False

        self._disabled = False

        self.termination_condition = threading.Condition()

    def initialize(self):
        pass

    def is_disabled(self):
        """Indicates that the Provisioner is in disabled mode, which means
        that no new launches will be allowed
        """
        return self._disabled

    def is_disabled_agreed(self):
        """Indicates that all Provisioner workers have recognized disabled mode

        This is used to determine whether it is safe to proceed with termination
        of all VMs as part of system shutdown
        """

        # for in-memory store, there is only one worker
        return self._disabled

    def enable_provisioning(self):
        """Allow new instance launches
        """
        self._disabled = False

    def disable_provisioning(self):
        """Disallow new instance launches
        """
        self._disabled = True

    def contend_leader(self, leader):
        """Provide a leader object to participate in an election
        """
        assert self.leader is None
        self.leader = leader

        # since this is in-memory store, we are the only possible leader
        self._make_leader()

    def _make_leader(self):
        assert self.leader
        assert not self.is_leading
        self.is_leading = True

        self.leader_thread = gevent.spawn(self.leader.inaugurate)

    # for tests
    def _break_leader(self):
        assert self.leader
        assert self.is_leading

        self.leader.depose()
        self.leader_thread.join()


    #########################################################################
    # LAUNCHES
    #########################################################################

    def add_launch(self, launch):
        """
        Store a new launch record
        @param launch: launch dictionary
        @raise WriteConflictError if launch exists
        """
        launch_id = launch['launch_id']
        if launch_id in self.launches:
            raise WriteConflictError()

        # store the launch, along with its version
        self.launches[launch_id] = json.dumps(launch), 0

        # also add a version to the input dict.
        launch[VERSION_KEY] = 0

    def update_launch(self, launch):
        """
        @brief updates a launch record in the store
        @param launch Launch record to store
        """
        launch_id = launch['launch_id']

        existing = self.launches.get(launch_id)
        if not existing:
            raise NotFoundError()

        _, version = existing

        if launch[VERSION_KEY] != version:
            raise WriteConflictError()

        version += 1
        self.launches[launch_id] = json.dumps(launch), version
        launch[VERSION_KEY] = version

    def get_launch(self, launch_id):
        """
        @brief Retrieves a launch record by id
        @param launch_id Id of launch record to retrieve
        @retval launch dictionary or None if not found
        """
        record = self.launches.get(launch_id)
        if record:
            launch_json, version = record
            ret = json.loads(launch_json)
            ret[VERSION_KEY] = version
        else:
            ret = None
        return ret

    def get_launches(self, state=None, min_state=None, max_state=None):
        """
        @brief Retrieves the latest record for all launches within a state range
        @param state Only retrieve nodes in this state.
        @param min_state Inclusive start bound
        @param max_state Inclusive end bound
        @retval list of launch records
        """
        records = self._get_records(self.launches, state, min_state, max_state)
        return records

    def remove_launch(self, launch_id):
        """
        Remove a launch record from the store
        @param launch_id:
        @return:
        """
        if launch_id in self.launches:
            del self.launches[launch_id]
        else:
            raise NotFoundError()


    #########################################################################
    # NODES
    #########################################################################

    def add_node(self, node):
        """
        Store a new node record
        @param node: node dictionary
        @raise WriteConflictError if node exists
        """
        node_id = node['node_id']
        if node_id in self.nodes:
            raise WriteConflictError()

        # store the launch, along with a version
        self.nodes[node_id] = json.dumps(node), 0

        # also add a version to the input dict.
        node[VERSION_KEY] = 0

    def update_node(self, node):
        """
        @brief Updates an existing node record
        @param node Node record
        @retval Deferred for success
        """
        node_id = node['node_id']

        existing = self.nodes.get(node_id)
        if not existing:
            raise NotFoundError()

        _, version = existing

        if node[VERSION_KEY] != version:
            raise WriteConflictError()

        version += 1
        self.nodes[node_id] = json.dumps(node), version
        node[VERSION_KEY] = version

    def get_node(self, node_id):
        """
        @brief Retrieves a launch record by id
        @param node_id Id of node record to retrieve
        @retval node record or None if not found
        """
        record = self.nodes.get(node_id)
        if record:
            node_json, version = record
            ret = json.loads(node_json)
            ret[VERSION_KEY] = version
        else:
            ret = None
        return ret

    def get_nodes(self, state=None, min_state=None, max_state=None):
        """
        @brief Retrieves all launch record within a state range
        @param state Only retrieve nodes in this state.
        @param min_state Inclusive start bound.
        @param max_state Inclusive end bound
        @retval Deferred list of launch records
        """
        records = self._get_records(self.nodes, state, min_state, max_state)
        return records

    def remove_node(self, node_id):
        """Remove a node record from the store
        """
        if node_id in self.nodes:
            del self.nodes[node_id]
        else:
            raise NotFoundError()

    def _get_records(self, dct, state=None, min_state=None, max_state=None):

        # overrides range arguments
        if state:
            min_state = max_state = state

        records = []
        for r, version in dct.itervalues():
            record = json.loads(r)
            if not max_state or record['state'] <= max_state:
                if not min_state or record['state'] >= min_state:
                    record[VERSION_KEY] = version
                    records.append(record)
        return records

    #########################################################################
    # TERMINATING NODES
    #########################################################################

    def add_terminating(self, node_id):
        """
        Store a new terminating node
        @param node_id
        @raise WriteConflictError if node exists
        """
        if node_id in self.terminating:
            raise WriteConflictError()

        # store the node_id
        self.terminating[node_id] = node_id

        with self.termination_condition:
            self.termination_condition.notify_all()

    def get_terminating(self):
        if not self.terminating:
            with self.termination_condition:
                self.termination_condition.wait()

        return self.terminating.keys()

    def remove_terminating(self, node_id):
        if node_id not in self.terminating:
            raise NotFoundError()

        del self.terminating[node_id]


class ProvisionerZooKeeperStore(object):
    """ZooKeeper-backed Provisioner storage
    """

    # this path is used to store launch information. Each child of this path
    # is a launch, named with its launch_id
    LAUNCH_PATH = "/launch"

    # this path is used to store node information. Each child of this path
    # is a node, named with its node_id
    NODE_PATH = "/node"

    # this path is used for leader election. Provisioner workers line up for
    # an exclusive lock on leadership.
    ELECTION_PATH = "/election"

    # this path is used for tracking active Provisioner workers. While each
    # worker is alive it creates a node under this path, as long as it hasn't
    # detected disabled mode. When it does detect disabled mode, it deletes
    # its node.
    PARTICIPANT_PATH = "/participants"

    # this path is used to indicate that the provisioner is in disabled mode.
    # when the node exists, we are in disabled mode. All provisioner workers
    # maintain a watch on this node.
    DISABLED_PATH = "/disabled"

    # this path is used to store IDs of nodes that are being terminated.
    # the terminator thread will pick them up and perform the actual
    # termination.
    TERMINATING_PATH = "/TERMINATING"

    def __init__(self, hosts, base_path, timeout=None):
        self.kazoo = KazooClient(hosts, timeout=timeout, namespace=base_path)
        self.election = LeaderElection(self.kazoo, self.ELECTION_PATH)
        self.party = ZooParty(self.kazoo, self.PARTICIPANT_PATH)

        #  callback fired when the connection state changes
        self.kazoo.add_listener(self._connection_state_listener)

        self._election_enabled = False
        self._election_condition = threading.Condition()
        self._election_thread = None

        self._leader = None

        self._disabled = False
        self._disabled_condition = threading.Condition()

    def initialize(self):

        self.kazoo.connect()

        for path in (self.LAUNCH_PATH, self.NODE_PATH, self.TERMINATING_PATH):
            self.kazoo.ensure_path(path)

    def shutdown(self):
        # depose the leader and cancel the election just in case
        try:
            self._leader.depose()
        except Exception, e:
            log.exception("Error deposing leader: %s", e)

        self.election.cancel()
        self._election_thread.kill()
        self.kazoo.close()

    def _connection_state_listener(self, state):
        # called by kazoo when the connection state changes.
        # handle in background
        gevent.spawn(self._handle_connection_state, state)

    def _handle_connection_state(self, state):

        if state in (KazooState.LOST, KazooState.SUSPENDED):
            with self._election_condition:
                self._election_enabled = False
                self._election_condition.notify_all()

            # depose the leader and cancel the election just in case
            try:
                self._leader.depose()
            except Exception, e:
                log.exception("Error deposing leader: %s", e)

            self.election.cancel()

        elif state == KazooState.CONNECTED:
            with self._election_condition:
                self._election_enabled = True
                self._election_condition.notify_all()

            self._update_disabled_state()

    def _disabled_watch(self, event):
        gevent.spawn(self._update_disabled_state)

    def _update_disabled_state(self):
        with self._disabled_condition:

            # check if the node exists and set up a callback
            exists = self.kazoo.exists(self.DISABLED_PATH,
                self._disabled_watch)
            if exists:
                if not self._disabled:
                    log.warn("Detected provisioner DISABLED state began")
                    self._disabled = True

                # when we detect disabled mode, we leave the participant pool.
                # this allows the leader to detect that all Provisioner workers
                # have stopped launching instances.
                self.party.leave()

            else:
                if self._disabled:
                    log.warn("Detected provisioner DISABLED state ended")
                    self._disabled = False

                self.party.join()

            self._disabled_condition.notify_all()

    def is_disabled(self):
        """Indicates that the Provisioner is in disabled mode, which means
        that no new launches will be allowed
        """
        return self._disabled

    def is_disabled_agreed(self):
        """Indicates that all Provisioner workers have recognized disabled mode

        This is used to determine whether it is safe to proceed with termination
        of all VMs as part of system shutdown
        """

        return not self.party.get_participant_count()

    def enable_provisioning(self):
        """Allow new instance launches
        """
        try:
            self.kazoo.delete(self.DISABLED_PATH)
        except NoNodeException:
            pass

    def disable_provisioning(self):
        """Disallow new instance launches
        """
        try:
            self.kazoo.create(self.DISABLED_PATH, "")
        except NodeExistsException:
            pass

    def contend_leader(self, leader):
        """Provide a leader object to participate in an election
        """
        assert self._leader is None
        self._leader = leader
        self._election_thread = gevent.spawn(self._run_election)

    def _run_election(self):
        """Election thread function
        """
        while True:
            with self._election_condition:
                while not self._election_enabled:
                    self._election_condition.wait()

                try:
                    self.election.run(self._leader.inaugurate)
                except Exception, e:
                    log.exception("Error in leader election: %s", e)

    #########################################################################
    # LAUNCHES
    #########################################################################

    def _make_launch_path(self, launch_id):
        if not launch_id:
            raise ValueError('invalid launch_id')
        return self.LAUNCH_PATH + "/" + launch_id

    def add_launch(self, launch):
        """
        Store a new launch record
        @param launch: launch dictionary
        @raise WriteConflictError if launch exists
        """
        launch_id = launch['launch_id']

        value = json.dumps(launch)
        try:
            self.kazoo.create(self._make_launch_path(launch_id), value)
        except NodeExistsException:
            raise WriteConflictError()

        # also add a version to the input dict.
        launch[VERSION_KEY] = 0

    def update_launch(self, launch):
        """
        @brief updates a launch record in the store
        @param launch Launch record to store
        """
        launch_id = launch['launch_id']
        version = launch[VERSION_KEY]

        # make a shallow copy so we can prune the version
        launch = launch.copy()
        del launch[VERSION_KEY]

        value = json.dumps(launch)

        try:
            stat = self.kazoo.set(self._make_launch_path(launch_id), value,
                version)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

        launch[VERSION_KEY] = stat['version']

    def get_launch(self, launch_id):
        """
        @brief Retrieves a launch record by id
        @param launch_id Id of launch record to retrieve
        @retval launch dictionary or None if not found
        """
        try:
            data, stat = self.kazoo.get(self._make_launch_path(launch_id))
        except NoNodeException:
            return None

        launch = json.loads(data)
        launch[VERSION_KEY] = stat['version']
        return launch

    def get_launches(self, state=None, min_state=None, max_state=None):
        """
        @brief Retrieves the latest record for all launches within a state range
        @param state Only retrieve nodes in this state.
        @param min_state Inclusive start bound
        @param max_state Inclusive end bound
        @retval list of launch records
        """
        try:
            children = self.kazoo.get_children(self.LAUNCH_PATH)
        except NoNodeException:
            raise NotFoundError()

        records = []
        for launch_id in children:
            launch = self.get_launch(launch_id)
            if launch:
                records.append(launch)
        return self._filter_records(records, state=state, min_state=min_state,
            max_state=max_state)

    def remove_launch(self, launch_id):
        """
        Remove a launch record from the store
        @param launch_id:
        @return:
        """
        try:
            self.kazoo.delete(self._make_launch_path(launch_id))
        except NoNodeException:
            raise NotFoundError()


    #########################################################################
    # NODES
    #########################################################################

    def _make_node_path(self, node_id):
        if not node_id:
            raise ValueError('invalid node_id')
        return "/node/" + node_id

    def add_node(self, node):
        """
        Store a new node record
        @param node: node dictionary
        @raise WriteConflictError if node exists
        """
        node_id = node['node_id']
        value = json.dumps(node)
        try:
            self.kazoo.create(self._make_node_path(node_id), value)
        except NodeExistsException:
            raise WriteConflictError()

        # also add a version to the input dict.
        node[VERSION_KEY] = 0

    def update_node(self, node):
        """
        @brief Updates an existing node record
        @param node Node record
        """
        node_id = node['node_id']
        version = node[VERSION_KEY]

        # make a shallow copy so we can prune the version
        node = node.copy()
        del node[VERSION_KEY]

        value = json.dumps(node)

        try:
            stat = self.kazoo.set(self._make_node_path(node_id), value,
                version)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

        node[VERSION_KEY] = stat['version']

    def get_node(self, node_id):
        """
        @brief Retrieves a launch record by id
        @param node_id Id of node record to retrieve
        @retval node record or None if not found
        """
        try:
            data, stat = self.kazoo.get(self._make_node_path(node_id))
        except NoNodeException:
            return None

        node = json.loads(data)
        node[VERSION_KEY] = stat['version']
        return node

    def get_nodes(self, state=None, min_state=None, max_state=None):
        """
        @brief Retrieves all launch record within a state range
        @param state Only retrieve nodes in this state.
        @param min_state Inclusive start bound.
        @param max_state Inclusive end bound
        @retval Deferred list of launch records
        """
        try:
            children = self.kazoo.get_children(self.NODE_PATH)
        except NoNodeException:
            raise NotFoundError()

        records = []
        for node_id in children:
            node = self.get_node(node_id)
            if node:
                records.append(node)
        return self._filter_records(records, state=state, min_state=min_state,
            max_state=max_state)

    def remove_node(self, node_id):
        """Remove a node record from the store
        """
        try:
            self.kazoo.delete(self._make_node_path(node_id))
        except NoNodeException:
            raise NotFoundError()

    #########################################################################
    # TERMINATING NODES
    #########################################################################

    def _make_terminating_path(self, node_id):
        if not node_id:
            raise ValueError('invalid node_id')
        return self.TERMINATING_PATH + "/" + node_id

    def add_terminating(self, node_id):
        """
        Store a new terminating node
        @param node_id
        @raise WriteConflictError if node exists
        """
        try:
            self.kazoo.create(self._make_terminating_path(node_id), node_id)
        except NodeExistsException:
            raise WriteConflictError()

    def get_terminating(self):
        def get_children():
            try:
                children = self.kazoo.get_children(self.TERMINATING_PATH)
            except NoNodeException:
                raise NotFoundError()

            return children

        children = get_children()
        while not children:
            gevent.sleep(1)
            children = get_children()

        records = []
        for node_id in children:
            records.append(node_id)
        return records

    def remove_terminating(self, node_id):
        try:
            self.kazoo.delete(self._make_terminating_path(node_id))
        except NoNodeException:
            raise NotFoundError()

    def _filter_records(self, records, state=None, min_state=None, max_state=None):

        # overrides range arguments
        if state:
            min_state = max_state = state

        filtered = []
        for record in records:
            if not max_state or record['state'] <= max_state:
                if not min_state or record['state'] >= min_state:
                    filtered.append(record)
        return filtered


def group_records(records, *args):
    """Breaks records into groups of distinct values for the specified keys

    Returns a dict of record lists, keyed by the distinct values.
    """
    sorted_records = list(records)
    if not args:
        raise ValueError('Must specify at least one key to group by')
    if len(args) == 1:
        keyf = lambda record: record.get(args[0], None)
    else:
        keyf = lambda record: tuple([record.get(key, None) for key in args])
    sorted_records.sort(key=keyf)
    groups = {}
    for key, group in groupby(sorted_records, keyf):
        groups[key] = list(group)
    return groups


def sanitize_record(record):
    """Strips record of Provisioner Store metadata

    @param record: record dictionary
    @return:
    """
    if VERSION_KEY in record:
        del record[VERSION_KEY]
    return record
