#!/usr/bin/env python

"""
@file epu/provisioner/store.py
@author David LaBissoniere
@brief Provisioner storage abstraction
"""
from itertools import groupby
import logging

import gevent
import simplejson as json

from epu.exceptions import WriteConflictError, NotFoundError


log = logging.getLogger(__name__)

VERSION_KEY = "__version"

class ProvisionerStore(object):
    """In-memory version of Provisioner storage
    """
    def __init__(self):
        self.nodes = {}
        self.launches = {}

        self.leader = None
        self.leader_thread = None
        self.is_leading = False

        self._disabled = False

    def is_disabled(self, watch=None):
        """Indicates that the Provisioner is in disabled mode, which means
        that no new launches will be allowed
        """
        return self._disabled

    def is_disabled_agreed(self, watch=None):
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
        state = launch['state']

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
