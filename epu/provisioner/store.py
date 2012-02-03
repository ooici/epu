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


log = logging.getLogger(__name__)



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

    def put_launch(self, launch):
        """
        @brief Stores a single launch record
        @param launch Launch record to store
        @retval Deferred for success
        """
        launch_id = launch['launch_id']
        state = launch['state']

        existing = self.launches.get(launch_id)
        if not existing or json.loads(existing)['state'] <= state:
            self.launches[launch_id] = json.dumps(launch)
        return

    def put_nodes(self, nodes):
        """
        @brief Stores a set of node records
        @param nodes Iterable of node records
        @retval Deferred for success
        """

        # could be more efficient with a batch_mutate
        for node in nodes:
            self.put_node(node)

    def put_node(self, node):
        """
        @brief Stores a node record
        @param node Node record
        @retval Deferred for success
        """
        node_id = node['node_id']
        state = node['state']

        existing = self.nodes.get(node_id)
        if not existing or json.loads(existing)['state'] <= state:
                self.nodes[node_id] = json.dumps(node)
        return None

    def get_launch(self, launch_id, count=1):
        """
        @brief Retrieves a launch record by id
        @param launch_id Id of launch record to retrieve
        @param count Number of launch state records to retrieve
        @retval Deferred record(s), or None. A list of records if count > 1
        """
        assert count == 1
        record = self.launches.get(launch_id)
        if record:
            ret = json.loads(record)
        else:
            ret = None
        return ret


    def get_launches(self, state=None, min_state=None, max_state=None):
        """
        @brief Retrieves the latest record for all launches within a state range
        @param state Only retrieve nodes in this state.
        @param min_state Inclusive start bound
        @param max_state Inclusive end bound
        @retval Deferred list of launch records
        """
        records = self._get_records(self.launches, state, min_state, max_state)
        return records

    def get_node(self, node_id, count=1):
        """
        @brief Retrieves a launch record by id
        @param node_id Id of node record to retrieve
        @param count Number of node state records to retrieve
        @retval Deferred record(s), or None. A list of records if count > 1
        """
        assert count == 1
        record = self.nodes.get(node_id)
        if record:
            ret = json.loads(record)
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

    def _get_records(self, dct, state=None, min_state=None, max_state=None):

        # overrides range arguments
        if state:
            min_state = max_state = state

        records = []
        for r in dct.itervalues():
            record = json.loads(r)
            if not max_state or record['state'] <= max_state:
                if not min_state or record['state'] >= min_state:
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
