#!/usr/bin/env python

"""
@file epu/provisioner/store.py
@author David LaBissoniere
@brief Provisioner storage abstraction
"""
from itertools import groupby
import logging

import simplejson as json


log = logging.getLogger(__name__)



class ProvisionerStore(object):
    """In-memory version of Provisioner storage
    """
    def __init__(self):
        self.nodes = {}
        self.launches = {}

    def assure_schema(self):
        pass

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
