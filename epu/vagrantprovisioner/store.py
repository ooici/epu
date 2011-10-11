#!/usr/bin/env python

"""
@file epu/provisioner/store.py
@author David LaBissoniere
@brief Provisioner storage abstraction
"""
from itertools import groupby

from telephus.cassandra.ttypes import CfDef
from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory
from twisted.internet import defer, reactor
import simplejson as json

from ion.util.timeout import timeout

import ion.util.ionlog

import epu.cassandra

CASSANDRA_TIMEOUT = epu.cassandra.get_timeout()

log = ion.util.ionlog.getLogger(__name__)

# The provisioner stores state information about instances in Cassandra.
# Because there may be multiple processes writing and we are dealing with
# external services without guaranteed consistency or latency, we must
# ensure that old records do not overwrite newer ones.
#
# There are two objects being persisted: Launches and Nodes. A Launch
# corresponds to a provisioner request: a set of node requests potentially
# across different IaaS providers. The share a context in the Context Broker.
# A Node is a single VM instance on a single provider. It is part of a launch
# and has the usual instance information (IP address, IaaS-specific ID, etc).
#
# Both launches and nodes proceed through a series of state changes. The
# state changes are one-way and idempotent. For example, once a node is in
# the RUNNING state, it will never go back to the PENDING state.
#
# There is a column family for launches and a separate one for nodes. Each
# row is keyed by the unique ID of the record (launch_id or node_id). Within
# each row there are one or more columns with state names. For example, the
# node column family may have these records:
#
# Nodes = {                                    # The column family
#   1ce8111c-2d4d-42af-9f74-117a1a92c1f5: {    # a single node
#       200-REQUESTED : 'the actual record',
#       400-PENDING : 'the actual record',
#       500-STARTED : 'the actual record',
#   }
#   8f91b758-2e03-409c-ac65-6bc7ccf15d37: {    # another node entirely
#       200-REQUESTED : 'the actual record',
#       400-PENDING : 'the actual record',
#       900-FAILED : 'the actual record',
#   }
#
# Since states are stored in sorted order within each row, you can always
# pick the last one and get the current state of the node. Because state
# changes are idempotent, multiple processes writing the same state record
# will resolve harmlessly. If a process tries to write an old state, it
# will not overwrite more recent ones.
#
# There is room for denormalization of data here, to speed up queries. For
# example, there could be structures for correlating IaaS sites to nodes.


class CassandraProvisionerStore(object):
    """
    Provides high level provisioner storage operations for Cassandra
    """

    # default size of paged fetches
    _PAGE_SIZE = 100
    LAUNCH_CF_NAME = "ProvisionerLaunches"
    NODE_CF_NAME = "ProvisionerNodes"

    @classmethod
    def get_column_families(cls, keyspace=None, prefix=''):
        """Builds a list of column families needed by this store.
        @param keyspace Name of keyspace. If None, it must be added manually.
        @param prefix Optional prefix for cf names. Useful for testing.
        @retval list of CfDef objects
        """
        launch_cf_name = prefix + cls.LAUNCH_CF_NAME
        node_cf_name = prefix + cls.NODE_CF_NAME
        return [CfDef(keyspace, launch_cf_name,
                  comparator_type='org.apache.cassandra.db.marshal.UTF8Type'),
            CfDef(keyspace, node_cf_name,
                  comparator_type='org.apache.cassandra.db.marshal.UTF8Type')]

    def __init__(self, host, port, username, password, keyspace, prefix=''):

        self._launch_column_family = prefix + self.LAUNCH_CF_NAME
        self._node_column_family = prefix + self.NODE_CF_NAME

        authz= {'username': username, 'password': password}

        self._manager = ManagedCassandraClientFactory(
            credentials=authz, check_api_version=True, keyspace=keyspace)
        self.client = CassandraClient(self._manager)

        self._host = host
        self._port = port
        self._connector = None

    def connect(self):
        self._connector = reactor.connectTCP(self._host, self._port,
                                             self._manager)

    def disconnect(self):
        self._manager.shutdown()
        if self._connector:
            self._connector.disconnect()
            self._connector = None

    @timeout(CASSANDRA_TIMEOUT)
    def put_launch(self, launch):
        """
        @brief Stores a single launch record
        @param launch Launch record to store
        @retval Deferred for success
        """
        launch_id = launch['launch_id']
        state = launch['state']
        value = json.dumps(launch)
        return self.client.insert(launch_id, self._launch_column_family,
                                  value, column=state)

    @timeout(CASSANDRA_TIMEOUT)
    @defer.inlineCallbacks
    def put_nodes(self, nodes):
        """
        @brief Stores a set of node records
        @param nodes Iterable of node records
        @retval Deferred for success
        """

        # could be more efficient with a batch_mutate
        for node in nodes:
            yield self.put_node(node)

    @timeout(CASSANDRA_TIMEOUT)
    def put_node(self, node):
        """
        @brief Stores a node record
        @param node Node record
        @retval Deferred for success
        """
        node_id = node['node_id']
        state = node['state']
        value = json.dumps(node)
        return self.client.insert(node_id, self._node_column_family, value,
                                  column=state)

    @timeout(CASSANDRA_TIMEOUT)
    def get_launch(self, launch_id, count=1):
        """
        @brief Retrieves a launch record by id
        @param launch_id Id of launch record to retrieve
        @param count Number of launch state records to retrieve
        @retval Deferred record(s), or None. A list of records if count > 1
        """
        return self._get_record(launch_id, self._launch_column_family, count)


    @timeout(CASSANDRA_TIMEOUT)
    def get_launches(self, state=None, min_state=None, max_state=None):
        """
        @brief Retrieves the latest record for all launches within a state range
        @param state Only retrieve nodes in this state.
        @param min_state Inclusive start bound
        @param max_state Inclusive end bound
        @retval Deferred list of launch records
        """
        return self._get_records(self._launch_column_family,
                                 state=state,
                                 min_state=min_state,
                                 max_state=max_state)

    @timeout(CASSANDRA_TIMEOUT)
    def get_node(self, node_id, count=1):
        """
        @brief Retrieves a launch record by id
        @param node_id Id of node record to retrieve
        @param count Number of node state records to retrieve
        @retval Deferred record(s), or None. A list of records if count > 1
        """
        return self._get_record(node_id, self._node_column_family, count)

    @timeout(CASSANDRA_TIMEOUT)
    def get_nodes(self, state=None, min_state=None, max_state=None):
        """
        @brief Retrieves all launch record within a state range
        @param state Only retrieve nodes in this state.
        @param min_state Inclusive start bound.
        @param max_state Inclusive end bound
        @retval Deferred list of launch records
        """
        return self._get_records(self._node_column_family,
                                 state=state,
                                 min_state=min_state,
                                 max_state=max_state)

    @defer.inlineCallbacks
    def _get_record(self, key, column_family, count):
        slice = yield self.client.get_slice(key, column_family,
                                            reverse=True, count=count)
        # we're probably only interested in the last record, in sorted order.
        # This is the latest state the object has recorded.
        records = [json.loads(column.column.value) for column in slice]

        if count == 1:
            if records:
                ret = records[0]
            else:
                ret = None
        else:
            ret = records
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def _get_records(self, column_family, state=None, min_state=None,
                     max_state=None, reverse=True):

        # overrides range arguments
        if state:
            min_state = max_state = state

        start = ''
        end = min_state or ''
        if not reverse:
            start, end = end, start

        # this is tricky. We are only concerned with the latest state record
        # (by sort order not necessarily time). So when we look for records
        # within a state range, we effectively must pull down the latest state
        # for each record, and filter them locally. This is slightly improved
        # when a first_state (or last when reverse=False) is specified as the
        # server can skip any records not >= that state. 

        records = []
        done = False
        start_key = ''
        iterations = 0
        while not done:
            slices = yield self.client.get_range_slices(column_family,
                                                        column_start=start,
                                                        column_finish=end,
                                                        reverse=reverse,
                                                        column_count=1,
                                                        start=start_key,
                                                        count=self._PAGE_SIZE)

            skipped_one = False
            for slice in slices:
                if not skipped_one and iterations:
                    # if this not the first batch, skip the first element as it
                    # will be a dupe.
                    skipped_one = True
                    continue

                if not slice.columns:
                    # rows without matching columns will still be returned
                    continue

                record = json.loads(slice.columns[0].column.value)
                if not max_state or record['state'] <= max_state:
                    if not min_state or record['state'] >= min_state:
                        records.append(record)

            # page through results. by default only 100 are returned at a time
            if len(slices) == self._PAGE_SIZE:
                start_key = slices[-1].key
            else:
                done = True
            iterations += 1

        defer.returnValue(records)

    def on_deactivate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_deactivate: Lose Connection TCP')

    def on_terminate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_terminate: Lose Connection TCP')


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
        return defer.succeed(None)

    @defer.inlineCallbacks
    def put_nodes(self, nodes):
        """
        @brief Stores a set of node records
        @param nodes Iterable of node records
        @retval Deferred for success
        """

        # could be more efficient with a batch_mutate
        for node in nodes:
            yield self.put_node(node)

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
        return defer.succeed(None)

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
        return defer.succeed(ret)


    def get_launches(self, state=None, min_state=None, max_state=None):
        """
        @brief Retrieves the latest record for all launches within a state range
        @param state Only retrieve nodes in this state.
        @param min_state Inclusive start bound
        @param max_state Inclusive end bound
        @retval Deferred list of launch records
        """
        records = self._get_records(self.launches, state, min_state, max_state)
        return defer.succeed(records)

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
        return defer.succeed(ret)

    def get_nodes(self, state=None, min_state=None, max_state=None):
        """
        @brief Retrieves all launch record within a state range
        @param state Only retrieve nodes in this state.
        @param min_state Inclusive start bound.
        @param max_state Inclusive end bound
        @retval Deferred list of launch records
        """
        records = self._get_records(self.nodes, state, min_state, max_state)
        return defer.succeed(records)

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
