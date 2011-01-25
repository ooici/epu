#!/usr/bin/env python

"""
@file ion/services/cei/provisioner/store.py
@author David LaBissoniere
@brief Provisioner storage abstraction
"""
from telephus.cassandra.ttypes import KsDef, CfDef
from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory

import ion.util.ionlog
from ion.util.tcp_connections import TCPConnection

log = ion.util.ionlog.getLogger(__name__)
import uuid
import time
from itertools import groupby
from twisted.internet import defer

try:
    import json
except ImportError:
    import simplejson as json

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


def _build_column_family_defs(keyspace, launch_family_name, node_family_name):
    return [CfDef(keyspace, launch_family_name,
              comparator_type='UTF8Type'),
        CfDef(keyspace, node_family_name,
              comparator_type='UTF8Type')]

def _build_keyspace_def(keyspace):
    column_family_defs = _build_column_family_defs(keyspace)
    ksdef = KsDef(name=keyspace,
                  replication_factor=1,
                  strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                  cf_defs=column_family_defs)
    return ksdef

class CassandraProvisionerStore(TCPConnection):
    """
    Provides high level provisioner storage operations for Cassandra
    """

    def __init__(self, host, port, keyspace, username, password, prefix=''):

        authorization_dictionary = {'username': username, 'password': password}

        self._keyspace = keyspace
        self._created_schema = False
        ### Create the twisted factory for the TCP connection
        self._manager = ManagedCassandraClientFactory(
                keyspace=self._keyspace,
                credentials=authorization_dictionary,
                check_api_version=True)

        # Call the initialization of the Managed TCP connection base class
        TCPConnection.__init__(self, host, port, self._manager)
        self.client = CassandraClient(self._manager)

        self._launch_column_family = prefix + 'Launch'
        self._node_column_family = prefix + 'Node'

    @defer.inlineCallbacks
    def create_schema(self):
        """
        @brief Sets up Cassandra column families
        @retval Deferred for success
        """
        log.debug('Creating Cassandra schema for provisioner. KS=%s: %s %s',
                  self._keyspace,
                  self._launch_column_family,
                  self._node_column_family)

        cfs = _build_column_family_defs(self._keyspace,
                                        self._launch_column_family,
                                        self._node_column_family)
        self.created_schema = True
        for cf in cfs:
            yield self.client.system_add_column_family(cf)

    @defer.inlineCallbacks
    def drop_schema(self, force=False):
        """
        @brief Drops the keyspace used by this object
        @param Whether to remove a keyspace not created by this instance
        @note By default, this method will refuse to drop a schema that was
        not created with the same instance of this class.
        @retval Deferred for success
        """
        if not self.created_schema and not force:
            raise ValueError("Refusing to drop schema we didn't create")

        cfs = [self._launch_column_family, self._node_column_family]
        log.debug('Dropping Cassandra column families for provisioner: %s',
                  ', '.join(cfs))
        for cf in cfs:
            yield self.client.system_drop_column_family(cf)

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
        value = json.dumps(node)
        return self.client.insert(node_id, self._node_column_family, value,
                                  column=state)

    def get_launch(self, launch_id, count=1):
        """
        @brief Retrieves a launch record by id
        @param launch_id Id of launch record to retrieve
        @param count Number of launch state records to retrieve
        @retval Deferred record(s), or None. A list of records if count > 1
        """
        return self._get_record(launch_id, self._launch_column_family, count)


    def get_launches(self, first_state=None, last_state=None):
        """
        @brief Retrieves the latest record for all launches within a state range
        @param first_state Inclusive start bound
        @param last_state Inclusive end bound
        @retval Deferred list of launch records
        """
        return self._get_records(self._launch_column_family,
                                 first_state=first_state,
                                 last_state=last_state)

    def get_node(self, node_id, count=1):
        """
        @brief Retrieves a launch record by id
        @param node_id Id of node record to retrieve
        @param count Number of node state records to retrieve
        @retval Deferred record(s), or None. A list of records if count > 1
        """
        return self._get_record(node_id, self._node_column_family, count)

    def get_nodes(self, first_state=None, last_state=None):
        """
        @brief Retrieves all launch record within a state range
        @param first_state Inclusive start bound
        @param last_state Inclusive end bound
        @retval Deferred list of launch records
        """
        return self._get_records(self._node_column_family,
                                 first_state=first_state,
                                 last_state=last_state)

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
    def _get_records(self, column_family, first_state=None, last_state=None, reverse=True):

        start = ''
        end = first_state or ''
        if not reverse:
            start, end = end, start

        # this is tricky. We are only concerned with the latest state record
        # (by sort order not necessarily time). So when we look for records
        # within a state range, we effectively must pull down the latest state
        # for each record, and filter them locally. This is slightly improved
        # when a first_state (or last when reverse=False) is specified as the
        # server can skip any records not >= that state. 

        slices = yield self.client.get_range_slices(column_family,
                                                    column_start=start,
                                                    column_finish=end,
                                                    reverse=reverse,
                                                    column_count=1)
        records = []
        for slice in slices:

            if not slice.columns:
                # rows without matching columns will still be returned
                continue

            record = json.loads(slice.columns[0].column.value)
            if not last_state or record['state'] <= last_state:
                if not first_state or record['state'] >= first_state:
                    records.append(record)

        defer.returnValue(records)
    
    def on_deactivate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_deactivate: Lose Connection TCP')

    def on_terminate(self, *args, **kwargs):
        self._manager.shutdown()
        log.info('on_terminate: Lose Connection TCP')


class ProvisionerStore(object):
    """Abstraction for data storage routines by provisioner
    """

    # Using a simple in-memory dict for now, until it is clear how
    # to use CEI datastore
    def __init__(self):
        self.data = {}

    def put_record(self, record, newstate=None, timestamp=None):
        """Stores a record, optionally first updating state.
        """
        if newstate:
            record['state'] = newstate

        #these two are expected to be on every record
        launch_id = record['launch_id']
        state = record['state']

        #this one will be missing for launch records
        node_id = record.get('node_id', '')

        newid = str(uuid.uuid4())
        ts = str(timestamp or int(time.time() * 1e6))

        record['state_timestamp'] = ts
        key = '|'.join([launch_id, node_id, state, ts, newid])
        self.data[key] = json.dumps(record)
        log.debug('Added provisioner state: "%s"', key)
        return defer.succeed(key)

    def put_records(self, records, newstate=None, timestamp=None):
        """Stores a list of records, optionally first updating state.
        """
        ts = str(timestamp or int(time.time() * 1e6))
        return [self.put_record(r, newstate=newstate, timestamp=ts)
                for r in records]

    @defer.inlineCallbacks
    def get_site_nodes(self, site, before_state=None):
        """Retrieves the latest node record for all nodes at a site.
        """
        #for performance, we would probably want to store these
        # records denormalized in the store, by site id
        all = yield self.get_all()
        groups = group_records(all, 'node_id')
        site_nodes = []
        for node_id, records in groups.iteritems():
            if node_id and records[0]['site'] == site:
                site_nodes.append(records[0])
        defer.returnValue(site_nodes)

    @defer.inlineCallbacks
    def get_launches(self, state=None):
        """Retrieves all launches in the given state, or the latest state
        of all launches if state is unspecified.
        """
        records = yield self.get_all(node='')
        groups = group_records(records, 'launch_id')
        launches = []
        for launch_id, records in groups.iteritems():
            latest = records[0]
            if state:
                if latest['state'] == state:
                    launches.append(latest)
            else:
                launches.append(latest)
        defer.returnValue(launches)

    @defer.inlineCallbacks
    def get_launch(self, launch):
        """Retrieves the latest launch record, from the launch_id.
        """
        records = yield self.get_all(launch, '')
        defer.returnValue(records[0])

    @defer.inlineCallbacks
    def get_launch_nodes(self, launch):
        """Retrieves the latest node records, from the launch_id.
        """
        records = yield self.get_all(launch)
        groups = group_records(records, 'node_id')
        nodes = []
        for node_id, records in groups.iteritems():
            if node_id:
                nodes.append(records[0])
        defer.returnValue(nodes)

    @defer.inlineCallbacks
    def get_nodes_by_id(self, node_ids):
        """Retrieves the latest node records, from a list of node_ids
        """
        records = yield self.get_all()
        groups = group_records(records, 'node_id')
        nodes = []
        for node_id in node_ids:
            records = groups.get(node_id)
            if records:
                nodes.append(records[0])
            else:
                nodes.append(None)
        defer.returnValue(nodes)

    def get_all(self, launch=None, node=None):
        """Retrieves the states about an instance or launch.

        States are returned in order.
        """
        prefix = ''
        if launch:
            prefix = '%s|' % launch
            if node:
                prefix += '%s|' % node
        #TODO uhhh. regex..? don't know what matching functionality we
        # actually need here yet.

        matches = [(s[0], json.loads(s[1])) for s in self.data.iteritems()
                if s[0].startswith(prefix)]
        matches.sort(reverse=True)
        records = [r[1] for r in matches]
        return defer.succeed(records)

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

def calc_record_age(record):
    """Calculates the time since a record's timestamp, in seconds (float)
    """
    now = time.time()
    return now - (long(record['state_timestamp']) / 1e6)
