#!/usr/bin/env python

"""
@file epu/provisioner/test/test__store.py
@author David LaBissoniere
@brief Test provisioner store behavior
"""

import uuid

from twisted.internet import defer
from twisted.trial import unittest
from ion.test.iontest import IonTestCase
from ion.core import ioninit
from epu.cassandra import CassandraSchemaManager
import epu.cassandra as cassandra

from epu.provisioner.store import CassandraProvisionerStore, \
    ProvisionerStore, group_records
from epu import states

CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class BaseProvisionerStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = ProvisionerStore()
    def tearDown(self):
        self.store = None

    @defer.inlineCallbacks
    def test_put_get_launches(self):

        launch_id_1 = new_id()
        l1 = {'launch_id' : launch_id_1, 'state' : states.REQUESTED}
        yield self.store.put_launch(l1)

        latest = yield self.store.get_launch(launch_id_1)
        self.assertEqual(launch_id_1, latest['launch_id'])
        self.assertEqual(states.REQUESTED, latest['state'])

        l2 = l1.copy()
        l2['state'] = states.PENDING
        yield self.store.put_launch(l2)
        latest = yield self.store.get_launch(launch_id_1)
        self.assertEqual(launch_id_1, latest['launch_id'])
        self.assertEqual(states.PENDING, latest['state'])

        # store another launch altogether
        launch_id_2 = new_id()
        l3 = {'launch_id' : launch_id_2, 'state' : states.REQUESTED}
        yield self.store.put_launch(l3)

        latest = yield self.store.get_launch(launch_id_2)
        self.assertEqual(launch_id_2, latest['launch_id'])
        self.assertEqual(states.REQUESTED, latest['state'])

        # put the first launch record again, should not overwrite l2
        # because state is lower
        yield self.store.put_launch(l2)
        latest = yield self.store.get_launch(launch_id_1)
        self.assertEqual(launch_id_1, latest['launch_id'])
        self.assertEqual(states.PENDING, latest['state'])

        latest = yield self.store.get_launch(launch_id_2)
        self.assertEqual(launch_id_2, latest['launch_id'])
        self.assertEqual(states.REQUESTED, latest['state'])

        # add a third launch with request, pending, and running records
        launch_id_3 = new_id()
        l4 = {'launch_id' : launch_id_3, 'state' : states.REQUESTED}
        yield self.store.put_launch(l4)
        l5 = {'launch_id' : launch_id_3, 'state' : states.PENDING}
        yield self.store.put_launch(l5)
        l6 = {'launch_id' : launch_id_3, 'state' : states.RUNNING}
        yield self.store.put_launch(l6)

        all = yield self.store.get_launches()
        self.assertEqual(3, len(all))
        for l in all:
            self.assertTrue(l['launch_id'] in (launch_id_1, launch_id_2,
                                               launch_id_3))

        # try some range queries
        requested = yield self.store.get_launches(state=states.REQUESTED)
        self.assertEqual(1, len(requested))
        self.assertEqual(launch_id_2, requested[0]['launch_id'])

        requested = yield self.store.get_launches(
                min_state=states.REQUESTED,
                max_state=states.REQUESTED)
        self.assertEqual(1, len(requested))
        self.assertEqual(launch_id_2, requested[0]['launch_id'])

        at_least_requested = yield self.store.get_launches(
                min_state=states.REQUESTED)
        self.assertEqual(3, len(at_least_requested))
        for l in at_least_requested:
            self.assertTrue(l['launch_id'] in (launch_id_1, launch_id_2,
                                               launch_id_3))

        at_least_pending = yield self.store.get_launches(
                min_state=states.PENDING)
        self.assertEqual(2, len(at_least_pending))
        for l in at_least_pending:
            self.assertTrue(l['launch_id'] in (launch_id_1, launch_id_3))

        at_most_pending = yield self.store.get_launches(
            max_state=states.PENDING)
        self.assertEqual(2, len(at_most_pending))
        for l in at_most_pending:
            self.assertTrue(l['launch_id'] in (launch_id_1, launch_id_2))

    @defer.inlineCallbacks
    def put_node(self, node_id, *states):
        for state in states:
            record = {'node_id' : node_id, 'state':state}
            yield self.store.put_node(record)

    @defer.inlineCallbacks
    def put_many_nodes(self, count, *states):
        node_ids = set(str(uuid.uuid4()) for i in range(count))
        for node_id in node_ids:
            yield self.put_node(node_id, *states)
        defer.returnValue(node_ids)

    def assertNodesInSet(self, nodes, *sets):
        node_ids = set(node["node_id"] for node in nodes)
        self.assertEqual(len(nodes), len(node_ids))

        for node_id in node_ids:
            for aset in sets:
                if node_id in aset:
                    return
        self.fail("node not in any set")

class CassandraProvisionerStoreTests(BaseProvisionerStoreTests):
    """Runs same tests as BaseProvisionerStoreTests but cassandra backend
    """

    def setUp(self):
        return self.setup_cassandra()

    @itv(CONF)
    @defer.inlineCallbacks
    def setup_cassandra(self):
        prefix = str(uuid.uuid4())[:8]
        cf_defs = CassandraProvisionerStore.get_column_families(prefix=prefix)
        ks = cassandra.get_keyspace(cf_defs)

        self.cassandra_mgr = CassandraSchemaManager(ks)
        yield self.cassandra_mgr.create()

        host, port = cassandra.get_host_port()
        username, password = cassandra.get_credentials()
        
        self.store = CassandraProvisionerStore(host, port, username, password,
                                               keyspace=ks.name, prefix=prefix)
        self.store.connect()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.cassandra_mgr.teardown()
        self.cassandra_mgr.disconnect()
        self.store.disconnect()

    @defer.inlineCallbacks
    def test_paging(self):
        requested = yield self.put_many_nodes(3, states.REQUESTED)
        pending = yield self.put_many_nodes(140, states.REQUESTED,
                                            states.PENDING)
        running = yield self.put_many_nodes(160, states.REQUESTED,
                                            states.PENDING,
                                            states.RUNNING)
        terminated = yield self.put_many_nodes(120, states.REQUESTED,
                                               states.PENDING,
                                               states.RUNNING,
                                               states.TERMINATING,
                                               states.TERMINATED)
        failed = yield self.put_many_nodes(100, states.REQUESTED,
                                           states.FAILED)

        nodes = yield self.store.get_nodes(state=states.TERMINATED)
        self.assertEqual(len(nodes), 120)
        self.assertNodesInSet(nodes, terminated)

        nodes = yield self.store.get_nodes()
        self.assertEqual(len(nodes), 523)
        self.assertNodesInSet(nodes, requested, pending, running, terminated,
                              failed)

        nodes = yield self.store.get_nodes(state=states.FAILED)
        self.assertEqual(len(nodes), 100)
        self.assertNodesInSet(nodes, failed)

        nodes = yield self.store.get_nodes(min_state=states.REQUESTED)
        self.assertEqual(len(nodes), 523)
        self.assertNodesInSet(nodes, requested, pending, running, terminated,
                              failed)

        nodes = yield self.store.get_nodes(min_state=states.PENDING,
                                           max_state=states.RUNNING)
        self.assertEqual(len(nodes), 300)
        self.assertNodesInSet(nodes, pending, running)

        nodes = yield self.store.get_nodes(states.TERMINATING)
        self.assertEqual(len(nodes), 0)

        nodes = yield self.store.get_nodes(max_state=states.RUNNING)
        self.assertEqual(len(nodes), 303)
        self.assertNodesInSet(nodes, requested, pending, running)

    @defer.inlineCallbacks
    @itv(CONF)
    def test_clientbusy(self):
        node1_id = str(uuid.uuid4())
        node2_id = str(uuid.uuid4())

        # first store node1 record completely
        yield self.store.put_node(dict(node_id=node1_id, state=states.PENDING))

        # now attempt to store node2 and read node1 simultaneously
        d1 = self.store.put_node(dict(node_id=node2_id, state=states.PENDING))
        d2 =  self.store.get_node(node1_id)

        # wait for both to complete
        yield d2
        yield d1


class GroupRecordsTests(IonTestCase):

    def test_group_records(self):
        records = [
                {'site' : 'chicago', 'allocation' : 'big', 'name' : 'sandwich'},
                {'name' : 'pizza', 'allocation' : 'big', 'site' : 'knoxville'},
                {'name' : 'burrito', 'allocation' : 'small', 'site' : 'chicago'}
                ]

        groups = group_records(records, 'site')
        self.assertEqual(len(groups.keys()), 2)
        chicago = groups['chicago']
        self.assertTrue(isinstance(chicago, list))
        self.assertEqual(len(chicago), 2)
        self.assertEqual(len(groups['knoxville']), 1)

        groups = group_records(records, 'site', 'allocation')
        self.assertEqual(len(groups.keys()), 3)
        chicago_big = groups[('chicago','big')]
        self.assertEqual(chicago_big[0]['allocation'], 'big')
        self.assertEqual(chicago_big[0]['site'], 'chicago')
        for group in groups.itervalues():
            self.assertEqual(len(group), 1)


def new_id():
    return str(uuid.uuid4())
