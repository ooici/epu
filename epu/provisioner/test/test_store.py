# Copyright 2013 University of Chicago

#!/usr/bin/env python

"""
@file epu/provisioner/test/test__store.py
@author David LaBissoniere
@brief Test provisioner store behavior
"""

import uuid
import logging
import unittest
import threading

from kazoo.exceptions import KazooException

from epu.provisioner.store import ProvisionerStore, ProvisionerZooKeeperStore,\
    group_records
from epu.states import InstanceState
from epu.exceptions import WriteConflictError
from epu.test import ZooKeeperTestMixin, SocatProxyRestartWrapper


# alias for shorter code
states = InstanceState

log = logging.getLogger(__name__)


class BaseProvisionerStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = ProvisionerStore()

    def test_get_unknown_launch(self):
        launch = self.store.get_launch("not-a-real-launch")
        self.assertIsNone(launch)

    def test_get_unknown_node(self):
        node = self.store.get_node("not-a-real-node")
        self.assertIsNone(node)

    def test_put_get_launches(self):

        launch_id_1 = new_id()
        l1 = {'launch_id': launch_id_1, 'state': states.REQUESTED}
        l1_dupe = l1.copy()
        self.store.add_launch(l1)

        # adding it again should error
        try:
            self.store.add_launch(l1_dupe)
        except WriteConflictError:
            pass
        else:
            self.fail("expected WriteConflictError")

        l1_read = self.store.get_launch(launch_id_1)
        self.assertEqual(launch_id_1, l1_read['launch_id'])
        self.assertEqual(states.REQUESTED, l1_read['state'])

        # now make two changes, one from the original and one from what we read
        l2 = l1.copy()
        l2['state'] = states.PENDING
        self.store.update_launch(l2)
        l2_read = self.store.get_launch(launch_id_1)
        self.assertEqual(launch_id_1, l2_read['launch_id'])
        self.assertEqual(states.PENDING, l2_read['state'])

        # this one should hit a write conflict
        l1_read['state'] = states.PENDING
        try:
            self.store.update_launch(l1_read)
        except WriteConflictError:
            pass
        else:
            self.fail("expected WriteConflictError")

        # store another launch altogether
        launch_id_2 = new_id()
        l3 = {'launch_id': launch_id_2, 'state': states.REQUESTED}
        self.store.add_launch(l3)

        latest = self.store.get_launch(launch_id_2)
        self.assertEqual(launch_id_2, latest['launch_id'])
        self.assertEqual(states.REQUESTED, latest['state'])

        all = self.store.get_launches()
        self.assertEqual(2, len(all))
        for l in all:
            self.assertTrue(l['launch_id'] in (launch_id_1, launch_id_2))

        # try some range queries
        requested = self.store.get_launches(state=states.REQUESTED)
        self.assertEqual(1, len(requested))
        self.assertEqual(launch_id_2, requested[0]['launch_id'])

        requested = self.store.get_launches(
            min_state=states.REQUESTED,
            max_state=states.REQUESTED)
        self.assertEqual(1, len(requested))
        self.assertEqual(launch_id_2, requested[0]['launch_id'])

        at_least_requested = self.store.get_launches(
            min_state=states.REQUESTED)
        self.assertEqual(2, len(at_least_requested))
        for l in at_least_requested:
            self.assertTrue(l['launch_id'] in (launch_id_1, launch_id_2))

        at_least_pending = self.store.get_launches(
            min_state=states.PENDING)
        self.assertEqual(1, len(at_least_pending))
        self.assertEqual(at_least_pending[0]['launch_id'], launch_id_1)

        at_most_pending = self.store.get_launches(
            max_state=states.PENDING)
        self.assertEqual(2, len(at_most_pending))
        for l in at_most_pending:
            self.assertTrue(l['launch_id'] in (launch_id_1, launch_id_2))

    def test_put_get_nodes(self):

        node_id_1 = new_id()
        n1 = {'node_id': node_id_1, 'state': states.REQUESTED}
        n1_dupe = n1.copy()
        self.store.add_node(n1)

        # adding it again should error
        try:
            self.store.add_node(n1_dupe)
        except WriteConflictError:
            pass
        else:
            self.fail("expected WriteConflictError")

        n1_read = self.store.get_node(node_id_1)
        self.assertEqual(node_id_1, n1_read['node_id'])
        self.assertEqual(states.REQUESTED, n1_read['state'])

        # now make two changes, one from the original and one from what we read
        n2 = n1.copy()
        n2['state'] = states.PENDING
        self.store.update_node(n2)
        n2_read = self.store.get_node(node_id_1)
        self.assertEqual(node_id_1, n2_read['node_id'])
        self.assertEqual(states.PENDING, n2_read['state'])

        # this one should hit a write conflict
        n1_read['state'] = states.PENDING
        try:
            self.store.update_node(n1_read)
        except WriteConflictError:
            pass
        else:
            self.fail("expected WriteConflictError")

        # store another node altogether
        node_id_2 = new_id()
        n3 = {'node_id': node_id_2, 'state': states.REQUESTED}
        self.store.add_node(n3)

        latest = self.store.get_node(node_id_2)
        self.assertEqual(node_id_2, latest['node_id'])
        self.assertEqual(states.REQUESTED, latest['state'])

        all = self.store.get_nodes()
        self.assertEqual(2, len(all))
        for n in all:
            self.assertTrue(n['node_id'] in (node_id_1, node_id_2))

        # try some range queries
        requested = self.store.get_nodes(state=states.REQUESTED)
        self.assertEqual(1, len(requested))
        self.assertEqual(node_id_2, requested[0]['node_id'])

        requested = self.store.get_nodes(
            min_state=states.REQUESTED,
            max_state=states.REQUESTED)
        self.assertEqual(1, len(requested))
        self.assertEqual(node_id_2, requested[0]['node_id'])

        at_least_requested = self.store.get_nodes(
            min_state=states.REQUESTED)
        self.assertEqual(2, len(at_least_requested))
        for n in at_least_requested:
            self.assertTrue(n['node_id'] in (node_id_1, node_id_2))

        at_least_pending = self.store.get_nodes(
            min_state=states.PENDING)
        self.assertEqual(1, len(at_least_pending))
        self.assertEqual(at_least_pending[0]['node_id'], node_id_1)

        at_most_pending = self.store.get_nodes(
            max_state=states.PENDING)
        self.assertEqual(2, len(at_most_pending))
        for n in at_most_pending:
            self.assertTrue(n['node_id'] in (node_id_1, node_id_2))


class ProvisionerZooKeeperStoreTests(BaseProvisionerStoreTests, ZooKeeperTestMixin):

    # this runs all of the BaseProvisionerStoreTests tests plus any
    # ZK-specific ones

    def setUp(self):
        self.setup_zookeeper()

        self.store = ProvisionerZooKeeperStore(self.zk_hosts,
            self.zk_base_path, use_gevent=self.use_gevent)
        self.store.initialize()

    def tearDown(self):
        if self.store:
            self.store.shutdown()
        self.teardown_zookeeper()

    def test_leader_election(self):
        leader = FakeLeader()
        self.store.contend_leader(leader)

        with leader.condition:
            if not leader.is_leader:
                leader.condition.wait(5)
                self.assertTrue(leader.is_leader)

    def test_disable(self):
        self.assertFalse(self.store.is_disabled())
        self.assertFalse(self.store.is_disabled_agreed())

        self.store.disable_provisioning()

        # peek into internals
        with self.store._disabled_condition:
            if not self.store.is_disabled():
                self.store._disabled_condition.wait(5)
        self.assertTrue(self.store.is_disabled())
        self.assertTrue(self.store.is_disabled_agreed())

        self.store.enable_provisioning()
        with self.store._disabled_condition:
            if self.store.is_disabled():
                self.store._disabled_condition.wait(5)
        self.assertFalse(self.store.is_disabled())
        self.assertFalse(self.store.is_disabled_agreed())


class FakeLeader(object):
    def __init__(self):
        self.condition = threading.Condition()
        self.is_leader = False

    def inaugurate(self):
        with self.condition:
            assert not self.is_leader
            self.is_leader = True
            self.condition.notify_all()
            self.condition.wait()

    def depose(self):
        with self.condition:
            assert self.is_leader
            self.is_leader = False
            self.condition.notify_all()


class ProvisionerZooKeeperStoreProxyKillTests(BaseProvisionerStoreTests, ZooKeeperTestMixin):

    # this runs all of the BaseProvisionerStoreTests tests plus any
    # ZK-specific ones, but uses a proxy in front of ZK and restarts
    # the proxy before each call to the store. The effect is that for each store
    # operation, the first call to kazoo fails with a connection error, but the
    # client should handle that and retry

    def setUp(self):
        self.setup_zookeeper(base_path_prefix="/provisioner_store_tests_", use_proxy=True)
        self.real_store = ProvisionerZooKeeperStore(self.zk_hosts,
            self.zk_base_path, use_gevent=self.use_gevent)

        self.real_store.initialize()

        # have the tests use a wrapped store that restarts the connection before each call
        self.store = SocatProxyRestartWrapper(self.proxy, self.real_store)

    def tearDown(self):
        if self.store:
            self.store.shutdown()
        self.teardown_zookeeper()

    def test_the_fixture(self):
        # make sure test fixture actually works like we think

        def fake_operation():
            self.store.kazoo.get("/")
        self.real_store.fake_operation = fake_operation

        self.assertRaises(KazooException, self.store.fake_operation)


class GroupRecordsTests(unittest.TestCase):

    def test_group_records(self):
        records = [
            {'site': 'chicago', 'allocation': 'big', 'name': 'sandwich'},
            {'name': 'pizza', 'allocation': 'big', 'site': 'knoxville'},
            {'name': 'burrito', 'allocation': 'small', 'site': 'chicago'}
        ]

        groups = group_records(records, 'site')
        self.assertEqual(len(groups.keys()), 2)
        chicago = groups['chicago']
        self.assertTrue(isinstance(chicago, list))
        self.assertEqual(len(chicago), 2)
        self.assertEqual(len(groups['knoxville']), 1)

        groups = group_records(records, 'site', 'allocation')
        self.assertEqual(len(groups.keys()), 3)
        chicago_big = groups[('chicago', 'big')]
        self.assertEqual(chicago_big[0]['allocation'], 'big')
        self.assertEqual(chicago_big[0]['site'], 'chicago')
        for group in groups.itervalues():
            self.assertEqual(len(group), 1)


def new_id():
    return str(uuid.uuid4())
