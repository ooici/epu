#!/usr/bin/env python

"""
@file epu/provisioner/test/test__store.py
@author David LaBissoniere
@brief Test provisioner store behavior
"""

import uuid
import logging
import unittest

from epu.provisioner.store import ProvisionerStore, group_records
from epu.states import InstanceState
from epu.exceptions import NotFoundError, WriteConflictError

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
        l1 = {'launch_id' : launch_id_1, 'state' : states.REQUESTED}
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
        l3 = {'launch_id' : launch_id_2, 'state' : states.REQUESTED}
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


class GroupRecordsTests(unittest.TestCase):

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
