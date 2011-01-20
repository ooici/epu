#!/usr/bin/env python

"""
@file ion/services/cei/provisioner/test/test__store.py
@author David LaBissoniere
@brief Test provisioner store behavior
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import uuid

from twisted.internet import defer
from twisted.trial import unittest
from ion.test.iontest import IonTestCase

from ion.services.cei.provisioner.store import *
from ion.services.cei import states

class CassandraProvisionerStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = CassandraProvisionerStore('localhost', 9160,
                                               'ProvisionerTests',
                                               'ooiuser', 'oceans11')
        self.store.initialize()
        self.store.activate()

    def tearDown(self):
        self.store.terminate()

    @defer.inlineCallbacks
    def test_put_get_launches(self):

        launch_id = new_id()
        l1 = {'launch_id' : launch_id, 'state' : states.REQUESTED}
        yield self.store.put_launch(l1)

        latest = yield self.store.get_launch(launch_id)
        self.assertEqual(launch_id, latest['launch_id'])
        self.assertEqual(states.REQUESTED, latest['state'])

        l2 = l1.copy()
        l2['state'] = states.PENDING
        yield self.store.put_launch(l2)
        latest = yield self.store.get_launch(launch_id)
        self.assertEqual(launch_id, latest['launch_id'])
        self.assertEqual(states.PENDING, latest['state'])

        # store another launch altogether
        another_launch_id = new_id()
        l3 = {'launch_id' : another_launch_id, 'state' : states.REQUESTED}
        yield self.store.put_launch(l3)

        latest = yield self.store.get_launch(another_launch_id)
        self.assertEqual(another_launch_id, latest['launch_id'])
        self.assertEqual(states.REQUESTED, latest['state'])

        # put the first launch record again, should not overwrite l2
        # because state is lower
        yield self.store.put_launch(l2)
        latest = yield self.store.get_launch(launch_id)
        self.assertEqual(launch_id, latest['launch_id'])
        self.assertEqual(states.PENDING, latest['state'])

        latest = yield self.store.get_launch(another_launch_id)
        self.assertEqual(another_launch_id, latest['launch_id'])
        self.assertEqual(states.REQUESTED, latest['state'])



class ProvisionerStoreTests(IonTestCase):
    """Testing the provisioner datastore abstraction
    """
    def setUp(self):
        self.store = ProvisionerStore()
    
    def tearDown(self):
        self.store = None
    
    @defer.inlineCallbacks
    def test_put_get_states(self):

        launch_id = new_id()
        
        records = [{'launch_id' : launch_id, 'node_id' : new_id(), 
            'state' : states.REQUESTED} for i in range(5)]

        yield self.store.put_records(records)

        result = yield self.store.get_all()
        self.assertEqual(len(result), len(records))

        one_rec = records[0]
        yield self.store.put_record(one_rec, states.PENDING)
        result = yield self.store.get_all()
        self.assertEqual(len(result), len(records)+1)
        
        result = yield self.store.get_all(launch_id, one_rec['node_id'])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['state'], states.PENDING)
        self.assertEqual(result[1]['state'], states.REQUESTED)

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


    @defer.inlineCallbacks
    def test_calc_record_age(self):
        launch_id = new_id()

        yield self.store.put_record({'launch_id': launch_id, 'node_id' : new_id(),
            'state' : states.REQUESTED
            })
        record = yield self.store.get_launch(launch_id)

        age = calc_record_age(record) 
        self.assertTrue(age >= 0)
        self.assertTrue(age < 5)
        

def new_id():
    return str(uuid.uuid4())
