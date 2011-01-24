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
        prefix = str(uuid.uuid4())[:8]
        self.store = CassandraProvisionerStore('localhost', 9160,
                                               'ProvisionerTests',
                                               'ooiuser', 'oceans11',
                                               prefix=prefix)
        self.store.initialize()
        self.store.activate()

        return self.store.create_schema()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.store.drop_schema()
        yield self.store.terminate()

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

        # try some range queries
        requested = yield self.store.get_launches(states.REQUESTED,
                                                  states.REQUESTED)
        log.debug(requested)
        self.assertEqual(1, len(requested))
        self.assertEqual(launch_id_2, requested[0]['launch_id'])

        at_least_requested = yield self.store.get_launches(states.REQUESTED)
        self.assertEqual(3, len(at_least_requested))
        for l in at_least_requested:
            self.assertTrue(l['launch_id'] in (launch_id_1, launch_id_2,
                                               launch_id_3))

        at_least_pending = yield self.store.get_launches(states.PENDING)
        self.assertEqual(2, len(at_least_pending))
        for l in at_least_pending:
            self.assertTrue(l['launch_id'] in (launch_id_1, launch_id_3))


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
