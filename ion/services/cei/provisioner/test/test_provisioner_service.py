#!/usr/bin/env python

"""
@file ion/services/cei/provisioner/test/test_provisioner_service.py
@author David LaBissoniere
@brief Test provisioner behavior
"""

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import uuid
import os

from twisted.internet import defer
from twisted.trial import unittest
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

from ion.services.cei.provisioner.provisioner_service import ProvisionerClient
from ion.services.cei.provisioner.test.util import FakeProvisionerNotifier
import ion.services.cei.states as states

def _new_id():
    return str(uuid.uuid4())

class ProvisionerServiceTest(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def _set_it_up(self):
        messaging = {'cei':{'name_type':'worker', 'args':{'scope':'local'}}}
        notifier = FakeProvisionerNotifier()
        procs = [{'name':'provisioner',
            'module':'ion.services.cei.provisioner.provisioner_service',
            'class':'ProvisionerService', 'spawnargs' : {'notifier' : notifier}},
            {'name':'dtrs','module':'ion.services.cei.dtrs',
                'class':'DeployableTypeRegistryService'}
        ]
        yield self._declare_messaging(messaging)
        supervisor = yield self._spawn_processes(procs)

        pId = yield self.procRegistry.get("provisioner")
        
        client = ProvisionerClient(pid=pId)
        defer.returnValue((client, notifier))

    @defer.inlineCallbacks
    def test_provisioner(self):

        # skip this test if IaaS credentials are unavailable
        maybe_skip_test()

        client, notifier = yield self._set_it_up()
        
        worker_node_count = 3
        deployable_type = 'base-cluster'
        nodes = {'head-node' : FakeLaunchItem(1, 'nimbus-test', 'small', None),
                'worker-node' : FakeLaunchItem(worker_node_count, 
                    'nimbus-test', 'small', None)}
        
        launch_id = _new_id()

        # sent to ProvisionerClient.query to make call be rpc-style
        query_kwargs={'rpc' : True}

        node_ids = [node_id for node in nodes.itervalues() 
                for node_id in node.instance_ids]
        self.assertEqual(len(node_ids), worker_node_count + 1)

        yield client.provision(launch_id, deployable_type, nodes)

        ok = yield notifier.wait_for_state(states.PENDING, node_ids)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(2))
        
        ok = yield notifier.wait_for_state(states.STARTED, node_ids, 
                before=client.query, before_kwargs=query_kwargs)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(3))

        yield client.terminate_launches(launch_id)
        
        ok = yield notifier.wait_for_state(states.TERMINATED, node_ids,
                before=client.query, before_kwargs=query_kwargs)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(5))

        self.assertEqual(len(notifier.nodes), len(node_ids))

class FakeLaunchItem(object):
    def __init__(self, count, site, allocation_id, data):
        self.instance_ids = [str(uuid.uuid4()) for i in range(count)]
        self.site = site 
        self.allocation_id = allocation_id
        self.data = data

def maybe_skip_test():
    """Some tests require IaaS credentials. Skip if they are not available
    """
    for key in ['NIMBUS_KEY', 'NIMBUS_SECRET', 
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
        if not os.environ.get(key):
            raise unittest.SkipTest('Test requires IaaS credentials, skipping')
