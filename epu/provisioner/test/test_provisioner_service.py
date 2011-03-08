#!/usr/bin/env python

"""
@file cei/provisioner/test/test_provisioner_service.py
@author David LaBissoniere
@brief Test provisioner behavior
"""
from cei.provisioner.store import ProvisionerStore

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import uuid
import os

from twisted.internet import defer
from twisted.trial import unittest

from ion.test.iontest import IonTestCase
from ion.core import ioninit

from cei.ionproc.provisioner_service import ProvisionerClient
from cei.provisioner.test.util import FakeProvisionerNotifier
import cei.states as states

CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

def _new_id():
    return str(uuid.uuid4())


_BASE_CLUSTER_DOC = """
<cluster>
  <workspace>
    <name>head-node</name>
    <quantity>1</quantity>
    <image>base-cluster</image>
  </workspace>
  <workspace>
    <name>worker-node</name>
    <quantity>3</quantity>
    <image>base-cluster</image>
  </workspace>
</cluster>
"""

_BASE_CLUSTER_SITES = {
        'nimbus-test' : {
            'head-node' : {
                'image' : 'base-cluster',
            },
            'worker-node' : {
                'image' : 'base-cluster',
                }
            }
        }

_DT_REGISTRY = {'base-cluster': {
    'document': _BASE_CLUSTER_DOC,
    'sites': _BASE_CLUSTER_SITES, }
}

class ProvisionerServiceTest(IonTestCase):

    @itv(CONF)
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.store = ProvisionerStore()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def _set_it_up(self):
        messaging = {'cei':{'name_type':'worker', 'args':{'scope':'local'}}}
        notifier = FakeProvisionerNotifier()
        procs = [{'name':'provisioner',
            'module':'cei.ionproc.provisioner_service',
            'class':'ProvisionerService', 'spawnargs' :
                {'notifier' : notifier, 'store' : self.store}},
            {'name':'dtrs','module':'cei.ionproc.dtrs',
                'class':'DeployableTypeRegistryService',
                'spawnargs' : {'registry' : _DT_REGISTRY}
            }
        ]
        yield self._declare_messaging(messaging)
        yield self._spawn_processes(procs)

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

class ProvisionerServiceCassandraTest(ProvisionerServiceTest):
    @defer.inlineCallbacks
    def _set_it_up(self):
        messaging = {'cei':{'name_type':'worker', 'args':{'scope':'local'}}}
        notifier = FakeProvisionerNotifier()
        procs = [{'name':'provisioner',
            'module':'cei.ionproc.provisioner_service',
            'class':'ProvisionerService', 'spawnargs' :
                {'notifier' : notifier,
                 'cassandra_store':{'host':'localhost',
                                    'port':9160,
                                    'username':'ooiuser',
                                    'password':'oceans11',
                                    'keyspace':'CEIProvisioner',
                                    'prefix':str(uuid.uuid4())[:8]
                 }}},
            {'name':'dtrs','module':'cei.ionproc.dtrs',
                'class':'DeployableTypeRegistryService',
                'spawnargs' : {'registry' : _DT_REGISTRY}
            }
        ]
        yield self._declare_messaging(messaging)
        yield self._spawn_processes(procs)

        pId = yield self.procRegistry.get("provisioner")

        client = ProvisionerClient(pid=pId)
        defer.returnValue((client, notifier))

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


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
