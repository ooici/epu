#!/usr/bin/env python

"""
@file epu/provisioner/test/test_provisioner_service.py
@author David LaBissoniere
@brief Test provisioner behavior
"""

import uuid
from libcloud.drivers.ec2 import EC2USWestNodeDriver
from nimboss.node import NimbusNodeDriver
import os

from twisted.internet import defer
from twisted.trial import unittest

import ion.util.ionlog
from ion.test.iontest import IonTestCase
from ion.core import ioninit
from ion.util.itv_decorator import itv

from epu.ionproc import provisioner
from epu.ionproc.provisioner import ProvisionerClient
from epu.provisioner.core import ProvisionerContextClient
from epu.provisioner.test.util import FakeProvisionerNotifier

import epu.states as states
from epu.provisioner.store import ProvisionerStore

log = ion.util.ionlog.getLogger(__name__)

CONF = ioninit.config(__name__)

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

SITES_DICT = {
    "ec2-west": {
        "driver_class": "libcloud.drivers.ec2.EC2USWestNodeDriver",
        "driver_kwargs": {
            "key": "myec2key",
            "secret": "myec2secret"
        }
    },
    "nimbus-test": {
        "driver_class": "nimboss.node.NimbusNodeDriver",
        "driver_kwargs": {
            "key": "mynimbuskey",
            "secret": "mynimbussecret",
            "host": "nimbus.ci.uchicago.edu",
            "port": 8444
        }
    }
}
class ProvisionerConfigTest(unittest.TestCase):

    def test_get_site_drivers(self):
        site_drivers = provisioner.get_site_drivers(SITES_DICT)
        nimbus_test = site_drivers['nimbus-test']
        ec2_west = site_drivers['ec2-west']
        self.assertIsInstance(nimbus_test, NimbusNodeDriver)
        self.assertIsInstance(ec2_west, EC2USWestNodeDriver)
        self.assertEqual(nimbus_test.key, 'mynimbuskey')
        self.assertEqual(ec2_west.key, 'myec2key')


class ProvisionerServiceTest(IonTestCase):

    # these integration tests can run a little long
    timeout = 60

    @itv(CONF)
    @defer.inlineCallbacks
    def setUp(self):
        # skip this test if IaaS credentials are unavailable
        maybe_skip_test()

        self.store = ProvisionerStore()
        self.notifier = FakeProvisionerNotifier()

        #overridden in child classes to allow more granular uses of @itv
        procs = self.setup_processes()

        yield self._start_container()
        yield self._spawn_processes(procs)

        pId = yield self.procRegistry.get("provisioner")
        self.client = ProvisionerClient(pid=pId)


    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    def setup_processes(self):
        return [{'name': 'provisioner',
                  'module': 'epu.ionproc.provisioner',
                  'class': 'ProvisionerService',
                  'spawnargs': {
                      'notifier': self.notifier,
                      'store': self.store,
                      'site_drivers' : provisioner.get_site_drivers(get_nimbus_test_sites()),
                      'context_client' : get_context_client()}
                },
                {'name': 'dtrs', 'module': 'epu.ionproc.dtrs',
                 'class': 'DeployableTypeRegistryService',
                 'spawnargs': {'registry': _DT_REGISTRY}}
        ]

    @defer.inlineCallbacks
    def test_provisioner(self):

        client = self.client
        notifier = self.notifier
        
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

        yield client.provision(launch_id, deployable_type, nodes, ('subscriber',))

        ok = yield notifier.wait_for_state(states.PENDING, node_ids)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(2))
        
        ok = yield notifier.wait_for_state(states.STARTED, node_ids, 
                before=client.query, before_kwargs=query_kwargs)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(3))

        # terminate two nodes by name, then the launch as a whole
        yield client.terminate_nodes(node_ids[:2])
        ok = yield notifier.wait_for_state(states.TERMINATED, node_ids[:2],
                before=client.query, before_kwargs=query_kwargs)
        self.assertTrue(ok)

        yield client.terminate_launches([launch_id])
        ok = yield notifier.wait_for_state(states.TERMINATED, node_ids,
                before=client.query, before_kwargs=query_kwargs)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(5))

        self.assertEqual(len(notifier.nodes), len(node_ids))

class ProvisionerServiceCassandraTest(ProvisionerServiceTest):

    def setup_processes(self):
        return self.setup_cassandra()

    @itv(CONF)
    def setup_cassandra(self):
        store = provisioner.get_cassandra_store('localhost',
                                                'ooiuser',
                                                'oceans11',
                                                prefix=str(uuid.uuid4())[:8])
        return [{'name':'provisioner',
            'module':'epu.ionproc.provisioner',
            'class':'ProvisionerService',
            'spawnargs':{
                'store' : store,
                'notifier': self.notifier,
                'site_drivers' : provisioner.get_site_drivers(get_nimbus_test_sites()),
                'context_client' : get_context_client()
                 }},
            {'name':'dtrs','module':'epu.ionproc.dtrs',
                'class':'DeployableTypeRegistryService',
                'spawnargs' : {'registry' : _DT_REGISTRY}
            }
        ]

class FakeLaunchItem(object):
    def __init__(self, count, site, allocation_id, data):
        self.instance_ids = [str(uuid.uuid4()) for i in range(count)]
        self.site = site 
        self.allocation_id = allocation_id
        self.data = data

def get_context_client():
    return ProvisionerContextClient(
        "https://nimbus.ci.uchicago.edu:8888/ContextBroker/ctx/",
        os.environ["NIMBUS_KEY"],
        os.environ["NIMBUS_SECRET"])

def get_nimbus_test_sites():
    return {
        'nimbus-test' : {
            "driver_class" : "nimboss.node.NimbusNodeDriver",
            "driver_kwargs" : {
                "key":os.environ['NIMBUS_KEY'],
                "secret":os.environ['NIMBUS_SECRET'],
                "host":"nimbus.ci.uchicago.edu",
                "port":8444
            }
        }
    }
def maybe_skip_test():
    """Some tests require IaaS credentials. Skip if they are not available
    """
    for key in ['NIMBUS_KEY', 'NIMBUS_SECRET', 
            'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
        if not os.environ.get(key):
            raise unittest.SkipTest('Test requires IaaS credentials, skipping')
