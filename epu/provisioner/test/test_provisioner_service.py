#!/usr/bin/env python

"""
@file epu/provisioner/test/test_provisioner_service.py
@author David LaBissoniere
@brief Test provisioner behavior
"""

import uuid
from libcloud.drivers.ec2 import EC2USWestNodeDriver
from nimboss.ctx import BrokerError
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
from epu import cassandra
from epu.test import cassandra_test

import epu.states as states
from epu.provisioner.store import ProvisionerStore, CassandraProvisionerStore

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

        self.notifier = FakeProvisionerNotifier()
        self.context_client = get_context_client()

        #overridden in child classes to allow more granular uses of @itv
        self.store = yield self.setup_store()

        procs = self.setup_processes()

        yield self._start_container()
        yield self._spawn_processes(procs)

        pId = yield self.procRegistry.get("provisioner")
        self.client = ProvisionerClient(pid=pId)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.teardown_store()
        yield self._shutdown_processes()
        yield self._stop_container()

    def setup_store(self):
        return defer.succeed(ProvisionerStore())

    def teardown_store(self):
        return defer.succeed(None)

    def setup_processes(self):
        return [{'name': 'provisioner',
                  'module': 'epu.ionproc.provisioner',
                  'class': 'ProvisionerService',
                  'spawnargs': {
                      'notifier': self.notifier,
                      'store': self.store,
                      'site_drivers' : provisioner.get_site_drivers(get_nimbus_test_sites()),
                      'context_client' : self.context_client}
                },
                {'name': 'dtrs', 'module': 'epu.ionproc.dtrs',
                 'class': 'DeployableTypeRegistryService',
                 'spawnargs': {'registry': _DT_REGISTRY}}
        ]

    @defer.inlineCallbacks
    def assertStoreNodeRecords(self, state, *node_ids):
        for node_id in node_ids:
            node = yield self.store.get_node(node_id)
            self.assertTrue(node)
            self.assertEqual(node['state'], state)

    @defer.inlineCallbacks
    def assertStoreLaunchRecord(self, state, launch_id):
        launch = yield self.store.get_launch(launch_id)
        self.assertTrue(launch)
        self.assertEqual(launch['state'], state)

    @defer.inlineCallbacks
    def test_provision_bad_dt(self):
        client = self.client
        notifier = self.notifier

        worker_node_count = 3
        deployable_type = 'this-doesnt-exist'
        nodes = {'head-node' : FakeLaunchItem(1, 'nimbus-test', 'small', None),
                'worker-node' : FakeLaunchItem(worker_node_count,
                    'nimbus-test', 'small', None)}

        launch_id = _new_id()

        node_ids = [node_id for node in nodes.itervalues()
                for node_id in node.instance_ids]
        self.assertEqual(len(node_ids), worker_node_count + 1)

        yield client.provision(launch_id, deployable_type, nodes, ('subscriber',))

        ok = yield notifier.wait_for_state(states.FAILED, node_ids)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(1))

        yield self.assertStoreNodeRecords(states.FAILED, *node_ids)
        yield self.assertStoreLaunchRecord(states.FAILED, launch_id)

    @defer.inlineCallbacks
    def test_provision_broker_error(self):
        client = self.client
        notifier = self.notifier

        worker_node_count = 3
        deployable_type = 'base-cluster'
        nodes = {'head-node' : FakeLaunchItem(1, 'nimbus-test', 'small', None),
                'worker-node' : FakeLaunchItem(worker_node_count,
                    'nimbus-test', 'small', None)}

        launch_id = _new_id()

        node_ids = [node_id for node in nodes.itervalues()
                for node_id in node.instance_ids]
        self.assertEqual(len(node_ids), worker_node_count + 1)

        self.context_client.create_error = BrokerError("fake failure")

        yield client.provision(launch_id, deployable_type, nodes, ('subscriber',))

        ok = yield notifier.wait_for_state(states.FAILED, node_ids)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(1))

        yield self.assertStoreNodeRecords(states.FAILED, *node_ids)
        yield self.assertStoreLaunchRecord(states.FAILED, launch_id)

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

        yield self.assertStoreNodeRecords(states.PENDING, *node_ids)
        yield self.assertStoreLaunchRecord(states.PENDING, launch_id)
        
        ok = yield notifier.wait_for_state(states.STARTED, node_ids, 
                before=client.query, before_kwargs=query_kwargs)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(3))
        yield self.assertStoreNodeRecords(states.STARTED, *node_ids)
        yield self.assertStoreLaunchRecord(states.PENDING, launch_id)

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
        yield self.assertStoreNodeRecords(states.TERMINATED, *node_ids)
        yield self.assertStoreLaunchRecord(states.TERMINATED, launch_id)

        self.assertEqual(len(notifier.nodes), len(node_ids))

    @defer.inlineCallbacks
    def test_provisioner_terminate_all(self):

        # This test would be better if it created more launches before calling terminate_all

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

        yield self.assertStoreNodeRecords(states.PENDING, *node_ids)
        yield self.assertStoreLaunchRecord(states.PENDING, launch_id)

        ok = yield notifier.wait_for_state(states.STARTED, node_ids,
                before=client.query, before_kwargs=query_kwargs)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(3))
        yield self.assertStoreNodeRecords(states.STARTED, *node_ids)
        yield self.assertStoreLaunchRecord(states.PENDING, launch_id)

        yield self.client.terminate_all(rpcwait=True)

        yield self.assertStoreNodeRecords(states.TERMINATED, *node_ids)
        yield self.assertStoreLaunchRecord(states.TERMINATED, launch_id)


class ProvisionerServiceCassandraTest(ProvisionerServiceTest):

    def __init__(self, *args, **kwargs):
        self.cassandra_mgr = None
        ProvisionerServiceTest.__init__(self, *args, **kwargs)

    @cassandra_test
    @defer.inlineCallbacks
    def setup_store(self):
        prefix=str(uuid.uuid4())[:8]
        username, password = cassandra.get_credentials()
        host, port = cassandra.get_host_port()

        cf_defs = CassandraProvisionerStore.get_column_families(prefix=prefix)
        ks = cassandra.get_keyspace(cf_defs)

        self.cassandra_mgr = cassandra.CassandraSchemaManager(ks)
        yield self.cassandra_mgr.create()

        store = provisioner.get_cassandra_store(host, username, password,
                                                ks.name, port=port,
                                                prefix=prefix)
        defer.returnValue(store)

    @defer.inlineCallbacks
    def teardown_store(self):
        if self.cassandra_mgr:
            yield self.cassandra_mgr.teardown()
            self.cassandra_mgr.disconnect()


class FakeLaunchItem(object):
    def __init__(self, count, site, allocation_id, data):
        self.instance_ids = [str(uuid.uuid4()) for i in range(count)]
        self.site = site 
        self.allocation_id = allocation_id
        self.data = data


class ErrorableContextClient(ProvisionerContextClient):
    def __init__(self, *args, **kwargs):
        self.create_error = None
        self.query_error = None
        ProvisionerContextClient.__init__(self, *args, **kwargs)

    def create(self):
        if self.create_error:
            return defer.fail(self.create_error)
        return ProvisionerContextClient.create(self)

    def query(self, resource):
        if self.query_error:
            return defer.fail(self.query_error)
        return ProvisionerContextClient.query(self, resource)


def get_context_client():
    return ErrorableContextClient(
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
                "port":8444,
                "ex_oldnimbus_xml":True
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
