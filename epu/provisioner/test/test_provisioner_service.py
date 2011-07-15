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

from epu.ionproc import provisioner
from epu.ionproc.provisioner import ProvisionerClient
from epu.provisioner.core import ProvisionerContextClient
from epu.provisioner.test.util import FakeProvisionerNotifier, \
    FakeNodeDriver, FakeContextClient, make_launch, make_node, \
    make_launch_and_nodes
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
    'nimbus-test': {
        'head-node': {
            'image': 'base-cluster',
            },
        'worker-node': {
            'image': 'base-cluster',
            }
    },
    'fake-site1': {
        'head-node': {
            'image': 'base-cluster',
            },
        'worker-node': {
            'image': 'base-cluster',
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


class BaseProvisionerServiceTests(IonTestCase):

    def __init__(self, *args, **kwargs):
        IonTestCase.__init__(self, *args, **kwargs)

        # these are to be set in a subclass' setUp()
        self.store = None
        self.notifier = None
        self.site_drivers = None
        self.context_client = None

        self.dt_registry = _DT_REGISTRY

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

    def get_procs(self):
        return [{'name': 'provisioner',
                  'module': 'epu.ionproc.provisioner',
                  'class': 'ProvisionerService',
                  'spawnargs': {
                      'notifier': self.notifier,
                      'store': self.store,
                      'site_drivers' : self.site_drivers,
                      'context_client' : self.context_client}
                },
                {'name': 'dtrs', 'module': 'epu.ionproc.dtrs',
                 'class': 'DeployableTypeRegistryService',
                 'spawnargs': {'registry': self.dt_registry}}
        ]

class ProvisionerServiceTest(BaseProvisionerServiceTests):
    """Integration tests that use fake context broker and IaaS driver fixtures
    """
    @defer.inlineCallbacks
    def setUp(self):

        self.notifier = FakeProvisionerNotifier()
        self.context_client = FakeContextClient()

        self.store = yield self.setup_store()
        self.site_drivers = {'fake-site1' : FakeNodeDriver()}

        procs = self.get_procs()
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

    @defer.inlineCallbacks
    def test_provision_bad_dt(self):
        client = self.client
        notifier = self.notifier

        worker_node_count = 3
        deployable_type = 'this-doesnt-exist'
        nodes = {'head-node' : FakeLaunchItem(1, 'fake-site1', 'small', None),
                'worker-node' : FakeLaunchItem(worker_node_count,
                    'fake-site1', 'small', None)}

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
        nodes = {'head-node' : FakeLaunchItem(1, 'fake-site1', 'small', None),
                'worker-node' : FakeLaunchItem(worker_node_count,
                    'fake-site1', 'small', None)}

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
    def test_dump_state(self):
        running_launch, running_nodes = make_launch_and_nodes(_new_id(), 10, states.RUNNING)
        yield self.store.put_launch(running_launch)
        yield self.store.put_nodes(running_nodes)

        pending_launch, pending_nodes = make_launch_and_nodes(_new_id(), 3, states.PENDING)
        yield self.store.put_launch(pending_launch)
        yield self.store.put_nodes(pending_nodes)

        running_node_ids = [node['node_id'] for node in running_nodes]
        pending_node_ids = [node['node_id'] for node in pending_nodes]
        all_node_ids = running_node_ids + pending_node_ids

        yield self.client.dump_state(running_node_ids)
        ok = yield self.notifier.wait_for_state(states.RUNNING, nodes=running_node_ids)
        self.assertTrue(ok)
        self.assertEqual(len(self.notifier.nodes), len(running_nodes))

        yield self.client.dump_state(pending_node_ids)
        ok = yield self.notifier.wait_for_state(states.PENDING, nodes=pending_node_ids)
        self.assertTrue(ok)
        self.assertEqual(len(self.notifier.nodes), len(all_node_ids))

        # we should have not gotten any dupe records yet
        self.assertTrue(self.notifier.assure_record_count(1))

        # empty dump request should dump nothing
        yield self.client.dump_state([])
        self.assertTrue(self.notifier.assure_record_count(1))

    @defer.inlineCallbacks
    def test_dump_state_unknown_node(self):
        node_ids = ["09ddd3f8-a5a5-4196-ac13-eab4d4b0c777"]
        subscribers = ["hello1_subscriber"]
        yield self.client.dump_state(node_ids, force_subscribe=subscribers[0])
        ok = yield self.notifier.wait_for_state(states.FAILED, nodes=node_ids)
        self.assertTrue(ok)
        self.assertEqual(len(self.notifier.nodes), len(node_ids))
        for node_id in node_ids:
            ok = yield self.notifier.assure_subscribers(node_id, subscribers)
            self.assertTrue(ok)

    @defer.inlineCallbacks
    def test_terminate(self):
        launch_id = _new_id()
        running_launch, running_nodes = make_launch_and_nodes(launch_id, 10,
                                                              states.RUNNING,
                                                              site="fake-site1")
        yield self.store.put_launch(running_launch)
        yield self.store.put_nodes(running_nodes)

        node_ids = [node['node_id'] for node in running_nodes]

        # terminate half of the nodes then the launch as a whole
        first_five = node_ids[:5]
        yield self.client.terminate_nodes(first_five)
        ok = yield self.notifier.wait_for_state(states.TERMINATED, nodes=first_five)
        self.assertTrue(ok)
        self.assertEqual(set(first_five), set(self.notifier.nodes))

        yield self.client.terminate_launches((launch_id,))
        ok = yield self.notifier.wait_for_state(states.TERMINATED, nodes=node_ids)
        self.assertTrue(ok)
        self.assertEqual(set(node_ids), set(self.notifier.nodes))
        # should be TERMINATING and TERMINATED record for each node
        self.assertTrue(self.notifier.assure_record_count(2))

        self.assertEqual(len(self.site_drivers['fake-site1'].destroyed), 
                         len(node_ids))

    @defer.inlineCallbacks
    def test_terminate_all(self):
        # create a ton of launches
        launch_specs = [(30, 3, states.RUNNING), (50, 1, states.TERMINATED), (80, 1, states.RUNNING)]

        to_be_terminated_node_ids = []

        for launchcount, nodecount, state in launch_specs:
            for i in range(launchcount):
                launch_id = _new_id()
                launch, nodes = make_launch_and_nodes(
                    launch_id, nodecount, state, site="fake-site1")
                yield self.store.put_launch(launch)
                yield self.store.put_nodes(nodes)

                if state < states.TERMINATED:
                    to_be_terminated_node_ids.extend(node["node_id"] for node in nodes)

        log.debug("Expecting %d nodes to be terminated", len(to_be_terminated_node_ids))

        yield self.client.terminate_all(rpcwait=True)
        yield self.assertStoreNodeRecords(states.TERMINATED, *to_be_terminated_node_ids)

        ok = self.notifier.assure_state(states.TERMINATED, nodes=to_be_terminated_node_ids)
        self.assertTrue(ok)
        self.assertEqual(set(to_be_terminated_node_ids), set(self.notifier.nodes))

        self.assertEqual(len(self.site_drivers['fake-site1'].destroyed),
                         len(to_be_terminated_node_ids))

    @defer.inlineCallbacks
    def test_query(self):
        #default is non-rpc. should be None result
        res = yield self.client.query()
        self.assertEqual(res, None)

        #returns true in RPC case
        res = yield self.client.query(rpc=True)
        self.assertEqual(res, True)


class NimbusProvisionerServiceTest(BaseProvisionerServiceTests):
    """Integration tests that use a live Nimbus cluster (in fake mode)
    """

    # these integration tests can run a little long
    timeout = 60

    @defer.inlineCallbacks
    def setUp(self):

        # @itv decorator is gone. This test could probably go away entirely but I'v
        # found it personally useful. Unconditionally skipping for now, til we know
        # what to do with it.
        raise unittest.SkipTest("developer-only Nimbus integration test")

        # skip this test if IaaS credentials are unavailable
        maybe_skip_test()

        self.notifier = FakeProvisionerNotifier()
        self.context_client = get_context_client()

        self.store = yield self.setup_store()
        self.site_drivers = provisioner.get_site_drivers(get_nimbus_test_sites())

        procs = self.get_procs()
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


class ProvisionerServiceCassandraTest(ProvisionerServiceTest):
    """Runs ProvisionerServiceTests with a cassandra backing store instead of in-memory
    """
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
