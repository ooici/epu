#!/usr/bin/env python

"""
@file epu/provisioner/test/test_provisioner_service.py
@author David LaBissoniere
@brief Test provisioner behavior
"""
import dashi.bootstrap as bootstrap

import uuid
from libcloud.compute.drivers.ec2 import EC2USWestNodeDriver, NimbusNodeDriver
from nimboss.ctx import BrokerError
import unittest
import gevent
import logging


from epu.dashiproc import provisioner
from epu.dashiproc.provisioner import ProvisionerClient, ProvisionerService
from epu.provisioner.test.util import FakeProvisionerNotifier, \
    FakeNodeDriver, FakeContextClient, make_launch_and_nodes
from epu.localdtrs import LocalDTRS

from epu.states import InstanceState

from epu.provisioner.store import ProvisionerStore

log = logging.getLogger(__name__)


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
        "driver_class": "libcloud.compute.drivers.ec2.EC2USWestNodeDriver",
        "driver_kwargs": {
            "key": "myec2key",
            "secret": "myec2secret"
        }
    },
    "nimbus-test": {
        "driver_class": "libcloud.compute.drivers.ec2.NimbusNodeDriver",
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
        #TODO libcloud giving me grief (DL)
        import libcloud.security
        libcloud.security.VERIFY_SSL_CERT_STRICT = False
        
        site_drivers = provisioner.ProvisionerService._get_site_drivers(SITES_DICT)
        nimbus_test = site_drivers['nimbus-test']
        ec2_west = site_drivers['ec2-west']
        self.assertIsInstance(nimbus_test, NimbusNodeDriver)
        self.assertIsInstance(ec2_west, EC2USWestNodeDriver)
        self.assertEqual(nimbus_test.key, 'mynimbuskey')
        self.assertEqual(ec2_west.key, 'myec2key')


class BaseProvisionerServiceTests(unittest.TestCase):


    def __init__(self, *args, **kwargs):
        super(BaseProvisionerServiceTests, self).__init__(*args, **kwargs)
        # these are to be set in a subclass' setUp()
        self.store = None
        self.notifier = None
        self.site_drivers = None
        self.context_client = None
        self.dtrs = LocalDTRS("./epu/dashiproc/test/dt/")
        #TODO improve the switch for in-mem transport
        self.amqp_uri = "memory://hello"
        #self.amqp_uri = "amqp://guest:guest@localhost/"
        self.greenlets = []

    def assertStoreNodeRecords(self, state, *node_ids):
        for node_id in node_ids:
            node = self.store.get_node(node_id)
            self.assertTrue(node)
            self.assertEqual(node['state'], state)

    def assertStoreLaunchRecord(self, state, launch_id):
        launch = self.store.get_launch(launch_id)
        self.assertTrue(launch)
        self.assertEqual(launch['state'], state)

    def spawn_procs(self):
        self.provisioner = ProvisionerService(dtrs=self.dtrs,
                                              site_drivers=self.site_drivers,
                                              store=self.store, 
                                              context_client=self.context_client,
                                              notifier=self.notifier,
                                              amqp_uri=self.amqp_uri)
        self._spawn_process(self.provisioner.start)

       
    def shutdown_procs(self):
        self._shutdown_processes(self.greenlets)

    def _spawn_process(self, process):

        glet = gevent.spawn(process)
        self.greenlets.append(glet)

    def _shutdown_processes(self, greenlets):
        self.provisioner.dashi.cancel()
        gevent.joinall(greenlets)


class ProvisionerServiceTest(BaseProvisionerServiceTests):
    """Integration tests that use fake context broker and IaaS driver fixtures
    """

    def __init__(self, *args, **kwargs):
        super(ProvisionerServiceTest, self).__init__(*args, **kwargs)

    def setUp(self):

        self.notifier = FakeProvisionerNotifier()
        self.context_client = FakeContextClient()

        self.store = self.setup_store()
        self.site_drivers = {'fake-site1' : FakeNodeDriver()}

        self.spawn_procs()

        # this sucks. sometimes service doesn't bind its queue before client
        # sends a message to it.
        gevent.sleep(0.05)

        client_topic = "provisioner_client_%s" % uuid.uuid4()
        amqp_uri = "memory://hello"

        client_dashi = bootstrap.dashi_connect(client_topic, amqp_uri=amqp_uri) 

        self.client = ProvisionerClient(client_dashi)

    def tearDown(self):
        self.shutdown_procs()
        self.teardown_store()

    def setup_store(self):
        return ProvisionerStore()

    def teardown_store(self):
        return

    def test_provision_bad_dt(self):
        client = self.client
        notifier = self.notifier

        deployable_type = 'this-doesnt-exist'
        launch_id = _new_id()

        node_ids = [_new_id()]

        client.provision(launch_id, node_ids, deployable_type,
            ('subscriber',), 'fake-site1')

        ok = notifier.wait_for_state(InstanceState.FAILED, node_ids)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(1))

        self.assertStoreNodeRecords(InstanceState.FAILED, *node_ids)
        self.assertStoreLaunchRecord(InstanceState.FAILED, launch_id)

    def test_provision_broker_error(self):
        client = self.client
        notifier = self.notifier

        deployable_type = 'base-cluster'

        launch_id = _new_id()

        self.context_client.create_error = BrokerError("fake failure")

        node_ids = [_new_id()]

        client.provision(launch_id, node_ids, deployable_type,
            ('subscriber',), 'fake-site1')

        ok = notifier.wait_for_state(InstanceState.FAILED, node_ids)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(1))

        self.assertStoreNodeRecords(InstanceState.FAILED, *node_ids)
        self.assertStoreLaunchRecord(InstanceState.FAILED, launch_id)

    def test_dump_state(self):
        running_launch, running_nodes = make_launch_and_nodes(_new_id(), 10, InstanceState.RUNNING)
        self.store.add_launch(running_launch)
        for node in running_nodes:
            self.store.add_node(node)

        pending_launch, pending_nodes = make_launch_and_nodes(_new_id(), 3, InstanceState.PENDING)
        self.store.add_launch(pending_launch)
        for node in pending_nodes:
            self.store.add_node(node)

        running_node_ids = [node['node_id'] for node in running_nodes]
        pending_node_ids = [node['node_id'] for node in pending_nodes]
        all_node_ids = running_node_ids + pending_node_ids

        self.client.dump_state(running_node_ids)
        ok = self.notifier.wait_for_state(InstanceState.RUNNING, nodes=running_node_ids)
        self.assertTrue(ok)
        self.assertEqual(len(self.notifier.nodes), len(running_nodes))

        self.client.dump_state(pending_node_ids)
        ok = self.notifier.wait_for_state(InstanceState.PENDING, nodes=pending_node_ids)
        self.assertTrue(ok)
        self.assertEqual(len(self.notifier.nodes), len(all_node_ids))

        # we should have not gotten any dupe records yet
        self.assertTrue(self.notifier.assure_record_count(1))

        # empty dump request should dump nothing
        self.client.dump_state([])
        self.assertTrue(self.notifier.assure_record_count(1))

    def test_dump_state_unknown_node(self):
        node_ids = ["09ddd3f8-a5a5-4196-ac13-eab4d4b0c777"]
        subscribers = ["hello1_subscriber"]
        self.client.dump_state(node_ids, force_subscribe=subscribers[0])
        ok = self.notifier.wait_for_state(InstanceState.FAILED, nodes=node_ids)
        self.assertTrue(ok)
        self.assertEqual(len(self.notifier.nodes), len(node_ids))
        for node_id in node_ids:
            ok = self.notifier.assure_subscribers(node_id, subscribers)
            self.assertTrue(ok)

    def test_terminate(self):
        launch_id = _new_id()
        running_launch, running_nodes = make_launch_and_nodes(launch_id, 10,
                                                              InstanceState.RUNNING,
                                                              site="fake-site1")
        self.store.add_launch(running_launch)
        for node in running_nodes:
            self.store.add_node(node)

        node_ids = [node['node_id'] for node in running_nodes]

        # terminate half of the nodes then the rest
        first_five = node_ids[:5]
        last_five = node_ids[5:]
        self.client.terminate_nodes(first_five)
        ok = self.notifier.wait_for_state(InstanceState.TERMINATED, nodes=first_five)
        self.assertTrue(ok)
        self.assertEqual(set(first_five), set(self.notifier.nodes))

        self.client.terminate_nodes(last_five)
        ok = self.notifier.wait_for_state(InstanceState.TERMINATED, nodes=last_five)
        self.assertTrue(ok)
        self.assertEqual(set(node_ids), set(self.notifier.nodes))
        # should be TERMINATING and TERMINATED record for each node
        self.assertTrue(self.notifier.assure_record_count(2))

        self.assertEqual(len(self.site_drivers['fake-site1'].destroyed),
                         len(node_ids))

    def test_terminate_all(self):
        # create a ton of launches
        launch_specs = [(30, 3, InstanceState.RUNNING), (50, 1, InstanceState.TERMINATED), (80, 1, InstanceState.RUNNING)]

        to_be_terminated_node_ids = []

        for launchcount, nodecount, state in launch_specs:
            for i in range(launchcount):
                launch_id = _new_id()
                launch, nodes = make_launch_and_nodes(
                    launch_id, nodecount, state, site="fake-site1")
                self.store.add_launch(launch)
                for node in nodes:
                    self.store.add_node(node)

                if state < InstanceState.TERMINATED:
                    to_be_terminated_node_ids.extend(node["node_id"] for node in nodes)

        log.debug("Expecting %d nodes to be terminated", len(to_be_terminated_node_ids))

        self.client.terminate_all(rpcwait=True)
        self.assertStoreNodeRecords(InstanceState.TERMINATED, *to_be_terminated_node_ids)

        ok = self.notifier.assure_state(InstanceState.TERMINATED, nodes=to_be_terminated_node_ids)
        self.assertTrue(ok)
        self.assertEqual(set(to_be_terminated_node_ids), set(self.notifier.nodes))

        self.assertEqual(len(self.site_drivers['fake-site1'].destroyed),
                         len(to_be_terminated_node_ids))

    def test_describe(self):
        node_ids = []
        for _ in range(3):
            launch_id = _new_id()
            running_launch, running_nodes = make_launch_and_nodes(launch_id, 1,
                InstanceState.RUNNING,
                site="fake-site1")
            self.store.add_launch(running_launch)
            for node in running_nodes:
                self.store.add_node(node)
            node_ids.append(running_nodes[0]['node_id'])

        all_nodes = self.client.describe_nodes()
        self.assertEqual(len(all_nodes), len(node_ids))

        one_node = self.client.describe_nodes([node_ids[0]])
        self.assertEqual(len(one_node), 1)
        self.assertEqual(one_node[0]['node_id'], node_ids[0])
