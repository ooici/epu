#!/usr/bin/env python

"""
@file epu/provisioner/test/test_provisioner_service.py
@author David LaBissoniere
@brief Test provisioner behavior
"""
import dashi.bootstrap as bootstrap

import uuid
from nimboss.ctx import BrokerError
import unittest
import gevent
import logging


from epu.dashiproc.provisioner import ProvisionerClient, ProvisionerService
from epu.provisioner.test.util import FakeProvisionerNotifier, \
    FakeNodeDriver, FakeContextClient, make_launch_and_nodes
from epu.provisioner.sites import ProvisionerSites
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


class BaseProvisionerServiceTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(BaseProvisionerServiceTests, self).__init__(*args, **kwargs)
        # these are to be set in a subclass' setUp()
        self.store = None
        self.notifier = None
        self.sites = None
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
                                              sites=self.sites,
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
        self.driver = FakeNodeDriver()

        self.sites = ProvisionerSites({},
            driver_creators={'fake-site1': lambda : self.driver})

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

        self.assertEqual(len(self.driver.destroyed),
                         len(node_ids))

    def test_launch_many_terminate_all(self):

        all_node_ids = []

        # after the terminate_all, provision requests should be REJECTED
        rejected_node_ids = []

        for _ in range(100):
            node_id = _new_id()
            all_node_ids.append(node_id)
            self.client.provision(_new_id(), [node_id], "empty",
                ('subscriber',), site="fake-site1")

        self.notifier.wait_for_state(InstanceState.PENDING, all_node_ids,
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.PENDING, *all_node_ids)

        for node_id in all_node_ids:
            node = self.store.get_node(node_id)
            self.driver.set_node_running(node['iaas_id'])

        self.notifier.wait_for_state(InstanceState.STARTED, all_node_ids,
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.STARTED, *all_node_ids)

        log.debug("Expecting %d nodes to be terminated", len(all_node_ids))

        self.assertIs(self.client.terminate_all(), False)

        # future requests should be rejected
        for _ in range(5):
            node_id = _new_id()
            rejected_node_ids.append(node_id)
            self.client.provision(_new_id(), [node_id], "empty",
                ('subscriber',), site="fake-site1")

        self.notifier.wait_for_state(InstanceState.TERMINATED, all_node_ids,
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.TERMINATED, *all_node_ids)

        self.notifier.wait_for_state(InstanceState.REJECTED, rejected_node_ids)
        self.assertStoreNodeRecords(InstanceState.REJECTED, *rejected_node_ids)

        self.assertEqual(len(self.driver.destroyed),
                         len(all_node_ids))

        self.assertIs(self.client.terminate_all(), True)


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
