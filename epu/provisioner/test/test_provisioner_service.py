#!/usr/bin/env python

"""
@file epu/provisioner/test/test_provisioner_service.py
@author David LaBissoniere
@brief Test provisioner behavior
"""
import dashi.bootstrap as bootstrap

import time
import uuid
import unittest
import logging

import epu.tevent as tevent

from epu.dashiproc.dtrs import DTRS
from epu.dashiproc.provisioner import ProvisionerClient, ProvisionerService
from epu.provisioner.ctx import BrokerError
from epu.provisioner.test.util import FakeProvisionerNotifier, \
    FakeNodeDriver, FakeContextClient, make_launch_and_nodes, make_node, \
    make_launch
from epu.states import InstanceState
from epu.provisioner.store import ProvisionerStore, ProvisionerZooKeeperStore
from epu.test import ZooKeeperTestMixin


log = logging.getLogger(__name__)


def _new_id():
    return str(uuid.uuid4())


class BaseProvisionerServiceTests(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(BaseProvisionerServiceTests, self).__init__(*args, **kwargs)
        # these are to be set in a subclass' setUp()
        self.store = None
        self.notifier = None
        self.sites = None
        self.context_client = None
        self.default_user = 'default'
        # TODO improve the switch for in-mem transport
        self.amqp_uri = "memory://hello"
        # self.amqp_uri = "amqp://guest:guest@localhost/"
        self.record_reaping_max_age = 3600
        self.threads = []

    def assertStoreNodeRecords(self, state, *node_ids):
        for node_id in node_ids:
            node = self.store.get_node(node_id)
            self.assertTrue(node)
            self.assertEqual(node['state'], state)

    def assertNoStoreNodeRecords(self, *node_ids):
        for node_id in node_ids:
            node = self.store.get_node(node_id)
            self.assertEqual(node, None)

    def assertStoreLaunchRecord(self, state, launch_id):
        launch = self.store.get_launch(launch_id)
        self.assertTrue(launch)
        self.assertEqual(launch['state'], state)

    def assertNoStoreLaunchRecord(self, launch_id):
        launch = self.store.get_launch(launch_id)
        self.assertEqual(launch, None)

    def spawn_procs(self):
        self.dtrs = DTRS(amqp_uri=self.amqp_uri)
        self._spawn_process(self.dtrs.start)

        self.provisioner = ProvisionerService(sites=self.sites,
                                              store=self.store,
                                              context_client=self.context_client,
                                              notifier=self.notifier,
                                              amqp_uri=self.amqp_uri,
                                              default_user=self.default_user,
                                              record_reaping_max_age=self.record_reaping_max_age)
        self._spawn_process(self.provisioner.start)
        self.sysname = self.provisioner.dashi.sysname

    def shutdown_procs(self):
        self._shutdown_processes(self.threads)

    def _spawn_process(self, process):
        thread = tevent.spawn(process)
        self.threads.append(thread)

    def _shutdown_processes(self, threads):
        self.dtrs.dashi.cancel()
        self.provisioner.dashi.cancel()
        tevent.joinall(threads)

    def tearDown(self):
        self.shutdown_procs()
        self.teardown_store()

    def setup_store(self):
        return ProvisionerStore()

    def teardown_store(self):
        return


class ProvisionerServiceTest(BaseProvisionerServiceTests):
    """Integration tests that use fake context broker and IaaS driver fixtures
    """

    def setUp(self):

        self.notifier = FakeProvisionerNotifier()
        self.context_client = FakeContextClient()

        self.store = self.setup_store()
        self.driver = FakeNodeDriver()
        self.driver.initialize()

        self.spawn_procs()

        # this sucks. sometimes service doesn't bind its queue before client
        # sends a message to it.
        time.sleep(0.05)

        client_topic = "provisioner_client_%s" % uuid.uuid4()
        amqp_uri = "memory://hello"

        client_dashi = bootstrap.dashi_connect(client_topic, amqp_uri=amqp_uri,
                sysname=self.sysname)

        self.client = ProvisionerClient(client_dashi)

        site_definition = {
            'name': 'fake-site1',
            'description': 'Fake site 1',
            'driver_class': 'epu.provisioner.test.util.FakeNodeDriver'
        }
        self.dtrs.add_site("fake-site1", site_definition)

        caller = "asterix"
        credentials_definition = {
            'access_key': 'myec2access',
            'secret_key': 'myec2secret',
            'key_name': 'ooi'
        }
        self.dtrs.add_credentials(caller, "fake-site1", credentials_definition)

        dt1 = {
            'mappings': {
                'fake-site1': {
                    'iaas_image': 'fake-image',
                    'iaas_allocation': 'm1.small'
                }
            }
        }

        dt2 = {
            'mappings': {
                'fake-site1': {
                    'iaas_image': '${image_id}',
                    'iaas_allocation': 'm1.small'
                }
            }
        }

        self.dtrs.add_dt(caller, "empty", dt1)
        self.dtrs.add_dt(caller, "empty-with-vars", dt2)

    def test_provision_bad_dt(self):
        client = self.client
        notifier = self.notifier

        deployable_type = 'this-doesnt-exist'
        launch_id = _new_id()

        node_ids = [_new_id()]

        client.provision(launch_id, node_ids, deployable_type,
            ('subscriber',), 'fake-site1', caller="asterix")

        ok = notifier.wait_for_state(InstanceState.FAILED, node_ids)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(1))

        self.assertStoreNodeRecords(InstanceState.FAILED, *node_ids)
        self.assertStoreLaunchRecord(InstanceState.FAILED, launch_id)

    def test_provision_with_vars(self):
        client = self.client
        notifier = self.notifier
        caller = 'asterix'

        deployable_type = 'empty-with-vars'
        launch_id = _new_id()

        node_ids = [_new_id()]

        vars = {'image_id': 'fake-image'}
        client.provision(launch_id, node_ids, deployable_type,
            ('subscriber',), 'fake-site1', vars=vars, caller=caller)
        self.notifier.wait_for_state(InstanceState.PENDING, node_ids,
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.PENDING, *node_ids)

    def test_provision_with_missing_vars(self):
        client = self.client
        notifier = self.notifier
        caller = 'asterix'

        deployable_type = 'empty-with-vars'
        launch_id = _new_id()

        node_ids = [_new_id()]

        vars = {'foo': 'bar'}
        client.provision(launch_id, node_ids, deployable_type,
            ('subscriber',), 'fake-site1', vars=vars, caller=caller)

        ok = notifier.wait_for_state(InstanceState.FAILED, node_ids)
        self.assertTrue(ok)
        self.assertTrue(notifier.assure_record_count(1))

        self.assertStoreNodeRecords(InstanceState.FAILED, *node_ids)
        self.assertStoreLaunchRecord(InstanceState.FAILED, launch_id)

    def test_provision_broker_error(self):
        client = self.client
        notifier = self.notifier

        deployable_type = 'empty'

        launch_id = _new_id()

        self.context_client.create_error = BrokerError("fake failure")

        node_ids = [_new_id()]

        client.provision(launch_id, node_ids, deployable_type,
            ('subscriber',), 'fake-site1', caller="asterix")

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
                                                              site="fake-site1",
                                                              caller="asterix")
        self.store.add_launch(running_launch)
        for node in running_nodes:
            self.store.add_node(node)

        node_ids = [node['node_id'] for node in running_nodes]

        # terminate half of the nodes then the rest
        first_five = node_ids[:5]
        last_five = node_ids[5:]
        self.client.terminate_nodes(first_five, caller="asterix")
        ok = self.notifier.wait_for_state(InstanceState.TERMINATED, nodes=first_five)
        self.assertTrue(ok)
        self.assertEqual(set(first_five), set(self.notifier.nodes))

        self.client.terminate_nodes(last_five, caller="asterix")
        ok = self.notifier.wait_for_state(InstanceState.TERMINATED, nodes=last_five)
        self.assertTrue(ok)
        self.assertEqual(set(node_ids), set(self.notifier.nodes))
        # should be TERMINATING and TERMINATED record for each node
        self.assertTrue(self.notifier.assure_record_count(2))

        self.assertEqual(len(self.driver.destroyed),
                         len(node_ids))

    def test_launch_allocation(self):

        node_id = _new_id()
        self.client.provision(_new_id(), [node_id], "empty", ('subscriber',),
            site="fake-site1", caller="asterix")

        self.notifier.wait_for_state(InstanceState.PENDING, [node_id],
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.PENDING)

        self.assertEqual(len(self.driver.created), 1)
        libcloud_node = self.driver.created[0]
        self.assertEqual(libcloud_node.size.id, "m1.small")

    def test_launch_many_terminate_all(self):

        all_node_ids = []

        # after the terminate_all, provision requests should be REJECTED
        rejected_node_ids = []

        for _ in range(100):
            node_id = _new_id()
            all_node_ids.append(node_id)
            self.client.provision(_new_id(), [node_id], "empty",
                ('subscriber',), site="fake-site1", caller="asterix")

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
                ('subscriber',), site="fake-site1", caller="asterix")

        self.notifier.wait_for_state(InstanceState.TERMINATED, all_node_ids,
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.TERMINATED, *all_node_ids)

        self.notifier.wait_for_state(InstanceState.REJECTED, rejected_node_ids)
        self.assertStoreNodeRecords(InstanceState.REJECTED, *rejected_node_ids)

        self.assertEqual(len(self.driver.destroyed),
                         len(all_node_ids))

        self.assertIs(self.client.terminate_all(), True)

        # now re-enable
        self.client.enable()

        node_id = _new_id()
        log.debug("Launching node %s which should be accepted", node_id)
        self.client.provision(_new_id(), [node_id], "empty",
            ('subscriber',), site="fake-site1", caller="asterix")

        self.notifier.wait_for_state(InstanceState.PENDING, [node_id],
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.PENDING, node_id)

    def test_describe(self):
        node_ids = []
        for _ in range(3):
            launch_id = _new_id()
            running_launch, running_nodes = make_launch_and_nodes(launch_id, 1,
                InstanceState.RUNNING,
                site="fake-site1", caller=self.default_user)
            self.store.add_launch(running_launch)
            for node in running_nodes:
                self.store.add_node(node)
            node_ids.append(running_nodes[0]['node_id'])

        all_nodes = self.client.describe_nodes()
        self.assertEqual(len(all_nodes), len(node_ids))

        one_node = self.client.describe_nodes([node_ids[0]])
        self.assertEqual(len(one_node), 1)
        self.assertEqual(one_node[0]['node_id'], node_ids[0])

    def test_multiuser(self):
        """Test that nodes started by one user can't be modified by
        another user
        """
        permitted_user = "asterix"
        disallowed_user = "cacaphonix"

        client = self.client
        notifier = self.notifier

        deployable_type = 'empty'
        launch_id = _new_id()

        node_ids = [_new_id()]

        vars = {'image_id': 'fake-image'}
        client.provision(launch_id, node_ids, deployable_type,
            ('subscriber',), 'fake-site1', vars=vars, caller=permitted_user)
        self.notifier.wait_for_state(InstanceState.PENDING, node_ids,
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.PENDING, *node_ids)

        # Test describe
        permitted_nodes = client.describe_nodes(caller=permitted_user)
        self.assertEqual(len(permitted_nodes), len(node_ids))

        disallowed_nodes = client.describe_nodes(caller=disallowed_user)
        self.assertEqual(len(disallowed_nodes), 0)

        # Test terminate
        client.terminate_nodes(node_ids, caller=disallowed_user)

        terminate_timed_out = False
        try:
            self.notifier.wait_for_state(InstanceState.TERMINATED, node_ids,
                before=self.provisioner.leader._force_cycle, timeout=2)
        except Exception:
            terminate_timed_out = True

        self.assertTrue(terminate_timed_out,
                msg="Terminate worked with non-matching user")

        client.terminate_nodes(node_ids, caller=permitted_user)
        self.notifier.wait_for_state(InstanceState.TERMINATED, node_ids,
            before=self.provisioner.leader._force_cycle, timeout=2)
        self.assertStoreNodeRecords(InstanceState.TERMINATED, *node_ids)

    def test_record_reaper(self):
        launch_id1 = _new_id()
        launch_id2 = _new_id()

        now = time.time()
        node1 = make_node(launch_id1, InstanceState.TERMINATED, caller=self.default_user,
                          state_changes=[(InstanceState.TERMINATED, now - self.record_reaping_max_age - 1)])
        node2 = make_node(launch_id1, InstanceState.FAILED, caller=self.default_user,
                          state_changes=[(InstanceState.FAILED, now - self.record_reaping_max_age - 1)])
        node3 = make_node(launch_id1, InstanceState.REJECTED, caller=self.default_user,
                          state_changes=[(InstanceState.REJECTED, now - self.record_reaping_max_age - 1)])
        nodes1 = [node1, node2, node3]
        launch1 = make_launch(launch_id1, InstanceState.RUNNING, nodes1, caller=self.default_user)

        node4 = make_node(launch_id2, InstanceState.RUNNING, caller=self.default_user,
                          state_changes=[(InstanceState.RUNNING, now - self.record_reaping_max_age - 1)])
        node5 = make_node(launch_id2, InstanceState.TERMINATED, caller=self.default_user,
                          state_changes=[(InstanceState.TERMINATED, now - self.record_reaping_max_age - 1)])
        nodes2 = [node4, node5]
        launch2 = make_launch(launch_id2, InstanceState.RUNNING, nodes2, caller=self.default_user)

        self.store.add_launch(launch1)
        for node in nodes1:
            self.store.add_node(node)

        self.store.add_launch(launch2)
        for node in nodes2:
            self.store.add_node(node)

        # Force a record reaping cycle
        self.provisioner.leader._force_record_reaping()

        # Check that the first launch is completely removed
        node_ids1 = map(lambda x: x['node_id'], nodes1)
        self.assertNoStoreNodeRecords(*node_ids1)
        self.assertNoStoreLaunchRecord(launch_id1)

        # Check that the second launch is still here but with only the running node
        self.assertStoreNodeRecords(InstanceState.RUNNING, node4['node_id'])
        self.assertStoreLaunchRecord(InstanceState.RUNNING, launch_id2)


class ProvisionerServiceNoContextualizationTest(BaseProvisionerServiceTests):

    def setUp(self):

        self.notifier = FakeProvisionerNotifier()
        self.context_client = None

        self.store = self.setup_store()
        self.driver = FakeNodeDriver()
        self.driver.initialize()

        self.spawn_procs()

        # this sucks. sometimes service doesn't bind its queue before client
        # sends a message to it.
        time.sleep(0.05)

        client_topic = "provisioner_client_%s" % uuid.uuid4()
        amqp_uri = "memory://hello"

        client_dashi = bootstrap.dashi_connect(client_topic, amqp_uri=amqp_uri,
                sysname=self.sysname)

        self.client = ProvisionerClient(client_dashi)

        site_definition = {
            'name': 'fake-site1',
            'description': 'Fake site 1',
            'driver_class': 'epu.provisioner.test.util.FakeNodeDriver'
        }
        self.dtrs.add_site("fake-site1", site_definition)

        caller = "asterix"
        credentials_definition = {
            'access_key': 'myec2access',
            'secret_key': 'myec2secret',
            'key_name': 'ooi'
        }
        self.dtrs.add_credentials(caller, "fake-site1", credentials_definition)

        dt_definition = {
            'mappings': {
                'fake-site1': {
                    'iaas_image': 'fake-image',
                    'iaas_allocation': 'm1.small'
                }
            }
        }
        self.dtrs.add_dt(caller, "empty", dt_definition)

    def test_launch_no_context(self):

        all_node_ids = []

        for _ in range(10):
            node_id = _new_id()
            all_node_ids.append(node_id)
            self.client.provision(_new_id(), [node_id], "empty",
                ('subscriber',), site="fake-site1", caller="asterix")

        self.notifier.wait_for_state(InstanceState.PENDING, all_node_ids,
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.PENDING, *all_node_ids)

        for node_id in all_node_ids:
            node = self.store.get_node(node_id)
            self.driver.set_node_running(node['iaas_id'])

        self.notifier.wait_for_state(InstanceState.RUNNING, all_node_ids,
            before=self.provisioner.leader._force_cycle)
        self.assertStoreNodeRecords(InstanceState.RUNNING, *all_node_ids)


class ProvisionerZooKeeperServiceTest(ProvisionerServiceTest, ZooKeeperTestMixin):

    # this runs all of the ProvisionerServiceTest tests wih a ZK store

    def setup_store(self):
        try:
            import kazoo # noqa
        except ImportError:
            raise unittest.SkipTest("kazoo not found: ZooKeeper integration tests disabled.")

        self.setup_zookeeper(base_path_prefix="/provisioner_service_tests_")
        store = ProvisionerZooKeeperStore(self.zk_hosts, self.zk_base_path, use_gevent=self.use_gevent)
        store.initialize()

        return store

    def teardown_store(self):
        if self.store:
            self.store.shutdown()

        self.teardown_zookeeper()
