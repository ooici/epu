#!/usr/bin/env python

import uuid
import time
import logging
import unittest

from mock import patch
from nose.tools import raises
from socket import timeout

from libcloud.compute.types import InvalidCredsError

from epu.provisioner.ctx import BrokerError, ContextNotFoundError
from epu.exceptions import DeployableTypeLookupError
from epu.provisioner.core import ProvisionerCore, match_nodes_from_context, \
    update_nodes_from_context, update_node_ip_info, INSTANCE_READY_TIMEOUT
from epu.provisioner.store import ProvisionerStore, VERSION_KEY
from epu.states import InstanceState
from epu.provisioner.test.util import FakeProvisionerNotifier, \
    FakeNodeDriver, FakeContextClient, make_launch, make_node, \
    make_launch_and_nodes, FakeDTRS
from epu.test import Mock

log = logging.getLogger(__name__)

# alias for shorter code
states = InstanceState


class ProvisionerCoreTests(unittest.TestCase):
    """Testing the provisioner core functionality
    """
    def setUp(self):
        self.notifier = FakeProvisionerNotifier()
        self.store = ProvisionerStore()
        self.ctx = FakeContextClient()
        self.dtrs = FakeDTRS()

        self.dtrs.sites["site1"] = self.dtrs.sites["site2"] = {
            "type": "fake"
        }

        self.dtrs.credentials[("asterix", "site1")] = self.dtrs.credentials[("asterix", "site2")] = {
            "access_key": "mykey",
            "secret_key": "mysecret"
        }

        self.site1_driver = FakeNodeDriver()
        self.site2_driver = FakeNodeDriver()
        self.site1_driver.initialize()
        self.site2_driver.initialize()

        self.core = ProvisionerCore(store=self.store, notifier=self.notifier,
                                    dtrs=self.dtrs, context=self.ctx)

    def test_terminate_all(self):
        caller = 'asterix'
        running_launch_id = _new_id()
        running_launch, running_nodes = make_launch_and_nodes(
            running_launch_id, 3, states.RUNNING, caller=caller)
        self.store.add_launch(running_launch)
        for node in running_nodes:
            self.store.add_node(node)

        pending_launch_id = _new_id()
        pending_launch, pending_nodes = make_launch_and_nodes(
            pending_launch_id, 3, states.PENDING, caller=caller)
        self.store.add_launch(pending_launch)
        for node in pending_nodes:
            self.store.add_node(node)

        terminated_launch_id = _new_id()
        terminated_launch, terminated_nodes = make_launch_and_nodes(
            terminated_launch_id, 3, states.TERMINATED, caller=caller)
        self.store.add_launch(terminated_launch)
        for node in terminated_nodes:
            self.store.add_node(node)

        self.core.terminate_all()

        all_nodes = self.store.get_nodes()
        self.assertEqual(9, len(all_nodes))
        self.assertTrue(all(n['state'] == states.TERMINATING or
                        n['state'] == states.TERMINATED for n in all_nodes))

    def test_prepare_dtrs_error(self):
        self.dtrs.error = DeployableTypeLookupError()

        self.core.prepare_provision(
            launch_id=_new_id(), deployable_type="foo",
            instance_ids=[_new_id()], site="chicago")
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    def test_prepare_broker_error(self):
        self.ctx.create_error = BrokerError("fake ctx create failed")
        self.dtrs.result = {'document': "<fake>document</fake>",
                            "node": {}}
        self.core.prepare_provision(
            launch_id=_new_id(), deployable_type="foo",
            instance_ids=[_new_id()], site="chicago")
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    def test_prepare_execute(self):
        self._prepare_execute()
        self.assertTrue(self.notifier.assure_state(states.PENDING))

    def test_prepare_execute_iaas_fail(self):
        with patch('epu.provisioner.test.util.FakeNodeDriver.create_node') as mock_method:
            mock_method.return_value = InvalidCredsError()
            self._prepare_execute()
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    def test_prepare_execute_no_ctx(self):
        self.core.context = None

        # just in case
        self.ctx.create_error = NotImplementedError()
        self.ctx.query_error = NotImplementedError()

        self._prepare_execute(context_enabled=False)
        self.assertTrue(self.notifier.assure_state(states.PENDING))

    def test_prepare_execute_existing_launch(self):
        self.core.context = None
        launch_id = _new_id()
        instance_id = _new_id()

        self._prepare_execute(
            launch_id=launch_id, instance_ids=[instance_id],
            context_enabled=False)
        self._prepare_execute(
            launch_id=launch_id, instance_ids=[instance_id],
            context_enabled=False, assure_state=False)

        self.assertTrue(self.notifier.assure_state(states.PENDING))

    def _prepare_execute(self, launch_id=None, instance_ids=None,
                         context_enabled=True, assure_state=True):
        self.dtrs.result = {'document': _get_one_node_cluster_doc("node1", "image1"),
                            "node": {}}

        caller = "asterix"
        if not launch_id:
            launch_id = _new_id()
        if not instance_ids:
            instance_ids = [_new_id()]
        launch, nodes = self.core.prepare_provision(
            launch_id=launch_id,
            deployable_type="foo", instance_ids=instance_ids,
            site="site1", caller=caller)

        self.assertEqual(len(nodes), 1)
        node = nodes[0]
        self.assertEqual(node['node_id'], instance_ids[0])
        self.assertEqual(launch['launch_id'], launch_id)
        self.assertEqual(launch['node_ids'], instance_ids)

        if context_enabled:
            self.assertTrue(self.ctx.last_create)
            self.assertEqual(launch['context'], self.ctx.last_create)
            for key in ('uri', 'secret', 'context_id', 'broker_uri'):
                self.assertIn(key, launch['context'])
        else:
            self.assertEqual(launch['context'], None)

        if assure_state:
            self.assertTrue(self.notifier.assure_state(states.REQUESTED))

        self.core.execute_provision(launch, nodes, caller)

    def test_execute_bad_doc(self):
        caller = "asterix"
        ctx = self.ctx.create()
        launch_record = {
            'launch_id': "thelaunchid",
            'document': "<this><isnt><a><real><doc>",
            'deployable_type': "dt",
            'context': ctx,
            'state': states.PENDING,
            'node_ids': ['node1']}
        nodes = [{'node_id': 'node1', 'launch_id': "thelaunchid",
                 'state': states.REQUESTED, 'creator': caller}]

        self.store.add_launch(launch_record)
        self.store.add_node(nodes[0])

        self.core.execute_provision(launch_record, nodes, caller)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

        # TODO this should be a better error coming from nimboss
        # self.assertEqual(self.notifier.nodes['node1']['state_desc'], "CONTEXT_DOC_INVALID")

    def test_execute_bad_doc_nodes(self):
        caller = 'asterix'
        ctx = self.ctx.create()
        launch_record = {
            'launch_id': "thelaunchid",
            'document': _get_one_node_cluster_doc("node1", "image1"),
            'deployable_type': "dt",
            'context': ctx,
            'state': states.PENDING,
            'node_ids': ['node1']}
        nodes = [{'node_id': 'node1', 'launch_id': "thelaunchid",
                  'state': states.REQUESTED, 'ctx_name': "adifferentname",
                  'creator': caller}]

        self.store.add_launch(launch_record)
        self.store.add_node(nodes[0])

        self.core.execute_provision(launch_record, nodes, caller)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    def test_execute_bad_doc_node_count(self):
        caller = "asterix"
        ctx = self.ctx.create()
        launch_record = {
            'launch_id': "thelaunchid",
            'document': _get_one_node_cluster_doc("node1", "image1"),
            'deployable_type': "dt",
            'context': ctx,
            'state': states.PENDING,
            'node_ids': ['node1']}

        # two nodes where doc expects 1
        nodes = [{'node_id': 'node1', 'launch_id': "thelaunchid",
                 'state': states.REQUESTED, 'ctx_name': "node1", 'creator': caller},
                 {'node_id': 'node2', 'launch_id': "thelaunchid",
                     'state': states.REQUESTED, 'ctx_name': "node1",
                     'creator': caller}]

        self.store.add_launch(launch_record)
        for node in nodes:
            self.store.add_node(node)

        self.core.execute_provision(launch_record, nodes, caller)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    def test_query_missing_node_within_window(self):
        launch_id = _new_id()
        node_id = _new_id()
        caller = 'asterix'
        ts = time.time() - 30.0
        launch = {'launch_id': launch_id, 'node_ids': [node_id],
                  'state': states.PENDING,
                  'creator': caller}
        node = {'launch_id': launch_id,
                'node_id': node_id,
                'state': states.PENDING,
                'pending_timestamp': ts,
                'creator': caller}
        self.store.add_launch(launch)
        self.store.add_node(node)

        with patch.object(FakeNodeDriver, 'list_nodes', return_value=[]):
            self.core.query_one_site('site1', [node], caller=caller)
        self.assertEqual(len(self.notifier.nodes), 0)

    def test_query_missing_started_node_within_window(self):
        launch_id = _new_id()
        node_id = _new_id()
        caller = 'asterix'
        ts = time.time() - 30.0
        launch = {'launch_id': launch_id, 'node_ids': [node_id],
                  'state': states.PENDING,
                  'creator': caller}
        node = {'launch_id': launch_id,
                'node_id': node_id,
                'state': states.STARTED,
                'pending_timestamp': ts,
                'creator': caller}
        self.store.add_launch(launch)
        self.store.add_node(node)

        with patch.object(FakeNodeDriver, 'list_nodes', return_value=[]):
            self.core.query_one_site('site1', [node], caller=caller)
        self.assertEqual(len(self.notifier.nodes), 1)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    def test_query_missing_node_past_window(self):
        launch_id = _new_id()
        node_id = _new_id()

        caller = 'asterix'
        ts = time.time() - 120.0
        launch = {
            'launch_id': launch_id, 'node_ids': [node_id],
            'state': states.PENDING,
            'creator': caller}
        node = {'launch_id': launch_id,
                'node_id': node_id,
                'state': states.PENDING,
                'pending_timestamp': ts,
                'creator': caller}
        self.store.add_launch(launch)
        self.store.add_node(node)

        with patch.object(FakeNodeDriver, 'list_nodes', return_value=[]):
            self.core.query_one_site('site1', [node], caller=caller)
        self.assertEqual(len(self.notifier.nodes), 1)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    def test_query_missing_node_terminating(self):
        launch_id = _new_id()
        node_id = _new_id()

        caller = 'asterix'
        launch = {
            'launch_id': launch_id, 'node_ids': [node_id],
            'state': states.RUNNING,
            'creator': caller}
        node = {'launch_id': launch_id,
                'node_id': node_id,
                'state': states.TERMINATING,
                'creator': caller}
        self.store.add_launch(launch)
        self.store.add_node(node)

        with patch.object(FakeNodeDriver, 'list_nodes', return_value=[]):
            self.core.query_one_site('site1', [node], caller=caller)
        self.assertEqual(len(self.notifier.nodes), 1)
        self.assertTrue(self.notifier.assure_state(states.TERMINATED))

    def test_query(self):
        caller = "asterix"
        launch_id = _new_id()
        node_id = _new_id()

        iaas_node = self.site1_driver.create_node()[0]
        self.site1_driver.set_node_running(iaas_node.id)

        ts = time.time() - 120.0
        launch = {
            'launch_id': launch_id, 'node_ids': [node_id],
            'state': states.PENDING,
            'creator': caller}
        node = {'launch_id': launch_id,
                'node_id': node_id,
                'state': states.PENDING,
                'pending_timestamp': ts,
                'iaas_id': iaas_node.id,
                'creator': caller,
                'site': 'site1'}

        req_node = {'launch_id': launch_id,
                    'node_id': _new_id(),
                    'state': states.REQUESTED}
        nodes = [node, req_node]
        self.store.add_launch(launch)
        self.store.add_node(node)
        self.store.add_node(req_node)

        self.core.query_one_site('site1', nodes, caller=caller)

        node = self.store.get_node(node_id)
        self.assertEqual(node.get('public_ip'), iaas_node.public_ip)
        self.assertEqual(node.get('private_ip'), iaas_node.private_ip)
        self.assertEqual(node.get('state'), states.STARTED)

        # query again should detect no changes
        self.core.query_one_site('site1', nodes, caller=caller)

        # now destroy
        self.core.terminate_nodes([node_id], remove_terminating=False)
        node = self.store.get_node(node_id)
        self.core.query_one_site('site1', [node], caller=caller)

        node = self.store.get_node(node_id)
        self.assertEqual(node['public_ip'], iaas_node.public_ip)
        self.assertEqual(node['private_ip'], iaas_node.private_ip)
        self.assertEqual(node['state'], states.TERMINATED)

    def test_terminate_requested_node(self):
        caller = "asterix"
        launch_id = _new_id()
        node_id = _new_id()

        launch = {
            'launch_id': launch_id, 'node_ids': [node_id],
            'state': states.PENDING,
            'creator': caller}
        req_node = {
            'launch_id': launch_id,
            'node_id': node_id,
            'state': states.REQUESTED,
            'site': 'site1'}
        self.store.add_launch(launch)
        self.store.add_node(req_node)

        # destroy
        self.core.terminate_nodes([node_id], remove_terminating=False)
        node = self.store.get_node(node_id)
        self.assertEqual(node['state'], states.TERMINATED)

    def test_query_no_contextualization(self):

        self.core.context = None

        launch_id = _new_id()
        node_id = _new_id()

        caller = 'asterix'

        iaas_node = self.site1_driver.create_node()[0]
        self.site1_driver.set_node_running(iaas_node.id)

        ts = time.time() - 120.0
        launch = {
            'launch_id': launch_id, 'node_ids': [node_id],
            'state': states.PENDING,
            'creator': caller}
        node = {'launch_id': launch_id,
                'node_id': node_id,
                'state': states.PENDING,
                'pending_timestamp': ts,
                'iaas_id': iaas_node.id,
                'site': 'site1',
                'creator': caller}

        req_node = {'launch_id': launch_id,
                    'node_id': _new_id(),
                    'state': states.REQUESTED}
        nodes = [node, req_node]
        self.store.add_launch(launch)
        self.store.add_node(node)
        self.store.add_node(req_node)

        self.core.query_one_site('site1', nodes, caller=caller)

        node = self.store.get_node(node_id)
        self.assertEqual(node.get('public_ip'), iaas_node.public_ip)
        self.assertEqual(node.get('private_ip'), iaas_node.private_ip)

        # since contextualization is disabled we should jump straight
        # to RUNNING
        self.assertEqual(node.get('state'), states.RUNNING)

    @raises(timeout)
    def test_query_iaas_timeout(self):
        launch_id = _new_id()
        node_id = _new_id()

        iaas_node = self.site1_driver.create_node()[0]
        self.site1_driver.set_node_running(iaas_node.id)

        caller = 'asterix'
        ts = time.time() - 120.0
        launch = {
            'launch_id': launch_id, 'node_ids': [node_id],
            'state': states.PENDING,
            'creator': caller}
        node = {'name': 'hello',
                'launch_id': launch_id,
                'node_id': node_id,
                'state': states.PENDING,
                'pending_timestamp': ts,
                'iaas_id': iaas_node.id,
                'site': 'site1',
                'creator': caller}

        req_node = {'launch_id': launch_id,
                    'node_id': _new_id(),
                    'state': states.REQUESTED}
        nodes = [node, req_node]
        self.store.add_launch(launch)
        self.store.add_node(node)
        self.store.add_node(req_node)

        def x():
            raise timeout("Took too long to query iaas")
        self.core._IAAS_DEFAULT_TIMEOUT = 0.5

        with patch.object(FakeNodeDriver, 'list_nodes', side_effect=x):
            self.core.query_one_site('site1', nodes, caller=caller)

    def test_launch_one_iaas_full(self):
        def x(**kwargs):
            raise Exception("InstanceLimitExceeded: too many vms :(")

        with patch.object(FakeNodeDriver, 'create_node', side_effect=x):
            self.core._IAAS_DEFAULT_TIMEOUT = 0.5

            node_id = _new_id()
            launch_id = _new_id()

            self._prepare_execute(launch_id=launch_id, instance_ids=[node_id])

            self.assertTrue(self.notifier.assure_state(states.FAILED))
            self.assertIn('IAAS_FULL', self.notifier.nodes[node_id]['state_desc'])
            launch = self.store.get_launch(launch_id)
            self.assertEqual(launch['state'], states.FAILED)

    def test_launch_one_iaas_timeout(self):
        def x(**kwargs):
            raise timeout("Launch took too long")

        with patch.object(FakeNodeDriver, 'create_node', side_effect=x):
            self.core._IAAS_DEFAULT_TIMEOUT = 0.5

            node_id = _new_id()
            launch_id = _new_id()

            self._prepare_execute(launch_id=launch_id, instance_ids=[node_id])

            self.assertTrue(self.notifier.assure_state(states.FAILED))
            self.assertEqual(self.notifier.nodes[node_id]['state_desc'], 'IAAS_TIMEOUT')
            launch = self.store.get_launch(launch_id)
            self.assertEqual(launch['state'], states.FAILED)

    def test_query_ctx(self):
        node_count = 3
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.STARTED)
                        for i in range(node_count)]
        launch_record = make_launch(launch_id, states.PENDING,
                                    node_records)

        self.store.add_launch(launch_record)
        for node in node_records:
            self.store.add_node(node)

        self.ctx.expected_count = len(node_records)
        self.ctx.complete = False
        self.ctx.error = False

        # first query with no ctx nodes. zero records should be updated
        self.core.query_contexts()
        self.assertTrue(self.notifier.assure_record_count(0))

        # all but 1 node have reported ok
        self.ctx.nodes = [_one_fake_ctx_node_ok(node_records[i]['public_ip'],
                          _new_id(), _new_id()) for i in range(node_count - 1)]

        self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING))
        self.assertEqual(len(self.notifier.nodes), node_count - 1)

        # last node reports ok
        self.ctx.nodes.append(_one_fake_ctx_node_ok(node_records[-1]['public_ip'],
                              _new_id(), _new_id()))

        self.ctx.complete = True
        self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING))
        self.assertTrue(self.notifier.assure_record_count(1))

    def test_query_ctx_error(self):
        node_count = 3
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.STARTED)
                        for i in range(node_count)]
        launch_record = make_launch(launch_id, states.PENDING,
                                    node_records)

        self.store.add_launch(launch_record)
        for node in node_records:
            self.store.add_node(node)

        self.ctx.expected_count = len(node_records)
        self.ctx.complete = False
        self.ctx.error = False

        # all but 1 node have reported ok
        self.ctx.nodes = [_one_fake_ctx_node_ok(node_records[i]['public_ip'],
                          _new_id(), _new_id()) for i in range(node_count - 1)]
        self.ctx.nodes.append(_one_fake_ctx_node_error(node_records[-1]['public_ip'],
                              _new_id(), _new_id()))

        ok_ids = [node_records[i]['node_id'] for i in range(node_count - 1)]
        error_ids = [node_records[-1]['node_id']]

        self.ctx.complete = True
        self.ctx.error = True

        self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING, ok_ids))
        self.assertTrue(self.notifier.assure_state(states.RUNNING_FAILED, error_ids))

    def test_query_ctx_nodes_not_pending(self):
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.REQUESTED)
                        for i in range(3)]
        node_records.append(make_node(launch_id, states.STARTED))
        launch_record = make_launch(launch_id, states.PENDING,
                                    node_records)
        self.store.add_launch(launch_record)
        for node in node_records:
            self.store.add_node(node)

        self.core.query_contexts()

        # ensure that no context was actually queried. See the note in
        # _query_one_context for the reason why this is important.
        self.assertEqual(len(self.ctx.queried_uris), 0)

    def test_query_ctx_nodes_pending_but_actually_running(self):
        """
        When doing large runs, a few EC2 instances get their status changed to
        "running" a long time after having requested them (up to 15 minutes,
        compared to about 30 seconds normally).
        It appears that these instances have been booted successfully for a
        while, because they are reachable through SSH and the context broker
        has OK'ed them.
        Test that we detect these "pending but actually running" instances
        early.
        """
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.PENDING)
                        for i in range(3)]
        node_records.append(make_node(launch_id, states.STARTED))
        launch_record = make_launch(launch_id, states.PENDING,
                                    node_records)
        self.store.add_launch(launch_record)
        for node in node_records:
            self.store.add_node(node)

        self.ctx.nodes = [_one_fake_ctx_node_ok(node['public_ip'], _new_id(),
                          _new_id()) for node in node_records]
        self.ctx.complete = True

        self.core.query_contexts()

        launch = self.store.get_launch(launch_id)
        self.assertEqual(launch['state'], states.RUNNING)

        for node_id in launch['node_ids']:
            node = self.store.get_node(node_id)
            self.assertEqual(states.RUNNING, node['state'])

    def test_query_ctx_permanent_broker_error(self):
        node_count = 3
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.STARTED)
                        for i in range(node_count)]
        node_ids = [node['node_id'] for node in node_records]
        launch_record = make_launch(launch_id, states.PENDING, node_records)
        self.store.add_launch(launch_record)
        for node in node_records:
            self.store.add_node(node)

        self.ctx.query_error = ContextNotFoundError()
        self.core.query_contexts()

        self.assertTrue(self.notifier.assure_state(states.RUNNING_FAILED, node_ids))
        launch = self.store.get_launch(launch_id)
        self.assertEqual(launch['state'], states.FAILED)

    def test_query_ctx_with_one_node_timeout(self):
        launch_id = _new_id()
        node_record = make_node(launch_id, states.STARTED)
        launch_record = make_launch(launch_id, states.PENDING, [node_record])

        ts = time.time()
        node_record['running_timestamp'] = ts - INSTANCE_READY_TIMEOUT - 10

        self.store.add_launch(launch_record)
        self.store.add_node(node_record)

        self.ctx.expected_count = 1
        self.ctx.complete = False
        self.ctx.error = False

        self.ctx.nodes = []
        self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING_FAILED))
        self.assertTrue(self.notifier.assure_record_count(1))

    def test_query_ctx_with_several_nodes_timeout(self):
        node_count = 3
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.STARTED)
                        for i in range(node_count)]
        launch_record = make_launch(launch_id, states.PENDING,
                                    node_records)
        node_ids = map(lambda node: node['node_id'], node_records)

        ts = time.time()
        for i in range(node_count - 1):
            node_records[i]['running_timestamp'] = ts - INSTANCE_READY_TIMEOUT + 10
        node_records[-1]['running_timestamp'] = ts - INSTANCE_READY_TIMEOUT - 10

        self.store.add_launch(launch_record)
        for node in node_records:
            self.store.add_node(node)

        self.ctx.expected_count = len(node_records)
        self.ctx.complete = False
        self.ctx.error = False

        # all but 1 node have reported ok
        self.ctx.nodes = [_one_fake_ctx_node_ok(node_records[i]['public_ip'],
                          _new_id(), _new_id()) for i in range(node_count - 1)]

        self.core.query_contexts()

        self.assertTrue(self.notifier.assure_state(states.RUNNING, node_ids[:node_count - 1]))
        self.assertEqual(len(self.notifier.nodes), node_count)
        self.assertTrue(self.notifier.assure_state(states.RUNNING_FAILED, node_ids[node_count - 1:]))
        self.assertTrue(self.notifier.assure_record_count(1, node_ids[node_count - 1:]))

    def test_query_ctx_with_no_timeout(self):
        caller = "asterix"
        launch_id = _new_id()
        node_record = make_node(launch_id, states.STARTED)
        launch_record = make_launch(launch_id, states.PENDING, [node_record],
                                    caller=caller)

        ts = time.time()
        node_record['running_timestamp'] = ts - INSTANCE_READY_TIMEOUT - 10

        self.store.add_launch(launch_record)
        self.store.add_node(node_record)

        self.ctx.expected_count = 1
        self.ctx.complete = False
        self.ctx.error = False

        self.ctx.nodes = [_one_fake_ctx_node_not_done(node_record['public_ip'],
                          _new_id(), _new_id())]
        self.core.query_contexts()

        self.assertTrue(self.notifier.assure_record_count(0))

    def test_update_node_ip_info(self):
        node = dict(public_ip=None)
        iaas_node = Mock(public_ip=None, private_ip=None, extra={'dns_name': None})
        update_node_ip_info(node, iaas_node)
        self.assertEqual(node['public_ip'], None)
        self.assertEqual(node['private_ip'], None)

        iaas_node = Mock(public_ip=["pub1"], private_ip=["priv1"], extra={'dns_name': 'host'})
        update_node_ip_info(node, iaas_node)
        self.assertEqual(node['public_ip'], "pub1")
        self.assertEqual(node['private_ip'], "priv1")
        self.assertEqual(node['hostname'], "host")

        iaas_node = Mock(public_ip=[], private_ip=[], extra={'dns_name': []})
        update_node_ip_info(node, iaas_node)
        self.assertEqual(node['public_ip'], "pub1")
        self.assertEqual(node['private_ip'], "priv1")
        self.assertEqual(node['hostname'], "host")

    def test_update_nodes_from_ctx(self):
        launch_id = _new_id()
        nodes = [make_node(launch_id, states.STARTED)
                 for i in range(5)]
        ctx_nodes = [_one_fake_ctx_node_ok(node['public_ip'], _new_id(),
                     _new_id()) for node in nodes]

        self.assertEquals(
            len(nodes),
            len(update_nodes_from_context(match_nodes_from_context(nodes, ctx_nodes))))

    def test_update_nodes_from_ctx_with_hostname(self):
        launch_id = _new_id()
        nodes = [make_node(launch_id, states.STARTED)
                 for i in range(5)]
        # libcloud puts the hostname in the public_ip field
        ctx_nodes = [_one_fake_ctx_node_ok(ip=_new_id(), hostname=node['public_ip'],
                     pubkey=_new_id()) for node in nodes]

        self.assertEquals(
            len(nodes),
            len(update_nodes_from_context(match_nodes_from_context(nodes, ctx_nodes))))

    def test_query_broker_exception(self):
        caller = "asterix"
        for i in range(2):
            launch_id = _new_id()
            node_records = [make_node(launch_id, states.STARTED)]
            launch_record = make_launch(launch_id, states.PENDING, node_records, caller=caller)

            self.store.add_launch(launch_record)
            for node in node_records:
                self.store.add_node(node)

        # no guaranteed order here so grabbing first launch from store
        # and making that one return a BrokerError during context query.
        # THe goal is to ensure that one error doesn't prevent querying
        # for other contexts.

        launches = self.store.get_launches(state=states.PENDING)
        error_launch = launches[0]
        error_launch_ctx = error_launch['context']['uri']
        ok_node_id = launches[1]['node_ids'][0]
        ok_node = self.store.get_node(ok_node_id)

        self.ctx.uri_query_error[error_launch_ctx] = BrokerError("bad broker")
        self.ctx.nodes = [_one_fake_ctx_node_ok(ok_node['public_ip'],
                          _new_id(), _new_id())]
        self.ctx.complete = True
        self.core.query_contexts()

        launches = self.store.get_launches()
        for launch in launches:
            self.assertIn(launch['context']['uri'], self.ctx.queried_uris)

            if launch['launch_id'] == error_launch['launch_id']:
                self.assertEqual(launch['state'], states.PENDING)
                expected_node_state = states.STARTED
            else:
                self.assertEqual(launch['state'], states.RUNNING)
                expected_node_state = states.RUNNING

            node = self.store.get_node(launch['node_ids'][0])
            self.assertEqual(node['state'], expected_node_state)

    def test_query_ctx_without_valid_nodes(self):

        caller = "asterix"
        # if there are no nodes < TERMINATING, no broker query should happen
        for i in range(3):
            launch_id = _new_id()
            node_records = [make_node(launch_id, states.STARTED)]
            launch_record = make_launch(launch_id, states.PENDING,
                                        node_records, caller=caller)

            self.store.add_launch(launch_record)
            for node in node_records:
                self.store.add_node(node)

        launches = self.store.get_launches(state=states.PENDING)
        error_launch = launches[0]

        # mark first launch's node as TERMINATING, should prevent
        # context query and result in launch being marked FAILED
        error_launch_node = self.store.get_node(error_launch['node_ids'][0])
        error_launch_node['state'] = states.TERMINATING
        self.store.update_node(error_launch_node)

        self.core.query_contexts()
        self.assertNotIn(error_launch['context']['uri'], self.ctx.queried_uris)

        launches = self.store.get_launches()
        for launch in launches:
            if launch['launch_id'] == error_launch['launch_id']:
                self.assertEqual(launch['state'], states.FAILED)
                expected_node_state = states.TERMINATING
            else:
                self.assertEqual(launch['state'], states.PENDING)
                expected_node_state = states.STARTED

            node = self.store.get_node(launch['node_ids'][0])
            self.assertEqual(node['state'], expected_node_state)

    def test_dump_state(self):
        caller = "asterix"
        node_ids = []
        node_records = []
        for i in range(3):
            launch_id = _new_id()
            nodes = [make_node(launch_id, states.PENDING)]
            node_ids.append(nodes[0]['node_id'])
            node_records.extend(nodes)
            launch = make_launch(launch_id, states.PENDING,
                                 nodes, caller=caller)
            self.store.add_launch(launch)
            for node in nodes:
                self.store.add_node(node)

        self.core.dump_state(node_ids[:2])

        # should have gotten notifications about the 2 nodes
        self.assertEqual(self.notifier.nodes_rec_count[node_ids[0]], 1)
        self.assertEqual(node_records[0], self.notifier.nodes[node_ids[0]])
        self.assertEqual(node_records[1], self.notifier.nodes[node_ids[1]])
        self.assertEqual(self.notifier.nodes_rec_count[node_ids[1]], 1)
        self.assertNotIn(node_ids[2], self.notifier.nodes)

    def test_mark_nodes_terminating(self):
        caller = "asterix"
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.RUNNING)
                        for i in range(3)]
        launch_record = make_launch(launch_id, states.PENDING,
                                    node_records, caller=caller)

        self.store.add_launch(launch_record)
        for node in node_records:
            self.store.add_node(node)

        first_two_node_ids = [node_records[0]['node_id'],
                              node_records[1]['node_id']]
        self.core.mark_nodes_terminating(first_two_node_ids)

        self.assertTrue(self.notifier.assure_state(states.TERMINATING,
                                                   nodes=first_two_node_ids))
        self.assertNotIn(node_records[2]['node_id'], self.notifier.nodes)

        for node_id in first_two_node_ids:
            terminating_node = self.store.get_node(node_id)
            self.assertEqual(terminating_node['state'], states.TERMINATING)

    def test_describe(self):
        caller = "asterix"
        node_ids = []
        for _ in range(3):
            launch_id = _new_id()
            node_records = [make_node(launch_id, states.RUNNING)]
            node_ids.append(node_records[0]['node_id'])
            launch_record = make_launch(
                launch_id, states.PENDING,
                node_records, caller=caller)
            self.store.add_launch(launch_record)
            for node in node_records:
                self.store.add_node(node)

        all_nodes = self.core.describe_nodes()
        all_node_ids = [n['node_id'] for n in all_nodes]
        self.assertEqual(set(all_node_ids), set(node_ids))
        self.assertFalse(any(VERSION_KEY in n for n in all_nodes))

        all_nodes = self.core.describe_nodes(node_ids)
        all_node_ids = [m['node_id'] for m in all_nodes]
        self.assertEqual(set(all_node_ids), set(node_ids))

        subset_nodes = self.core.describe_nodes(node_ids[1:])
        subset_node_ids = [o['node_id'] for o in subset_nodes]
        self.assertEqual(set(subset_node_ids), set(node_ids[1:]))

        one_node = self.core.describe_nodes([node_ids[0]])
        self.assertEqual(len(one_node), 1)
        self.assertEqual(one_node[0]['node_id'], node_ids[0])
        self.assertEqual(one_node[0]['state'], states.RUNNING)

        self.assertNotIn(VERSION_KEY, one_node[0])

        try:
            self.core.describe_nodes([node_ids[0], "not-a-real-node"])
        except KeyError:
            pass
        else:
            self.fail("Expected exception for bad node_id")

    def test_maybe_update_node(self):

        node = dict(launch_id="somelaunch", node_id="anode",
                    state=states.REQUESTED)
        self.store.add_node(node)

        node2 = self.store.get_node("anode")

        node['state'] = states.PENDING
        self.store.update_node(node)

        # this should succeed even though we are basing off of an older copy
        node2['state'] = states.RUNNING
        node3, updated = self.core.maybe_update_node(node2)
        self.assertTrue(updated)
        self.assertEqual(node3['state'], states.RUNNING)

        node4 = self.store.get_node("anode")
        self.assertEqual(node4['state'], states.RUNNING)

    def test_out_of_order_launch_and_terminate(self):

        # test case where a node terminate request arrives before
        # the launch request.
        self.core.context = None
        launch_id = _new_id()
        instance_id = _new_id()

        self.core.mark_nodes_terminating([instance_id])
        self.assertTrue(self.notifier.assure_state(states.TERMINATED,
                                                   nodes=[instance_id]))
        self._prepare_execute(
            launch_id=launch_id, instance_ids=[instance_id],
            context_enabled=False, assure_state=False)
        self.assertTrue(self.notifier.assure_state(states.TERMINATED,
                                                   nodes=[instance_id]))
        # make sure nothing was launched
        self.assertFalse(self.site1_driver.list_nodes())


def _one_fake_ctx_node_ok(ip, hostname, pubkey):
    identity = Mock(ip=ip, hostname=hostname, pubkey=pubkey)
    return Mock(ok_occurred=True, error_occurred=False, identities=[identity])


def _one_fake_ctx_node_error(ip, hostname, pubkey):
    identity = Mock(ip=ip, hostname=hostname, pubkey=pubkey)
    return Mock(ok_occurred=False, error_occurred=True, identities=[identity],
                error_code=42, error_message="bad bad fake error")


def _one_fake_ctx_node_not_done(ip, hostname, pubkey):
    identity = Mock(ip=ip, hostname=hostname, pubkey=pubkey)
    return Mock(ok_occurred=False, error_occurred=False, identities=[identity])


def _new_id():
    return str(uuid.uuid4())


_ONE_NODE_CLUSTER_DOC = """
<cluster>
  <workspace>
    <name>%s</name>
    <quantity>%d</quantity>
    <image>%s</image>
    <ctx></ctx>
  </workspace>
</cluster>
"""


def _get_one_node_cluster_doc(name, imagename, quantity=1):
    return _ONE_NODE_CLUSTER_DOC % (name, quantity, imagename)
