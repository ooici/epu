#!/usr/bin/env python
from libcloud.base import Node, NodeDriver
from libcloud.types import NodeState

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import uuid
import time

from twisted.internet import defer
from twisted.trial import unittest

from epu.provisioner.core import ProvisionerCore, update_nodes_from_context
from epu.provisioner.store import ProvisionerStore
from epu import states
from epu.provisioner.test.util import FakeProvisionerNotifier
from epu.test import Mock

class FakeRecoveryDriver(NodeDriver):
    type = 42 # libcloud uses a driver type number in id generation.
    def __init__(self):
        self.created = []
        self.destroyed = []

    def create_node(self, **kwargs):
        count = int(kwargs['ex_mincount']) if 'ex_mincount' in kwargs else 1
        nodes  = [Node(_new_id(), None, NodeState.PENDING, _new_id(), _new_id(),
                    self) for i in range(count)]
        self.created.extend(nodes)
        return nodes

    def destroy_node(self, node):
        self.destroyed.append(node)

class ProvisionerCoreRecoveryTests(unittest.TestCase):

    def setUp(self):
        self.notifier = FakeProvisionerNotifier()
        self.store = ProvisionerStore()
        self.ctx = FakeContextClient()
        self.driver = FakeRecoveryDriver()
        drivers = {'fake' : self.driver}
        self.core = ProvisionerCore(store=self.store, notifier=self.notifier,
                                    dtrs=None, site_drivers=drivers,
                                    context=self.ctx)

    @defer.inlineCallbacks
    def test_recover_launch_incomplete(self):
        """Ensures that launches in REQUESTED state are completed
        """
        launch_id = _new_id()
        doc = "<cluster><workspace><name>node</name><image>fake</image>"+\
              "<quantity>3</quantity>"+\
              "</workspace><workspace><name>running</name><image>fake"+\
              "</image><quantity>1</quantity></workspace></cluster>"
        context = {'broker_uri' : _new_id(), 'context_id' : _new_id(),
                  'secret' : _new_id(), 'uri' : _new_id()}

        requested_node_ids = [_new_id(), _new_id()]

        node_records = [_one_fake_node_record(launch_id, states.RUNNING,
                                              site='fake',
                                              ctx_name='running'),
                        _one_fake_node_record(launch_id, states.REQUESTED,
                                              site='fake',
                                              node_id=requested_node_ids[0],
                                              ctx_name='node'),
                        _one_fake_node_record(launch_id, states.REQUESTED,
                                              site='fake',
                                              node_id=requested_node_ids[1],
                                              ctx_name='node'),
                        _one_fake_node_record(launch_id, states.RUNNING,
                                              ctx_name='node')]
        launch_record = _one_fake_launch_record(launch_id, states.REQUESTED,
                                                node_records, document=doc,
                                                context=context)

        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        # 2 nodes are in REQUESTED state, so those should be launched
        yield self.core.recover()

        # because we rely on IaaS idempotency, we get full Node responses
        # for all nodes in the group. What really would cause this scenario
        # is successfully launching the full group but failing before records
        # could be written for the two REQUESTED nodes.
        self.assertEqual(3, len(self.driver.created))
        iaas_ids = set(node.id for node in self.driver.created)
        self.assertEqual(3, len(iaas_ids))

        for node_id in requested_node_ids:
            node = yield self.store.get_node(node_id)
            self.assertEqual(states.PENDING, node['state'])
            self.assertTrue(node['iaas_id'] in iaas_ids)

        launch = yield self.store.get_launch(launch_id)
        self.assertEqual(states.PENDING, launch['state'])

    @defer.inlineCallbacks
    def test_recovery_nodes_terminating(self):
        launch_id = _new_id()

        terminating_iaas_id = _new_id()

        node_records = [_one_fake_node_record(launch_id, states.TERMINATING,
                                              iaas_id=terminating_iaas_id,
                                              site='fake'),
                        _one_fake_node_record(launch_id, states.TERMINATED),
                        _one_fake_node_record(launch_id, states.RUNNING)]

        launch_record = _one_fake_launch_record(launch_id, states.RUNNING,
                                                node_records)

        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        yield self.core.recover()

        self.assertEqual(1, len(self.driver.destroyed))
        self.assertEqual(self.driver.destroyed[0].id, terminating_iaas_id)

        terminated = yield self.store.get_nodes(state=states.TERMINATED)
        self.assertEqual(2, len(terminated))

    @defer.inlineCallbacks
    def test_recovery_launch_terminating(self):
        launch_id = _new_id()

        terminating_iaas_ids = [_new_id(), _new_id()]

        node_records = [_one_fake_node_record(launch_id, states.TERMINATING,
                                              iaas_id=terminating_iaas_ids[0],
                                              site='fake'),
                        _one_fake_node_record(launch_id, states.TERMINATED),
                        _one_fake_node_record(launch_id, states.RUNNING,
                                              iaas_id=terminating_iaas_ids[1],
                                              site='fake')]

        launch_record = _one_fake_launch_record(launch_id, states.TERMINATING,
                                                node_records)

        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        yield self.core.recover()

        self.assertEqual(2, len(self.driver.destroyed))
        self.assertTrue(self.driver.destroyed[0].id in terminating_iaas_ids)
        self.assertTrue(self.driver.destroyed[1].id in terminating_iaas_ids)

        terminated = yield self.store.get_nodes(state=states.TERMINATED)
        self.assertEqual(3, len(terminated))

        launch_record = yield self.store.get_launch(launch_id)
        self.assertEqual(launch_record['state'], states.TERMINATED)

    @defer.inlineCallbacks
    def test_terminate_all(self):
        running_launch_id = _new_id()
        running_launch, running_nodes = _fake_launch_and_nodes(
                running_launch_id, 3, states.RUNNING)
        yield self.store.put_launch(running_launch)
        yield self.store.put_nodes(running_nodes)

        pending_launch_id = _new_id()
        pending_launch, pending_nodes = _fake_launch_and_nodes(
                pending_launch_id, 3, states.PENDING)
        yield self.store.put_launch(pending_launch)
        yield self.store.put_nodes(pending_nodes)

        terminated_launch_id = _new_id()
        terminated_launch, terminated_nodes = _fake_launch_and_nodes(
                terminated_launch_id, 3, states.TERMINATED)
        yield self.store.put_launch(terminated_launch)
        yield self.store.put_nodes(terminated_nodes)

        yield self.core.terminate_all()

        self.assertEqual(6, len(self.driver.destroyed))

        all_launches = yield self.store.get_launches()
        self.assertEqual(3, len(all_launches))
        self.assertTrue(all(l['state'] == states.TERMINATED
                           for l in all_launches))

        all_nodes = yield self.store.get_nodes()
        self.assertEqual(9, len(all_nodes))
        self.assertTrue(all(n['state'] == states.TERMINATED
                           for n in all_nodes))

class ProvisionerCoreTests(unittest.TestCase):
    """Testing the provisioner core functionality
    """
    def setUp(self):
        self.notifier = FakeProvisionerNotifier()
        self.store = ProvisionerStore()
        self.ctx = FakeContextClient()

        drivers = {'fake' : None}
        self.core = ProvisionerCore(store=self.store, notifier=self.notifier,
                                    dtrs=None, context=self.ctx,
                                    site_drivers=drivers)
    
    def tearDown(self):
        self.notifier = None
        self.store = None
        self.core = None
        
    @defer.inlineCallbacks
    def test_query_missing_node_within_window(self):
        launch_id = _new_id()
        node_id = _new_id()
        ts = time.time() - 30.0
        launch = {'launch_id' : launch_id, 'node_ids' : [node_id],
                'state' : states.PENDING,
                'subscribers' : 'fake-subscribers'}
        node = {'launch_id' : launch_id,
                'node_id' : node_id,
                'state' : states.PENDING,
                'pending_timestamp' : ts}
        yield self.store.put_launch(launch)
        yield self.store.put_node(node)

        yield self.core.query_one_site('fake-site', [node],
                driver=FakeEmptyNodeQueryDriver())
        self.assertEqual(len(self.notifier.nodes), 0)
    
    @defer.inlineCallbacks
    def test_query_missing_node_past_window(self):
        launch_id = _new_id()
        node_id = _new_id()

        ts = time.time() - 120.0
        launch = {
                'launch_id' : launch_id, 'node_ids' : [node_id],
                'state' : states.PENDING,
                'subscribers' : 'fake-subscribers'}
        node = {'launch_id' : launch_id,
                'node_id' : node_id,
                'state' : states.PENDING,
                'pending_timestamp' : ts}
        yield self.store.put_launch(launch)
        yield self.store.put_node(node)

        yield self.core.query_one_site('fake-site', [node],
                driver=FakeEmptyNodeQueryDriver())
        self.assertEqual(len(self.notifier.nodes), 1)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    @defer.inlineCallbacks
    def test_query_ctx(self):
        node_count = 3
        launch_id = _new_id()
        node_records = [_one_fake_node_record(launch_id, states.STARTED)
                for i in range(node_count)]
        launch_record = _one_fake_launch_record(launch_id, states.PENDING,
                                                node_records)

        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        self.ctx.expected_count = len(node_records)
        self.ctx.complete = False
        self.ctx.error = False

        #first query with no ctx nodes. zero records should be updated
        yield self.core.query_contexts()
        self.assertTrue(self.notifier.assure_record_count(0))
        
        # all but 1 node have reported ok
        self.ctx.nodes = [_one_fake_ctx_node_ok(node_records[i]['public_ip'], 
            _new_id(),  _new_id()) for i in range(node_count-1)]

        yield self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING))
        self.assertEqual(len(self.notifier.nodes), node_count-1)

        #last node reports ok
        self.ctx.nodes.append(_one_fake_ctx_node_ok(node_records[-1]['public_ip'],
            _new_id(), _new_id()))

        self.ctx.complete = True
        yield self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING))
        self.assertTrue(self.notifier.assure_record_count(1))
    
    @defer.inlineCallbacks
    def test_query_ctx_error(self):
        node_count = 3
        launch_id = _new_id()
        node_records = [_one_fake_node_record(launch_id, states.STARTED)
                for i in range(node_count)]
        launch_record = _one_fake_launch_record(launch_id, states.PENDING,
                                                node_records)

        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        self.ctx.expected_count = len(node_records)
        self.ctx.complete = False
        self.ctx.error = False

        # all but 1 node have reported ok
        self.ctx.nodes = [_one_fake_ctx_node_ok(node_records[i]['public_ip'], 
            _new_id(),  _new_id()) for i in range(node_count-1)]
        self.ctx.nodes.append(_one_fake_ctx_node_error(node_records[-1]['public_ip'],
            _new_id(), _new_id()))

        ok_ids = [node_records[i]['node_id'] for i in range(node_count-1)]
        error_ids = [node_records[-1]['node_id']]

        self.ctx.complete = True
        self.ctx.error = True

        yield self.core.query_contexts()
        self.assertTrue(self.notifier.assure_state(states.RUNNING, ok_ids))
        self.assertTrue(self.notifier.assure_state(states.STARTED, error_ids))

    def test_update_nodes_from_ctx(self):
        launch_id = _new_id()
        nodes = [_one_fake_node_record(launch_id, states.STARTED)
                for i in range(5)]
        ctx_nodes = [_one_fake_ctx_node_ok(node['public_ip'], _new_id(), 
            _new_id()) for node in nodes]

        self.assertEquals(len(nodes), len(update_nodes_from_context(nodes, ctx_nodes)))
        
    def test_update_nodes_from_ctx_with_hostname(self):
        launch_id = _new_id()
        nodes = [_one_fake_node_record(launch_id, states.STARTED)
                for i in range(5)]
        #libcloud puts the hostname in the public_ip field
        ctx_nodes = [_one_fake_ctx_node_ok(ip=_new_id(), hostname=node['public_ip'],
            pubkey=_new_id()) for node in nodes]

        self.assertEquals(len(nodes), len(update_nodes_from_context(nodes, ctx_nodes)))

def _one_fake_launch_record(launch_id, state, node_records, **kwargs):
    node_ids = [n['node_id'] for n in node_records]
    r = {'launch_id' : launch_id,
            'state' : state, 'subscribers' : 'fake-subscribers',
            'node_ids' : node_ids,
            'context' : {'uri' : 'http://fakey.com'}}
    r.update(kwargs)
    return r

def _one_fake_node_record(launch_id, state, node_id=None, **kwargs):
    r = {'launch_id' : launch_id, 'node_id' : node_id or _new_id(),
            'state' : state, 'public_ip' : _new_id()}
    r.update(kwargs)
    return r

def _fake_launch_and_nodes(launch_id, node_count, state, site='fake'):
    node_records = []
    node_kwargs = {'site' : site}
    for i in range(node_count):
        if state >= states.PENDING:
            node_kwargs['iaas_id'] = _new_id()
        rec = _one_fake_node_record(launch_id, state, **node_kwargs)
        node_records.append(rec)
    launch_record = _one_fake_launch_record(launch_id, state,
                                            node_records)
    return launch_record, node_records

def _one_fake_ctx_node_ok(ip, hostname, pubkey):
    identity = Mock(ip=ip, hostname=hostname, pubkey=pubkey)
    return Mock(ok_occurred=True, error_occurred=False, identities=[identity])

def _one_fake_ctx_node_error(ip, hostname, pubkey):
    identity = Mock(ip=ip, hostname=hostname, pubkey=pubkey)
    return Mock(ok_occurred=False, error_occurred=True, identities=[identity],
            error_code=42, error_message="bad bad fake error")

class FakeContextClient(object):
    def __init__(self):
        self.nodes = []
        self.expected_count = 0
        self.complete = False
        self.error = False
    
    def query(self, uri):
        response = Mock(nodes=self.nodes, expected_count=self.expected_count,
        complete=self.complete, error=self.error)
        return defer.succeed(response)


class FakeEmptyNodeQueryDriver(object):
    def list_nodes(self):
        return []

def _new_id():
    return str(uuid.uuid4())

    
