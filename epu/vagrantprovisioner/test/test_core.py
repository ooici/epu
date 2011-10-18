#!/usr/bin/env python


import uuid
import time

from libcloud.compute.types import InvalidCredsError
from twisted.internet import defer, threads
from twisted.trial import unittest

import ion.util.ionlog

from epu.ionproc.dtrs import DeployableTypeLookupError
from epu.vagrantprovisioner.core import VagrantProvisionerCore, update_nodes_from_context, \
    update_node_ip_info
from epu.vagrantprovisioner.vagrant import FakeVagrant
from epu.vagrantprovisioner.directorydtrs import DirectoryDTRS
from epu.provisioner.store import ProvisionerStore
from epu import states
from epu.provisioner.test.util import FakeProvisionerNotifier, \
    FakeNodeDriver, FakeContextClient, make_launch, make_node
from epu.vagrantprovisioner.test.util import make_launch_and_nodes
from epu.test import Mock

log = ion.util.ionlog.getLogger(__name__)


class ProvisionerCoreRecoveryTests(unittest.TestCase):

    def setUp(self):
        self.notifier = FakeProvisionerNotifier()
        self.store = ProvisionerStore()
        self.ctx = FakeContextClient()
        self.driver = FakeNodeDriver()
        #TODO: DirectoryDTRS should point to some sane defaults
        self.dtrs = DirectoryDTRS("/opt/venv/jsondt/", "/opt/venv/dt-data/cookbooks")
        drivers = {'fake' : self.driver}
        self.core = VagrantProvisionerCore(store=self.store, notifier=self.notifier,
                                    dtrs=self.dtrs, site_drivers=drivers,
                                    context=self.ctx)
        self.core.vagrant_manager.vagrant = FakeVagrant

    @defer.inlineCallbacks
    def test_recover_launch_incomplete(self):
        """Ensures that launches in REQUESTED state are completed
        """

        launch_id = _new_id()

        requested_node_ids = [_new_id(), _new_id()]

        node_records = [make_node(launch_id, states.RUNNING,
                                              site='fake',
                                              ctx_name='running'),
                        make_node(launch_id, states.REQUESTED,
                                              site='fake',
                                              node_id=requested_node_ids[0],
                                              ctx_name='node'),
                        make_node(launch_id, states.REQUESTED,
                                              site='fake',
                                              node_id=requested_node_ids[1],
                                              ctx_name='node'),
                        make_node(launch_id, states.RUNNING,
                                              ctx_name='node')]
        launch_record = make_launch(launch_id, states.REQUESTED,
                                                node_records)

        print "putting launch"
        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        # 2 nodes are in REQUESTED state, so those should be launched
        yield self.core.recover()

        # because we rely on IaaS idempotency, we get full Node responses
        # for all nodes in the group. What really would cause this scenario
        # is successfully launching the full group but failing before records
        # could be written for the two REQUESTED nodes.
        self.assertEqual(2, len(self.core.vagrant_manager.vms))
        vagrant_directories = set(vagrant_directory for vagrant_directory in self.core.vagrant_manager.vms)
        self.assertEqual(2, len(vagrant_directories))

        for node_id in requested_node_ids:
            node = yield self.store.get_node(node_id)
            print node['state']
            self.assertEqual(states.PENDING, node['state'])
            self.assertTrue(node['vagrant_directory'] in vagrant_directories)

        launch = yield self.store.get_launch(launch_id)
        self.assertEqual(states.PENDING, launch['state'])

    @defer.inlineCallbacks
    def test_recovery_nodes_terminating(self):
        launch_id = _new_id()

        vagrant_vm = self.core.vagrant_manager.new_vm()
        terminating_vagrant_dir = vagrant_vm.directory

        node_records = [make_node(launch_id, states.TERMINATING,
                                             vagrant_directory=terminating_vagrant_dir,
                                             site='fake'),
                        make_node(launch_id, states.TERMINATED),
                        make_node(launch_id, states.RUNNING)]

        launch_record = make_launch(launch_id, states.RUNNING,
                                                node_records)

        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        yield self.core.recover()

        self.assertEqual(1, len(self.core.vagrant_manager.terminated_vms))
        self.assertEqual(self.core.vagrant_manager.terminated_vms[0], terminating_vagrant_dir)

        terminated = yield self.store.get_nodes(state=states.TERMINATED)
        self.assertEqual(2, len(terminated))

    @defer.inlineCallbacks
    def test_recovery_launch_terminating(self):
        launch_id = _new_id()

        terminating_vagrant_dirs = []
        terminating_vagrant_dirs.append(self.core.vagrant_manager.new_vm().directory)
        terminating_vagrant_dirs.append(self.core.vagrant_manager.new_vm().directory)

        node_records = [make_node(launch_id, states.TERMINATING,
                                              vagrant_directory=terminating_vagrant_dirs[0],
                                              site='fake'),
                        make_node(launch_id, states.TERMINATED),
                        make_node(launch_id, states.RUNNING,
                                              vagrant_directory=terminating_vagrant_dirs[1],
                                              site='fake')]

        launch_record = make_launch(launch_id, states.TERMINATING,
                                                node_records)

        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        yield self.core.recover()

        self.assertEqual(2, len(self.core.vagrant_manager.terminated_vms))
        self.assertTrue(self.core.vagrant_manager.terminated_vms[0] in terminating_vagrant_dirs)
        self.assertTrue(self.core.vagrant_manager.terminated_vms[1] in terminating_vagrant_dirs)

        terminated = yield self.store.get_nodes(state=states.TERMINATED)
        self.assertEqual(3, len(terminated))

        launch_record = yield self.store.get_launch(launch_id)
        self.assertEqual(launch_record['state'], states.TERMINATED)

    @defer.inlineCallbacks
    def test_terminate_all(self):
        running_launch_id = _new_id()
        running_launch, running_nodes = make_launch_and_nodes(
                running_launch_id, 3, states.RUNNING)
        yield self.store.put_launch(running_launch)
        yield self.store.put_nodes(running_nodes)

        pending_launch_id = _new_id()
        pending_launch, pending_nodes = make_launch_and_nodes(
                pending_launch_id, 3, states.PENDING)
        yield self.store.put_launch(pending_launch)
        yield self.store.put_nodes(pending_nodes)

        terminated_launch_id = _new_id()
        terminated_launch, terminated_nodes = make_launch_and_nodes(
                terminated_launch_id, 3, states.TERMINATED)
        yield self.store.put_launch(terminated_launch)
        yield self.store.put_nodes(terminated_nodes)

        yield self.core.terminate_all()

        self.assertEqual(6, len(self.core.vagrant_manager.terminated_vms))

        all_launches = yield self.store.get_launches()
        self.assertEqual(3, len(all_launches))
        self.assertTrue(all(l['state'] == states.TERMINATED
                           for l in all_launches))

        all_nodes = yield self.store.get_nodes()
        self.assertEqual(9, len(all_nodes))
        self.assertTrue(all(n['state'] == states.TERMINATED
                           for n in all_nodes))

        state = yield self.core.check_terminate_all()
        self.assertTrue(state)


class ProvisionerCoreTests(unittest.TestCase):
    """Testing the provisioner core functionality
    """
    def setUp(self):
        self.notifier = FakeProvisionerNotifier()
        self.store = ProvisionerStore()
        self.ctx = FakeContextClient()
        self.dtrs = FakeDTRS()

        self.site1_driver = FakeNodeDriver()
        self.site2_driver = FakeNodeDriver()

        drivers = {'site1' : self.site1_driver, 'site2' : self.site2_driver}
        self.core = VagrantProvisionerCore(store=self.store, notifier=self.notifier,
                                    dtrs=self.dtrs, context=self.ctx,
                                    site_drivers=drivers)

    @defer.inlineCallbacks
    def test_prepare_dtrs_error(self):

        request = dict(launch_id=_new_id(), deployable_type="foo",
                       subscribers=('blah',))
        yield self.core.prepare_provision(request)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    @defer.inlineCallbacks
    def _test_prepare_broker_error(self):
        self.ctx.create_error = BrokerError("fake ctx create failed")
        self.dtrs.result = {'document' : "<fake>document</fake>",
                            "nodes" : {"i1" : {}}}
        nodes = {"i1" : dict(ids=[_new_id()], site="site1", allocation="small")}
        request = dict(launch_id=_new_id(), deployable_type="foo",
                       subscribers=('blah',), nodes=nodes)
        yield self.core.prepare_provision(request)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    @defer.inlineCallbacks
    def test_prepare_execute(self):
        yield self._prepare_execute()
        self.assertTrue(self.notifier.assure_state(states.PENDING))
        yield self._shutdown_all()
        self.assertTrue(self.notifier.assure_state(states.TERMINATED))

    @defer.inlineCallbacks
    def test_prepare_execute_vagrant_fail(self):
        oldvagrant = self.core.vagrant_manager.vagrant
        self.core.vagrant_manager.vagrant = FakeVagrant
        self.core.vagrant_manager.vagrant.fail = True

        yield self._prepare_execute()
        self.assertTrue(self.notifier.assure_state(states.FAILED))

        self.core.vagrant_manager.vagrant = oldvagrant

    @defer.inlineCallbacks
    def _prepare_execute(self):
        self.dtrs.result = {'document' : _get_one_node_cluster_doc("node1", "image1"),
                            "nodes" : {"node1" : {}}}
        request_node = dict(ids=[_new_id()], site="site1", allocation="small")
        request_nodes = {"node1" : request_node}
        request = dict(launch_id=_new_id(), deployable_type="foo",
                       subscribers=('blah',), nodes=request_nodes)

        log.debug("PDA: request: %s" % request)
        launch, nodes = yield self.core.prepare_provision(request)
        log.debug("PDA: launch: %s nodes: %s" % (launch, nodes))
        self.assertEqual(len(nodes), 1)
        node = nodes[0]
        self.assertEqual(node['node_id'], request_node['ids'][0])
        self.assertEqual(launch['launch_id'], request['launch_id'])

        self.assertTrue(self.notifier.assure_state(states.REQUESTED))

        yield self.core.execute_provision(launch, nodes)

    @defer.inlineCallbacks
    def _shutdown_all(self):
        yield self.core.terminate_all()
        

    @defer.inlineCallbacks
    def _test_execute_bad_doc(self):
        ctx = yield self.ctx.create()
        launch_record = {
                'launch_id' : "thelaunchid",
                'document' : "<this><isnt><a><real><doc>",
                'deployable_type' : "dt",
                'context' : ctx,
                'subscribers' : [],
                'state' : states.PENDING,
                'node_ids' : ['node1']}
        nodes = [{'node_id' : 'node1', 'launch_id' : "thelaunchid",
                  'state' : states.REQUESTED}]

        yield self.core.execute_provision(launch_record, nodes)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

        # TODO this should be a better error coming from nimboss
        #self.assertEqual(self.notifier.nodes['node1']['state_desc'], "CONTEXT_DOC_INVALID")

    @defer.inlineCallbacks
    def _test_execute_bad_doc_nodes(self):
        ctx = yield self.ctx.create()
        launch_record = {
                'launch_id' : "thelaunchid",
                'document' : _get_one_node_cluster_doc("node1", "image1"),
                'deployable_type' : "dt",
                'context' : ctx,
                'subscribers' : [],
                'state' : states.PENDING,
                'node_ids' : ['node1']}
        nodes = [{'node_id' : 'node1', 'launch_id' : "thelaunchid",
                  'state' : states.REQUESTED, 'ctx_name' : "adifferentname"}]

        yield self.core.execute_provision(launch_record, nodes)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    @defer.inlineCallbacks
    def _test_execute_bad_doc_node_count(self):
        ctx = yield self.ctx.create()
        launch_record = {
                'launch_id' : "thelaunchid",
                'document' : _get_one_node_cluster_doc("node1", "image1"),
                'deployable_type' : "dt",
                'context' : ctx,
                'subscribers' : [],
                'state' : states.PENDING,
                'node_ids' : ['node1']}

        # two nodes where doc expects 1
        nodes = [{'node_id' : 'node1', 'launch_id' : "thelaunchid",
                  'state' : states.REQUESTED, 'ctx_name' : "node1"},
                 {'node_id' : 'node1', 'launch_id' : "thelaunchid",
                  'state' : states.REQUESTED, 'ctx_name' : "node1"}]

        yield self.core.execute_provision(launch_record, nodes)
        self.assertTrue(self.notifier.assure_state(states.FAILED))


    @defer.inlineCallbacks
    def _test_query_missing_node_within_window(self):
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
    def _test_query_missing_node_past_window(self):
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
    def test_query(self):

        oldvagrant = self.core.vagrant_manager.vagrant
        self.core.vagrant_manager.vagrant = FakeVagrant

        launch_id = _new_id()
        node_id = _new_id()

        vagrant_node = yield threads.deferToThread(self.core.vagrant_manager.new_vm)
        yield threads.deferToThread(vagrant_node.up)

        ts = time.time() - 120.0
        launch = {
                'launch_id' : launch_id, 
                'vagrant_directory' : [vagrant_node.directory],
                'state' : states.PENDING,
                'subscribers' : 'fake-subscribers'}
        node = {'launch_id' : launch_id,
                'node_id' : node_id,
                'vagrant_directory' : vagrant_node.directory,
                'state' : states.PENDING,
                'ip' : vagrant_node.ip,
                'pending_timestamp' : ts,
                'site':'site1'}

        req_node = {'launch_id' : launch_id,
                'node_id' : _new_id(),
                'state' : states.REQUESTED}
        nodes = [node, req_node]
        yield self.store.put_launch(launch)
        yield self.store.put_node(node)
        yield self.store.put_node(req_node)

        yield self.core.query_nodes()

        node = yield self.store.get_node(node_id)
        self.assertEqual(node['ip'], vagrant_node.ip)
        self.assertEqual(node['state'], states.STARTED)

        # query again should detect no changes
        yield self.core.query_nodes()
        node = yield self.store.get_node(node_id)

        # now destroy
        yield self.core.terminate_nodes([node_id])
        yield self.core.query_nodes()

        node = yield self.store.get_node(node_id)
        self.assertEqual(node['ip'], vagrant_node.ip)
        self.assertEqual(node['state'], states.TERMINATED)

        self.core.vagrant_manager.vagrant = oldvagrant


    @defer.inlineCallbacks
    def _test_query_ctx(self):
        node_count = 3
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.STARTED)
                for i in range(node_count)]
        launch_record = make_launch(launch_id, states.PENDING,
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
    def _test_query_ctx_error(self):
        node_count = 3
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.STARTED)
                for i in range(node_count)]
        launch_record = make_launch(launch_id, states.PENDING,
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
        self.assertTrue(self.notifier.assure_state(states.RUNNING_FAILED, error_ids))

    @defer.inlineCallbacks
    def _test_query_ctx_nodes_not_started(self):
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.PENDING)
                for i in range(3)]
        node_records.append(make_node(launch_id, states.STARTED))
        launch_record = make_launch(launch_id, states.PENDING,
                                                node_records)
        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        yield self.core.query_contexts()

        # ensure that no context was actually queried. See the note in
        # _query_one_context for the reason why this is important.
        self.assertEqual(len(self.ctx.queried_uris), 0)

    @defer.inlineCallbacks
    def _test_query_ctx_permanent_broker_error(self):
        node_count = 3
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.STARTED)
                for i in range(node_count)]
        node_ids = [node['node_id'] for node in node_records]
        launch_record = make_launch(launch_id, states.PENDING,
                                                node_records)
        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        self.ctx.query_error = ContextNotFoundError()
        yield self.core.query_contexts()

        self.assertTrue(self.notifier.assure_state(states.RUNNING_FAILED, node_ids))
        launch = yield self.store.get_launch(launch_id)
        self.assertEqual(launch['state'], states.FAILED)

    def _test_update_node_ip_info(self):
        node = dict(public_ip=None)
        iaas_node = Mock(public_ip=None, private_ip=None)
        update_node_ip_info(node, iaas_node)
        self.assertEqual(node['public_ip'], None)
        self.assertEqual(node['private_ip'], None)

        iaas_node = Mock(public_ip=["pub1"], private_ip=["priv1"])
        update_node_ip_info(node, iaas_node)
        self.assertEqual(node['public_ip'], "pub1")
        self.assertEqual(node['private_ip'], "priv1")

        iaas_node = Mock(public_ip=[], private_ip=[])
        update_node_ip_info(node, iaas_node)
        self.assertEqual(node['public_ip'], "pub1")
        self.assertEqual(node['private_ip'], "priv1")

    def _test_update_nodes_from_ctx(self):
        launch_id = _new_id()
        nodes = [make_node(launch_id, states.STARTED)
                for i in range(5)]
        ctx_nodes = [_one_fake_ctx_node_ok(node['public_ip'], _new_id(), 
            _new_id()) for node in nodes]

        self.assertEquals(len(nodes), len(update_nodes_from_context(nodes, ctx_nodes)))
        
    def _test_update_nodes_from_ctx_with_hostname(self):
        launch_id = _new_id()
        nodes = [make_node(launch_id, states.STARTED)
                for i in range(5)]
        #libcloud puts the hostname in the public_ip field
        ctx_nodes = [_one_fake_ctx_node_ok(ip=_new_id(), hostname=node['public_ip'],
            pubkey=_new_id()) for node in nodes]

        self.assertEquals(len(nodes), len(update_nodes_from_context(nodes, ctx_nodes)))

    @defer.inlineCallbacks
    def _test_query_broker_exception(self):
        for i in range(2):
            launch_id = _new_id()
            node_records = [make_node(launch_id, states.STARTED)]
            launch_record = make_launch(launch_id, states.PENDING,
                                                    node_records)

            yield self.store.put_launch(launch_record)
            yield self.store.put_nodes(node_records)

        # no guaranteed order here so grabbing first launch from store
        # and making that one return a BrokerError during context query.
        # THe goal is to ensure that one error doesn't prevent querying
        # for other contexts.

        launches = yield self.store.get_launches(state=states.PENDING)
        error_launch = launches[0]
        error_launch_ctx = error_launch['context']['uri']
        ok_node_id = launches[1]['node_ids'][0]
        ok_node = yield self.store.get_node(ok_node_id)

        self.ctx.uri_query_error[error_launch_ctx] = BrokerError("bad broker")
        self.ctx.nodes = [_one_fake_ctx_node_ok(ok_node['public_ip'],
            _new_id(), _new_id())]
        self.ctx.complete = True
        yield self.core.query_contexts()

        launches = yield self.store.get_launches()
        for launch in launches:
            self.assertIn(launch['context']['uri'], self.ctx.queried_uris)

            if launch['launch_id'] == error_launch['launch_id']:
                self.assertEqual(launch['state'], states.PENDING)
                expected_node_state = states.STARTED
            else:
                self.assertEqual(launch['state'], states.RUNNING)
                expected_node_state = states.RUNNING

            node = yield self.store.get_node(launch['node_ids'][0])
            self.assertEqual(node['state'], expected_node_state)

    @defer.inlineCallbacks
    def _test_query_ctx_without_valid_nodes(self):

        # if there are no nodes < TERMINATING, no broker query should happen
        for i in range(3):
            launch_id = _new_id()
            node_records = [make_node(launch_id, states.STARTED)]
            launch_record = make_launch(launch_id, states.PENDING,
                                                    node_records)

            yield self.store.put_launch(launch_record)
            yield self.store.put_nodes(node_records)

        launches = yield self.store.get_launches(state=states.PENDING)
        error_launch = launches[0]

        # mark first launch's node as TERMINATING, should prevent
        # context query and result in launch being marked FAILED
        error_launch_node = yield self.store.get_node(error_launch['node_ids'][0])
        error_launch_node['state'] = states.TERMINATING
        yield self.store.put_node(error_launch_node)

        yield self.core.query_contexts()
        self.assertNotIn(error_launch['context']['uri'], self.ctx.queried_uris)

        launches = yield self.store.get_launches()
        for launch in launches:
            if launch['launch_id'] == error_launch['launch_id']:
                self.assertEqual(launch['state'], states.FAILED)
                expected_node_state = states.TERMINATING
            else:
                self.assertEqual(launch['state'], states.PENDING)
                expected_node_state = states.STARTED

            node = yield self.store.get_node(launch['node_ids'][0])
            self.assertEqual(node['state'], expected_node_state)


    @defer.inlineCallbacks
    def _test_query_unexpected_exception(self):
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.STARTED)]
        launch_record = make_launch(launch_id, states.PENDING,
                                                node_records)
        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)
        self.ctx.query_error = ValueError("bad programmer")


        # digging into internals a bit: patching one of the methods query()
        # calls to raise an exception. This will let us ensure exceptions do
        # not bubble up
        def raiser(self):
            raise KeyError("notreallyaproblem")
        self.patch(self.core, 'query_nodes', raiser)

        yield self.core.query() # ensure that exception doesn't bubble up

    @defer.inlineCallbacks
    def _test_dump_state(self):
        node_ids = []
        node_records = []
        for i in range(3):
            launch_id = _new_id()
            nodes = [make_node(launch_id, states.PENDING)]
            node_ids.append(nodes[0]['node_id'])
            node_records.extend(nodes)
            launch = make_launch(launch_id, states.PENDING,
                                                    nodes)
            yield self.store.put_launch(launch)
            yield self.store.put_nodes(nodes)

        yield self.core.dump_state(node_ids[:2])

        # should have gotten notifications about the 2 nodes
        self.assertEqual(self.notifier.nodes_rec_count[node_ids[0]], 1)
        self.assertEqual(node_records[0], self.notifier.nodes[node_ids[0]])
        self.assertEqual(node_records[1], self.notifier.nodes[node_ids[1]])
        self.assertEqual(self.notifier.nodes_rec_count[node_ids[1]], 1)
        self.assertNotIn(node_ids[2], self.notifier.nodes)

    @defer.inlineCallbacks
    def _test_mark_nodes_terminating(self):
        launch_id = _new_id()
        node_records = [make_node(launch_id, states.RUNNING)
                        for i in range(3)]
        launch_record = make_launch(launch_id, states.PENDING,
                                                node_records)

        yield self.store.put_launch(launch_record)
        yield self.store.put_nodes(node_records)

        first_two_node_ids = [node_records[0]['node_id'],
                              node_records[1]['node_id']]
        yield self.core.mark_nodes_terminating(first_two_node_ids)

        self.assertTrue(self.notifier.assure_state(states.TERMINATING,
                                                   nodes=first_two_node_ids))
        self.assertNotIn(node_records[2]['node_id'], self.notifier.nodes)

        for node_id in first_two_node_ids:
            terminating_node = yield self.store.get_node(node_id)
            self.assertEqual(terminating_node['state'], states.TERMINATING)


def _one_fake_ctx_node_ok(ip, hostname, pubkey):
    identity = Mock(ip=ip, hostname=hostname, pubkey=pubkey)
    return Mock(ok_occurred=True, error_occurred=False, identities=[identity])

def _one_fake_ctx_node_error(ip, hostname, pubkey):
    identity = Mock(ip=ip, hostname=hostname, pubkey=pubkey)
    return Mock(ok_occurred=False, error_occurred=True, identities=[identity],
            error_code=42, error_message="bad bad fake error")


class FakeEmptyNodeQueryDriver(object):
    def list_nodes(self):
        return []


class FakeDTRS(object):
    def __init__(self):
        self.result = None
        self.error = None

    def lookup(self, dt, nodes=None, vars=None):
        if self.error is not None:
            return defer.fail(self.error)

        if self.result is not None:
            return defer.succeed(self.result)

        raise Exception("bad fixture: nothing to return")


def _new_id():
    return str(uuid.uuid4())

def _new_vagrant_dir(state="running"):
    fake_vagrant = FakeVagrant()
    return fake_vagrant.directory

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
    
