#!/usr/bin/env python

import os
import uuid
import time

from libcloud.compute.types import InvalidCredsError
from twisted.internet import defer, threads
from twisted.trial import unittest

import ion.util.ionlog

from epu.ionproc.dtrs import DeployableTypeLookupError
from epu.vagrantprovisioner.core import VagrantProvisionerCore
from epu.vagrantprovisioner.vagrant import FakeVagrant
from epu.vagrantprovisioner.directorydtrs import DirectoryDTRS
from epu.provisioner.store import ProvisionerStore
from epu import states
from epu.vagrantprovisioner.test.util import FakeProvisionerNotifier, \
    FakeNodeDriver, FakeContextClient, make_launch, make_node
from epu.vagrantprovisioner.test.util import make_launch_and_nodes
from epu.test import Mock

log = ion.util.ionlog.getLogger(__name__)


class ProvisionerCoreRecoveryTests(unittest.TestCase):

    def setUp(self):
        self.notifier = FakeProvisionerNotifier()
        self.store = ProvisionerStore()
        self.ctx = FakeContextClient()
        test_dir = os.path.dirname(os.path.realpath(__file__))
        self.cookbooks_path = os.path.join(test_dir, "dt-data", "cookbooks")
        self.dt_path = os.path.join(test_dir, "dt-data", "dt")
        self.dtrs = DirectoryDTRS(self.dt_path, self.cookbooks_path)
        self.core = VagrantProvisionerCore(store=self.store, notifier=self.notifier,
                                    dtrs=self.dtrs, site_drivers=None, context=None,
                                    fake=True)

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
            self.assertEqual(states.STARTED, node['state'])
            self.assertTrue(node['vagrant_directory'] in vagrant_directories)

        launch = yield self.store.get_launch(launch_id)
        self.assertEqual(states.STARTED, launch['state'])

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
        test_dir = os.path.dirname(os.path.realpath(__file__))
        cookbooks_path = os.path.join(test_dir, "dt-data", "cookbooks")
        dt_path = os.path.join(test_dir, "dt-data", "dt")
        self.dtrs = DirectoryDTRS(dt_path, cookbooks_path)

        self.core = VagrantProvisionerCore(store=self.store, notifier=self.notifier,
                                    dtrs=self.dtrs, context=None,
                                    site_drivers=None)

    @defer.inlineCallbacks
    def test_prepare_dtrs_error(self):

        nodes = {"i1" : dict(ids=[_new_id()], site="chicago", allocation="small")}
        request = dict(launch_id=_new_id(), deployable_type="foo",
                       subscribers=('blah',), nodes=nodes)
        yield self.core.prepare_provision(request)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    @defer.inlineCallbacks
    def test_prepare_execute(self):
        #self.core.vagrant_manager.vagrant = FakeVagrant
        yield self._prepare_execute()
        self.assertTrue(self.notifier.assure_state(states.STARTED))
        yield self._shutdown_all()
        self.assertTrue(self.notifier.assure_state(states.TERMINATED))
    test_prepare_execute.timeout = 480

    @defer.inlineCallbacks
    def test_prepare_execute_vagrant_fail(self):
        self.core.vagrant_manager.vagrant = FakeVagrant
        self.core.vagrant_manager.fail = True

        yield self._prepare_execute()
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    @defer.inlineCallbacks
    def _prepare_execute(self):
        request_node = dict(ids=[_new_id()], site="site1", allocation="small")
        request_nodes = {"node1" : request_node}
        request = dict(launch_id=_new_id(), deployable_type="simple",
                       subscribers=('blah',), nodes=request_nodes)

        launch, nodes = yield self.core.prepare_provision(request)
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
    def test_execute_bad_dt(self):
        launch_record = {
                'launch_id' : "thelaunchid",
                'document' : "<this><isnt><a><real><doc>",
                'deployable_type' : "xxxdt",
                'chef_json' : '/bad/path/to/json',
                'cookbook_dir' : '/path/to/cookbooks',
                'subscribers' : [],
                'state' : states.PENDING,
                'node_ids' : ['node1']}
        nodes = [{'node_id' : 'node1', 'launch_id' : "thelaunchid",
                  'state' : states.REQUESTED}]

        yield self.core.execute_provision(launch_record, nodes)
        self.assertTrue(self.notifier.assure_state(states.FAILED))

    @defer.inlineCallbacks
    def test_execute_bad_doc_node_count(self):
        self.core.vagrant_manager.vagrant = FakeVagrant
        launch_record = {
                'launch_id' : "thelaunchid",
                'deployable_type' : "dt",
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
    def test_dump_state(self):
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
    def test_mark_nodes_terminating(self):
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


def _new_id():
    return str(uuid.uuid4())
