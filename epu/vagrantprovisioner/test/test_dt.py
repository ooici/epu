#!/usr/bin/env python

import os
import uuid
import time

from twisted.internet import defer, threads
from twisted.trial import unittest

import ion.util.ionlog

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

class ProvisionerCoreDTTests(unittest.TestCase):
    """Test framework for testing DTs
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

    def tearDown(self):
        self._shutdown_all()


    @defer.inlineCallbacks
    def test_dt_template(self):
        """A template for testing a dt with LWP.
        
        Simply plug in your dt and cookbook and dt locations to test it. Then
        use vagrant.ssh() to test whether things are in place as you'd expect.
        """
        deployable_type = "simple"
        vagrant_box = "base"
        vagrant_memory = 128
        cookbooks_path = None # change to "/path/to/cookbooks"
        dt_path = None # change to "/path/to/dts"


        if cookbooks_path and dt_path:
            self.dtrs = DirectoryDTRS(dt_path, cookbooks_path)

            self.core = VagrantProvisionerCore(store=self.store, notifier=self.notifier,
                                        dtrs=self.dtrs, context=None, site_drivers=None)

        launch, nodes = yield self._execute_provision(deployable_type, vagrant_box, vagrant_memory)

        node = nodes[0]
        vagrant_directory = node['vagrant_directory']
        vagrant_vm = self.core.vagrant_manager.get_vm(vagrant_directory)
        
        # Poke at VM to see that its working as expected
        stdout, stderr, ret = vagrant_vm.ssh("ls -d /home/foo")
        self.assertEqual("/home/foo\n", stdout)

    # You may need to adjust this timeout depending on how long your Chef recipe takes
    test_dt_template.timeout = 240


    @defer.inlineCallbacks
    def _execute_provision(self, dt=None, box="base", memory=128):
        request_node = dict(ids=[_new_id()], vagrant_box=box, vagrant_memory=memory)
        request_nodes = {"node1" : request_node}
        request = dict(launch_id=_new_id(), deployable_type=dt,
                       subscribers=('blah',), nodes=request_nodes)

        launch, nodes = yield self.core.prepare_provision(request)
        self.assertEqual(len(nodes), 1)
        node = nodes[0]
        self.assertEqual(node['node_id'], request_node['ids'][0])
        self.assertEqual(launch['launch_id'], request['launch_id'])

        self.assertTrue(self.notifier.assure_state(states.REQUESTED))

        yield self.core.execute_provision(launch, nodes)
        print nodes

        defer.returnValue((launch, nodes))


    @defer.inlineCallbacks
    def _shutdown_all(self):
        yield self.core.terminate_all()

def _new_id():
    return str(uuid.uuid4())
