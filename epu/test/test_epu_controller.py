import unittest

from ion.util import procutils
from twisted.internet import defer

from ion.test.iontest import IonTestCase
from epu.ionproc.epu_controller_client import EPUControllerClient

class EPUControllerServiceTest(IonTestCase):
    def setUp(self):
        raise unittest.SkipTest()

    @defer.inlineCallbacks
    def tearDown(self):
        if self.controller:
            if self.controller.worker_queue_receiver:
                yield self.controller.worker_queue_receiver.activate()
            self.controller = None
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_basic_init(self):
        spawnargs = {"queue_name_work" : "testqueuename"}
        controller = EPUControllerService(spawnargs=spawnargs)
        self.controller = controller
        controller_id = yield self._spawn_process(controller)
        self.assertIn("testqueuename", controller.queue_name_work)

        # testing workaround for race between slc_init and queue binding.
        # this is hopefully short term and the workaround can be removed
        # after the bug is fixed in ioncore.
        self.assertTrue(controller.core.control_loop is None)
        yield procutils.asleep(1.1)
        self.assertTrue(controller.core.control_loop is not None)

    @defer.inlineCallbacks
    def test_no_workqueue(self):
        spawnargs = {}
        controller = EPUControllerService(spawnargs=spawnargs)
        self.controller = controller
        controller_id = yield self._spawn_process(controller)
        self.assertEqual(controller.queue_name_work, None)

    @defer.inlineCallbacks
    def test_reconfigure_recovery(self):

        # test that engine config is correctly using reconfigured engine values
        # in subsequent starts of the controller. Strategy is:
        #   1. Start up a controller with some config
        #   2. Call reconfigure with some more config
        #   3. Kill controller and restart it, verify engine got the merged
        #      config.

        store = ControllerStore()
        spawnargs = {'engine_class' : 'epu.epucontroller.test.test_controller_core.FakeEngine',
                     'engine_conf' : {'a' : 'a1', 'b' : 'b1'},
                     'servicename' : 'TestEPUController',
                     'store' : store}
        controller = EPUControllerService(spawnargs=spawnargs)
        self.controller = controller
        yield self._spawn_process(controller)

        client = EPUControllerClient(targetname='TestEPUController')
        yield client.attach()

        yield procutils.asleep(1.01)

        yield client.reconfigure_rpc({'a' : 'a2', 'c' : 'c1'})
        yield procutils.asleep(1.01)
        controller.terminate()

        controller = EPUControllerService(spawnargs=spawnargs)
        self.controller = controller
        yield self._spawn_process(controller)
        yield procutils.asleep(1.01)

        self.assertEqual(controller.core.engine.initialize_conf,
                {'a' : 'a2', 'b': 'b1', 'c' : 'c1'})
