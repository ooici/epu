from twisted.internet import defer

from ion.test.iontest import IonTestCase
from epu.ionproc.epu_controller import EPUControllerService

class EPUControllerServiceTest(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()

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

    @defer.inlineCallbacks
    def test_no_workqueue(self):
        spawnargs = {}
        controller = EPUControllerService(spawnargs=spawnargs)
        self.controller = controller
        controller_id = yield self._spawn_process(controller)
        self.assertEqual(controller.queue_name_work, None)



