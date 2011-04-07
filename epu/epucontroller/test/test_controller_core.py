from twisted.trial import unittest
from twisted.internet import defer, reactor
from epu.decisionengine.engineapi import Engine

from epu.epucontroller.controller_core import ControllerCore, \
    PROVISIONER_VARS_KEY, MONITOR_HEALTH_KEY, HEALTH_BOOT_KEY, \
    HEALTH_ZOMBIE_KEY, HEALTH_MISSING_KEY

class ControllerCoreTests(unittest.TestCase):

    def setUp(self):
        self.prov_client = FakeProvisionerClient()
        self.prov_vars = {"a" : "b"}

    def test_setup_nohealth(self):
        core = ControllerCore(self.prov_client, "%s.FakeEngine" % __name__,
                              "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars})
        self.assertEqual(core.state.health, None)
        self.assertEqual(core.state.get_all("instance-health"), None)

    def test_setup_nohealth2(self):
        core = ControllerCore(self.prov_client, "%s.FakeEngine" % __name__,
                              "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars,
                               MONITOR_HEALTH_KEY : False
                               })
        self.assertEqual(core.state.health, None)
        self.assertEqual(core.state.get_all("instance-health"), None)

    def test_setup_health(self):
        core = ControllerCore(self.prov_client, "%s.FakeEngine" % __name__,
                              "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars,
                               MONITOR_HEALTH_KEY : True, HEALTH_BOOT_KEY:1,
                               HEALTH_ZOMBIE_KEY:2, HEALTH_MISSING_KEY:3
                               })
        health = core.state.health
        self.assertNotEqual(health, None)
        self.assertEqual(health.boot_timeout, 1)
        self.assertEqual(health.zombie_timeout, 2)
        self.assertEqual(health.missing_timeout, 3)
        self.assertNotEqual(core.state.get_all("instance-health"), None)


    @defer.inlineCallbacks
    def test_deferred_engine(self):
        core = ControllerCore(self.prov_client, "%s.DeferredEngine" % __name__,
                              "controller",
                              {PROVISIONER_VARS_KEY : self.prov_vars})

        yield core.run_initialize({})

        self.assertEqual(1, core.engine.initialize_count)

        self.assertEqual(0, core.engine.decide_count)
        yield core.run_decide()
        self.assertEqual(1, core.engine.decide_count)
        yield core.run_decide()
        self.assertEqual(2, core.engine.decide_count)

        self.assertEqual(0, core.engine.reconfigure_count)
        yield core.run_reconfigure({})
        self.assertEqual(1, core.engine.reconfigure_count)


class FakeProvisionerClient(object):
    pass

class FakeEngine(Engine):
    def initialize(self, *args):
        pass

class DeferredEngine(Engine):
    """Test engine for verifying use of Deferreds in engine operations.

    If a method is only run up to the yield, there will be no increment.
    """
    def __init__(self):
        self.initialize_count = 0
        self.decide_count = 0
        self.reconfigure_count = 0

    @defer.inlineCallbacks
    def initialize(self, *args):
        d = defer.Deferred()
        reactor.callLater(0, d.callback, "hiiii")
        yield d

        self.initialize_count += 1

    @defer.inlineCallbacks
    def decide(self, control, state):

        d = defer.Deferred()
        reactor.callLater(0, d.callback, "hiiii")
        yield d

        self.decide_count += 1

    @defer.inlineCallbacks
    def reconfigure(self, control, newconf):
        d = defer.Deferred()
        reactor.callLater(0, d.callback, "hiiii")
        yield d

        self.reconfigure_count += 1


