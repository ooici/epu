import unittest

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


class FakeProvisionerClient(object):
    pass

class FakeEngine(object):
    def initialize(self, *args):
        pass
