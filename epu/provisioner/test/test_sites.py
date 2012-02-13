import unittest

from libcloud.compute.drivers.ec2 import EC2USWestNodeDriver, NimbusNodeDriver

from epu.provisioner.sites import ProvisionerSites

SITES_DICT = {
    "ec2-west": {
        "driver_class": "libcloud.compute.drivers.ec2.EC2USWestNodeDriver",
        "driver_kwargs": {
            "key": "myec2key",
            "secret": "myec2secret"
        }
    },
    "nimbus-test": {
        "driver_class": "libcloud.compute.drivers.ec2.NimbusNodeDriver",
        "driver_kwargs": {
            "key": "mynimbuskey",
            "secret": "mynimbussecret",
            "host": "nimbus.ci.uchicago.edu",
            "port": 8444
        }
    }
}

class ProvisionerSitesTests(unittest.TestCase):
    def test_get_site_drivers(self):
        #libcloud giving me grief (DL)
        import libcloud.security
        libcloud.security.VERIFY_SSL_CERT_STRICT = False

        sites = ProvisionerSites(SITES_DICT)
        nimbus_test = sites.acquire_driver('nimbus-test')
        ec2_west = sites.acquire_driver('ec2-west')
        self.assertIsInstance(nimbus_test.driver, NimbusNodeDriver)
        self.assertIsInstance(ec2_west.driver, EC2USWestNodeDriver)
        self.assertEqual(nimbus_test.driver.key, 'mynimbuskey')
        self.assertEqual(ec2_west.driver.key, 'myec2key')
        self.assertEqual(nimbus_test.site, 'nimbus-test')
        self.assertEqual(ec2_west.site, 'ec2-west')

        nimbus_test.release()
        ec2_west.release()

        #ensure we get different drivers from concurrent calls
        nimbus1 = sites.acquire_driver('nimbus-test')
        nimbus2 = sites.acquire_driver('nimbus-test')
        self.assertTrue(nimbus1 and nimbus2 and nimbus1 is not nimbus2)
        nimbus1.release()
        nimbus2.release()

        # try context manager
        with sites.acquire_driver('nimbus-test') as site_driver:
            self.assertIsInstance(site_driver.driver, NimbusNodeDriver)
            self.assertEqual(site_driver.site, 'nimbus-test')

  