import unittest

from libcloud.compute.drivers.ec2 import EC2USWestNodeDriver, NimbusNodeDriver

from epu.provisioner.sites import SiteDriver

site1 = {
    "name": "futuregrid.hotel",
    "description": "Nimbus cloud on the Hotel FutureGrid site",
    "driver_class": "libcloud.compute.drivers.ec2.NimbusNodeDriver",
    "driver_kwargs": {
        "host": "svc.uc.futuregrid.org",
        "port": 8444
    }
}

site2 = {
    "name": "ec2.us-west-1",
    "description": "Amazon EC2, US West (Northern California)",
    "driver_class": "libcloud.compute.drivers.ec2.EC2USWestNodeDriver"
}

credentials1 = {
    "access_key": "mynimbuskey",
    "secret_key": "mynimbussecret",
    "key_name": "mynimbussshkeyname"
}

credentials2 = {
    "access_key": "myec2key",
    "secret_key": "myec2secret",
    "key_name": "myec2sshkeyname"
}


class ProvisionerSitesTests(unittest.TestCase):
    def test_site_drivers(self):
        # libcloud giving me grief (DL)
        import libcloud.security
        libcloud.security.VERIFY_SSL_CERT_STRICT = False

        nimbus_test = SiteDriver(site1, credentials1)
        ec2_west = SiteDriver(site2, credentials2)

        self.assertIsInstance(nimbus_test.driver, NimbusNodeDriver)
        self.assertIsInstance(ec2_west.driver, EC2USWestNodeDriver)
        self.assertEqual(nimbus_test.driver.key, 'mynimbuskey')
        self.assertEqual(ec2_west.driver.key, 'myec2key')
