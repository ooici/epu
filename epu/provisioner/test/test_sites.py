# Copyright 2013 University of Chicago

import unittest

from libcloud.compute.drivers.ec2 import EC2NodeDriver, EC2USWestNodeDriver, NimbusNodeDriver, EucNodeDriver

from epu.provisioner.sites import SiteDriver

credentials1 = {
    "access_key": "myec2key",
    "secret_key": "myec2secret",
    "key_name": "myec2sshkeyname"
}

credentials2 = {
    "access_key": "mynimbuskey",
    "secret_key": "mynimbussecret",
    "key_name": "mynimbussshkeyname"
}

credentials3 = {
    "access_key": "myopenstackkey",
    "secret_key": "myopenstacksecret",
    "key_name": "myopenstacksshkeyname"
}

site_without_type = {
    "host": "cloud.example.com",
    "port": 4242
}

site1 = {
    "type": "ec2",
}

site2 = {
    "type": "ec2",
    "region": "us-west-1"
}

site3 = {
    "type": "nimbus",
    "host": "svc.uc.futuregrid.org",
    "port": 8444
}

site4 = {
    "type": "openstack",
    "host": "149.165.146.50",
    "port": 8773,
    "secure": False,
    "path": "/services/Cloud"

}


class ProvisionerSitesTests(unittest.TestCase):
    def test_site_driver(self):
        # libcloud giving me grief (DL)
        import libcloud.security
        libcloud.security.VERIFY_SSL_CERT_STRICT = False

        try:
            SiteDriver(site_without_type, credentials1)
        except KeyError:
            pass
        else:
            self.fail("Expected exception")

        ec2 = SiteDriver(site1, credentials1)
        ec2_west = SiteDriver(site2, credentials1)
        nimbus_test = SiteDriver(site3, credentials2)
        openstack_test = SiteDriver(site4, credentials3)

        # Test driver classes
        self.assertIsInstance(ec2.driver, EC2NodeDriver)
        self.assertIsInstance(ec2_west.driver, EC2USWestNodeDriver)
        self.assertIsInstance(nimbus_test.driver, NimbusNodeDriver)
        self.assertIsInstance(openstack_test.driver, EucNodeDriver)

        # Test hosts
        self.assertEqual(nimbus_test.driver.connection.host, "svc.uc.futuregrid.org")
        self.assertEqual(openstack_test.driver.connection.host, "149.165.146.50")

        # Test ports
        self.assertEqual(nimbus_test.driver.connection.port, 8444)
        self.assertEqual(openstack_test.driver.connection.port, 8773)

        # Test secure
        self.assertEqual(nimbus_test.driver.secure, True)
        self.assertEqual(openstack_test.driver.secure, False)

        # Test SSH keys
        self.assertEqual(ec2.driver.key, 'myec2key')
        self.assertEqual(ec2_west.driver.key, 'myec2key')
        self.assertEqual(nimbus_test.driver.key, 'mynimbuskey')
        self.assertEqual(openstack_test.driver.key, 'myopenstackkey')
