import unittest
import uuid

from epu.dtrs.store import DTRSStore
from epu.exceptions import WriteConflictError, NotFoundError


class BaseDTRSStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = DTRSStore()

    def test_store_dts(self):
        dt_id_1 = new_id()
        dt1 = {"mappings": {"ec2.us-east-1": {"iaas_image": "ami-foobar",
            "iaas_allocation": "m1.small"}}}
        self.store.add_dt('mr_white', dt_id_1, dt1)

        # adding it again should error
        try:
            self.store.add_dt('mr_white', dt_id_1, dt1)
        except WriteConflictError:
            pass
        else:
            self.fail("expected WriteConflictError")

        dt1_read = self.store.describe_dt('mr_white', dt_id_1)
        self.assertEqual(dt1, dt1_read)

        # Get with a different user should throw an exception
        try:
            dt1_read = self.store.describe_dt('mr_pink', dt_id_1)
        except NotFoundError:
            pass

        # Store another DT for the same user
        dt_id_2 = new_id()
        dt2 = {"mappings": {"ec2.us-east-1": {"iaas_image": "ami-foobar",
            "iaas_allocation": "m1.small"}}}
        self.store.add_dt('mr_white', dt_id_2, dt2)

        # Listing DTs should return both
        dts = self.store.list_dts('mr_white')
        self.assertEqual(len(dts), 2)
        self.assertIn(dt_id_1, dts)
        self.assertIn(dt_id_2, dts)

    def test_store_sites(self):
        site_id_1 = new_id()
        site1 = {
            "name": "ec2.us-east-1",
            "description": "Amazon EC2, US East (Virginia)",
            "driver_class": "libcloud.compute.drivers.ec2.EC2NodeDriver"
        }
        self.store.add_site(site_id_1, site1)

        # adding it again should error
        try:
            self.store.add_site(site_id_1, site1)
        except WriteConflictError:
            pass
        else:
            self.fail("expected WriteConflictError")

        site1_read = self.store.describe_site(site_id_1)
        self.assertEqual(site1, site1_read)

        # Get with a different user should throw an exception
        try:
            site1_read = self.store.describe_site(site_id_1)
        except NotFoundError:
            pass

        # Store another site for the same user
        site_id_2 = new_id()
        site2 = {
            "name": "futuregrid.hotel",
            "description": "Nimbus cloud on the Hotel FutureGrid site",
            "driver_class": "libcloud.compute.drivers.ec2.NimbusNodeDriver",
            "driver_kwargs": {
                "host": "svc.uc.futuregrid.org",
                "port": 8444
            }
        }
        self.store.add_site(site_id_2, site2)

        # Listing sites should return both
        sites = self.store.list_sites()
        self.assertEqual(len(sites), 2)
        self.assertIn(site_id_1, sites)
        self.assertIn(site_id_2, sites)

    def test_store_credentials(self):
        site_id_1 = new_id()
        site1 = {
            "name": "ec2.us-east-1",
            "description": "Amazon EC2, US East (Virginia)",
            "driver_class": "libcloud.compute.drivers.ec2.EC2NodeDriver"
        }
        self.store.add_site(site_id_1, site1)

        credentials_1 = {
           "access_key": "EC2_ACCESS_KEY",
           "secret_key": "EC2_SECRET_KEY",
           "key_name": "EC2_KEYPAIR"
        }
        self.store.add_credentials('mr_white', site_id_1, credentials_1)

        # adding it again should error
        try:
            self.store.add_credentials('mr_white', site_id_1, credentials_1)
        except WriteConflictError:
            pass
        else:
            self.fail("expected WriteConflictError")

        credentials_1_read = self.store.describe_credentials('mr_white', site_id_1)
        self.assertEqual(credentials_1, credentials_1_read)

        # Get with a different user should throw an exception
        try:
            credentials_1_read = self.store.describe_credentials('mr_pink',
                    site_id_1)
        except NotFoundError:
            pass

        # Listing credentials should return both
        credentials = self.store.list_credentials('mr_white')
        self.assertEqual([site_id_1], credentials)


def new_id():
    return str(uuid.uuid4())
