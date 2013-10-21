# Copyright 2013 University of Chicago

import unittest
import uuid

from kazoo.exceptions import KazooException

from epu.dtrs.store import DTRSStore, DTRSZooKeeperStore
from epu.dtrs.core import CredentialType
from epu.exceptions import WriteConflictError, NotFoundError, BadRequestError
from epu.test import ZooKeeperTestMixin, SocatProxyRestartWrapper


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
        self.assertEqual(dt1["mappings"], dt1_read["mappings"])

        # Get with a different user should return None
        dt1_pink_read = self.store.describe_dt('mr_pink', dt_id_1)
        self.assertEqual(None, dt1_pink_read)

        # now make two changes, one from the original and one from what we read
        dt2 = dt1.copy()
        dt2["mappings"]["ec2.us-east-1"]["iaas_allocation"] = "t1.micro"
        self.store.update_dt('mr_white', dt_id_1, dt2)
        dt2_read = self.store.describe_dt('mr_white', dt_id_1)
        self.assertEqual("t1.micro", dt2_read["mappings"]["ec2.us-east-1"]["iaas_allocation"])

        # Store another DT for the same user
        dt_id_2 = new_id()
        dt2 = {"mappings": {"ec2.us-east-1": {"iaas_image": "ami-foobar", "iaas_allocation": "m1.small"}}}
        self.store.add_dt('mr_white', dt_id_2, dt2)

        # Listing DTs should return both
        dts = self.store.list_dts('mr_white')
        self.assertEqual(len(dts), 2)
        self.assertIn(dt_id_1, dts)
        self.assertIn(dt_id_2, dts)

    def test_store_sites(self):
        site_id_1 = new_common_id()
        site_id_2 = new_common_id()
        site1 = {
            "type": "ec2",
            "region": "eu-west-1"
        }
        self.store.add_site(None, site_id_1, site1)

        # adding it again should error
        try:
            self.store.add_site(None, site_id_1, site1)
        except WriteConflictError:
            pass
        else:
            self.fail("expected WriteConflictError")

        site1_read = self.store.describe_site(None, site_id_1)
        self.assertEqual(site1["type"], site1_read["type"])
        self.assertEqual(site1["region"], site1_read["region"])

        # Get with an unknown ID should return None
        site2 = self.store.describe_site(None, site_id_2)
        self.assertEqual(None, site2)

        # now make two changes, one from the original and one from what we read
        site2 = site1.copy()
        site2["region"] = "us-west-2"
        self.store.update_site(None, site_id_1, site2)
        site2_read = self.store.describe_site(None, site_id_1)
        self.assertEqual("us-west-2", site2_read["region"])

        # Store another site
        site2 = {
            "type": "nimbus",
            "host": "svc.uc.futuregrid.org",
            "port": 8444,
            "secure": True
        }
        self.store.add_site(None, site_id_2, site2)

        # Listing sites should return both
        sites = self.store.list_sites(None)
        self.assertEqual(len(sites), 2)
        self.assertIn(site_id_1, sites)
        self.assertIn(site_id_2, sites)

        # Now testing with a caller parameter

        # Listing sites should return both common sites
        sites = self.store.list_sites("mr_white")
        self.assertEqual(len(sites), 2)

        # Describe a common site should work too
        site2_read = self.store.describe_site("mr_white", site_id_2)
        self.assertEqual(site2["type"], site2_read["type"])
        self.assertEqual(site2["host"], site2_read["host"])
        self.assertEqual(site2["port"], site2_read["port"])

        own_site_id = new_id()
        own_site = {
            "type": "openstack",
            "host": "openstack.example.com",
            "port": 5678
        }

        # Add a new user site
        self.store.add_site("mr_white", own_site_id, own_site)
        sites = self.store.list_sites("mr_white")
        self.assertEqual(len(sites), 3)
        own_site_read = self.store.describe_site("mr_white", own_site_id)
        self.assertEqual(own_site["type"], own_site_read["type"])
        self.assertEqual(own_site["host"], own_site_read["host"])

        # It should not be visible by another user
        sites = self.store.list_sites("mr_pink")
        self.assertEqual(len(sites), 2)
        own_site_read = self.store.describe_site("mr_pink", own_site_id)
        self.assertEqual(None, own_site_read)

        # Add site should not work with a common site prefix
        try:
            self.store.add_site("mr_white", site_id_1, own_site)
        except BadRequestError:
            pass
        else:
            self.fail("expected BadRequestError")

        # Update site should not work on common sites
        try:
            self.store.update_site("mr_white", site_id_1, own_site)
        except NotFoundError:
            pass
        else:
            self.fail("expected NotFoundError")

        own_site["port"] = 7890
        self.store.update_site("mr_white", own_site_id, own_site)
        sites = self.store.list_sites("mr_white")
        self.assertEqual(len(sites), 3)
        own_site_read = self.store.describe_site("mr_white", own_site_id)
        self.assertEqual(own_site["type"], own_site_read["type"])
        self.assertEqual(own_site["host"], own_site_read["host"])

        # Removing a common site should not work for a user
        try:
            self.store.remove_site("mr_white", site_id_1)
        except NotFoundError:
            pass
        else:
            self.fail("expected NotFoundError")
        sites = self.store.list_sites("mr_white")
        self.assertEqual(len(sites), 3)

        self.store.remove_site("mr_white", own_site_id)
        sites = self.store.list_sites("mr_white")
        self.assertEqual(len(sites), 2)
        self.assertIn(site_id_1, sites)
        self.assertIn(site_id_2, sites)

    def test_store_credentials(self):
        site_id_1 = new_common_id()
        site1 = {
            "type": "ec2",
            "region": "eu-west-1"
        }
        self.store.add_site(None, site_id_1, site1)

        credentials_1 = {
            "access_key": "EC2_ACCESS_KEY",
            "secret_key": "EC2_SECRET_KEY",
            "key_name": "EC2_KEYPAIR"
        }
        self.store.add_credentials('mr_white', CredentialType.SITE, site_id_1, credentials_1)

        # adding it again should error
        with self.assertRaises(WriteConflictError):
            self.store.add_credentials('mr_white', CredentialType.SITE, site_id_1, credentials_1)

        # but another type should succeed
        self.store.add_credentials('mr_white', CredentialType.CHEF, site_id_1, credentials_1)

        credentials_1_read = self.store.describe_credentials('mr_white', CredentialType.SITE, site_id_1)
        self.assertEqual(credentials_1["access_key"], credentials_1_read["access_key"])
        self.assertEqual(credentials_1["secret_key"], credentials_1_read["secret_key"])
        self.assertEqual(credentials_1["key_name"], credentials_1_read["key_name"])

        # now make two changes, one from the original and one from what we read
        credentials_2 = credentials_1.copy()
        credentials_2["key_name"] = "NEW_KEY"
        self.store.update_credentials('mr_white', CredentialType.SITE, site_id_1, credentials_2)
        credentials_2_read = self.store.describe_credentials('mr_white', CredentialType.SITE, site_id_1)
        self.assertEqual("NEW_KEY", credentials_2_read["key_name"])

        # Get with a different user should return None
        credentials_1_read = self.store.describe_credentials('mr_pink', CredentialType.SITE, site_id_1)
        self.assertEqual(None, credentials_1_read)

        # Listing credentials should return both
        credentials = self.store.list_credentials('mr_white', CredentialType.SITE)
        self.assertEqual([site_id_1], credentials)


class DTRSZooKeeperStoreTests(BaseDTRSStoreTests, ZooKeeperTestMixin):

    # this runs all of the BaseDTRSStoreTests tests plus any
    # ZK-specific ones

    def setUp(self):
        self.setup_zookeeper(base_path_prefix="/dtrs_store_tests_")
        self.store = DTRSZooKeeperStore(self.zk_hosts, self.zk_base_path, use_gevent=self.use_gevent)

        self.store.initialize()

    def tearDown(self):
        self.teardown_zookeeper()


class DTRSZooKeeperStoreProxyKillsTests(BaseDTRSStoreTests, ZooKeeperTestMixin):

    # this runs all of the BaseDTRSStoreTests tests plus any
    # ZK-specific ones, but uses a proxy in front of ZK and restarts
    # the proxy before each call to the store. The effect is that for each store
    # operation, the first call to kazoo fails with a connection error, but the
    # client should handle that and retry

    def setUp(self):
        self.setup_zookeeper(base_path_prefix="/dtrs_store_tests_", use_proxy=True)
        self.real_store = DTRSZooKeeperStore(self.zk_hosts, self.zk_base_path, use_gevent=self.use_gevent)

        self.real_store.initialize()

        # have the tests use a wrapped store that restarts the connection before each call
        self.store = SocatProxyRestartWrapper(self.proxy, self.real_store)

    def tearDown(self):
        self.teardown_zookeeper()

    def test_the_fixture(self):
        # make sure test fixture actually works like we think

        def fake_operation():
            self.store.kazoo.get("/")
        self.real_store.fake_operation = fake_operation

        self.assertRaises(KazooException, self.store.fake_operation)


def new_id():
    return str(uuid.uuid4())


def new_common_id():
    return str("common::" + new_id())
