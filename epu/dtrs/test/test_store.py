import unittest
import uuid

from kazoo.exceptions import ConnectionLoss

from epu.dtrs.store import DTRSStore, DTRSZooKeeperStore
from epu.exceptions import WriteConflictError, NotFoundError
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
        site_id_1 = new_id()
        site_id_2 = new_id()
        site1 = {
            "type": "ec2",
            "region": "eu-west-1"
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
        self.assertEqual(site1["type"], site1_read["type"])
        self.assertEqual(site1["region"], site1_read["region"])

        # Get with an unknown ID should return None
        dt = self.store.describe_site(site_id_2)
        self.assertEqual(None, dt)

        # now make two changes, one from the original and one from what we read
        site2 = site1.copy()
        site2["region"] = "us-west-2"
        self.store.update_site(site_id_1, site2)
        site2_read = self.store.describe_site(site_id_1)
        self.assertEqual("us-west-2", site2_read["region"])

        # Store another site
        site2 = {
            "type": "nimbus",
            "host": "svc.uc.futuregrid.org",
            "port": 8444,
            "secure": True
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
            "type": "ec2",
            "region": "eu-west-1"
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
        self.assertEqual(credentials_1["access_key"], credentials_1_read["access_key"])
        self.assertEqual(credentials_1["secret_key"], credentials_1_read["secret_key"])
        self.assertEqual(credentials_1["key_name"], credentials_1_read["key_name"])

        # now make two changes, one from the original and one from what we read
        credentials_2 = credentials_1.copy()
        credentials_2["key_name"] = "NEW_KEY"
        self.store.update_credentials('mr_white', site_id_1, credentials_2)
        credentials_2_read = self.store.describe_credentials('mr_white', site_id_1)
        self.assertEqual("NEW_KEY", credentials_2_read["key_name"])

        # Get with a different user should return None
        credentials_1_read = self.store.describe_credentials('mr_pink', site_id_1)
        self.assertEqual(None, credentials_1_read)

        # Listing credentials should return both
        credentials = self.store.list_credentials('mr_white')
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

        self.assertRaises(ConnectionLoss, self.store.fake_operation)


def new_id():
    return str(uuid.uuid4())
