# Copyright 2013 University of Chicago

import os
import uuid
import unittest
import logging
from nose.plugins.skip import SkipTest

try:
    from epuharness.harness import EPUHarness
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("epuharness not available.")

log = logging.getLogger(__name__)

default_user = 'default'


basic_deployment = """
process-dispatchers:
  pd_0:
    config:
      processdispatcher:
        engines:
          default:
            deployable_type: eeagent
            slots: 4
            base_need: 1
epums:
  epum_0:
    config:
      epumanagement:
        default_user: %(default_user)s
        provisioner_service_name: prov_0
      logging:
        handlers:
          file:
            filename: /tmp/epum_0.log
provisioners:
  prov_0:
    config:
      provisioner:
        default_user: %(default_user)s
        epu_management_service_name: epum_0
dt_registries:
  dtrs:
    config: {}
"""


fake_credentials = {
    'access_key': 'xxx',
    'secret_key': 'xxx',
    'key_name': 'ooi'
}

dt_name = "example"
example_dt = {
    'mappings': {
        'ec2-fake': {
            'iaas_image': 'ami-fake',
            'iaas_allocation': 't1.micro',
        }
    },
    'contextualization': {
        'method': 'chef-solo',
        'chef_config': {}
    }
}

g_epuharness = None
g_deployment = basic_deployment % {"default_user": default_user}


def setUpModule():
    epuh_persistence = os.environ.get('EPUHARNESS_PERSISTENCE_DIR', '/tmp/SupD/epuharness')
    if os.path.exists(epuh_persistence):
        raise SkipTest("EPUHarness running. Can't run this test")

    global g_epuharness
    exchange = "testexchange-%s" % str(uuid.uuid4())
    sysname = "testsysname-%s" % str(uuid.uuid4())
    g_epuharness = EPUHarness(exchange=exchange, sysname=sysname)
    g_epuharness.start(deployment_str=g_deployment)


def tearDownModule():
    global g_epuharness
    g_epuharness.stop()


class TestIntegrationCreds(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.user = default_user

        clients = self.get_clients(g_deployment, g_epuharness.dashi)
        self.dtrs_client = clients['dtrs']

        self.block_until_ready(g_deployment, g_epuharness.dashi)

    def site_simple_add_remove_test(self):
        site_name = str(uuid.uuid4())
        fake_site, driver = self.make_fake_libcloud_site(site_name)
        self.dtrs_client.add_site(site_name, fake_site)
        self.dtrs_client.add_credentials(self.user, site_name, fake_credentials)
        creds = self.dtrs_client.list_credentials(self.user)
        self.assertTrue(site_name in creds, "The added site as not found")
        self.dtrs_client.remove_credentials(self.user, site_name)
        creds = self.dtrs_client.list_credentials(self.user)
        self.assertFalse(site_name in creds, "The added site should not be found")

    def site_simple_add_update_remove_test(self):
        site_name = str(uuid.uuid4())
        fake_site, driver = self.make_fake_libcloud_site(site_name)
        self.dtrs_client.add_site(site_name, fake_site)
        self.dtrs_client.add_credentials(self.user, site_name, fake_credentials)
        back_cred = self.dtrs_client.describe_credentials(self.user, site_name)

        self.assertEqual(back_cred, fake_credentials)

        key = str(uuid.uuid4())
        back_cred['access_key'] = key
        self.dtrs_client.update_credentials(self.user, site_name, back_cred)
        update_cred = self.dtrs_client.describe_credentials(self.user, site_name)
        self.assertEqual(update_cred['access_key'], key)
        self.assertEqual(back_cred, update_cred)

        self.dtrs_client.remove_credentials(self.user, site_name)
