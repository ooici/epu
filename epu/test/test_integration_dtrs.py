# Copyright 2013 University of Chicago

import os
import uuid
import unittest
import logging

from dashi import DashiError
from nose.plugins.skip import SkipTest

try:
    from epuharness.harness import EPUHarness
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("epuharness not available.")
try:
    from epu.mocklibcloud import MockEC2NodeDriver  # noqa
except ImportError:
    raise SkipTest("sqlalchemy not available.")


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


class TestIntegrationDTRS(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.user = default_user

        clients = self.get_clients(g_deployment, g_epuharness.dashi)
        self.dtrs_client = clients['dtrs']

        self.block_until_ready(g_deployment, g_epuharness.dashi)

    def dtrs_simple_add_remove_test(self):
        new_dt_name = str(uuid.uuid4())
        self.dtrs_client.add_dt(self.user, new_dt_name, example_dt)
        dts = self.dtrs_client.list_dts(self.user)
        self.assertTrue(new_dt_name in dts, "The name %s was not found in %s" % (new_dt_name, str(dts)))
        self.dtrs_client.remove_dt(self.user, new_dt_name)
        dts = self.dtrs_client.list_dts(self.user)
        self.assertFalse(new_dt_name in dts, "The name %s should not have been found" % (new_dt_name))

    def dtrs_simple_add_describe_remove_test(self):
        new_dt_name = str(uuid.uuid4())
        self.dtrs_client.add_dt(self.user, new_dt_name, example_dt)
        desc = self.dtrs_client.describe_dt(self.user, new_dt_name)

        self.assertEquals(example_dt, desc, "The 2 dts did not match ||| %s ||| %s" % (str(desc), str(example_dt)))

        self.dtrs_client.remove_dt(self.user, new_dt_name)

    def dtrs_simple_add_update_remove_test(self):
        new_dt_name = str(uuid.uuid4())
        self.dtrs_client.add_dt(self.user, new_dt_name, example_dt)
        desc = self.dtrs_client.describe_dt(self.user, new_dt_name)

        key = unicode(uuid.uuid4())
        val = unicode(uuid.uuid4())
        desc[key] = val
        self.dtrs_client.update_dt(self.user, new_dt_name, desc)
        new_desc = self.dtrs_client.describe_dt(self.user, new_dt_name)

        self.assertEqual(new_desc[key], val)
        self.assertEqual(desc, new_desc, "The 2 dts did not match ||| %s ||| %s" % (str(desc), str(new_desc)))

        self.dtrs_client.remove_dt(self.user, new_dt_name)

    def dtrs_simple_add_same_twice_remove_test(self):
        new_dt_name = str(uuid.uuid4())
        self.dtrs_client.add_dt(self.user, new_dt_name, example_dt)

        passed = False
        try:
            self.dtrs_client.add_dt(self.user, new_dt_name, example_dt)
        except DashiError:
            passed = True
        self.assertTrue(passed, "an exception should have been raised")
        self.dtrs_client.remove_dt(self.user, new_dt_name)

    def dtrs_remove_dt_that_doesnt_exist_test(self):
        new_dt_name = str(uuid.uuid4())
        passed = False
        try:
            self.dtrs_client.remove_dt(self.user, new_dt_name)
        except DashiError:
            passed = True
        self.assertTrue(passed, "an exception should have been raised")

    def dtrs_remove_dt_twice_exist_test(self):
        new_dt_name = str(uuid.uuid4())
        self.dtrs_client.add_dt(self.user, new_dt_name, example_dt)
        self.dtrs_client.remove_dt(self.user, new_dt_name)
        passed = False
        try:
            self.dtrs_client.remove_dt(self.user, new_dt_name)
        except DashiError:
            passed = True
        self.assertTrue(passed, "an exception should have been raised")

    def dtrs_update_dt_that_doesnt_exist_test(self):
        new_dt_name = str(uuid.uuid4())
        passed = False
        try:
            self.dtrs_client.update_dt(self.user, new_dt_name, example_dt)
        except DashiError:
            passed = True
        self.assertTrue(passed, "an exception should have been raised")

    def dtrs_describe_dt_that_doesnt_exist_test(self):
        new_dt_name = str(uuid.uuid4())
        dt = self.dtrs_client.describe_dt(self.user, new_dt_name)
        self.assertEqual(dt, None)
