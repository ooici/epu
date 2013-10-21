# Copyright 2013 University of Chicago

import os
import uuid
import unittest

from nose.plugins.skip import SkipTest

try:
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("epuharness not available.")

from epu.dashiproc.provisioner import ProvisionerClient
from epu.dashiproc.epumanagement import EPUManagementClient
from epu.dashiproc.dtrs import DTRSClient

default_user = 'default'
default_password = 'test'
phantom_port = 8080

deployment = """
epums:
  epum:
    config:
      epumanagement:
        default_user: %s
        provisioner_topic: provisioner
      logging:
        handlers:
          file:
            filename: /tmp/epum_0.log
provisioners:
  provisioner:
    config:
      provisioner:
        default_user: %s
dt_registries:
  dtrs:
    config: {}
phantom-instances:
  phantom:
    config: {}
    users:
      - {user: %s, password: %s}
    port: %s
""" % (default_user, default_user, default_user, default_password, phantom_port)

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


class TestPhantomIntegration(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        try:
            from epuharness.harness import EPUHarness  # noqa
        except ImportError:
            raise SkipTest("epuharness not available.")
        try:
            from epu.mocklibcloud import MockEC2NodeDriver  # noqa
        except ImportError:
            raise SkipTest("sqlalchemy not available.")

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.sysname = "testsysname-%s" % str(uuid.uuid4())
        self.user = default_user

        self.epuh_persistence = os.environ.get('EPUHARNESS_PERSISTENCE_DIR', '/tmp/SupD/epuharness')
        if os.path.exists(self.epuh_persistence):
            raise SkipTest("EPUHarness running. Can't run this test")

        # Set up fake libcloud and start deployment
        self.setup_harness(exchange=self.exchange, sysname=self.sysname)
        self.addCleanup(self.cleanup_harness)

        self.epuharness.start(deployment_str=deployment)

        self.provisioner_client = ProvisionerClient(self.dashi, topic='provisioner')
        self.epum_client = EPUManagementClient(self.dashi, topic='epum')
        self.dtrs_client = DTRSClient(self.dashi, topic='dtrs')

        self.phantom_url = "http://localhost:%s/" % phantom_port

        self.site_name = "site1"
        self.fake_site, self.driver = self.make_fake_libcloud_site(self.site_name)

        self.block_until_ready(deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(self.site_name, self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.site_name, fake_credentials)

    def xtest_example(self):
        # Place integration tests here!
        self.phantom_url
