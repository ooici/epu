import os
import uuid
import unittest

from nose.plugins.skip import SkipTest

try:
    from epuharness.harness import EPUHarness
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("epuharness not available.")


default_user = 'default'

fake_libcloud_deployment = """
provisioners:
  prov_0:
    config:
      provisioner:
        default_user: %s
dt_registries:
  dtrs:
    config: {}
"""

fake_credentials = {
  'access_key': 'xxx',
  'secret_key': 'xxx',
  'key_name': 'ooi'
}

dt_name = "sleeper"
sleeper_dt = {
  'mappings': {
    'site1': {
      'iaas_image': 'ami-fake',
      'iaas_allocation': 't1.micro',
    }
  }
}


class TestProvisionerIntegration(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.user = default_user

        deployment = fake_libcloud_deployment % default_user

        self.setup_harness(exchange=self.exchange)
        self.addCleanup(self.cleanup_harness)

        self.epuharness.start(deployment_str=deployment)

        clients = self.get_clients(deployment, self.epuharness.dashi)
        self.dtrs_client = clients['dtrs']
        self.provisioner_client = clients['prov_0']

        self.fake_site, self.driver = self.make_fake_libcloud_site("site1")

        self.block_until_ready(deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(default_user, dt_name, sleeper_dt)
        self.dtrs_client.add_site(self.fake_site['name'], self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.fake_site['name'], fake_credentials)

    def test_example(self):

        launch_id = "test"
        instance_ids = ["test"]
        deployable_type = "sleeper"
        site = self.fake_site['name']
        subscribers = []

        self.provisioner_client.provision(launch_id, instance_ids, deployable_type, subscribers, site=site)

        while True:
            instances = self.provisioner_client.describe_nodes()
            if (instances[0]['state'] == '200-REQUESTED' or
                instances[0]['state'] == '400-PENDING'):
                continue
            elif instances[0]['state'] == '600-RUNNING':
                break
            else:
                assert False, "Got unexpected state %s" % instances[0]['state']

        # check that mock has a VM
        mock_vms = self.driver.list_nodes()
        assert len(mock_vms) == 1
