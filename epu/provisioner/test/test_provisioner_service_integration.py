import os
import uuid
import time
import unittest

from nose.plugins.skip import SkipTest

try:
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("epuharness not available.")

from epu.provisioner.core import EPU_CLIENT_TOKEN_PREFIX
from libcloud.compute.types import NodeState as LibcloudNodeState


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
            'needs_elastic_ip': True,
        }
    }
}


class TestProvisionerIntegration(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.sysname = "testsysname-%s" % str(uuid.uuid4())
        self.user = default_user

        deployment = fake_libcloud_deployment % default_user

        self.setup_harness(exchange=self.exchange, sysname=self.sysname)
        self.addCleanup(self.cleanup_harness)

        self.epuharness.start(deployment_str=deployment)

        clients = self.get_clients(deployment, self.epuharness.dashi)
        self.dtrs_client = clients['dtrs']
        self.provisioner_client = clients['prov_0']

        self.fake_site_name = "site1"
        self.fake_site, self.driver = self.make_fake_libcloud_site(self.fake_site_name, needs_elastic_ip=True)

        self.block_until_ready(deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(default_user, dt_name, sleeper_dt)
        self.dtrs_client.add_site(self.fake_site_name, self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.fake_site_name, fake_credentials)

    def test_example(self):

        launch_id = "test"
        instance_ids = ["test"]
        deployable_type = "sleeper"
        site = self.fake_site_name

        self.provisioner_client.provision(launch_id, instance_ids, deployable_type, site=site)

        while True:
            instances = self.provisioner_client.describe_nodes()
            if instances[0]['state'] in ('200-REQUESTED', '400-PENDING'):
                time.sleep(0.1)
                continue
            elif instances[0]['state'] == '600-RUNNING':
                break
            else:
                assert False, "Got unexpected state %s" % instances[0]['state']

        # check that mock has a VM
        mock_vms = self.driver.list_nodes()
        assert len(mock_vms) == 1

    def test_elastic_ip(self):

        launch_id = "test"
        instance_ids = ["test"]
        deployable_type = "sleeper"
        site = self.fake_site_name

        self.provisioner_client.provision(launch_id, instance_ids, deployable_type, site=site)

        while True:
            instances = self.provisioner_client.describe_nodes()
            if instances[0]['state'] in ('200-REQUESTED', '400-PENDING'):
                time.sleep(0.1)
                continue
            elif instances[0]['state'] == '600-RUNNING':
                break
            else:
                assert False, "Got unexpected state %s" % instances[0]['state']

        instance = instances[0]
        print instance
        assert instance.get('elastic_ip')

        # check that mock has a VM
        mock_vms = self.driver.list_nodes()
        assert len(mock_vms) == 1

    def test_zombie_node(self):

        launch_id = "test"
        instance_ids = ["test"]
        deployable_type = "sleeper"
        site = self.fake_site_name

        self.provisioner_client.provision(launch_id, instance_ids, deployable_type, site=site)

        while True:
            instances = self.provisioner_client.describe_nodes()
            if instances[0]['state'] in ('200-REQUESTED', '400-PENDING'):
                time.sleep(0.1)
                continue
            elif instances[0]['state'] == '600-RUNNING':
                break
            else:
                assert False, "Got unexpected state %s" % instances[0]['state']

        # check that mock has a VM
        mock_vms = self.driver.list_nodes(immediate=True)
        assert len(mock_vms) == 1

        # Now create a non-epu node, and an epu node that the provisioner doesn't
        # know about

        self.driver.create_node()

        token = "%s_my_test" % EPU_CLIENT_TOKEN_PREFIX
        self.driver.create_node(ex_clienttoken=token)

        # Wait for provisioner to confirm its a zombie
        time.sleep(30)

        while True:
            node = self.driver.get_mock_node_by_client_token(token)
            if node.state == LibcloudNodeState.RUNNING:
                time.sleep(0.1)
                continue
            elif node.state == LibcloudNodeState.TERMINATED:
                break
            else:
                assert False, "Got unexpected state %s" % node.state
