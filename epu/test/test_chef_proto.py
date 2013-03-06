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
    from epu.mocklibcloud import MockEC2NodeDriver
except ImportError:
    raise SkipTest("sqlalchemy not available.")

from epu.test import ZooKeeperTestMixin
from epu.states import InstanceState

log = logging.getLogger(__name__)

default_user = 'default'

basic_deployment = """
provisioners:
  prov_0:
    config:
      ssl_no_host_check: true
      provisioner:
        default_user: %(default_user)s
dt_registries:
  dtrs:
    config: {}
"""

site = {'name': "ec2.us-west-1", 'description': 'EC2 West-1 region',
            'driver_class': "libcloud.compute.drivers.ec2.EC2NodeDriver"}


credentials = {
  'access_key': os.environ['AWS_ACCESS_KEY_ID'],
  'secret_key': os.environ['AWS_SECRET_ACCESS_KEY'],
  'key_name': 'ooi'
}

dt_name = "chefdemo"
example_dt = {
  'mappings': {
    site['name']: {
      # 'iaas_image': 'ami-7539b41c',  # ubuntu
      'iaas_image': 'ami-ef5ff086',  # centos6
      'iaas_allocation': 't1.micro',
    }
  },
  'contextualization': {
    'method': 'chef',
    'run_list': ["recipe[apache2]", "recipe[chef-client::config]"],
    'attributes': {}
  }
}

g_epuharness = None
g_deployment = basic_deployment % {"default_user": default_user}


def setUpModule():
    epuh_persistence = "/tmp/SupD/epuharness"
    if os.path.exists(epuh_persistence):
        raise SkipTest("EPUHarness running. Can't run this test")

    global g_epuharness
    exchange = "testexchange-%s" % str(uuid.uuid4())
    g_epuharness = EPUHarness(exchange=exchange)
    g_epuharness.start(deployment_str=g_deployment)


def tearDownModule():
    global g_epuharness
    g_epuharness.stop()


class TestChefProto(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.user = default_user

        clients = self.get_clients(g_deployment, g_epuharness.dashi)
        self.dtrs_client = clients['dtrs']
        self.prov_client = clients['prov_0']

        self.block_until_ready(g_deployment, g_epuharness.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(site['name'], site)
        self.dtrs_client.add_credentials(self.user, site['name'], credentials)

    def test_chef(self):
        launch_id = uuid.uuid4().hex
        instance_ids = [uuid.uuid4().hex]
        deployable_type = dt_name
        subscribers = []

        self.prov_client.provision(launch_id, instance_ids, deployable_type, subscribers, site=site['name'])

        while True:
            instances = self.prov_client.describe_nodes()
            if (instances[0]['state'] == '200-REQUESTED' or
                instances[0]['state'] == '400-PENDING'):
                continue
            elif instances[0]['state'] == '600-RUNNING':
                break
            else:
                assert False, "Got unexpected state %s" % instances[0]['state']

