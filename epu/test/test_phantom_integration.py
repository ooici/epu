import os
import uuid
import tempfile

from socket import timeout
from nose.plugins.skip import SkipTest

import epu
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

site_name = 'ec2-fake'
fake_site = {
    'name': site_name,
    'description': 'Fake EC2',
    'driver_class': 'epu.mocklibcloud.MockEC2NodeDriver',
    'driver_kwargs': {}
}

fake_credentials = {
  'access_key': 'xxx',
  'secret_key': 'xxx',
  'key_name': 'ooi'
}

dt_name = "example"
example_dt = {
  'mappings': {
    'ec2-fake':{
      'iaas_image': 'ami-fake',
      'iaas_allocation': 't1.micro',
    }
  },
  'contextualization':{
    'method': 'chef-solo',
    'chef_config': {}
  }
}

class TestPhantomIntegration(object):

    def setup(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        try:
            from epuharness.harness import EPUHarness
        except ImportError:
            raise SkipTest("epuharness not available.")
        try:
            from epu.mocklibcloud import MockEC2NodeDriver
        except ImportError:
            raise SkipTest("sqlalchemy not available.")

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.user = default_user

        self.epuh_persistence = "/tmp/SupD/epuharness"
        if os.path.exists(self.epuh_persistence):
            raise SkipTest("EPUHarness running. Can't run this test")

        # Set up fake libcloud and start deployment
        _, self.fake_libcloud_db = tempfile.mkstemp()
        fake_site['driver_kwargs']['sqlite_db'] = self.fake_libcloud_db
        self.epuharness = EPUHarness(exchange=self.exchange)
        self.dashi = self.epuharness.dashi

        self.epuharness.start(deployment_str=deployment)

        self.libcloud = MockEC2NodeDriver(sqlite_db=self.fake_libcloud_db)

        self.provisioner_client = ProvisionerClient(self.dashi, topic='provisioner')
        self.epum_client = EPUManagementClient(self.dashi, topic='epum')
        self.dtrs_client = DTRSClient(self.dashi, topic='dtrs')

        self.phantom_url = "http://localhost:%s/" % phantom_port

        #wait until dtrs starts
        attempts = 10
        for i in range(0, attempts):
            try:
                self.dtrs_client.list_dts(self.user)
                break
            except timeout:
                continue
        else:
            assert False, "Wasn't able to talk to dtrs"


        #wait until provisioner starts
        attempts = 10
        for i in range(0, attempts):
            try:
                self.provisioner_client.describe_nodes()
                break
            except timeout:
                continue
        else:
            assert False, "Wasn't able to talk to provisioner"

        #wait until epum starts
        attempts = 10
        for i in range(0, attempts):
            try:
                self.epum_client.list_domains()
                break
            except timeout:
                continue
        else:
            assert False, "Wasn't able to talk to epum"

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(site_name, fake_site)
        self.dtrs_client.add_credentials(self.user, site_name, fake_credentials)

    def teardown(self):
        self.epuharness.stop()
        os.remove(self.fake_libcloud_db)

    def test_example(self):
        # Place integration tests here!
        self.phantom_url
