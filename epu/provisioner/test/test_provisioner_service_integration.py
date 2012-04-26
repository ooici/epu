import os
import uuid
import tempfile

from socket import timeout
from nose.plugins.skip import SkipTest

import epu
from epuharness.harness import EPUHarness
from epu.dashiproc.provisioner import ProvisionerClient

fake_libcloud_deployment = """
provisioners:
  prov_0:
    config:
      provisioner:
        default_user: test
        dt_path: %s
      sites:
        ec2-mock:
          driver_class: epu.mocklibcloud.MockEC2NodeDriver
          driver_kwargs:
            sqlite_db: %s
"""

class TestProvisionerItegration(object):

    def setup(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        try:
            from epu.mocklibcloud import MockEC2NodeDriver
        except ImportError:
            raise SkipTest("sqlalchemy not available.")

        self.exchange = "testexchange-%s" % str(uuid.uuid4())

        self.epuh_persistence = "/tmp/SupD/epuharness"
        if os.path.exists(self.epuh_persistence):
            raise SkipTest("EPUHarness running. Can't run this test")

        epu_path = os.path.dirname(epu.__file__)
        self.dt_data = os.path.join(epu_path, "test", "filedts")
        _, self.fake_libcloud_db = tempfile.mkstemp()

        deployment = fake_libcloud_deployment % (self.dt_data, self.fake_libcloud_db)
        self.epuharness = EPUHarness(exchange=self.exchange)
        self.dashi = self.epuharness.dashi

        self.epuharness.start(deployment_str=deployment)

        self.libcloud = MockEC2NodeDriver(sqlite_db=self.fake_libcloud_db)

        self.provisioner_client = ProvisionerClient(self.dashi, topic='prov_0')

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


    def teardown(self):
        self.epuharness.stop()
        os.remove(self.fake_libcloud_db)

    def test_example(self):

        launch_id = "test"
        instance_ids = ["test"]
        deployable_type = "sleeper"
        site = "ec2-mock"
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

        #check that mock has a VM
        mock_vms = self.libcloud.list_nodes()
        assert len(mock_vms) == 1
