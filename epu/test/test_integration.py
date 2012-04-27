import os
import uuid
import tempfile

from socket import timeout
from nose.plugins.skip import SkipTest

import epu
from epu.dashiproc.provisioner import ProvisionerClient
from epu.dashiproc.epumanagement import EPUManagementClient

fake_libcloud_deployment = """
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
        default_user: default                                                    
        provisioner_topic: prov_0                                         
      logging:                                                                   
        handlers:                                                                
          file:                                                                  
            filename: /tmp/epum_0.log   
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

class TestItegration(object):

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

        self.epuh_persistence = "/tmp/SupD/epuharness"
        if os.path.exists(self.epuh_persistence):
            raise SkipTest("EPUHarness running. Can't run this test")

        # Set up dtrs
        epu_path = os.path.dirname(epu.__file__)
        self.dt_data = os.path.join(epu_path, "test", "filedts")

        # Set up fake libcloud and start deployment
        _, self.fake_libcloud_db = tempfile.mkstemp()
        deployment = fake_libcloud_deployment % (self.dt_data, self.fake_libcloud_db)
        self.epuharness = EPUHarness(exchange=self.exchange)
        self.dashi = self.epuharness.dashi

        self.epuharness.start(deployment_str=deployment)

        self.libcloud = MockEC2NodeDriver(sqlite_db=self.fake_libcloud_db)

        self.provisioner_client = ProvisionerClient(self.dashi, topic='prov_0')
        self.epum_client = EPUManagementClient(self.dashi, topic='epum_0')

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
                self.epum_client.list_epus()
                break
            except timeout:
                continue
        else:
            assert False, "Wasn't able to talk to epum"

    def teardown(self):
        self.epuharness.stop()
        os.remove(self.fake_libcloud_db)

    def test_example(self):
        # Place integration tests here!
        pass
