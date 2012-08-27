import os
import uuid
import unittest
import logging
from dashi import DashiError
import tempfile
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

def make_fake_libcloud_site(site_name):
    from epu.mocklibcloud import MockEC2NodeDriver
    fh, fake_libcloud_db = tempfile.mkstemp()
    os.close(fh)

    fake_site = {
        'name': site_name,
        'description': 'Fake EC2',
        'driver_class': 'epu.mocklibcloud.MockEC2NodeDriver',
        'driver_kwargs': {
            'sqlite_db': fake_libcloud_db
        }
    }
    libcloud = MockEC2NodeDriver(sqlite_db=fake_libcloud_db)

    return (fake_site, libcloud, fake_libcloud_db)



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

g_epuharness = None
g_deployment = basic_deployment % {"default_user" : default_user}

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

class TestIntegrationSite(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.user = default_user


        clients = self.get_clients(g_deployment, g_epuharness.dashi) 
        self.dtrs_client = clients['dtrs']

        self.block_until_ready(g_deployment, g_epuharness.dashi)


    def teardown(self):
        os.remove(self.fake_libcloud_db)


    def site_simple_add_remove_test(self):
        name = str(uuid.uuid4())
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site(name)
        self.dtrs_client.add_site(fake_site['name'], fake_site)
        sites = self.dtrs_client.list_sites()
        self.assertTrue(fake_site['name'] in sites)
        self.dtrs_client.remove_site(fake_site['name'])

    def site_simple_add_describe_remove_test(self):
        name = str(uuid.uuid4())
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site(name)
        self.dtrs_client.add_site(fake_site['name'], fake_site)
        description = self.dtrs_client.describe_site(fake_site['name'])
        self.assertEqual(fake_site, description, "These are not equal ||| %s ||| %s" % (str(description), str(fake_site)))
        self.dtrs_client.remove_site(fake_site['name'])

    def site_simple_add_update_remove_test(self):
        name = str(uuid.uuid4())
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site(name)
        self.dtrs_client.add_site(fake_site['name'], fake_site)
        description = self.dtrs_client.describe_site(fake_site['name'])
        
        key = str(uuid.uuid4())
        val = str(uuid.uuid4())
        description[key] = val
        self.dtrs_client.update_site(fake_site['name'], description)
        new_description = self.dtrs_client.describe_site(fake_site['name'])
        self.assertEqual(description, new_description)
        self.dtrs_client.remove_site(fake_site['name'])

    def site_simple_add_twice_test(self):
        name = str(uuid.uuid4())
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site(name)

        self.dtrs_client.add_site(fake_site['name'], fake_site)
        passed = False
        try:
            self.dtrs_client.add_site(fake_site['name'], fake_site)
        except DashiError, de:
            passed = True
        self.assertTrue(passed, "An exception should have been raised")
        self.dtrs_client.remove_site(fake_site['name'])

    def site_simple_delete_no_there_test(self):
        name = str(uuid.uuid4())
        passed = False
        try:
            self.dtrs_client.remove_site(name)
        except DashiError, de:
            passed = True
        self.assertTrue(passed, "An exception should have been raised")

    def site_simple_update_no_there_test(self):
        name = str(uuid.uuid4())
        passed = False
        try:
            self.dtrs_client.update_site(name, {})
        except DashiError, de:
            passed = True
        self.assertTrue(passed, "An exception should have been raised")

    def site_simple_add_describe_not_exist_remove_test(self):
        name = str(uuid.uuid4())
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site(name)
        passed = False
        try:
            description = self.dtrs_client.describe_site(name)
        except DashiError, de:
            passed = True
        self.assertTrue(passed, "An exception should have been raised")

