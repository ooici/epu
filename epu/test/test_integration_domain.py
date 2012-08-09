import os
import uuid
import unittest
import logging
from dashi import DashiError
import tempfile
from nose.plugins.skip import SkipTest
import gevent
from gevent import Timeout
import time

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

def make_fake_libcloud_site():
    from epu.mocklibcloud import MockEC2NodeDriver
    _, fake_libcloud_db = tempfile.mkstemp()

    site_name = str(uuid.uuid4())
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


def _make_dt(site_name):
    mapping = {
      'iaas_image': 'ami-fake',
      'iaas_allocation': 't1.micro',
    }

    example_dt = {
        'mappings': {
        },
        'contextualization':{
        'method': 'chef-solo',
        'chef_config': {}
        }
    }

    example_dt['mappings'][site_name] = mapping
    return example_dt
    


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


example_definition = {
    'general' : {
        'engine_class' : 'epu.decisionengine.impls.simplest.SimplestEngine',
    },
    'health' : {
        'monitor_health' : False
    }
}

def _make_domain_def(n, epuworker_type, site_name):

    example_domain = {
        'engine_conf' : {
            'preserve_n' : n,
            'epuworker_type' : epuworker_type,
            'force_site' : site_name
        }
    }
    return example_domain


class TestIntegrationDomain(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.user = default_user

        clients = self.get_clients(g_deployment, g_epuharness.dashi) 
        self.dtrs_client = clients['dtrs']
        self.epum_client = clients['epum_0']

        self.block_until_ready(g_deployment, g_epuharness.dashi)

    def teardown(self):
        os.remove(self.fake_libcloud_db)

    def _load_dtrs(self, fake_site):
        dt_name = str(uuid.uuid4())
        self.dtrs_client.add_dt(self.user, dt_name, _make_dt(fake_site['name']))
        self.dtrs_client.add_site(fake_site['name'], fake_site)
        self.dtrs_client.add_credentials(self.user, fake_site['name'], fake_credentials)
        return dt_name


    def domain_add_all_params_not_exist_test(self):
        domain_id = str(uuid.uuid4())
        definition_id = str(uuid.uuid4())
        caller = str(uuid.uuid4())

        passed = False
        try:
            self.epum_client.add_domain(domain_id, definition_id, _make_domain_def(1, None, None), caller=self.user)
        except DashiError, de:
            print de
            passed = True

        self.assertTrue(passed)

    def domain_add_bad_definition_test(self):
        domain_id = str(uuid.uuid4())
        definition_id = str(uuid.uuid4())
        caller = self.user

        passed = False
        try:
            self.epum_client.add_domain(domain_id, definition_id, _make_domain_def(1, None, None), caller=self.user)
        except DashiError, de:
            print de
            passed = True

        self.assertTrue(passed)

    def domain_remove_unknown_domain_test(self):
        passed = False
        try:
            domain_id = str(uuid.uuid4())
            self.epum_client.remove_domain(domain_id)
        except DashiError, de:
            print de
            passed = True

        self.assertTrue(passed)


    def domain_add_remove_immediately_test(self):
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site()
        dt_name = self._load_dtrs(fake_site)

        dt = _make_domain_def(1, dt_name, fake_site['name'])
        dt['engine_conf']['epuworker_type'] = dt_name
        dt['engine_conf']['preserve_n'] = 2
        def_id = str(uuid.uuid4())
        self.epum_client.add_domain_definition(def_id, example_definition)
        domain_id = str(uuid.uuid4())

        self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)
        self.epum_client.remove_domain(domain_id)

        nodes = lc.list_nodes()

        # if they never clean up this should result in a timeout error
        while len(nodes) != 0:
            time.sleep(0.1)
            nodes = lc.list_nodes()

    def domain_add_check_n_remove_test(self):
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site()
        dt_name = self._load_dtrs(fake_site)

        n = 3
        dt = _make_domain_def(n, dt_name, fake_site['name'])
        def_id = str(uuid.uuid4())
        self.epum_client.add_domain_definition(def_id, example_definition)
        domain_id = str(uuid.uuid4())

        self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)

        # if they never clean up this should result in a timeout error
        nodes = lc.list_nodes()
        while len(nodes) != n:
            time.sleep(0.1)
            gevent.sleep(0.01)
            nodes = lc.list_nodes()

        self.epum_client.remove_domain(domain_id)

        # if they never clean up this should result in a timeout error
        nodes = lc.list_nodes()
        while len(nodes) != 0:
            time.sleep(0.1)
            gevent.sleep(0.01)
            nodes = lc.list_nodes()



