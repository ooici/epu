import os
import uuid
import unittest
import logging

from nose.plugins.skip import SkipTest
import gevent
from gevent import Timeout

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

example_domain = {
    'general' : {
        'engine_class' : 'epu.decisionengine.impls.simplest.SimplestEngine',
    },
    'health' : {
        'monitor_health' : False
    },
    'engine_conf' : {
        'preserve_n' : 0,
        'epuworker_type' : dt_name,
        'force_site' : 'ec2-fake'
    }
}

class TestIntegration(TestFixture):

    def setup(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.deployment = basic_deployment % {"default_user" : default_user}

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.user = default_user

        self.epuh_persistence = "/tmp/SupD/epuharness"
        if os.path.exists(self.epuh_persistence):
            raise SkipTest("EPUHarness running. Can't run this test")

        # Set up fake libcloud and start deployment
        self.fake_site = self.make_fake_libcloud_site()

        self.epuharness = EPUHarness(exchange=self.exchange)
        self.dashi = self.epuharness.dashi

        self.epuharness.start(deployment_str=self.deployment)

        clients = self.get_clients(self.deployment, self.dashi)
        self.provisioner_client = clients['prov_0']
        self.epum_client = clients['epum_0']
        self.dtrs_client = clients['dtrs']

        self.block_until_ready(self.deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(self.fake_site['name'], self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.fake_site['name'], fake_credentials)

    def teardown(self):
        self.epuharness.stop()
        os.remove(self.fake_libcloud_db)

    def test_example(self):
        # Place integration tests here!
        launch_id = "test"
        instance_ids = ["test"]
        deployable_type = dt_name
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

        #check that mock has a VM
        mock_vms = self.libcloud.list_nodes()
        assert len(mock_vms) == 1


epum_zk_deployment = """
epums:
  epum_0:
    config:
      replica_count: %(epum_replica_count)s
      epumanagement:
        default_user: %(default_user)s
        provisioner_service_name: prov_0
        persistence_type: zookeeper
        zookeeper_hosts: %(zk_hosts)s
        zookeeper_path: %(epum_zk_path)s
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

class TestEPUMZKIntegration(unittest.TestCase, TestFixture, ZooKeeperTestMixin):

    replica_count = 3

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.setup_zookeeper("/EPUMIntTests")

        self.deployment = epum_zk_deployment % dict(default_user=default_user,
            zk_hosts=self.zk_hosts, epum_zk_path=self.zk_base_path,
            epum_replica_count=self.replica_count)

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.user = default_user

        self.epuh_persistence = "/tmp/SupD/epuharness"
        if os.path.exists(self.epuh_persistence):
            raise SkipTest("EPUHarness running. Can't run this test")

        # Set up fake libcloud and start deployment
        self.fake_site = self.make_fake_libcloud_site()

        self.epuharness = EPUHarness(exchange=self.exchange)
        self.dashi = self.epuharness.dashi

        self.epuharness.start(deployment_str=self.deployment)

        clients = self.get_clients(self.deployment, self.dashi)
        self.provisioner_client = clients['prov_0']
        self.epum_client = clients['epum_0']
        self.dtrs_client = clients['dtrs']

        self.block_until_ready(self.deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(self.fake_site['name'], self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.fake_site['name'], fake_credentials)

    def tearDown(self):
        self.epuharness.stop()
        os.remove(self.fake_libcloud_db)
        self.teardown_zookeeper()

    def _get_reconfigure_n(self, n):
        return dict(engine_conf=dict(preserve_n=n))

    def wait_for_libcloud_nodes(self, count, timeout=30):
        nodes = []
        with Timeout(timeout):
            while len(nodes) != count:
                nodes = self.libcloud.list_nodes()

                print "Got %d libcloud nodes" % len(nodes)
                gevent.sleep(0.01)
        return nodes

    def wait_for_domain_set(self, expected, timeout=30):
        expected = set(expected)
        domains = set()
        with Timeout(timeout):
            while domains != expected:
                domains = set(self.epum_client.list_domains())

                gevent.sleep(0.01)

    def test_add_remove_domain(self):

        self.epum_client.add_domain("dom1", example_domain)

        domains = self.epum_client.list_domains()
        self.assertEqual(domains, ['dom1'])

        self.assertFalse(self.libcloud.list_nodes())

        # reconfigure N to cause some instances to start
        self.epum_client.reconfigure_domain("dom1", self._get_reconfigure_n(5))

        self.wait_for_libcloud_nodes(5)

        # and more instances
        self.epum_client.reconfigure_domain("dom1", self._get_reconfigure_n(100))
        self.wait_for_libcloud_nodes(100)

        # and less
        self.epum_client.reconfigure_domain("dom1", self._get_reconfigure_n(5))
        self.wait_for_libcloud_nodes(5)

        # remove the domain, all should be killed
        self.epum_client.remove_domain("dom1")
        self.wait_for_libcloud_nodes(0)

        self.wait_for_domain_set([])

