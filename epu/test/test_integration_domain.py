import os
import uuid
import unittest
import logging
from dashi import DashiError
import tempfile
from nose.plugins.skip import SkipTest
import time
import random

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

from libcloud.compute.types import NodeState

log = logging.getLogger(__name__)

default_user = 'default'

def make_fake_libcloud_site():
    from epu.mocklibcloud import MockEC2NodeDriver
    fh, fake_libcloud_db = tempfile.mkstemp()
    os.close(fh)

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
        'engine_class' : 'epu.decisionengine.impls.phantom.PhantomEngine',
    },
    'health' : {
        'monitor_health' : False
    }
}

sensor_definition = {
    'general' : {
        'engine_class' : 'epu.decisionengine.impls.sensor.SensorEngine',
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

def _make_sensor_domain_def(metric, sample_function, minimum_n, maximum_n,
        scale_up_threshold,
        scale_up_n_vms, scale_down_threshold, scale_down_n_vms, sensor_data,
        epuworker_type, site_name):

    example_domain = {
        'engine_conf' : {
            'sensor_type' : 'mockcloudwatch',
            'metric' : metric,
            'sample_function' : sample_function,
            'minimum_vms' : minimum_n,
            'maximum_vms' : maximum_n,
            'scale_up_threshold' : scale_up_threshold,
            'scale_up_n_vms' : scale_up_n_vms,
            'scale_down_threshold' : scale_down_threshold,
            'scale_down_n_vms' : scale_down_n_vms,
            'sensor_data': sensor_data,
            'deployable_type' : epuworker_type,
            'iaas_site' : site_name,
            'iaas_allocation': 't1.micro',
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
        self.provisioner_client = clients['prov_0']
        self.block_until_ready(g_deployment, g_epuharness.dashi)

    def tearDown(self):

        self._wait_for_domains_to_exit()

        for i in range(0, 100):
            nodes = self.provisioner_client.describe_nodes()
            for node in nodes:
                if node['state'] not in ('800-TERMINATED', '900-FAILED'):
                    print node
                    break
            else:
                break
        else:
            print self.provisioner_client.describe_nodes()
            assert False, "There were non-terminated nodes left on teardown"

        #os.remove(self.fake_libcloud_db)

    def _wait_for_domains_to_exit(self):
        print "Wait for domains to exit..."
        for i in range(0, 600):
            if len(self.epum_client.list_domains()) == 0:
                break
            time.sleep(1)
        else:
            assert len(self.epum_client.list_domains()) == 0, str(self.epum_client.list_domains())

    def _load_dtrs(self, fake_site):
        dt_name = str(uuid.uuid4())
        self.dtrs_client.add_dt(self.user, dt_name, _make_dt(fake_site['name']))
        self.dtrs_client.add_site(fake_site['name'], fake_site)
        self.dtrs_client.add_credentials(self.user, fake_site['name'], fake_credentials)
        return dt_name

    def _wait_states(self, n, lc, states=None):
        if states is None:
            states = [NodeState.RUNNING, NodeState.PENDING]
        nodes = lc.list_nodes()
        running_count  = 0
        while running_count != n:
            running_count  = 0
            for nd in nodes:
                if nd.state in states:
                    running_count = running_count + 1
            time.sleep(0.1)
            time.sleep(0.01)
            nodes = lc.list_nodes()


    def domain_add_all_params_not_exist_test(self):
        domain_id = str(uuid.uuid4())
        definition_id = str(uuid.uuid4())
        caller = str(uuid.uuid4())

        passed = False
        try:
            self.epum_client.add_domain(domain_id, definition_id, _make_domain_def(1, None, None), caller=caller)
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

    def domain_sensor_engine_test(self):
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site()
        dt_name = self._load_dtrs(fake_site)

        minimum_n = 1
        maximum_n = 3
        scale_up_threshold = 2.0
        scale_up_n_vms = 1
        scale_down_threshold = 0.5
        scale_down_n_vms = 1
        scale_down_sensor_data = [0, 0, 0]
        scale_up_sensor_data = [3, 3, 5]
        metric = 'load'
        sample_function = 'Average'
        dt = _make_sensor_domain_def(metric, sample_function, minimum_n,
                maximum_n, scale_up_threshold,
                scale_up_n_vms, scale_down_threshold, scale_down_n_vms,
                scale_down_sensor_data, dt_name, fake_site['name'])
        def_id = str(uuid.uuid4())
        self.epum_client.add_domain_definition(def_id, sensor_definition)
        domain_id = str(uuid.uuid4())

        self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)

        # make sure we hit the minimum number of nodes
        nodes = lc.list_nodes()
        while len(nodes) < minimum_n:
            time.sleep(0.1)
            nodes = lc.list_nodes()

        # Now get it to scale up
        new_config = {'engine_conf': {'sensor_data': scale_up_sensor_data}}
        self.epum_client.reconfigure_domain(domain_id, new_config, caller=self.user)

        # make sure we hit the maximum number of nodes
        nodes = lc.list_nodes()
        while len(nodes) != maximum_n:
            time.sleep(0.1)
            nodes = lc.list_nodes()

        # Now get it to scale down
        new_config = {'engine_conf': {'sensor_data': scale_down_sensor_data}}
        self.epum_client.reconfigure_domain(domain_id, new_config, caller=self.user)

        nodes = lc.list_nodes()
        while len(nodes) != minimum_n:
            time.sleep(0.1)
            nodes = lc.list_nodes()

        # Now test the cooldown
        new_config = {'engine_conf': {
            'sensor_data': scale_up_sensor_data,
            'cooldown_period': 100,
            }
        }
        self.epum_client.reconfigure_domain(domain_id, new_config, caller=self.user)

        # Wait 10s for a few decides to happen:
        time.sleep(10)

        # And ensure we're still a minimum scaling
        nodes = lc.list_nodes()
        self.assertEqual(len(nodes), minimum_n)

        # Now set cooldown to 10s (which have already passed)
        new_config = {'engine_conf': {'cooldown_period': 10}}
        self.epum_client.reconfigure_domain(domain_id, new_config, caller=self.user)

        # And watch it scale up
        nodes = lc.list_nodes()
        while len(nodes) != maximum_n:
            time.sleep(0.1)
            nodes = lc.list_nodes()

        self.epum_client.remove_domain(domain_id)

        # if they never clean up this should result in a timeout error
        nodes = lc.list_nodes()
        while len(nodes) != 0:
            time.sleep(0.1)
            time.sleep(0.01)
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
            time.sleep(0.01)
            nodes = lc.list_nodes()

        self.epum_client.remove_domain(domain_id)

        # if they never clean up this should result in a timeout error
        nodes = lc.list_nodes()
        while len(nodes) != 0:
            time.sleep(0.1)
            time.sleep(0.01)
            nodes = lc.list_nodes()

    def domain_n_preserve_remove_node_test(self):
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
            time.sleep(0.01)
            nodes = lc.list_nodes()

        lc.destroy_node(random.choice(nodes))
        nodes = lc.list_nodes()
        while len(nodes) != n:
            time.sleep(0.1)
            time.sleep(0.01)
            nodes = lc.list_nodes()

        self.epum_client.remove_domain(domain_id)

        # if they never clean up this should result in a timeout error
        nodes = lc.list_nodes()
        while len(nodes) != 0:
            time.sleep(0.1)
            time.sleep(0.01)
            nodes = lc.list_nodes()

    def domain_n_preserve_alter_state_test(self):

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
            time.sleep(0.01)
            nodes = lc.list_nodes()

        lc.set_node_state(nodes[0], NodeState.TERMINATED)
        nodes = lc.list_nodes()

        running_count  = 0
        while running_count != n:
            running_count  = 0
            for nd in nodes:
                if nd.state == NodeState.RUNNING or nd.state == NodeState.PENDING:
                    running_count = running_count + 1
            time.sleep(0.1)
            time.sleep(0.01)
            nodes = lc.list_nodes()

        print "terminating"
        self.epum_client.remove_domain(domain_id)

        # wait until the domain is gone
        domain_list = self.epum_client.list_domains(caller=self.user)
        while domain_id in domain_list:
            time.sleep(0.1)
            time.sleep(0.01)
            domain_list = self.epum_client.list_domains(caller=self.user)

        # check the node list
        nodes = lc.list_nodes()
        for nd in nodes:
            # verify that any node that is still around is terminated
            self.assertEqual(nd.state, NodeState.TERMINATED)

    def domain_n_preserve_resource_full_test(self):

        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site()
        dt_name = self._load_dtrs(fake_site)

        n = 3
        max_vms = 1
        lc._set_max_VMS(max_vms)

        dt = _make_domain_def(n, dt_name, fake_site['name'])
        def_id = str(uuid.uuid4())
        self.epum_client.add_domain_definition(def_id, example_definition)
        domain_id = str(uuid.uuid4())
        error_count = lc.get_create_error_count()
        self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)

        print "waiting on error count"
        while error_count == lc.get_create_error_count():
            nodes = lc.list_nodes()
            print "%d %d %d %d" % (error_count, lc.get_create_error_count(), len(nodes), lc.get_max_vms())
            time.sleep(0.5)
        print "change max"

        lc._set_max_VMS(n)
        self._wait_states(n, lc)

        print "terminating"
        self.epum_client.remove_domain(domain_id)

    def domain_n_preserve_adjust_n_up_test(self):
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site()
        dt_name = self._load_dtrs(fake_site)

        n = 3
        dt = _make_domain_def(n, dt_name, fake_site['name'])
        def_id = str(uuid.uuid4())
        self.epum_client.add_domain_definition(def_id, example_definition)
        domain_id = str(uuid.uuid4())
        self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)
        self._wait_states(n, lc)

        n = n + 3
        dt = _make_domain_def(n, dt_name, fake_site['name'])
        self.epum_client.reconfigure_domain(domain_id, dt, caller=self.user)

        lc._set_max_VMS(n)
        self._wait_states(n, lc)

        self.epum_client.remove_domain(domain_id)

    def domain_n_preserve_adjust_n_down_test(self):
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site()
        dt_name = self._load_dtrs(fake_site)

        n = 3
        dt = _make_domain_def(n, dt_name, fake_site['name'])
        def_id = str(uuid.uuid4())
        self.epum_client.add_domain_definition(def_id, example_definition)
        domain_id = str(uuid.uuid4())
        self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)
        self._wait_states(n, lc)

        n = n - 1
        dt = _make_domain_def(n, dt_name, fake_site['name'])
        self.epum_client.reconfigure_domain(domain_id, dt, caller=self.user)

        lc._set_max_VMS(n)
        self._wait_states(n, lc)

        self.epum_client.remove_domain(domain_id)


    def many_domain_simple_test(self):
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site()
        dt_name = self._load_dtrs(fake_site)

        n = 1
        domains = []
        for i in range(0, 128):
            dt = _make_domain_def(n, dt_name, fake_site['name'])
            def_id = str(uuid.uuid4())
            self.epum_client.add_domain_definition(def_id, example_definition)
            domain_id = str(uuid.uuid4())
            self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)
            domains.append(domain_id)

        time.sleep(0.5)

        for domain_id in domains:
            self.epum_client.remove_domain(domain_id)

    def many_domain_vary_n_test(self):
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site()
        dt_name = self._load_dtrs(fake_site)

        domains = []
        for i in range(0, 128):
            # this test is slooooowwww to cleanup
            #n = int(random.random() * 256)
            n = int(random.random() * 2)
            dt = _make_domain_def(n, dt_name, fake_site['name'])
            def_id = str(uuid.uuid4())
            self.epum_client.add_domain_definition(def_id, example_definition)
            domain_id = str(uuid.uuid4())
            self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)
            domains.append(domain_id)

        time.sleep(0.5)

        for domain_id in domains:
            self.epum_client.remove_domain(domain_id)


    def many_domain_vary_remove_test(self):
        (fake_site, lc, fake_libcloud_db) = make_fake_libcloud_site()
        dt_name = self._load_dtrs(fake_site)

        n = 4
        dt = _make_domain_def(n, dt_name, fake_site['name'])
        def_id = str(uuid.uuid4())
        self.epum_client.add_domain_definition(def_id, example_definition)

        domains = []

        # add some domains
        for i in range(0, 64):
            domain_id = str(uuid.uuid4())
            self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)
            domains.append(domain_id)

        time.sleep(0.5)

        for i in range(0, 64):
            # every other time add a VM then remove a VM

            if i % 2 == 0:
                print "add a VM"
                domain_id = str(uuid.uuid4())
                self.epum_client.add_domain(domain_id, def_id, dt, caller=self.user)
                domains.append(domain_id)
            else:
                print "remove a VM"
                domain_id = random.choice(domains)
                domains.remove(domain_id)
                self.epum_client.remove_domain(domain_id)


        for domain_id in domains:
            print "Removing %s" % domain_id
            self.epum_client.remove_domain(domain_id)


