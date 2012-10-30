import os
import time
import uuid
import unittest
import logging

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
      ssl_no_host_check: True
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
    'real-site':{
      'iaas_image': 'r2-worker',
      'iaas_allocation': 'm1.large',
    },
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

example_definition = {
    'general' : {
        'engine_class' : 'epu.decisionengine.impls.simplest.SimplestEngine',
    },
    'health' : {
        'monitor_health' : False
    }
}

example_domain = {
    'engine_conf' : {
        'preserve_n' : 0,
        'epuworker_type' : dt_name,
        'force_site' : 'ec2-fake'
    }
}

dt_name2 = "with-userdata"
example_userdata = 'Hello Cloudy World'
example_dt2 = {
  'mappings': {
    'ec2-fake':{
      'iaas_image': 'ami-fake',
      'iaas_allocation': 't1.micro',
    }
  },
  'contextualization':{
    'method': 'userdata',
    'userdata': example_userdata
  }
}


class TestIntegration(unittest.TestCase, TestFixture):

    def setUp(self):

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

    def tearDown(self):
        self.epuharness.stop()
        self.libcloud.shutdown()
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

    def test_userdata(self):

        launch_id = "test"
        instance_ids = ["test"]
        deployable_type = dt_name2
        site = self.fake_site['name']
        subscribers = []

        self.dtrs_client.add_dt(self.user, deployable_type, example_dt2)
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

        nodes = self.libcloud.list_nodes()
        node = nodes[0]
        self.assertTrue('ex_userdata' in node.extra)
        self.assertEqual(example_userdata, node.extra['ex_userdata'])

pd_epum_deployment = """
process-dispatchers:
  pd_0:
    config:
      processdispatcher:
        static_resources: False
        epum_service_name: epum_0
        definition_id: pd_definition
        domain_config:
          engine_conf:
           iaas_site: %(iaas_site)s
           iaas_allocation: m1.small
           deployable_type: %(worker_dt)s
        engines:
          default:
            slots: 4
            replicas: 2
            base_need: 0
epums:
  epum_0:
    config:
      epumanagement:
        default_user: %(default_user)s
        provisioner_service_name: prov_0
        initial_definitions:
          pd_definition:
            general:
              engine_class: epu.decisionengine.impls.needy.NeedyEngine
              health:
                monitor_health: false
      logging:
        handlers:
          file:
            filename: /tmp/epum_0.log
provisioners:
  prov_0:
    config:
      ssl_no_host_check: True
      provisioner:
        default_user: %(default_user)s
dt_registries:
  dtrs:
    config: {}
"""

class TestPDEPUMIntegration(unittest.TestCase, TestFixture):

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.worker_dt = dt_name
        self.iaas_site = "ec2-fake"

        self.deployment = pd_epum_deployment % {"default_user" : default_user,
                'worker_dt': self.worker_dt, 'iaas_site': self.iaas_site}

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
        self.pd_client = clients['pd_0']

        self.block_until_ready(self.deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, self.worker_dt, example_dt)
        self.dtrs_client.add_site(self.fake_site['name'], self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.fake_site['name'], fake_credentials)

    def tearDown(self):
        self.epuharness.stop()
        self.libcloud.shutdown()
        os.remove(self.fake_libcloud_db)

    def _wait_for_value(self, callme, value, args=(), kwargs={}, timeout=60):

        result = None
        for i in range(0, timeout):
            result = callme(*args, **kwargs)
            if result == value:
                return
            time.sleep(1)
        assert result == value

    def _wait_for_instances(self, want_n_instances, timeout=60):

        instances = None
        for i in range(0, timeout):
            instances = self.epum_client.describe_domain('pd_domain_default')['instances']
            if len(instances) == want_n_instances:
                return
            time.sleep(1)
        assert len(instances) == want_n_instances

    def test_epum_pd_integration(self):

        # First ensure base_need of 0 is respected:
        nodes = self.provisioner_client.describe_nodes()
        self.assertEqual(nodes, [])

        instances = self.epum_client.describe_domain('pd_domain_default')['instances']
        self.assertEqual(len(instances), 0)

        # Now we submit a process, to ensure that the need is registered, and
        procs = []
        exe = {"exec": "sleep", "argv": ["1"]}

        self.pd_client.create_definition("def1", "supd", exe)
        self.assertEqual(self.pd_client.describe_processes(), [])

        upid = uuid.uuid4().hex
        procs.append(upid)
        self.pd_client.schedule_process(upid, "def1")

        self._wait_for_instances(1)

        for i in range(0, 5):
            upid = uuid.uuid4().hex
            procs.append(upid)
            self.pd_client.schedule_process(upid, "def1")

        self._wait_for_instances(2)


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
        self.libcloud.shutdown()
        os.remove(self.fake_libcloud_db)
        self.teardown_zookeeper()

    def _get_reconfigure_n(self, n):
        return dict(engine_conf=dict(preserve_n=n))

    def wait_for_libcloud_nodes(self, count, timeout=60):
        nodes = None
        timeleft = float(timeout)
        sleep_amount = 0.01

        while timeleft > 0 and (nodes is None or len(nodes) != count):
            nodes = self.libcloud.list_nodes()

            time.sleep(sleep_amount)
            timeleft -= sleep_amount
        return nodes

    def wait_for_domain_set(self, expected, timeout=30):
        expected = set(expected)
        domains = set()
        timeleft = float(timeout)
        sleep_amount = 0.01

        while timeleft > 0 and domains != expected:
            domains = set(self.epum_client.list_domains())

            time.sleep(sleep_amount)
            timeleft -= sleep_amount

    def wait_for_all_domains(self, timeout=30):
        timeleft = float(timeout)
        sleep_amount = 0.01
        while timeleft > 0 and not self.verify_all_domain_instances():
            time.sleep(sleep_amount)
            timeleft -= sleep_amount

    def verify_all_domain_instances(self):
        libcloud_nodes  = self.libcloud.list_nodes()

        libcloud_nodes_by_id = dict((n.id, n) for n in libcloud_nodes)
        self.assertEqual(len(libcloud_nodes), len(libcloud_nodes_by_id))

        found_nodes = set()
        all_complete = True

        domains = self.epum_client.list_domains()
        for domain_id in domains:
            domain = self.epum_client.describe_domain(domain_id)

            # this may need to change if we make engine conf more static
            preserve_n = int(domain['config']['engine_conf']['preserve_n'])

            domain_instances = domain['instances']

            valid_count = 0
            for domain_instance in domain_instances:
                state = domain_instance['state']

                if InstanceState.PENDING <= state <= InstanceState.TERMINATING:
                    iaas_id = domain_instance['iaas_id']
                    self.assertIn(iaas_id, libcloud_nodes_by_id)
                    found_nodes.add(iaas_id)
                    valid_count += 1

            if valid_count != preserve_n:
                all_complete = False

        # ensure the set of seen iaas IDs matches the total set
        self.assertEqual(found_nodes, set(libcloud_nodes_by_id.keys()))

        return all_complete

    def test_add_remove_domain(self):

        self.epum_client.add_domain_definition("def1", example_definition)

        self.epum_client.add_domain("dom1", "def1", example_domain)

        domains = self.epum_client.list_domains()
        self.assertEqual(domains, ['dom1'])

        self.assertFalse(self.libcloud.list_nodes())

        # reconfigure N to cause some instances to start
        self.epum_client.reconfigure_domain("dom1", self._get_reconfigure_n(5))

        self.wait_for_libcloud_nodes(5)
        self.wait_for_all_domains()

        # and more instances
        self.epum_client.reconfigure_domain("dom1", self._get_reconfigure_n(100))
        self.wait_for_libcloud_nodes(100)
        self.wait_for_all_domains()

        # and less
        self.epum_client.reconfigure_domain("dom1", self._get_reconfigure_n(5))
        self.wait_for_libcloud_nodes(5)
        self.wait_for_all_domains()

        # remove the domain, all should be killed
        self.epum_client.remove_domain("dom1")
        self.wait_for_libcloud_nodes(0)
        self.wait_for_domain_set([])


pd_zk_deployment = """
process-dispatchers:
  pd_0:
    config:
      replica_count: %(pd_replica_count)s
      zookeeper:
        hosts: %(zk_hosts)s
        processdispatcher_path: %(pd_zk_path)s
      processdispatcher:
        engines:
          default:
            slots: 4
            base_need: 1
    eeagents: [eeagent_nodeone]
nodes:
  nodeone:
    engine: default
    process-dispatcher: pd_0
    eeagents:
      eeagent_nodeone:
        launch_type: supd
        logfile: /tmp/eeagent_nodeone.log
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

class TestPDZKIntegration(unittest.TestCase, TestFixture, ZooKeeperTestMixin):

    replica_count = 3

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.setup_zookeeper("/PDIntTests")

        self.deployment = pd_zk_deployment % dict(default_user=default_user,
            zk_hosts=self.zk_hosts, pd_zk_path=self.zk_base_path,
            pd_replica_count=self.replica_count)

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
        self.pd_client = clients['pd_0']

        self.block_until_ready(self.deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(self.fake_site['name'], self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.fake_site['name'], fake_credentials)

    def tearDown(self):
        self.epuharness.stop()
        self.libcloud.shutdown()
        os.remove(self.fake_libcloud_db)
        self.teardown_zookeeper()

    def wait_for_terminated_processes(self, count, timeout=60):
        terminated_processes = None
        timeleft = float(timeout)
        sleep_amount = 1
        while timeleft > 0 and (
              terminated_processes is None or len(terminated_processes) < count):
            processes = self.pd_client.describe_processes()
            terminated_processes = filter(lambda x: x['state'] == '800-EXITED', processes)
            time.sleep(sleep_amount)
            timeleft -= sleep_amount

        return terminated_processes

    def test_dispatch_run_process(self):
        procs = []
        exe = {"exec": "sleep", "argv": ["1"]}

        self.pd_client.create_definition("def1", "supd", exe)

        self.assertEqual(self.pd_client.describe_processes(), [])

        for i in range(10):
            upid = uuid.uuid4().hex
            procs.append(upid)
            self.pd_client.schedule_process(upid, "def1")

        terminated_processes = self.wait_for_terminated_processes(10)
        self.assertEqual(len(terminated_processes), 10)

timeout_deployment = """
provisioners:
  prov_0:
    config:
      ssl_no_host_check: True
      provisioner:
        iaas_timeout: %(iaas_timeout)s
        default_user: %(default_user)s
dt_registries:
  dtrs:
    config: {}
"""

class TestProvisionerIntegration(TestFixture):

    def setup(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.deployment = timeout_deployment % {"default_user" : default_user,
                                                "iaas_timeout" : 0.0001}

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.user = default_user

        self.epuh_persistence = "/tmp/SupD/epuharness"
        if os.path.exists(self.epuh_persistence):
            raise SkipTest("EPUHarness running. Can't run this test")


        if (os.environ.get("LIBCLOUD_DRIVER") and os.environ.get("IAAS_HOST")
            and os.environ.get("IAAS_PORT") and os.environ.get("AWS_ACCESS_KEY_ID")
            and os.environ.get("AWS_SECRET_ACCESS_KEY")):
            self.site = self.make_real_libcloud_site(
                    'real-site', os.environ.get("LIBCLOUD_DRIVER"),
                    os.environ.get("IAAS_HOST"), os.environ.get("IAAS_PORT")
            )
            self.credentials = {
                'access_key': os.environ.get("AWS_ACCESS_KEY_ID"),
                'secret_key': os.environ.get("AWS_SECRET_ACCESS_KEY"),
                'key_name': 'ooi'
            }
        else:
            print "Using fake site"
            # Set up fake libcloud and start deployment
            self.site = self.make_fake_libcloud_site()
            self.credentials = fake_credentials

        self.epuharness = EPUHarness(exchange=self.exchange)
        self.dashi = self.epuharness.dashi

        self.epuharness.start(deployment_str=self.deployment)

        clients = self.get_clients(self.deployment, self.dashi)
        self.provisioner_client = clients['prov_0']
        self.dtrs_client = clients['dtrs']

        self.block_until_ready(self.deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(self.site['name'], self.site)
        self.dtrs_client.add_credentials(self.user, self.site['name'], self.credentials)

    def teardown(self):
        self.epuharness.stop()
        if hasattr(self, 'fake_libcloud_db'):
            self.libcloud.shutdown()
            os.remove(self.fake_libcloud_db)

    def test_create_timeout(self):

        launch_id = "test"
        instance_ids = ["test"]
        deployable_type = dt_name
        site = self.site['name']
        subscribers = []

        self.provisioner_client.provision(launch_id, instance_ids, deployable_type, subscribers, site=site)

        while True:
            instances = self.provisioner_client.describe_nodes()
            if (instances[0]['state'] == '200-REQUESTED' or
                instances[0]['state'] == '400-PENDING'):
                continue
            elif instances[0]['state'] == '900-FAILED':
                print instances[0]['state_desc']
                assert instances[0]['state_desc'] == 'IAAS_TIMEOUT'
                break
            else:
                assert False, "Got unexpected state %s" % instances[0]['state']
