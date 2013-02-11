import os
import time
import uuid
import unittest
import logging
import sys

from nose.plugins.skip import SkipTest
import signal

try:
    from epuharness.harness import EPUHarness
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("epuharness not available.")
try:
    from epu.mocklibcloud import MockEC2NodeDriver, NodeState
except ImportError:
    raise SkipTest("sqlalchemy not available.")

from epu.test import ZooKeeperTestMixin
from epu.states import InstanceState

log = logging.getLogger(__name__)

default_user = 'default'


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

def _example_domain(n):
    return {
        'engine_conf' : {
        'preserve_n' : n,
        'epuworker_type' : dt_name,
        'force_site' : 'ec2-fake'
    }
}

epum_zk_deployment = """
epums:
  epum_0:
    config:
      server:
        zookeeper:
          hosts: %(zk_hosts)s
          path: %(epum_zk_path)s
      replica_count: %(epum_replica_count)s
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
      replica_count: %(prov_replica_count)s
      provisioner:
        default_user: %(default_user)s
dt_registries:
  dtrs:
    config: {}
"""


class TestEPUMZKWithKills(unittest.TestCase, TestFixture, ZooKeeperTestMixin):

    epum_replica_count = 3
    prov_replica_count = 1

    DECIDER_ELECTION_PATH = "/elections/decider"
    DOCTOR_ELECTION_PATH = "/elections/doctor"
    ZK_BASE = "/EPUMKillTests"

    def setUp(self):

        if not os.environ.get('INT'):
            raise SkipTest("Slow integration test")

        self.setup_zookeeper(self.ZK_BASE)
        self.addCleanup(self.cleanup_zookeeper)

        self.deployment = epum_zk_deployment % dict(default_user=default_user,
            zk_hosts=self.zk_hosts, epum_zk_path=self.zk_base_path,
            epum_replica_count=self.epum_replica_count, prov_replica_count=self.prov_replica_count)

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.user = default_user

        # Set up fake libcloud and start deployment
        self.setup_harness()
        self.addCleanup(self.cleanup_harness)

        self.fake_site, self.libcloud = self.make_fake_libcloud_site("ec2-fake")

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

    def _get_reconfigure_n(self, n):
        return dict(engine_conf=dict(preserve_n=n))

    def get_valid_nodes(self):
        nodes = self.libcloud.list_nodes()
        return [node for node in nodes if node.state != NodeState.TERMINATED]

    def wait_for_libcloud_nodes(self, count, timeout=60):
        nodes = None
        timeleft = float(timeout)
        sleep_amount = 0.01

        while timeleft > 0 and (nodes is None or len(nodes) != count):
            nodes = self.get_valid_nodes()

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
        libcloud_nodes = self.libcloud.list_nodes()

        libcloud_nodes_by_id = dict((n.id, n) for n in libcloud_nodes
            if n.state != NodeState.TERMINATED)
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

    def _kill_cb(self, place_at, place_want_list, kill_func):
        if not kill_func:
            return place_at
        if place_want_list == None:
            return place_at

        if place_at in place_want_list:
            kill_func()
        return place_at + 1

    def _add_reconfigure_remove_domain(self, kill_func=None, places_to_kill=None):
        test_pc = 1
        self.epum_client.add_domain_definition("def1", example_definition)

        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)
        domain = _example_domain(0)
        self.epum_client.add_domain("dom1", "def1", domain)
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        domains = self.epum_client.list_domains()
        self.assertEqual(domains, ['dom1'])

        self.assertFalse(self.get_valid_nodes())

        # reconfigure N to cause some instances to start
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)
        self.epum_client.reconfigure_domain("dom1", self._get_reconfigure_n(2))
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        self.wait_for_libcloud_nodes(2)
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)
        self.wait_for_all_domains()
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        # remove the domain, all should be killed
        self.epum_client.remove_domain("dom1")
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)
        self.wait_for_libcloud_nodes(0)
        self.wait_for_domain_set([])

    def _add_remove_many_domains(self, kill_func=None, places_to_kill=None, n=1):

        test_pc = 1
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        self.epum_client.add_domain_definition("def1", example_definition)
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        ed = _example_domain(1)

        domains_started = []
        for i in range(n):
            name = "dom%d" % (i)
            self.epum_client.add_domain(name, "def1", ed)
            domains_started.append(name)

        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        domains = self.epum_client.list_domains()
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)
        domains_started.sort()
        domains.sort()
        self.assertEqual(domains, domains_started)

        self.wait_for_libcloud_nodes(n)
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)
        self.wait_for_all_domains()

        # remove the domain, all should be killed
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)
        for name in domains_started:
            self.epum_client.remove_domain(name)
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)
        self.wait_for_libcloud_nodes(0)
        self.wait_for_domain_set([])

    def _get_leader_supd_name(self, path, ndx=0):
        election = self.kazoo.Election(path)
        contenders = election.contenders()
        leader = contenders[ndx]
        name = leader.split(":")[0]
        return name

    def _get_leader_pid(self, path, ndx=0):
        election = self.kazoo.Election(path)
        contenders = election.contenders()
        leader = contenders[ndx]
        pid = leader.split(":")[2]
        return int(pid)

    def _kill_decider_epum_supd(self):
        name = self._get_leader_supd_name(self.DECIDER_ELECTION_PATH)
        self.epuharness.stop(services=[name])

    def _kill_decider_epum_pid(self):
        pid = self._get_leader_pid(self.DECIDER_ELECTION_PATH)
        os.kill(pid, signal.SIGTERM)

    def _kill_notdecider_epum_supd(self):
        name = self._get_leader_supd_name(self.DECIDER_ELECTION_PATH, 1)
        self.epuharness.stop(services=[name])

    def _kill_not_decider_epum_pid(self):
        pid = self._get_leader_pid(self.DECIDER_ELECTION_PATH, 1)
        os.kill(pid, signal.SIGTERM)

    def _kill_doctor_epum_supd(self):
        name = self._get_leader_supd_name(self.DOCTOR_ELECTION_PATH)
        self.epuharness.stop(services=[name])

    def _kill_doctor_epum_pid(self):
        pid = self._get_leader_pid(self.DOCTOR_ELECTION_PATH)
        os.kill(pid, signal.SIGTERM)

    def _kill_notdoctor_epum_supd(self):
        name = self._get_leader_supd_name(self.DOCTOR_ELECTION_PATH, 1)
        self.epuharness.stop(services=[name])

    def _kill_not_doctor_epum_pid(self):
        pid = self._get_leader_pid(self.DOCTOR_ELECTION_PATH, 1)
        os.kill(pid, signal.SIGTERM)


def create_reconfigure(kill_func_name, places_to_kill):
    def doit(self):
        kill_func = getattr(self, kill_func_name)
        self._add_reconfigure_remove_domain(kill_func=kill_func, places_to_kill=places_to_kill)
    return doit


def create_em(kill_func_name, places_to_kill, n):
    def doit(self):
        kill_func = getattr(self, kill_func_name)
        self._add_remove_many_domains(kill_func=kill_func, places_to_kill=places_to_kill, n=n)
    return doit

kill_func_names = [
    "_kill_decider_epum_supd",
    "_kill_decider_epum_pid",
    "_kill_notdecider_epum_supd",
    "_kill_not_decider_epum_pid",
    "_kill_doctor_epum_supd",
    "_kill_doctor_epum_pid",
    "_kill_notdoctor_epum_supd",
    "_kill_not_doctor_epum_pid"
    ]

for n in [1, 16]:
    for kill_name in kill_func_names:
        method = None
        for i in range(0, 8):
            method = create_em(kill_name, [i], n)
            method.__name__ = 'test_add_remove_domain_kill_point_%d_with_%s_n-%d' % (i, kill_name, n)
            setattr(TestEPUMZKWithKills, method.__name__, method)

for kill_name in kill_func_names:
    method = None
    for i in range(0, 7):
        method = create_reconfigure(kill_name, [i])
        method.__name__ = 'test_reconfigure_kill_point_%d_with_%s' % (i, kill_name)
        setattr(TestEPUMZKWithKills, method.__name__, method)


del method
