# Copyright 2013 University of Chicago

import os
import time
import uuid
import unittest
import logging
import signal

from nose.plugins.skip import SkipTest

try:
    from epuharness.fixture import TestFixture
except ImportError:
    raise SkipTest("epuharness not available.")
try:
    from epu.mocklibcloud import NodeState
except ImportError:
    raise SkipTest("sqlalchemy not available.")

from epu.test import ZooKeeperTestMixin
from epu.test.util import wait
from epu.states import InstanceState

log = logging.getLogger(__name__)

default_user = 'default'


fake_credentials = {
    'access_key': 'xxx',
    'secret_key': 'xxx',
    'key_name': 'ooi'
}

dt_name = "example_prov_zk_kill"
example_dt = {
    'mappings': {
        'real-site': {
            'iaas_image': 'r2-worker',
            'iaas_allocation': 'm1.large',
        },
        'ec2-fake': {
            'iaas_image': 'ami-fake',
            'iaas_allocation': 't1.micro',
        }
    },
    'contextualization': {
        'method': 'chef-solo',
        'chef_config': {}
    }
}

example_definition = {
    'general': {
        'engine_class': 'epu.decisionengine.impls.simplest.SimplestEngine',
    },
    'health': {
        'monitor_health': False
    }
}


def _example_domain(n):
    return {
        'engine_conf': {
            'preserve_n': n,
            'epuworker_type': dt_name,
            'force_site': 'ec2-fake'
        }
    }

epum_zk_deployment = """
epums:
  epum_0:
    config:
      replica_count: %(epum_replica_count)s
      epumanagement:
        default_user: %(default_user)s
        provisioner_service_name: prov_0
        decider_loop_interval: 0.1
      logging:
        handlers:
          file:
            filename: /tmp/epum_0.log
provisioners:
  prov_0:
    config:
      replica_count: %(prov_replica_count)s
      server:
        zookeeper:
          hosts: %(zk_hosts)s
          path: %(epum_zk_path)s
          timeout: %(zk_timeout)s
      provisioner:
        default_user: %(default_user)s
        epu_management_service_name: epum_0
dt_registries:
  dtrs:
    config: {}
"""


class BaseProvKillsFixture(unittest.TestCase, TestFixture, ZooKeeperTestMixin):
    use_zk_proxy = False
    zk_timeout = 5

    epum_replica_count = 1
    prov_replica_count = 3

    ZK_BASE = "/ProvKillTestsTwo"
    PROV_ELECTION_PATH = "/election"

    def setUp(self):

        if not os.environ.get('NIGHTLYINT'):
            raise SkipTest("Slow integration test")

        self.setup_zookeeper(self.ZK_BASE, use_proxy=self.use_zk_proxy)
        self.addCleanup(self.cleanup_zookeeper)

        self.deployment = epum_zk_deployment % dict(default_user=default_user,
            zk_hosts=self.zk_hosts, zk_timeout=self.zk_timeout, epum_zk_path=self.zk_base_path,
            epum_replica_count=self.epum_replica_count, prov_replica_count=self.prov_replica_count)

        self.exchange = "testexchange-%s" % str(uuid.uuid4())
        self.sysname = "testsysname-%s" % str(uuid.uuid4())
        self.user = default_user

        # Set up fake libcloud and start deployment
        self.site_name = "ec2-fake"
        self.fake_site, self.libcloud = self.make_fake_libcloud_site("ec2-fake")

        self.setup_harness(exchange=self.exchange, sysname=self.sysname)
        self.addCleanup(self.cleanup_harness)

        self.epuharness.start(deployment_str=self.deployment)

        clients = self.get_clients(self.deployment, self.dashi)
        self.provisioner_client = clients['prov_0']
        self.epum_client = clients['epum_0']
        self.dtrs_client = clients['dtrs']

        self.block_until_ready(self.deployment, self.dashi)

        self.load_dtrs()

    def load_dtrs(self):
        self.dtrs_client.add_dt(self.user, dt_name, example_dt)
        self.dtrs_client.add_site(self.site_name, self.fake_site)
        self.dtrs_client.add_credentials(self.user, self.site_name, fake_credentials)

    def _get_reconfigure_n(self, n):
        return dict(engine_conf=dict(preserve_n=n))

    def get_valid_libcloud_nodes(self):
        nodes = self.libcloud.list_nodes(immediate=True)
        return [node for node in nodes if node.state != NodeState.TERMINATED]

    def wait_for_libcloud_nodes(self, count, timeout=60):
        wait(lambda: len(self.get_valid_libcloud_nodes()) == count,
            timeout=timeout)

        return self.get_valid_libcloud_nodes()

    def wait_for_domain_set(self, expected, timeout=30):
        expected = set(expected)

        wait(lambda: set(self.epum_client.list_domains()) == expected,
            timeout=timeout)

    def wait_for_all_domains(self, timeout=30):
        wait(self.verify_all_domain_instances, timeout=timeout)

    def verify_all_domain_instances(self):
        libcloud_nodes = self.get_valid_libcloud_nodes()
        libcloud_nodes_by_id = dict((n.id, n) for n in libcloud_nodes)

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
                    found_nodes.add(iaas_id)
                    valid_count += 1

            if valid_count != preserve_n:
                all_complete = False

        # ensure the set of seen iaas IDs matches the total set
        nodes_match = found_nodes == set(libcloud_nodes_by_id.keys())

        return all_complete and nodes_match

    def _kill_cb(self, place_at, place_want_list, kill_func):
        if not kill_func:
            return place_at
        if place_want_list is None:
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

        self.assertFalse(self.get_valid_libcloud_nodes())

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

        def_name = str(uuid.uuid4())
        self.epum_client.add_domain_definition(def_name, example_definition)
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        domain = _example_domain(1)
        domains_started = []
        for i in range(n):
            name = "dom%d" % (i)
            self.epum_client.add_domain(name, def_name, domain)
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

    def _add_many_domains_terminate_all(self, kill_func=None,
            places_to_kill=None, n=1, nodes_per_domain=3):
        test_pc = 1
        def_name = str(uuid.uuid4())
        self.epum_client.add_domain_definition(def_name, example_definition)

        domain = _example_domain(nodes_per_domain)
        domains_started = []
        for i in range(n):
            name = "dom%d" % (i)
            self.epum_client.add_domain(name, def_name, domain)
            domains_started.append(name)

        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        domains = self.epum_client.list_domains()
        domains_started.sort()
        domains.sort()
        self.assertEqual(domains, domains_started)

        self.wait_for_libcloud_nodes(n * nodes_per_domain)
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        self.wait_for_all_domains()

        state = self.provisioner_client.terminate_all()
        self.assertFalse(state)  # cannot all be terminated this quickly

        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        # wait a little while until hopefully termination is underway
        time.sleep(2)
        test_pc = self._kill_cb(test_pc, places_to_kill, kill_func)

        # this will return true when everything is terminated
        wait(self.provisioner_client.terminate_all, timeout=20)

        self.assertFalse(self.get_valid_libcloud_nodes())

        for name in domains_started:
            self.epum_client.remove_domain(name)
        self.wait_for_domain_set([])

    def _get_contender(self, path, ndx=0):
        """returns name, hostname, pid tuple"""

        assert ndx < self.prov_replica_count
        contenders = []
        election = self.kazoo.Election(path)

        def getem():
            contenders[:] = election.contenders()
            return len(contenders) == self.prov_replica_count
        # retry getting contenders. may take them a while to emerge
        wait(getem, timeout=20)
        name, hostname, pid = contenders[ndx].split(':')
        return name, hostname, int(pid)

    def _kill_leader_supd(self):
        name = self._get_contender(self.PROV_ELECTION_PATH)[0]
        self.epuharness.stop(services=[name])

    def _kill_leader_pid(self):
        pid = self._get_contender(self.PROV_ELECTION_PATH)[2]
        os.kill(pid, signal.SIGTERM)

    def _kill_not_leader_supd(self):
        name = self._get_contender(self.PROV_ELECTION_PATH, 1)[0]
        self.epuharness.stop(services=[name])

    def _kill_not_leader_pid(self):
        pid = self._get_contender(self.PROV_ELECTION_PATH, 1)[2]
        os.kill(pid, signal.SIGTERM)

    def _kill_proxy_expire_session(self):
        self.proxy.stop()

        # wait long enough for the ZK session to expire, then recover
        time.sleep(self.zk_timeout * 1.5)
        self.proxy.start()

    def _kill_proxy_recover_session(self):
        self.proxy.stop()

        # wait a little while and restart. connection will be interrupted
        # but should reconnect with no loss of ephemeral nodes
        time.sleep(2)
        self.proxy.start()


# these two classes get the actual tests. They are added dynamically at
# runtime.

class TestProvisionerZKWithKills(BaseProvKillsFixture):
    use_zk_proxy = False


class TestProvisionerZKProxyWithKills(BaseProvKillsFixture):
    use_zk_proxy = True


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


def create_terminate_all(kill_func_name, places_to_kill, n):
    def doit(self):
        kill_func = getattr(self, kill_func_name)
        self._add_many_domains_terminate_all(
            kill_func=kill_func, places_to_kill=places_to_kill, n=n)
    return doit


kill_func_classes = [
    ("_kill_leader_supd", TestProvisionerZKWithKills),
    ("_kill_leader_pid", TestProvisionerZKWithKills),
    ("_kill_not_leader_supd", TestProvisionerZKWithKills),
    ("_kill_not_leader_pid", TestProvisionerZKWithKills),
    ("_kill_proxy_expire_session", TestProvisionerZKProxyWithKills),
    ("_kill_proxy_recover_session", TestProvisionerZKProxyWithKills)
]

for n in [1, 16]:
    for kill_name, cls in kill_func_classes:
        method = None
        for i in range(0, 8):
            method = create_em(kill_name, [i], n)
            method.__name__ = 'test_prov_add_remove_domain_kill_point_%d_with_%s_n-%d' % (i, kill_name, n)
            setattr(cls, method.__name__, method)

for kill_name, cls in kill_func_classes:
    method = None
    for i in range(0, 7):
        method = create_reconfigure(kill_name, [i])
        method.__name__ = 'test_prov_reconfigure_kill_point_%d_with_%s' % (i, kill_name)
        setattr(cls, method.__name__, method)

for n in [8]:
    for kill_name, cls in kill_func_classes:
        method = None
        for i in range(1, 5):
            method = create_terminate_all(kill_name, [i], n)
            method.__name__ = 'test_prov_terminate_all_kill_point_%d_with_%s_n-%d' % (i, kill_name, n)
            setattr(cls, method.__name__, method)

del method
del cls
