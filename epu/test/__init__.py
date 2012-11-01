
import os
import unittest
import uuid

from kazoo.client import KazooClient

FIXTURES_ROOT = 'fixtures'

class FileFixtures(object):
    def __init__(self, subdir=None):
        test_root = os.path.abspath(os.path.dirname(__file__))
        self.root = os.path.join(test_root, FIXTURES_ROOT)
        if subdir:
            self.root = os.path.join(self.root, subdir)
        assert os.path.exists(self.root), "No test fixtures?: " + self.root

    def path(self, name):
        return os.path.join(self.root, name)


class Mock(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return "Mock(" + ",".join("%s=%s" %(k,v) for k,v in self.__dict__.iteritems()) + ")"


class ZooKeeperTestMixin(object):

    zk_hosts = None
    zk_base_path = None

    def setup_zookeeper(self, base_path_prefix="/int_tests"):

        zk_hosts = os.environ.get("ZK_HOSTS")
        if not zk_hosts:
            raise unittest.SkipTest("export ZK_HOSTS env to run ZooKeeper integration tests")

        self.zk_hosts = zk_hosts
        self.zk_base_path = base_path_prefix + uuid.uuid4().hex

        if os.environ.get('EPU_USE_GEVENT'):
            from kazoo.handlers.gevent import SequentialGeventHandler
            handler = SequentialGeventHandler()
            self.use_gevent = True
        else:
            handler = None
            self.use_gevent = False

        self.kazoo = KazooClient(self.zk_hosts + self.zk_base_path, handler=handler)
        self.kazoo.start()

    def teardown_zookeeper(self):
        if self.kazoo:
            self.kazoo.delete("/", recursive=True)
            self.kazoo.stop()

    cleanup_zookeeper = teardown_zookeeper
