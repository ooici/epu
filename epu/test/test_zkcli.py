# Copyright 2013 University of Chicago

import os
import unittest
import tempfile

import yaml
import mock
from kazoo.exceptions import NoNodeException, NodeExistsException

from epu import zkcli

_ZK_CONFIG = {"server": {"zookeeper": {"hosts": "zk1,zk2", "path": "/base"}}}


def _write_temp_yaml(o):
    fd, path = tempfile.mkstemp()
    f = os.fdopen(fd, 'w')
    try:
        yaml.dump(o, f)
    except:
        os.remove(path)
        raise
    finally:
        f.close()
    return path


@mock.patch("epu.zkcli.KazooClient")
class ZKCliTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.zk_config = _write_temp_yaml(_ZK_CONFIG)

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'zk_config') and cls.zk_config:
            os.remove(cls.zk_config)

    def test_setup(self, kazoo_cls):
        kazoo_cls.return_value = kazoo = mock.Mock()

        zkcli.main(['--config', self.zk_config, "setup"])
        kazoo.ensure_path.assert_called_once_with("/base")
        kazoo.delete.assert_called_once_with("/base/provisioner/disabled")
        kazoo.create.assert_called_once_with("/base/pd/system_boot", "",
            makepath=True)

        kazoo.reset_mock()
        kazoo.delete.side_effect = NoNodeException()
        zkcli.main(['--config', self.zk_config, "setup"])
        kazoo.delete.assert_called_once_with("/base/provisioner/disabled")

        kazoo.reset_mock()
        kazoo.create.side_effect = NodeExistsException()
        zkcli.main(['--config', self.zk_config, "setup"])
        kazoo.create.assert_called_once_with("/base/pd/system_boot", "",
            makepath=True)

    def test_setup_clean(self, kazoo_cls):
        kazoo_cls.return_value = kazoo = mock.Mock()

        zkcli.main(['--config', self.zk_config, "setup", "--clean"])
        kazoo.ensure_path.assert_called_once_with("/base")
        kazoo.delete.assert_called_once_with("/base", recursive=True)
        kazoo.create.assert_called_once_with("/base/pd/system_boot", "",
            makepath=True)

    def test_setup_clean_epu(self, kazoo_cls):
        kazoo_cls.return_value = kazoo = mock.Mock()

        zkcli.main(['--config', self.zk_config, "setup", "--clean-epu"])
        kazoo.ensure_path.assert_called_once_with("/base")
        self.assertEqual(kazoo.delete.call_count, 2)
        kazoo.delete.assert_any_call("/base/provisioner", recursive=True)
        kazoo.delete.assert_any_call("/base/epum", recursive=True)
        kazoo.create.assert_called_once_with("/base/pd/system_boot", "",
            makepath=True)

    def test_destroy(self, kazoo_cls):
        kazoo_cls.return_value = kazoo = mock.Mock()

        zkcli.main(['--config', self.zk_config, "destroy"])
        kazoo.delete.assert_called_once_with("/base", recursive=True)
