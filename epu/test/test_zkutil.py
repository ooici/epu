# Copyright 2013 University of Chicago

import unittest
from epu import zkutil


class ZKUtilTests(unittest.TestCase):
    def test_is_zookeeper_enabled(self):

        config = {}
        assert not zkutil.is_zookeeper_enabled(config)

        config = {"server": {}}
        assert not zkutil.is_zookeeper_enabled(config)

        config = {"server": {"zookeeper": {}}}
        assert not zkutil.is_zookeeper_enabled(config)

        config = {"server": {"zookeeper": {"hosts": "localhost:2181",
            "path": "/hats"}}}
        assert zkutil.is_zookeeper_enabled(config)

        config = {"server": {"zookeeper": {"enabled": True,
            "hosts": "localhost:2181", "path": "/hats"}}}
        assert zkutil.is_zookeeper_enabled(config)

        config = {"server": {"zookeeper": {"enabled": "True",
            "hosts": "localhost:2181", "path": "/hats"}}}
        assert zkutil.is_zookeeper_enabled(config)

        config = {"server": {"zookeeper": {"enabled": False,
            "hosts": "localhost:2181", "path": "/hats"}}}
        assert not zkutil.is_zookeeper_enabled(config)

        config = {"server": {"zookeeper": {"enabled": "False",
            "hosts": "localhost:2181", "path": "/hats"}}}
        assert not zkutil.is_zookeeper_enabled(config)

        config = {"server": {"zookeeper": {"enabled": "false",
            "hosts": "localhost:2181", "path": "/hats"}}}
        assert not zkutil.is_zookeeper_enabled(config)
