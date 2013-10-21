# Copyright 2013 University of Chicago


import os
import tempfile
import multiprocessing

from libcloud.compute.types import NodeState
from nose.plugins.skip import SkipTest


class TestMockLibCloud(object):

    def setup(self):
        try:
            import sqlalchemy  # noqa
        except ImportError:
            raise SkipTest("SQLAlchemy not available.")

        from epu.mocklibcloud import MockEC2NodeDriver
        fh, self.sqlite_db_file = tempfile.mkstemp()
        os.close(fh)
        self.libcloud = MockEC2NodeDriver(sqlite_db=self.sqlite_db_file,
            operation_time=0.01)

    def teardown(self):
        try:
            self.libcloud.shutdown()
        finally:
            os.remove(self.sqlite_db_file)

    def test_start(self):

        name = "testnode"

        node = self.libcloud.create_node(name=name)

        assert node.name == name

        nodes = self.libcloud.list_nodes()
        assert len(nodes) == 1

        got_node = nodes[0]
        assert got_node.name == name
        assert got_node.state == NodeState.RUNNING

        self.libcloud.destroy_node(got_node)
        nodes = self.libcloud.list_nodes()
        assert len(nodes) == 1
        assert nodes[0].state == NodeState.TERMINATED

        # Ensure VMs come up broken
        self.libcloud._fail_to_start = True
        got_node = self.libcloud.create_node(name=name)
        assert got_node.name == name
        assert got_node.state == NodeState.TERMINATED

    def test_idempotency(self):

        name = "testnode"
        client_token = "testclienttoken"
        node = self.libcloud.create_node(
            name=name, ex_clienttoken=client_token)

        assert node.name == name

        nodes = self.libcloud.list_nodes()
        assert len(nodes) == 1

        # create another node with the same token. should return the same node.
        same_node = self.libcloud.create_node(
            name=name, ex_clienttoken=client_token)

        assert same_node.name == name
        assert same_node.id == node.id

        nodes = self.libcloud.list_nodes()
        assert len(nodes) == 1

        # a node with a different token should work
        another_client_token = "testclienttoken2"
        different_node = self.libcloud.create_node(
            name=name, ex_clienttoken=another_client_token)
        assert different_node.name == name
        assert different_node.id != node.id

        nodes = self.libcloud.list_nodes()
        assert len(nodes) == 2

        # kill original node. a create with the same client token should result
        # in a destroyed-state node (with same id)
        self.libcloud.destroy_node(node)

        nodes = self.libcloud.list_nodes()
        assert len(nodes) == 2

        same_node = self.libcloud.create_node(
            name=name, ex_clienttoken=client_token)
        assert node.id == same_node.id
        assert same_node.state == NodeState.TERMINATED


def _parallel_worker(sqlite_db, client_token, create_count):
    from epu.mocklibcloud import MockEC2NodeDriver
    libcloud = MockEC2NodeDriver(sqlite_db=sqlite_db)

    node_ids = []
    for _ in range(create_count):
        node = libcloud.create_node(
            name="somenode", ex_clienttoken=client_token)
        node_ids.append(node.id)
    return node_ids


class TestMockLibCloudParallel(object):
    def setUp(self):
        try:
            import sqlalchemy  # noqa
        except ImportError:
            raise SkipTest("SQLAlchemy not available.")

        if os.environ.get("JENKINS_URL") is not None:
            raise SkipTest("This test does not work on Jenkins")

        from epu.mocklibcloud import MockEC2NodeDriver
        fh, self.sqlite_db_file = tempfile.mkstemp()
        os.close(fh)
        self.libcloud = MockEC2NodeDriver(sqlite_db=self.sqlite_db_file)

        self.pool = None

    def tearDown(self):
        if self.pool:
            self.pool.terminate()
            self.pool.join()
        try:
            self.libcloud.shutdown()
        finally:
            os.remove(self.sqlite_db_file)

    def test_idempotency(self, process_count=10, create_count=10):

        # have several worker processes attempt to create nodes in parallel
        # with client tokens -- we should still end up with only one node
        # actually created.

        self.pool = multiprocessing.Pool(process_count)

        client_token = "someuniquetoken"
        results = []
        for _ in range(process_count):
            result = self.pool.apply_async(
                _parallel_worker,
                (self.sqlite_db_file, client_token, create_count))
            results.append(result)

        node_id_set = set()
        for result in results:
            node_ids = result.get()
            assert len(node_ids) == create_count
            node_id_set.update(node_ids)

        assert len(node_id_set) == 1 and list(node_id_set)[0]
