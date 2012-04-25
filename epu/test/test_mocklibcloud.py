
import os
import tempfile

from libcloud.compute.types import NodeState 

from epu.mocklibcloud import MockEC2NodeDriver

class TestMockLibCloud(object):

    def setup(self):
        _, self.sqlite_db_file = tempfile.mkstemp()
        self.libcloud = MockEC2NodeDriver(sqlite_db=self.sqlite_db_file)

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
        assert len(nodes) == 0


        #Ensure VMs come up broken
        self.libcloud._fail_to_start = True
        node = self.libcloud.create_node(name=name)
        nodes = self.libcloud.list_nodes()
        assert len(nodes) == 1

        got_node = nodes[0]
        assert got_node.name == name
        assert got_node.state == NodeState.TERMINATED


    def teardown(self):
        os.remove(self.sqlite_db_file)
