from uuid import uuid4
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from libcloud.compute.types import NodeState
from libcloud.compute.providers import Provider
from libcloud.compute.base import Node, NodeDriver, NodeLocation, NodeSize

SQLBackedObject = declarative_base()


class MockEC2NodeDriver(NodeDriver):
 
    type = Provider.EC2
    _sizes = []
    _nodes = []
    _fail_to_start = False

    def __init__(self, sqlite_db=None, **kwargs):

        if not sqlite_db:
            sqlite_db = ":memory:"

        self.sqlite_db_uri = "sqlite:///%s" % sqlite_db
        self.engine = create_engine(self.sqlite_db_uri)
        SQLBackedObject.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        self._add_size("t1.micro", "t1.micro", 512, 512, 512, 100)

    def list_sizes(self):

        return self._sizes

    def _add_size(self, size_id, name, ram, disk, bandwidth, price):
        self._sizes.append(NodeSize(size_id, name, ram, disk, bandwidth, price, MockEC2NodeDriver))

    def _add_node(self, new_node):
        self._nodes.append(new_node)

    def list_nodes(self):
        mock_nodes = self.session.query(MockNode)
        nodes = [mock_node.to_node() for mock_node in mock_nodes]
        return nodes

    def create_node(self, **kwargs):
        
        node_id = "%s" % uuid4()
        name = kwargs.get('name')
        public_ips = "0.0.0.0"
        private_ips = "0.0.0.0"
        driver = MockEC2NodeDriver

        if self._fail_to_start:
            state = NodeState.TERMINATED
        else:
            state = NodeState.RUNNING

        mock_node = MockNode(node_id=node_id, name=name, state=state, public_ips=public_ips, private_ips=private_ips)
        self.session.add(mock_node)
        self.session.commit()

        return mock_node.to_node()

    def set_node_state(self, node, state):
        mock_node = self.get_mock_node(node)
        mock_node.state = state
        self.session.commit()

    def destroy_node(self, node):
        mock_node = self.get_mock_node(node)
        self.session.delete(mock_node)
        self.session.commit()
        return

    def get_mock_node(self, node):
        mock_node = self.session.query(MockNode).filter_by(node_id=node.id).one()
        return mock_node

class MockNode(SQLBackedObject):

    __tablename__ = 'nodes'

    id = Column(Integer, primary_key=True)
    node_id = Column(String)
    name = Column(String)
    state = Column(Integer)
    public_ips = Column(String)
    private_ips = Column(String)

    def to_node(self):
        n = Node(id=self.node_id, name=self.name, state=int(self.state), public_ips=self.public_ips, private_ips=self.private_ips, driver=MockEC2NodeDriver)
        return n
