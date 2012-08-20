from uuid import uuid4
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from libcloud.compute.types import NodeState
from libcloud.compute.providers import Provider
from libcloud.compute.base import Node, NodeDriver, NodeLocation, NodeSize

SQLBackedObject = declarative_base()

class MockEC2NodeState(SQLBackedObject):
    __tablename__ = 'state'

    id = Column(Integer, primary_key=True)
    max_vms = Column(Integer)
    create_error_count = Column(Integer)


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

    def _get_state(self):
        this_state = self.session.query(MockEC2NodeState).first()
        if not this_state:
            this_state = MockEC2NodeState(max_vms=-1, create_error_count=0)
            self.session.add(this_state)
            self.session.commit()
        return this_state

    def get_max_vms(self):
        state = self._get_state()
        return state.max_vms

    def get_create_error_count(self):
        state = self._get_state()
        return state.create_error_count

    def list_nodes(self):
        mock_nodes = self.session.query(MockNode)
        nodes = [mock_node.to_node() for mock_node in mock_nodes]
        return nodes

    def create_node(self, **kwargs):

        max_vms = self.get_max_vms()
        if max_vms >= 0 and len(self.list_nodes()) >= max_vms:
            ec = self.get_create_error_count()
            self._set_error_count(ec + 1)
            raise Exception("The resource is full")
        
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

    def _set_max_VMS(self, n):
        state = self._get_state()
        state.max_vms = n
        self.session.commit()

    def _set_error_count(self, ec):
        state = self._get_state()
        state.create_error_count = ec
        self.session.commit()


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
