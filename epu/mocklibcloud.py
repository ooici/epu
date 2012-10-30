import time

from uuid import uuid4
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from socket import timeout

from libcloud.compute.types import NodeState
from libcloud.compute.providers import Provider
from libcloud.compute.base import Node, NodeDriver, NodeLocation, NodeSize

SQLBackedObject = declarative_base()

DEFAULT_TIMEOUT = 60

class MockEC2NodeState(SQLBackedObject):
    __tablename__ = 'state'

    id = Column(Integer, primary_key=True)
    max_vms = Column(Integer)
    create_error_count = Column(Integer)

class MockConnection(object):
    timeout = DEFAULT_TIMEOUT

class MockEC2NodeDriver(NodeDriver):
 
    type = Provider.EC2
    _sizes = []
    _nodes = []
    _fail_to_start = False
    connection = MockConnection()

    def __init__(self, sqlite_db=None, **kwargs):

        if not sqlite_db:
            sqlite_db = ":memory:"

        self.sqlite_db_uri = "sqlite:///%s" % sqlite_db
        self.engine = create_engine(self.sqlite_db_uri)
        SQLBackedObject.metadata.create_all(self.engine)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        self._operation_time = 0.5 # How long each operation should take

        self._add_size("t1.micro", "t1.micro", 512, 512, 512, 100)

    def shutdown(self):
        """Shut down this driver
        """
        self.session.close()

    def wait(self):
        if self.connection.timeout < self._operation_time:
            raise timeout("Operation took longer than %ss" % self.connection.timeout)
        time.sleep(self._operation_time)

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
        self.wait()
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
        userdata = kwargs.get('ex_userdata')

        if self._fail_to_start:
            state = NodeState.TERMINATED
        else:
            state = NodeState.RUNNING

        mock_node = MockNode(node_id=node_id, name=name, state=state, public_ips=public_ips, private_ips=private_ips, userdata=userdata)
        self.session.add(mock_node)
        self.session.commit()

        self.wait()

        return mock_node.to_node()

    def set_node_state(self, node, state):
        mock_node = self.get_mock_node(node)
        mock_node.state = state
        self.session.commit()

    def destroy_node(self, node):

        mock_node = self.get_mock_node(node)
        self.session.delete(mock_node)
        self.session.commit()

        self.wait()
        return

    def get_mock_node(self, node):
        mock_node = try_n_times(self.session.query, MockNode).filter_by(node_id=node.id).one()
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
    userdata = Column(String)
    create_time = Column(Integer)
    list_time = Column(Integer)

    def to_node(self):
        extra = None
        if self.userdata:
            extra = {'ex_userdata': self.userdata}

        n = Node(id=self.node_id, name=self.name, state=int(self.state), public_ips=self.public_ips, private_ips=self.private_ips, extra=extra, driver=MockEC2NodeDriver)
        return n

def try_n_times(fn, *args, **kwargs):
    exp = None
    for i in range(0, 100):
        try:
            return fn(*args, **kwargs)
        except SQLAlchemyError, e:
            exp = e
            time.sleep(0.5)
    else:
        raise exp
