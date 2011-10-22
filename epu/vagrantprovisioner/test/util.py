
#!/usr/bin/env python

"""
@file epu/provisioner/test/util.py
@author David LaBissoniere
@brief Provisioner testing fixtures and utils
"""
import uuid
from libcloud.compute.base import NodeDriver, Node, NodeSize
from libcloud.compute.types import NodeState
from nimboss.ctx import ContextResource

from twisted.internet import defer

import ion.util.procutils as pu

import ion.util.ionlog
from epu.test import Mock
import epu.states as states
from epu.vagrantprovisioner.vagrant import FakeVagrant


log = ion.util.ionlog.getLogger(__name__)


class FakeProvisionerNotifier(object):
    """Test fixture that captures node status updates
    """
    def __init__(self):
        self.nodes = {}
        self.nodes_rec_count = {}
        self.nodes_subscribers = {}

    def send_record(self, record, subscribers, operation='node_status'):
        """Send a single node record to all subscribers.
        """
        record = record.copy()
        node_id = record['node_id']
        state = record['state']
        if node_id in self.nodes:
            old_record = self.nodes[node_id]
            old_state = old_record['state']
            if old_state == state:
                log.debug('Got dupe state for node %s: %s', node_id, state)
            elif old_state < state:
                self.nodes[node_id] = record
                self.nodes_rec_count[node_id] += 1
                log.debug('Got updated state record for node %s: %s -> %s',
                        node_id, old_state, state)
            else:
                log.debug('Got out-of-order record for node %s. %s -> %s', 
                        node_id, old_state, state)
        else:
            self.nodes[node_id] = record
            self.nodes_rec_count[node_id] = 1
            log.debug('Recorded new state record for node %s: %s', 
                    node_id, state)

        if subscribers:
            if self.nodes_subscribers.has_key(node_id):
                self.nodes_subscribers[node_id].extend(subscribers)
            else:
                self.nodes_subscribers[node_id] = list(subscribers)
        return defer.succeed(None)

    @defer.inlineCallbacks
    def send_records(self, records, subscribers, operation='node_status'):
        for record in records:
            yield self.send_record(record, subscribers, operation)

    def assure_state(self, state, nodes=None):
        """Checks that all nodes have the same state.
        """
        if len(self.nodes) == 0:
            return False

        if nodes:
            for node in nodes:
                if not (node in self.nodes and 
                        self.nodes[node]['state'] == state):
                    return False
            return True

        for node in self.nodes.itervalues():
            if node['state'] != state:
                return False
        return True

    def assure_record_count(self, count, nodes=None):
        if len(self.nodes) == 0:
            return count == 0

        if nodes:
            for node in nodes:
                if self.nodes_rec_count.get(node, 0) != count:
                    return False
            return True

        for node_rec_count in self.nodes_rec_count.itervalues():
            if node_rec_count != count:
                return False
        return True

    def assure_subscribers(self, node_id, subscribers):
        if not self.nodes_subscribers.has_key(node_id):
            return False
        for subscriber in subscribers:
            if not subscriber in self.nodes_subscribers[node_id]:
                return False
        return True

    @defer.inlineCallbacks
    def wait_for_state(self, state, nodes=None, poll=0.1,
            before=None, before_kwargs={}):

        win = None
        while not win:
            if before:
                yield before(**before_kwargs)
            elif poll:
                yield pu.asleep(poll)
            win = self.assure_state(state, nodes)

        log.debug('All nodes in %s state', state)
        defer.returnValue(win)


class FakeNodeDriver(NodeDriver):
    
    type = 42 # libcloud uses a driver type number in id generation.
    def __init__(self):
        self.created = []
        self.destroyed = []
        self.running = {}
        self.create_node_error = None
        self.sizes = [NodeSize("m1.small", "small", 256, 200, 1000, 1.0, self)]

    def create_node(self, **kwargs):
        if self.create_node_error:
            raise self.create_node_error
        count = int(kwargs['ex_mincount']) if 'ex_mincount' in kwargs else 1
        nodes  = [Node(new_id(), None, NodeState.PENDING, new_id(), new_id(),
                    self) for i in range(count)]
        self.created.extend(nodes)
        for node in nodes:
            self.running[node.id] = node
        return nodes

    def set_node_running(self, iaas_id):
        self.running[iaas_id].state = NodeState.RUNNING

    def set_nodes_running(self, iaas_ids):
        for iaas_id in iaas_ids:
            self.set_node_running(iaas_id)

    def destroy_node(self, node):
        self.destroyed.append(node)
        self.running.pop(node.id, None)

    def list_nodes(self):
        return self.running.values()

    def list_sizes(self):
        return self.sizes[:]


class FakeContextClient(object):
    def __init__(self):
        self.nodes = []
        self.expected_count = 0
        self.complete = False
        self.error = False
        self.uri_query_error = {} # specific context errors
        self.queried_uris = []
        self.query_error = None
        self.create_error = None
        self.last_create = None

    def create(self):
        if self.create_error:
            return defer.fail(self.create_error)

        dct = {'broker_uri' : "http://www.sandwich.com",
            'context_id' : new_id(),
            'secret' : new_id(),
            'uri' : "http://www.sandwich.com/"+new_id()}
        result = ContextResource(**dct)
        self.last_create = result
        return defer.succeed(result)

    def query(self, uri):
        self.queried_uris.append(uri)
        if self.query_error:
            return defer.fail(self.query_error)
        if uri in self.uri_query_error:
            return defer.fail(self.uri_query_error[uri])
        response = Mock(nodes=self.nodes, expected_count=self.expected_count,
        complete=self.complete, error=self.error)
        return defer.succeed(response)


def new_id():
    return str(uuid.uuid4())

def new_fake_vagrant_vm():
    return FakeVagrant()

def make_launch(launch_id, state, node_records, **kwargs):
    node_ids = [n['node_id'] for n in node_records]
    r = {'launch_id' : launch_id,
            'state' : state, 'subscribers' : 'fake-subscribers',
            'node_ids' : node_ids,
            'chef_json' : '/path/to/json.json',
            'cookbook_dir' : '/path/to/cookbooks'}
    r.update(kwargs)
    return r

def make_node(launch_id, state, node_id=None, **kwargs):
    r = {'launch_id' : launch_id, 'node_id' : node_id or new_id(),
            'state' : state, 'public_ip' : new_id(), 'vagrant_box' : 'base',
            'vagrant_memory' : 128}
    r.update(kwargs)
    return r

def make_launch_and_nodes(launch_id, node_count, state, vagrant_box='base', vagrant_memory=128):
    node_records = []
    node_kwargs = {'vagrant_box' : vagrant_box, 'vagrant_memory' : vagrant_memory }
    for i in range(node_count):
        if state >= states.PENDING:
            node_kwargs['vagrant_directory'] = new_fake_vagrant_vm().directory
        rec = make_node(launch_id, state, **node_kwargs)
        node_records.append(rec)
    launch_record = make_launch(launch_id, state, node_records)
    return launch_record, node_records
