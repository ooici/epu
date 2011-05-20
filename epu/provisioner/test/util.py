
#!/usr/bin/env python

"""
@file epu/provisioner/test/util.py
@author David LaBissoniere
@brief Provisioner testing fixtures and utils
"""
import uuid
from libcloud.base import NodeDriver, Node
from libcloud.types import NodeState

from twisted.internet import defer

import ion.util.procutils as pu

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


class FakeProvisionerNotifier(object):
    """Test fixture that captures node status updates
    """
    def __init__(self):
        self.nodes = {}
        self.nodes_rec_count = {}

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

    def set_node_running(self, node_id):
        self.running[node_id].state = NodeState.RUNNING

    def destroy_node(self, node):
        self.destroyed.append(node)
        self.running.pop(node.id, None)

    def list_nodes(self):
        return self.running.values()

def new_id():
    return str(uuid.uuid4())