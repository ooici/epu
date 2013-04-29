
#!/usr/bin/env python

"""
@file epu/provisioner/test/util.py
@author David LaBissoniere
@brief Provisioner testing fixtures and utils
"""
import uuid
import threading
import time

from libcloud.compute.base import NodeDriver, Node, NodeSize
from libcloud.compute.types import NodeState

from epu.provisioner.ctx import ContextResource
from epu.test import Mock
from epu.states import InstanceState

import dashi.bootstrap

log = dashi.bootstrap.get_logger(__name__)


class FakeDTRS(object):
    def __init__(self):
        self.result = None
        self.error = None
        self.sites = {}
        self.credentials = {}

    def lookup(self, caller, dt, node=None, vars=None):
        if self.error is not None:
            raise self.error

        if self.result is not None:
            return self.result

        raise Exception("bad fixture: nothing to return")

    def describe_site(self, site_name):
        if site_name in self.sites:
            return self.sites[site_name]

        raise Exception("bad fixture: nothing to return for site %s" % site_name)

    def describe_credentials(self, caller, site_name):
        if (caller, site_name) in self.credentials:
            return self.credentials[(caller, site_name)]

        raise Exception("bad fixture: nothing to return")


class FakeProvisionerNotifier(object):
    """Test fixture that captures node status updates
    """
    def __init__(self):
        self.nodes = {}
        self.nodes_rec_count = {}

        self.condition = threading.Condition()

    def send_record(self, record, operation='node_status'):
        """Send a single node record to all subscribers.
        """
        with self.condition:
            self._send_record_internal(record)

            self.condition.notify_all()

    def _send_record_internal(self, record):

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

    def send_records(self, records, operation='node_status'):
        with self.condition:
            for record in records:
                self._send_record_internal(record)
                self.condition.notify_all()

    def assure_state(self, state, nodes=None):
        """Checks that all nodes have the same state.
        """
        if not self.nodes:
            return False

        if nodes:
            for node in nodes:
                if node not in self.nodes:
                    log.debug("node %s unknown so far", node)
                    return False
                node_state = self.nodes[node]['state']
                if not node_state == state:
                    log.debug("node %s in state %s", node, node_state)
                    return False
            return True

        for node in self.nodes.itervalues():
            if node['state'] != state:
                return False
        return True

    def assure_record_count(self, count, nodes=None):
        if not self.nodes:
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

    def wait_for_state(self, state, nodes=None,
            before=None, before_kwargs=None, timeout=30):

        if before_kwargs is None:
            before_kwargs = {}

        start_time = time.time()

        win = None
        while not win:
            if before:
                before(**before_kwargs)

            with self.condition:
                win = self.assure_state(state, nodes)
                if not win:

                    if timeout:
                        sleep = timeout - (time.time() - start_time)
                    else:
                        sleep = None

                    self.condition.wait(sleep)
                    log.debug("woke up")

            if timeout and time.time() - start_time >= timeout:
                raise Exception("timeout before state reached")

        log.debug('All nodes in %s state', state)
        return win


class FakeNodeDriver(NodeDriver):

    type = 42  # libcloud uses a driver type number in id generation.

    def __init__(self, **kwargs):
        pass

    def initialize(self):
        FakeNodeDriver.created = []
        FakeNodeDriver.destroyed = []
        FakeNodeDriver.running = {}
        FakeNodeDriver.create_node_error = None
        FakeNodeDriver.sizes = [NodeSize("m1.small", "small", 256, 200, 1000, 1.0, None)]

    def create_node(self, **kwargs):
        if self.create_node_error:
            raise self.create_node_error
        count = int(kwargs['ex_mincount']) if 'ex_mincount' in kwargs else 1
        nodes = [Node(new_id(), None, NodeState.PENDING, new_id(), new_id(),
                    self, size=kwargs.get('size')) for i in range(count)]
        self.created.extend(nodes)
        for node in nodes:
            self.running[node.id] = node
        return nodes

    def set_node_running(self, iaas_id):
        log.debug("IaaS node %s is now running", iaas_id)
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
        self.uri_query_error = {}  # specific context errors
        self.queried_uris = []
        self.query_error = None
        self.create_error = None
        self.last_create = None

    def create(self):
        if self.create_error:
            raise self.create_error

        dct = {'broker_uri': "http://www.sandwich.com",
            'context_id': new_id(),
            'secret': new_id(),
            'uri': "http://www.sandwich.com/" + new_id()}
        result = ContextResource(**dct)
        self.last_create = result
        return result

    def query(self, uri):
        self.queried_uris.append(uri)
        if self.query_error:
            raise self.query_error
        if uri in self.uri_query_error:
            raise self.uri_query_error[uri]
        response = Mock(nodes=self.nodes, expected_count=self.expected_count,
        complete=self.complete, error=self.error)
        return response


def new_id():
    return str(uuid.uuid4())


def make_launch(launch_id, state, node_records, caller=None, **kwargs):
    node_ids = [n['node_id'] for n in node_records]
    r = {'launch_id': launch_id,
            'state': state,
            'node_ids': node_ids,
            'creator': caller,
            'context': {'uri': 'http://fakey.com/' + new_id()}}
    r.update(kwargs)
    return r


def make_node(launch_id, state, node_id=None, caller=None, **kwargs):
    r = {'launch_id': launch_id, 'node_id': node_id or new_id(),
            'state': state, 'public_ip': new_id(),
            'running_timestamp': time.time(), 'creator': caller}
    r.update(kwargs)
    return r


def make_launch_and_nodes(launch_id, node_count, state, site='fake', caller=None):
    node_records = []
    node_kwargs = {'site': site, 'creator': caller}
    for i in range(node_count):
        if state >= InstanceState.PENDING:
            node_kwargs['iaas_id'] = new_id()
        rec = make_node(launch_id, state, caller=caller, **node_kwargs)
        node_records.append(rec)
    launch_record = make_launch(launch_id, state, node_records, caller=caller)
    return launch_record, node_records
