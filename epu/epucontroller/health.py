import time

import epu.states as InstanceStates

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class NodeHealthState(object):
    UNKNOWN = "UNKNOWN"
    OK = "OK"
    MONITOR_ERROR = "MONITOR_ERROR"
    PROCESS_ERROR = "PROCESS_ERROR"
    MISSING = "MISSING"
    ZOMBIE = "ZOMBIE"

    
class NodeHealth(object):
    def __init__(self, node_id):
        self.node_id = node_id

        self.iaas_state = None
        self.iaas_state_timestamp = None

        self.state = NodeHealthState.UNKNOWN
        self.error = None
        self.process_errors = None
        self.last_heartbeat = None

        
class HealthMonitor(object):
    def __init__(self, boot_seconds=300, missing_seconds=30, zombie_seconds=120):
        self.boot_timeout = boot_seconds
        self.missing_timeout = missing_seconds
        self.zombie_timeout = zombie_seconds

        # node_id -> NodeHealth object
        self.nodes = {}

    def __getitem__(self, item):
        return self.nodes[item]

    def node_state(self, node_id, state, timestamp=None):
        """Update the IaaS state for a node
        """
        if timestamp is None:
            now = time.time()
        else:
            now = timestamp

        node = self.nodes.get(node_id)
        if not node:
            node = NodeHealth(node_id)
            self.nodes[node_id] = node

        if node.iaas_state != state:
            node.iaas_state = state
            node.iaas_state_timestamp = now

    def new_heartbeat(self, content, timestamp=None):
        """Intake a new heartbeat from a node
        """
        if timestamp is None:
            now = time.time()
        else:
            now = timestamp

        node_id = content['node_id']
        node = self.nodes.get(node_id)
        if not node:
            log.warn("Got heartbeat message for unknown node '%s': %s",
                     node_id, content)
            return

        node.last_heartbeat = now
        state = content['state']
        if state == NodeHealthState.OK:
            if node.state != state and node.state != NodeHealthState.ZOMBIE:
                node.state = state
                node.error = None
                node.process_errors = None
            return

        procs = content.get('failed_processes')
        if procs:
            if node.process_errors:
                _merge_process_errors(node, procs)
            else:
                node.process_errors = [p.copy() for p in procs]
        else:
            node.process_errors = None

        node.state = state
        node.error = content.get('error')

    def update(self, timestamp=None):
        if timestamp is None:
            now = time.time()
        else:
            now = timestamp

        # walk a copy of values list so we can make changes
        for node in self.nodes.values():
            self._update_one_node(node, now)

    def _update_one_node(self, node, now):
        last_heard = node.last_heartbeat
        iaas_state_age = now - node.iaas_state_timestamp
        if node.iaas_state >= InstanceStates.TERMINATED:

            # terminated nodes get pruned from list once they are past
            # the zombie threshold without any contact
            if (iaas_state_age > self.zombie_timeout and
                (last_heard is None or now - last_heard >
                                       self.zombie_timeout)):
                #prune from list
                self.nodes.pop(node.node_id)

            elif last_heard is None:
                pass

            elif last_heard > node.iaas_state_timestamp + self.zombie_timeout:
                node.state = NodeHealthState.ZOMBIE

        elif node.iaas_state == InstanceStates.RUNNING:

            if last_heard is None:
                if iaas_state_age > self.boot_timeout:
                    node.state = NodeHealthState.MISSING

            elif now - last_heard > self.missing_timeout:
                node.state = NodeHealthState.MISSING


_PROCESS_KEYS = ('name', 'state', 'exitcode', 'stop_timestamp')
def _merge_process_errors(node_health, process_errors):
    """Merges incoming process errors into an existing set
    """

    #alright maybe this error caching thing is needless complication..
    merged = []
    for process in process_errors:
        if 'stderr' in process:
            merged.append(process)
        else:
            # if there is no stderr, this may be an existing failed process
            found = False
            for existing in node_health.process_errors:
                if all(existing[k] == process[k] for k in _PROCESS_KEYS):
                    merged.append(existing)
                    found = True
            if not found:
                merged.append(process)
    node_health.process_errors = merged