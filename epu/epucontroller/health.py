import time
from twisted.internet import defer

import epu.states as InstanceStates

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class InstanceHealthState(object):
    UNKNOWN = "UNKNOWN"
    OK = "OK"
    MONITOR_ERROR = "MONITOR_ERROR"
    PROCESS_ERROR = "PROCESS_ERROR"
    MISSING = "MISSING"
    ZOMBIE = "ZOMBIE"

class HealthMonitor(object):
    def __init__(self, state, boot_seconds=300, missing_seconds=30,
                 zombie_seconds=120):
        self.state = state
        self.boot_timeout = boot_seconds
        self.missing_timeout = missing_seconds
        self.zombie_timeout = zombie_seconds

        self.last_heard = {}
        self.error_time = {}

    def last_heartbeat_time(self, node_id):
        """Return time (seconds since epoch) of last heartbeat for a node, or -1"""
        last = self.last_heard.get(node_id)
        if last is not None:
            return last
        return -1

    @defer.inlineCallbacks
    def new_heartbeat(self, content, timestamp=None):
        """Intake a new heartbeat from a node
        """
        now = time.time() if timestamp is None else timestamp

        try:
            instance_id = content['node_id']
            state = content['state']
        except KeyError, e:
            log.warn("Got invalid heartbeat message: %s", content)
            defer.returnValue(None)

        instance = self.state.instances.get(instance_id)
        if not instance:
            log.warn("Got heartbeat message for unknown instance '%s': %s",
                     node_id, content)
            defer.returnValue(None)

        self.last_heard[instance_id] = now

        if state == InstanceHealthState.OK:
            if instance.health not in (InstanceHealthState.OK,
                                       InstanceHealthState.ZOMBIE):
                yield self.state.new_instance_health(instance_id, state)

        else:
            error_time = content.get('error_timestamp')
            if (state != instance.health or
                error_time != self.error_time.get(instance_id)):

                self.error_time[instance_id] = error_time

                errors = []
                err = content.get('error')
                if err:
                    errors.append(err)
                procs = content.get('failed_processes')
                if procs:
                    errors.extend(p.copy() for p in procs)

                yield self.state.new_instance_health(instance_id, state, errors)
                

    @defer.inlineCallbacks
    def update(self, timestamp=None):
        now = time.time() if timestamp is None else timestamp

        for node in self.state.instances.itervalues():
            yield self._update_one_node(node, now)

    def _update_one_node(self, node, now):
        last_heard = self.last_heard.get(node.instance_id)
        iaas_state_age = now - node.state_time

        new_state = None
        if node.state >= InstanceStates.TERMINATED:

            # nodes get a window of time to stop sending heartbeats after they
            # are marked TERMINATED. After this point they are considered
            # ZOMBIE nodes.

            if last_heard is None:
                pass
            elif (iaas_state_age > self.zombie_timeout and
                  now - last_heard > self.zombie_timeout):
                self.last_heard.pop(node.instance_id, None)
                self.error_time.pop(node.instance_id, None)

            elif last_heard > node.state_time + self.zombie_timeout:
                new_state = InstanceHealthState.ZOMBIE

        elif node.state == InstanceStates.RUNNING:
            # if the instance is marked running for a while but we haven't
            # gotten a heartbeat, it is MISSING
            if last_heard is None:
                if iaas_state_age > self.boot_timeout:
                    new_state = InstanceHealthState.MISSING

            # likewise if we heard from it in the past but haven't in a while
            elif now - last_heard > self.missing_timeout:
                new_state = InstanceHealthState.MISSING

        if new_state:
            return self.state.new_instance_health(node.instance_id, new_state)
        return defer.succeed(None)
