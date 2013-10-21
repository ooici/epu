# Copyright 2013 University of Chicago

import logging
import time

from epu.states import InstanceState, InstanceHealthState

log = logging.getLogger(__name__)

TESTCONF_HEALTH_INIT_TIME = "unit_tests_init_time"

# See: doctor.py


class HealthMonitor(object):
    def __init__(self, domain, ouagent_client, boot_seconds=300, missing_seconds=120,
                 really_missing_seconds=15, zombie_seconds=120, init_time=None):
        self.domain = domain
        self.ouagent_client = ouagent_client
        self.boot_timeout = boot_seconds
        self.missing_timeout = missing_seconds
        self.really_missing_timeout = really_missing_seconds
        self.zombie_timeout = zombie_seconds

        # we track the initialization time of the health monitor. In a
        # recovery situation we may not immediately have access to last
        # heard times as they are not persisted. So we use the init_time
        # in place of the iaas_time as the basis for window comparisons.
        self.init_time = time.time() if init_time is None else init_time

    def monitor_age(self, timestamp=None):
        now = time.time() if timestamp is None else timestamp
        return now - self.init_time

    def update(self, timestamp=None):
        now = time.time() if timestamp is None else timestamp
        for node in self.domain.get_instances():
            self._update_one_node(node, now)

    def _update_one_node(self, node, now):
        last_heard = self.domain.get_instance_heartbeat_time(node.instance_id)
        iaas_state_age = now - node.state_time

        new_state = None
        new_state_reason = None
        if node.state >= InstanceState.TERMINATING:

            # nodes get a window of time to stop sending heartbeats after they
            # are marked TERMINATING. After this point they are considered
            # ZOMBIE nodes.

            # note that if an instance is stuck in TERMINATING, there was
            # probably a failure in the provisioner. If heartbeats are still
            # coming in, then this extra check will trigger the controller to
            # (eventually) send another terminate request.

            if last_heard is None:

                # if we haven't heard from a terminated instance and it isn't
                # unknown, mark it so. if it is in fact a ZOMBIE, it will be
                # redetected as such after it sends another heartbeat. This is
                # most likely happening in a recovery and while cleaning up after
                # a wrong state in the persistence.
                if node.health != InstanceHealthState.UNKNOWN:
                    new_state = InstanceHealthState.UNKNOWN
                    new_state_reason = \
                        "was %s but state is %s and no heartbeat received" % (
                            node.health, node.state)

            elif (iaas_state_age > self.zombie_timeout and
                  now - last_heard > self.zombie_timeout):

                self.domain.set_instance_heartbeat_time(node.instance_id, None)

                if node.health != InstanceHealthState.UNKNOWN:
                    new_state = InstanceHealthState.UNKNOWN
                    new_state_reason = ("was %s but state is %s and no " +
                                        "heartbeat received for %.2f seconds"
                                        ) % (node.health, node.state, now - last_heard)

            elif last_heard > node.state_time + self.zombie_timeout:
                new_state = InstanceHealthState.ZOMBIE
                new_state_reason = "received heartbeat %.2f seconds after %s state" % (
                    last_heard - node.state_time, node.state)

        elif (node.state == InstanceState.RUNNING and
              node.health != InstanceHealthState.MISSING and
              node.health != InstanceHealthState.OUT_OF_CONTACT):
            # if the instance is marked running for a while but we haven't
            # gotten a heartbeat, it is OUT_OF_CONTACT
            if last_heard is None:

                # last_heard can be None for two reasons:
                #  1. We have never received a heartbeat from this instance.
                #     In this case we should mark it as OUT_OF_CONTACT once it
                #     crosses the boot_timeout threshold.
                #  2. We have recently recovered from controller restart and
                #     have yet to receive a heartbeat. We should not mark
                #     the instance as OUT_OF_CONTACT until it the timeout has passed
                #     starting from the initialization time of the monitor.

                # time since initialization of the monitor
                monitor_age = self.monitor_age(now)

                if monitor_age < iaas_state_age:

                    # our monitor started up *after* the instance reached the
                    # RUNNING state. We should base timeout comparisons on the
                    # initialization time.

                    # determine if we've ever gotten a heartbeat from this node
                    if node.health == InstanceHealthState.UNKNOWN:
                        if monitor_age > self.boot_timeout:
                            new_state = InstanceHealthState.OUT_OF_CONTACT
                            new_state_reason = "heartbeat never received, even " +\
                                "%.2f seconds after controller recovery" % monitor_age

                    elif monitor_age > self.missing_timeout:
                        new_state = InstanceHealthState.OUT_OF_CONTACT
                        new_state_reason = "another heartbeat not received for " +\
                                "%.2f seconds after controller recovery" % monitor_age

                elif iaas_state_age > self.boot_timeout:
                    new_state = InstanceHealthState.OUT_OF_CONTACT
                    new_state_reason = "heartbeat never received, " +\
                                "%.2f seconds after instance RUNNING" % iaas_state_age

            # likewise if we heard from it in the past but haven't in a while
            elif now - last_heard > self.missing_timeout:
                new_state = InstanceHealthState.OUT_OF_CONTACT
                new_state_reason = "no heartbeat received for %.2f seconds" % (now - last_heard)

        elif (node.state == InstanceState.RUNNING and
              node.health == InstanceHealthState.OUT_OF_CONTACT):

            # if the instance is marked OUT_OF_CONTACT for a while (really_missing_timeout) and
            # we haven't gotten a heartbeat, it is now MISSING
            if last_heard is None:
                log.critical("Inconsistency: node is %s but there is no last-heartbeat?" %
                             InstanceHealthState.OUT_OF_CONTACT)
                new_state = InstanceHealthState.MISSING
                new_state_reason = "inconsistency"
            elif now - last_heard > self.really_missing_timeout:
                new_state = InstanceHealthState.MISSING
                new_state_reason = "contacted node that was %s and waited %.2f more seconds" % \
                                   (InstanceHealthState.OUT_OF_CONTACT, (now - last_heard))

        if new_state:
            log.warn("Instance '%s' entering health state %s. Reason: %s",
                     node.instance_id, new_state, new_state_reason)
            self.domain.new_instance_health(node.instance_id, new_state)

        if new_state == InstanceHealthState.OUT_OF_CONTACT:
            next_state = InstanceHealthState.MISSING
            if not self.ouagent_client:
                log.error("No client to send dump_state with, changing directly to %s" % next_state)
                self.domain.new_instance_health(node.instance_id, next_state)
            ouagent_address = self.domain.ouagent_address(node.instance_id)
            if ouagent_address:
                log.warn("dump_state to %s --> One last check before it's %s" % (ouagent_address, next_state))
                self.domain.set_instance_heartbeat_time(node.instance_id, now)  # reset last_heard
                self.ouagent_client.dump_state(ouagent_address, mock_timestamp=now + 1)
            else:
                log.error("No address to send dump_state to, changing directly to %s" % next_state)
                self.domain.new_instance_health(node.instance_id, next_state)
