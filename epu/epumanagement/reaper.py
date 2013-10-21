# Copyright 2013 University of Chicago

import logging
import time

from dashi.util import LoopingCall
from epu.epumanagement.conf import *  # noqa
from epu.domain_log import EpuLoggerThreadSpecific
from epu.states import InstanceState as states

log = logging.getLogger(__name__)


class EPUMReaper(object):
    """This process infrequently queries each domain in the datastore. It finds
    VM records in a terminal state past the threshold and removes them.

    The instance of the EPUManagementService process that hosts a particular EPUMReaper instance
    might not be the elected reaper.  When it is the elected reaper, this EPUMReaper instance
    handles that functionality.  When it is not the elected reaper, this EPUMReaper instance
    handles being available in the election.
    """

    def __init__(self, epum_store,
                 record_reaping_max_age, disable_loop=False):
        """
        @param epum_store State abstraction for all EPUs
        @param record_reaping_max_age Instance records older than record_reaping_max_age will be deleted
        @param disable_loop For unit/integration tests, don't run a timed decision loop
        """
        self.epum_store = epum_store
        self.record_reaping_max_age = record_reaping_max_age

        self.control_loop = None
        self.enable_loop = not disable_loop
        self.is_leader = False

    def recover(self):
        """Called whenever the whole EPUManagement instance is instantiated.
        """
        # For callbacks: "now_leader()" and "not_leader()"
        self.epum_store.register_reaper(self)

    def now_leader(self, block=False):
        """Called when this instance becomes the reaper leader.
        """
        log.info("Elected as Reaper leader")
        self._leader_initialize()
        self.is_leader = True
        if block:
            if self.control_loop:
                self.control_loop.thread.join()
            else:
                raise ValueError("cannot block without a control loop")

    def not_leader(self):
        """Called when this instance is known not to be the reaper leader.
        """
        if self.control_loop:
            self.control_loop.stop()
            self.control_loop = None
        self.is_leader = False

    def _leader_initialize(self):
        """Performs initialization routines that may require async processing
        """
        if self.enable_loop:
            if not self.control_loop:
                self.control_loop = LoopingCall(self._loop_top)
            self.control_loop.start(300)

    def _loop_top(self):
        """Run the reaper loop.

        Every time this runs, each domain is checked for instances in terminal
        states TERMINATED, FAILED, or REJECTED.  They are deleted if they are
        older than self.record_reaping_max_age.

        """
        # Perhaps in the meantime, the leader connection failed, bail early
        if not self.is_leader:
            return

        now = time.time()
        domains = self.epum_store.get_all_domains()

        for domain in domains:
            with EpuLoggerThreadSpecific(domain=domain.domain_id, user=domain.owner):
                if not domain.is_removed():
                    instances = domain.get_instances()
                    for instance in instances:
                        log.info("Instance is " + instance['state'])
                        if instance['state'] in [states.TERMINATED, states.FAILED, states.REJECTED]:
                            state_time = instance['state_time']
                            if now > state_time + self.record_reaping_max_age:
                                log.info("Removing instance %s with no state change for %f seconds",
                                         instance['instance_id'], now - state_time)
                                domain.remove_instance(instance['instance_id'])

        # Perhaps in the meantime, the leader connection failed, bail early
        if not self.is_leader:
            return
