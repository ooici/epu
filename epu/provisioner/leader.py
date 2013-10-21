# Copyright 2013 University of Chicago

import logging
import threading
import time
from itertools import izip

import epu.tevent as tevent
from epu.tevent import Pool

log = logging.getLogger(__name__)


class ProvisionerLeader(object):
    """
    The Provisioner Leader is guaranteed to be running only within a single
    Provisioner worker process. It is enforced by leader election which also
    guarantees that if the leader process fails, it will be replaced within
    a matter of seconds.

    The Provisioner Leader has responsibilities which are best performed by
    a single process:

        - On a regular time frequency, the leader determines IaaS sites that
          need to be checked for instance state updates and performs these
          queries.

        - When terminate_all is requested, the leader detects this and, after
          all other Provisioner workers have recognized the state, marks the
          instances as TERMINATING so that they are killed by the terminator
          thread.
    """

    # Default time before removing node records in terminal state
    _RECORD_REAPING_DEFAULT_MAX_AGE = 7200

    def __init__(self, store, core, query_delay=10, concurrent_queries=20,
                 concurrent_terminations=10, record_reaper_delay=300,
                 record_reaping_max_age=None):
        """
        @type store ProvisionerStore
        @type core ProvisionerCore
        """
        self.store = store
        self.core = core
        self.query_delay = float(query_delay)
        self.record_reaper_delay = float(record_reaper_delay)
        self.concurrent_queries = int(concurrent_queries)
        self.concurrent_terminations = int(concurrent_terminations)

        if record_reaping_max_age is not None:
            self.record_reaping_max_age = float(record_reaping_max_age)
        else:
            self.record_reaping_max_age = self._RECORD_REAPING_DEFAULT_MAX_AGE

        self.is_leader = False
        self.condition = threading.Condition()

        # force a query cycle
        self.force_site_query = False
        self.force_context_query = False

        # Query threads
        self.site_query_thread = None
        self.context_query_thread = None

        # Thread for performing asynchronous termination of instances
        self.terminator_thread = None
        self.terminator_condition = threading.Condition()

        # Record reaper thread
        self.record_reaper_thread = None

        # For testing
        self.force_record_reaping = False

        self.site_query_condition = threading.Condition()
        self.context_query_condition = threading.Condition()
        self.record_reaper_condition = threading.Condition()

    def initialize(self):
        """Initiates participation in the leader election
        """
        self.store.contend_leader(self)

    def inaugurate(self):
        """Callback from the election fired when this leader is elected

        This routine is expected to never return as long as we want to
        remain the leader.
        """
        if self.is_leader:
            # already the leader???
            raise Exception("already the leader???")
        self.is_leader = True

        self.run()

    def depose(self):
        """Callback from election fired when the leader needs to stop running

        Usually this is because we have lost network access or something.
        """
        log.info("Stopping provisioner leader")

        with self.condition:
            self.is_leader = False
            self.condition.notify_all()

        if self.terminator_thread:
            self.kill_terminator()

        if self.site_query_thread:
            self.kill_site_query_thread()

        if self.context_query_thread:
            self.kill_context_query_thread()

        if self.record_reaper_thread:
            self.kill_record_reaper_thread()

    def run(self):

        log.info("Elected as provisioner leader!")

        # TODO need better handling of time, this is susceptible to system clock changes

        while self.is_leader:
            if self.store.is_disabled():
                    disabled_agreed = self.store.is_disabled_agreed()
                    log.debug("terminate_all: disabled_agreed=%s", disabled_agreed)

                    if not disabled_agreed:
                        log.info("provisioner termination detected but not all processes agree yet. waiting.")
                    else:
                        log.info("provisioner termination beginning")
                        self.core.terminate_all()

            if self.terminator_thread is None:
                self.terminator_thread = tevent.spawn(self.run_terminator, _fail_fast=True)

            if self.site_query_thread is None:
                self.site_query_thread = tevent.spawn(self.run_site_query_thread, _fail_fast=True)

            if self.context_query_thread is None:
                self.context_query_thread = tevent.spawn(self.run_context_query_thread, _fail_fast=True)

            if self.record_reaper_thread is None:
                self.record_reaper_thread = tevent.spawn(self.run_record_reaper_thread, _fail_fast=True)

            with self.condition:
                if self.is_leader:
                    self.condition.wait(self.query_delay)

    # for tests
    def _force_cycle(self):
        with self.site_query_condition:
            self.force_site_query = True
            self.site_query_condition.notify_all()

            while self.force_site_query:
                self.site_query_condition.wait()

        with self.context_query_condition:
            self.force_context_query = True
            self.context_query_condition.notify_all()

            while self.force_context_query:
                self.context_query_condition.wait()

    def _force_record_reaping(self):
        with self.record_reaper_condition:
            self.force_record_reaping = True
            self.record_reaper_condition.notify_all()

            while self.force_record_reaping:
                self.record_reaper_condition.wait()

    def run_terminator(self):
        log.info("Starting terminator")
        self.terminator_running = True

        while self.is_leader and self.terminator_running:
            try:
                self._terminate_pending_terminations()
            except Exception:
                log.exception("Problem terminating pending terminations")

            with self.terminator_condition:
                if self.terminator_running:
                    self.terminator_condition.wait(1)

    def _terminate_pending_terminations(self):
        if self.concurrent_terminations > 1:
            pool = Pool(self.concurrent_terminations)
        node_ids = self.store.get_terminating()
        nodes = self.core._get_nodes_by_id(node_ids, skip_missing=False)
        for node_id, node in izip(node_ids, nodes):
            if not node:
                # maybe an error should make it's way to controller from here?
                log.warn('Node %s unknown but requested for termination', node_id)
                self.store.remove_terminating(node_id)
                log.info("Removed terminating entry for node %s from store", node_id)
                continue

            log.info("Terminating node %s", node_id)
            try:
                if self.concurrent_terminations > 1:
                    pool.spawn(self.core.terminate_node, node)
                else:
                    self.core.terminate_node(node)
            except:
                log.exception("Termination of node %s failed:", node_id)

        pool.join()

    def kill_terminator(self):
        """He'll be back"""
        with self.terminator_condition:
            self.terminator_running = False
            self.terminator_condition.notify_all()
        if self.terminator_thread:
            self.terminator_thread.join()
        self.terminator_thread = None

    def run_site_query_thread(self):
        log.info("Starting site query thread")
        self.site_query_running = True

        while self.is_leader and self.site_query_running:
            next_query = time.time() + self.query_delay
            try:
                self.core.query_nodes(concurrency=self.concurrent_queries)
            except Exception:
                log.exception("IaaS query failed due to an unexpected error")

            if self.force_site_query:
                log.debug("forced cycle")
                with self.site_query_condition:
                    self.force_site_query = False
                    self.site_query_condition.notify_all()

            with self.site_query_condition:
                timeout = next_query - time.time()
                if self.site_query_running and timeout > 0:
                    self.site_query_condition.wait(timeout)

    def kill_site_query_thread(self):
        with self.site_query_condition:
            self.site_query_running = False
            self.site_query_condition.notify_all()
        if self.site_query_thread:
            self.site_query_thread.join()
        self.site_query_thread = None

    def run_context_query_thread(self):
        log.info("Starting context query thread")
        self.context_query_running = True

        while self.is_leader and self.context_query_running:
            next_query = time.time() + self.query_delay
            try:
                self.core.query_contexts(concurrency=self.concurrent_queries)
            except Exception:
                log.exception("Context query failed due to an unexpected error")

            if self.force_context_query:
                with self.context_query_condition:
                    self.force_context_query = False
                    self.context_query_condition.notify_all()

            with self.context_query_condition:
                timeout = next_query - time.time()
                if self.context_query_running and timeout > 0:
                    self.context_query_condition.wait(timeout)

    def kill_context_query_thread(self):
        with self.context_query_condition:
            self.context_query_running = False
            self.context_query_condition.notify_all()
        if self.context_query_thread:
            self.context_query_thread.join()
        self.context_query_thread = None

    def run_record_reaper_thread(self):
        log.info("Starting record reaper thread")
        self.record_reaper_running = True

        while self.is_leader and self.record_reaper_running:
            next_record_reaping = time.time() + self.record_reaper_delay
            try:
                self.core.reap_records(self.record_reaping_max_age)
            except Exception:
                log.exception("Record reaping failed due to an unexpected error")

            if self.force_record_reaping:
                with self.record_reaper_condition:
                    self.force_record_reaping = False
                    self.record_reaper_condition.notify_all()

            with self.record_reaper_condition:
                timeout = next_record_reaping - time.time()
                if self.record_reaper_running and timeout > 0:
                    self.record_reaper_condition.wait(timeout)

    def kill_record_reaper_thread(self):
        with self.record_reaper_condition:
            self.record_reaper_running = False
            self.record_reaper_condition.notify_all()

        if self.record_reaper_thread:
            self.record_reaper_thread.join()
        self.record_reaper_thread = None
