from itertools import izip
import logging
import threading
import time

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

    def __init__(self, store, core, query_delay=10, concurrent_queries=20, concurrent_terminations=10):
        """
        @type store ProvisionerStore
        @type core ProvisionerCore
        """
        self.store = store
        self.core = core
        self.query_delay = float(query_delay)
        self.concurrent_queries = int(concurrent_queries)
        self.concurrent_terminations = int(concurrent_terminations)

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

        self.site_query_condition = threading.Condition()
        self.context_query_condition = threading.Condition()

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
                self.terminator_thread = tevent.spawn(self.run_terminator)

            if self.site_query_thread is None:
                self.site_query_thread = tevent.spawn(self.run_site_query_thread)

            if self.context_query_thread is None:
                self.context_query_thread = tevent.spawn(self.run_context_query_thread)

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

    def run_terminator(self):
        log.info("Starting terminator")
        self.terminator_running = True

        while self.is_leader and self.terminator_running:
            # TODO: PDA is there something better that can be done than
            # reinitializing the pool each time?
            if self.concurrent_terminations > 1:
                pool = Pool(self.concurrent_terminations)
            node_ids = self.store.get_terminating()
            nodes = self.core._get_nodes_by_id(node_ids, skip_missing=False)
            for node_id, node in izip(node_ids, nodes):
                if not node:
                    #maybe an error should make it's way to controller from here?
                    log.warn('Node %s unknown but requested for termination',
                            node_id)
                    continue

                log.info("Terminating node %s", node_id)
                launch = self.store.get_launch(node['launch_id'])
                try:
                    if self.concurrent_terminations > 1:
                        pool.spawn(self.core._terminate_node, node, launch)
                    else:
                        self.core._terminate_node(node, launch)
                except:
                    log.exception("Termination of node %s failed:", node_id)
                    pass

            pool.join()

    def kill_terminator(self):
        """He'll be back"""
        self.terminator_running = False
        if self.terminator_thread:
            self.terminator_thread.join()

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
                with self.site_query_condition:
                    self.force_site_query = False
                    self.site_query_condition.notify_all()

            with self.site_query_condition:
                timeout = next_query - time.time()
                if timeout > 0:
                    self.site_query_condition.wait(timeout)

    def kill_site_query_thread(self):
        self.site_query_running = False
        if self.site_query_thread:
            self.site_query_thread.join()

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
                if timeout > 0:
                    self.context_query_condition.wait(timeout)

    def kill_context_query_thread(self):
        self.context_query_running = False
        if self.context_query_thread:
            self.context_query_thread.join()
