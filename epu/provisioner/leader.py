import logging
import threading
import time

import gevent

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
          all other Provisioner workers have recognized the state, performs
          the terminations.
    """

    def __init__(self, store, core, query_delay=10):
        """
        @type store ProvisionerStore
        @type core ProvisionerCore
        """
        self.store = store
        self.core = core
        self.query_delay = float(query_delay)

        self.is_leader = False

        self.condition = threading.Condition()

        # force a query cycle
        self.force_query = False

        self.terminator_thread = None

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

        with self.condition:
            self.is_leader = False
            self.condition.notify_all()

        if self.terminator_thread:
            self.terminator_thread.kill()

    def run(self):

        # TODO need better handling of time, this is susceptible to system clock changes

        next_query = time.time()
        while self.is_leader:

            if not self.terminator_thread:
                if self.store.is_disabled():

                    disabled_agreed = self.store.is_disabled_agreed()

                    if not disabled_agreed:
                        log.info("provisioner termination detected but not all processes agree yet. waiting.")
                    else:
                        log.info("provisioner termination beginning")
                        self.terminator_thread = gevent.spawn(self.terminate_all)

            force_query = self.force_query
            now = time.time()
            if force_query or now >= next_query:
                log.debug("Beginning query cycle")
                #TODO we can make this querying much more granular and run in parallel
                try:
                    self.core.query_nodes()
                except Exception:
                    log.exception("IaaS query failed due to an unexpected error")

                try:
                    self.core.query_contexts()
                except Exception:
                    log.exception("Context query failed due to an unexpected error")

                if force_query:
                    next_query = now + self.query_delay
                    with self.condition:
                        self.force_query = False
                        self.condition.notify_all()
                else:
                    next_query += self.query_delay

                log.debug("Ending query cycle")
            with self.condition:
                if self.is_leader and not self.force_query:
                    timeout = next_query - time.time()
                    if timeout > 0:
                        self.condition.wait(timeout)

    # for tests
    def _force_cycle(self):
        with self.condition:
            self.force_query = True
            self.condition.notify_all()

            while self.force_query:
                self.condition.wait()

    def terminate_all(self):
        #TODO run terminations concurrently?
        try:
            while self.is_leader:
                try:
                    if self.core.check_terminate_all():
                        break
                    self.core.terminate_all()
                except Exception:
                    log.exception("Problem terminating all. Retrying.")
                    gevent.sleep(10)
        except gevent.GreenletExit:
            pass