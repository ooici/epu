import logging
import threading

log = logging.getLogger(__name__)

class ProvisionerLeader(object):

    def __init__(self, store, core, query_delay=5):
        """
        @type store ProvisionerStore
        @type core ProvisionerCore
        """
        self.store = store
        self.core = core
        self.query_delay = float(query_delay)

        self.is_leader = False

        self.condition = threading.Condition()

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

        if not self.is_leader:
            raise Exception("not the leader")

        with self.condition:
            self.is_leader = False
            self.condition.notify_all()

    def run(self):
        while self.is_leader:
            #TODO we can make this querying much more granular and run in parallel
            self.core.query()

            with self.condition:
                if self.is_leader:
                    self.condition.wait(self.query_delay)


    # for tests
    def _force_cycle(self):
        with self.condition:
            self.condition.notify_all()