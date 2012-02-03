import unittest
import threading

from mock import Mock
import gevent

from epu.provisioner.leader import ProvisionerLeader

class ProvisionerLeaderTests(unittest.TestCase):

    def test_leader(self):
        core = Mock()
        store = Mock()

        leader = ProvisionerLeader(store, core)

        leader.initialize()
        store.contend_leader.assert_called_with(leader)

        query_called = threading.Event()
        core.query = query_called.set

        leader_thread = gevent.spawn(leader.inaugurate)

        # the leader should call core.query(). wait for that.
        assert query_called.wait(1)

        # reset and trigger another query cycle (peeking into impl)
        query_called.clear()
        leader._force_cycle()
        assert query_called.wait(1)

        leader.depose()
        self.assertFalse(leader.is_leader)

        leader_thread.join(1)
        self.assertTrue(leader_thread.successful())
