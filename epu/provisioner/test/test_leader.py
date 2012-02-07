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

        query_nodes_called = threading.Event()
        query_ctx_called = threading.Event()

        core.query_nodes = query_nodes_called.set
        core.query_contexts = query_ctx_called.set

        leader_thread = gevent.spawn(leader.inaugurate)

        # the leader should call core.query(). wait for that.
        assert query_nodes_called.wait(1)
        assert query_ctx_called.wait(1)

        # reset and trigger another query cycle (peeking into impl)
        query_nodes_called.clear()
        query_ctx_called.clear()
        leader._force_cycle()
        assert query_nodes_called.wait(1)
        assert query_ctx_called.wait(1)

        leader.depose()
        self.assertFalse(leader.is_leader)

        leader_thread.join(1)
        self.assertTrue(leader_thread.successful())
