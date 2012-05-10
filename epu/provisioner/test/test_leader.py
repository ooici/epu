import unittest
import threading

from mock import Mock, MagicMock
import gevent

from epu.provisioner.leader import ProvisionerLeader

class ProvisionerLeaderTests(unittest.TestCase):

    def test_leader(self):
        core = Mock()
        store = Mock()

        leader = ProvisionerLeader(store, core)

        leader.initialize()
        store.contend_leader.assert_called_with(leader)

        event = threading.Event()

        leader_thread = gevent.spawn(leader.inaugurate)

        def side_effect():
            event.set()

        # the leader should call core.query_nodes() and core.query_contexts()
        # wait for that.
        core.query_nodes = MagicMock(side_effect=side_effect)
        core.query_contexts = MagicMock(side_effect=side_effect)
        event.wait(1)
        assert core.query_nodes.called
        assert core.query_contexts.called
        event.clear()

        # reset and trigger another query cycle (peeking into impl)
        core.query_nodes = MagicMock(side_effect=side_effect)
        core.query_contexts = MagicMock(side_effect=side_effect)
        leader._force_cycle()
        event.wait(1)
        assert core.query_nodes.called
        assert core.query_contexts.called
        event.clear()

        leader.depose()
        self.assertFalse(leader.is_leader)

        leader_thread.join(1)
        self.assertTrue(leader_thread.successful())
