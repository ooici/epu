import unittest
import threading
import time

import epu.tevent as tevent

from mock import Mock, MagicMock
from nose.plugins.skip import SkipTest

from epu.provisioner.leader import ProvisionerLeader

class ProvisionerLeaderTests(unittest.TestCase):

    def test_leader(self):
        core = Mock()
        store = Mock()

        def get_terminating():
            time.sleep(0.5)
            return []

        store.get_terminating = MagicMock(side_effect=get_terminating)
        core._get_nodes_by_id = MagicMock(return_value=[])

        leader = ProvisionerLeader(store, core)

        leader.initialize()
        store.contend_leader.assert_called_with(leader)

        event = threading.Event()

        leader_thread = tevent.spawn(leader.inaugurate)

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
        # TODO: PDA: test that thread exited cleanly?
        #self.assertTrue(leader_thread.successful())

    def test_terminator_death(self):

        # This test will bring down the nose process. To test it, comment
        # out the following line, and ensure that the test brings down the
        # nose test
        raise SkipTest("Test should only be run manually, kills process")

        core = Mock()
        store = Mock()

        def dies():
            raise TypeError("thread dies!")

        core._get_nodes_by_id = MagicMock(return_value=[])

        leader = ProvisionerLeader(store, core)

        leader.run_terminator = dies

        leader.initialize()
        store.contend_leader.assert_called_with(leader)

        leader_thread = tevent.spawn(leader.inaugurate)

