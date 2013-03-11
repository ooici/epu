import os
import unittest
import threading
import time

from mock import Mock, MagicMock

import epu.tevent as tevent
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
        # self.assertTrue(leader_thread.successful())

    def test_terminator_death(self):

        # This test will bring down the nose process, so we run it in a child
        # process.

        child_pid = os.fork()
        if child_pid == 0:
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
            tevent.joinall([leader_thread])
        else:
            pid, exit = os.wait()

            # exit is a 16-bit number, whose low byte is the signal number that
            # killed the process, and whose high byte is the exit status (if
            # the signal number is zero); the high bit of the low byte is set
            # if a core file was produced.
            #
            # Check that the signal number is zero and the exit code is the
            # expected one.
            self.assertEqual(exit & 0xff, 0)
            self.assertEqual(exit >> 8 & 0xff, os.EX_SOFTWARE)
