#!/usr/bin/env python

"""
@file epu/test/test_torque.py
@author David LaBissoniere
@brief Test TorqueManagerService behavior
"""

from twisted.internet import defer

from ion.core.process.process import Process
import ion.test.iontest
from ion.test.iontest import IonTestCase

from ion.core import ioninit
from epu.ionproc.torque import TorqueManagerClient, TorqueManagerService
from epu.test import Mock

CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class TestTorqueManagerService(IonTestCase):

    @defer.inlineCallbacks
    @itv(CONF)
    def setUp(self):
        yield self._start_container()
        self.pbs = FakePBS()
        spawnargs = {'interval_seconds':0, 'pbs':self.pbs}
        self.service = TorqueManagerService(spawnargs=spawnargs)
        yield self._spawn_process(self.service)
        self.service.loop = FakeLoopingCall() # override looping call for testing

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

    @defer.inlineCallbacks
    def test_watch_unwatch(self):
        sub1 = TestSubscriber()
        sub1Id = yield self._spawn_process(sub1)
        client = TorqueManagerClient(sub1)

        sub2 = TestSubscriber()
        sub2Id = yield self._spawn_process(sub2)

        yield client.watch_queue(sub1Id, "stat", "q1")
        self.assertTrue(self.service.loop.running)
        yield self.service._do_poll()
        stat = yield sub1.deferred
        self.assertQueueStat(stat, "q1", 0)

        """
        self.pbs.set_queue_length("q1", 3)
        yield self.service._do_poll()
        stat = yield sub1.deferred
        self.assertQueueStat(stat, "q1", 3)

        self.pbs.clear()
        self.pbs.set_queue_length("q1", 5)
        yield self.service._do_poll()
        stat = yield sub1.deferred
        self.assertQueueStat(stat, "q1", 5)
        """

        # unsubscribe and loop should stop
        yield client.unwatch_queue(sub1Id, "stat", "q1")
        self.assertFalse(self.service.loop.running)
        self.assertFalse(self.service.watched_queues)

    def assertQueueStat(self, stat, name, length):
        self.assertEqual(stat['queue_name'], name)
        self.assertEqual(stat['queue_length'], length)

class FakeLoopingCall(object):
    def __init__(self):
        self.running = False
    def start(self, interval):
        self.interval = interval
        self.running = True
    def stop(self):
        self.running = False

class FakePBS(object):
    def __init__(self):
        self.queues = []
        self.stats = 0

    def pbs_default(self):
        return None

    def pbs_connect(self, server):
        return None

    def pbs_statque(self, *args):
        self.stats += 1
        return self.queues

    def pbs_statnode(self, *args):
        return []

    def clear(self):
        self.queues = []

    def set_queue_length(self, name, length):
        self.queues.append(Mock(name=name,
                                attribs=[Mock(name="total_jobs", value=length)]))

class TestSubscriber(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)

        self.queue_length = {}
        self.recv_count = {}
        self.deferred = defer.Deferred()

    def op_stat(self, content, headers, msg):
        q = content['queue_name']
        self.queue_length[q] = content['queue_length']

        count = self.recv_count.get(q, None)
        self.recv_count[q] = count + 1 if count else 1
        self.deferred.callback(content)
        self.deferred = defer.Deferred()
