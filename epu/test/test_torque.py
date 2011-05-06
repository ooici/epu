#!/usr/bin/env python

"""
@file epu/test/test_torque.py
@author David LaBissoniere
@brief Test TorqueManagerService behavior
"""

from collections import deque
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
        messages = yield sub1.get_messages(2)
        self.assertBoth(messages, "q1", 0, '')

        self.pbs.set_queue_length("q1", 3)
        self.pbs.set_worker_status("localhost", "down")
        yield self.service._do_poll()
        messages = yield sub1.get_messages(2)
        self.assertBoth(messages, "q1", 3, "localhost:down")

        self.pbs.clear()
        self.pbs.set_queue_length("q1", 5)
        self.pbs.set_worker_status("localhost", "job-exclusive")
        yield self.service._do_poll()
        messages = yield sub1.get_messages(2)
        self.assertBoth(messages, "q1", 5, "localhost:job-exclusive")

        # unsubscribe and loop should stop
        yield client.unwatch_queue(sub1Id, "stat", "q1")
        self.assertFalse(self.service.loop.running)
        self.assertFalse(self.service.watched_queues)

    def assertBoth(self, messages, name, length, status):
        self.assertEqual(2, len(messages))
        m1,m2 = messages
        if "queue_length" in m1:
            self.assertQueueStat(m1, name, length)
            self.assertWorkerStatus(m2, name, status)
        else:
            self.assertQueueStat(m2, name, length)
            self.assertWorkerStatus(m1, name, status)


    def assertQueueStat(self, stat, name, length):
        self.assertEqual(stat['queue_name'], name)
        self.assertEqual(stat['queue_length'], length)

    def assertWorkerStatus(self, stat, name, status):
        self.assertEqual(stat['queue_name'], name)
        self.assertEqual(stat['worker_status'], status)

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
        self.status = []
        self.stats = 0

    def pbs_default(self):
        return None

    def pbs_connect(self, server):
        return None

    def pbs_statque(self, *args):
        self.stats += 1
        return self.queues

    def pbs_statnode(self, *args):
        self.stats += 1
        return self.status

    def clear(self):
        self.queues = []
        self.status = []

    def set_queue_length(self, name, length):
        states = 'i:0 j:%s' % length
        self.queues.append(Mock(name=name,
                                attribs=[Mock(name="state_count", value=states)]))

    def set_worker_status(self, name, status):
        self.status.append(Mock(name=name,
                                attribs=[Mock(name="state", value=status)]))

    def error(self):
        return 0, 'no error'

class TestSubscriber(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)

        self.worker_status = {}
        self.queue_length = {}
        self.recv_count = {}
        self.deferred = defer.Deferred()
        self.queue = deque([self.deferred])

    def get_message(self):
        return self.queue.popleft()

    @defer.inlineCallbacks
    def get_messages(self, count):
        l = []
        for i in range(count):
            m = yield self.get_message()
            l.append(m)
        defer.returnValue(l)

    def op_stat(self, content, headers, msg):
        q = content['queue_name']
        if content.has_key('queue_length'):
            self.queue_length[q] = content['queue_length']
        elif content.has_key('worker_status'):
            self.worker_status[q] = content['worker_status']

        count = self.recv_count.get(q, None)
        self.recv_count[q] = count + 1 if count else 1
        d = self.deferred
        self.deferred = defer.Deferred()
        self.queue.append(self.deferred)
        d.callback(content)
