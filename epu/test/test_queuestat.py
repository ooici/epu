#!/usr/bin/env python

"""
@file epu/test/test_queuestat.py
@author David LaBissoniere
@brief Test queuestat behavior
"""


import os
import uuid
import tempfile
from collections import defaultdict

from twisted.internet import defer
from twisted.internet.task import LoopingCall
import unittest

from ion.core.process.process import Process
from ion.core.messaging.receiver import ServiceWorkerReceiver
import ion.util.procutils as pu
import ion.test.iontest
from ion.test.iontest import IonTestCase
import epu.ionproc.queuestat
from epu.ionproc.queuestat import QueueStatClient, QueueStatService

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)


class TestQueueStatService(IonTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        fh, path = tempfile.mkstemp()
        self.cookie_path = path

        f = os.fdopen(fh, 'w')
        try:
            f.write("COOOOKIE")
        finally:
            f.close()

        self.twotp_node = FakeTwotpNode()
        self.rabbitmqctl = FakeRabbitMQControlService()

        self.patch(LoopingCall, "start", self._fake_start)
        self.patch(epu.ionproc.queuestat.twotp, "node", self.twotp_node)
        self.patch(epu.ionproc.queuestat, "RabbitMQControlService",
                   self._rabbitmqctl)
        self.patch(QueueStatService, "send", self._fake_svc_send)
        self.patch(QueueStatClient, "send", self._fake_client_send)
        self.loop_interval = None

        self.queues = defaultdict(list)

    def _fake_start(self, interval, now=True):
        self.loop_interval = interval
    
    def _fake_svc_send(self, name, op, content):
        self.assertEqual(name, "sub_id")
        self.assertEqual(op, "stat")
        self.assertEqual(content['sensor_id'], "queue-length")
        val = content['value']
        queue_name = val['queue_name']
        queue_length = val['queue_length']
        self.queues[queue_name].append(queue_length)
        return defer.succeed(None)

    def _fake_client_send(self, op, content):
        if op == "watch_queue":
            self.queuestat.op_watch_queue(content, None, None)
        elif op == "unwatch_queue":
            self.queuestat.op_unwatch_queue(content, None, None)
        else:
            self.fail("unknown op %s" % op)
        return defer.succeed(None)

    def _rabbitmqctl(self, process, node_name):
        self.assertEqual(process, "theprocess")
        self.assertEqual(node_name, "thenodename")
        return self.rabbitmqctl

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stop_container()
        if self.cookie_path and os.path.exists(self.cookie_path):
            os.unlink(self.cookie_path)

    @defer.inlineCallbacks
    def test_queuestat(self):
        queuestat = QueueStatService(spawnargs=
            {'erlang_cookie_path': self.cookie_path,
             'interval_seconds': 5.0})
        self.queuestat = queuestat

        queuestat.slc_init()
        self.assertEqual(self.loop_interval, None)

        queuestat_client = QueueStatClient()

        yield queuestat_client.watch_queue("q1", "sub_id", 'stat')
        self.assertEqual(self.loop_interval, 5.0)
        yield queuestat._wrapped_do_poll()

        self.rabbitmqctl.set_qlen("q1", 5)
        yield queuestat._wrapped_do_poll()
        self.assertEqual(self.queues.pop("q1"), [5])

        yield queuestat_client.watch_queue("q2", "sub_id", 'stat')
        self.rabbitmqctl.set_qlen("q2", 0)
        yield queuestat._wrapped_do_poll()
        self.assertEqual(self.queues.pop("q1"), [5])
        self.assertEqual(self.queues.pop("q2"), [0])

        self.rabbitmqctl.set_qlen("q1", 8)
        self.rabbitmqctl.set_qlen("q2", 3)
        self.rabbitmqctl.set_qlen("q3", 1)

        yield queuestat._wrapped_do_poll()

        self.assertEqual(self.queues.pop("q1"), [8])
        self.assertEqual(self.queues.pop("q2"), [3])
        self.assertFalse(self.queues) # should not be a q3 message

        yield queuestat_client.unwatch_queue("q1", "sub_id", 'stat')
        yield queuestat._wrapped_do_poll()

        self.rabbitmqctl.set_qlen("q1", 10)
        self.assertEqual(self.queues.pop("q2"), [3])
        self.assertFalse(self.queues) # should not be a q1 or q3 message


class FakeRabbitMQControlService(object):
    def __init__(self):
        self.queues = {}

    def set_qlen(self, queue_name, len):
        self.queues[queue_name] = len

    def list_queues(self):
        all_queues = {'result': [(qname, dict(messages=qlen))
                                 for qname, qlen in self.queues.iteritems()]}
        return defer.succeed(all_queues)


class FakeTwotpNode(object):
    def buildNodeName(self, node):
        return "thenodename"

    def Process(self, nodename, cookie):
        assert nodename == "thenodename"
        assert cookie == "COOOOKIE"
        return "theprocess"


class TestQueueStatServiceLive(IonTestCase):
    """Queuestat tests that use a live broker on localhost
    """

    @defer.inlineCallbacks
    def setUp(self):

        #unconditional skip now that @itv is gone. we'll make our own decorator??
        raise unittest.SkipTest("Skipping test that requires localhost rabbit broker")

        if not os.path.exists(os.path.expanduser('~/.erlang.cookie')):
            raise unittest.SkipTest('Needs a RabbitMQ server on localhost')

        log.debug('Temporarily changing broker_host to 127.0.0.1')
        self.other_broker_host = ion.test.iontest.CONF.obj['broker_host']
        ion.test.iontest.CONF.obj['broker_host'] = '127.0.0.1'

        yield self._start_container()
        procs = [
            {'name':'queuestat','module':'epu.ionproc.queuestat', 
                'class':'QueueStatService', 
                'spawnargs' : {'interval_seconds' : 0.1}},
                ]
        self.sup = yield self._spawn_processes(procs)

        
        
        id = str(uuid.uuid4())
        id = id[id.rfind('-')+1:] # shorter id
        queuename = '_'.join((__name__, id))
        yield self._make_queue(queuename)

        self.queuename = pu.get_scoped_name(queuename, "system")

    @defer.inlineCallbacks
    def _make_queue(self, name):
        self.receiver = ServiceWorkerReceiver(
            label=name,
            name=name,
            scope='system')
        yield self.receiver.initialize()

    @defer.inlineCallbacks
    def tearDown(self):

        # activating the receiver causes the queue to drain, apparently
        yield self.receiver.activate()
        yield self._shutdown_processes()
        yield self._stop_container()

        log.debug('Resetting broker_host')
        ion.test.iontest.CONF.obj['broker_host'] = self.other_broker_host

    @defer.inlineCallbacks
    def test_queuestat(self):
        subscriber = TestSubscriber()
        subId = yield self._spawn_process(subscriber)
        queuestat_client = QueueStatClient(subscriber)

        yield queuestat_client.watch_queue(self.queuename, str(subId), 'stat')
        yield pu.asleep(0.3)

        assert subscriber.queue_length[self.queuename] == 0
        
        yield self._add_messages(5)
        yield pu.asleep(0.3)
        assert subscriber.queue_length[self.queuename] == 5
        
        yield self._add_messages(3)
        yield pu.asleep(0.3)
        assert subscriber.queue_length[self.queuename] == 8

        yield pu.asleep(0.3)
        yield queuestat_client.unwatch_queue(self.queuename, str(subId), 'stat')
        
        yield self._add_messages(3)
        yield pu.asleep(0.3)
        assert subscriber.queue_length[self.queuename] == 8


    @defer.inlineCallbacks
    def _add_messages(self, count):
        for i in range(count):
            yield self.sup.send(self.queuename, 'work', 
                {'deal' : 'this is a fake message'})


class TestSubscriber(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)

        self.queue_length = {}
        self.recv_count = {}
    
    def op_stat(self, content, headers, msg):
        log.info('Got queuestat: %s', content)

        assert content['sensor_id'] == "queue-length"
        assert content['time']

        value = content['value']

        q = value['queue_name']
        self.queue_length[q] = value['queue_length']

        count = self.recv_count.get(q, None)
        self.recv_count[q] = count + 1 if count else 1

