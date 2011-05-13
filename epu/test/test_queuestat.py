#!/usr/bin/env python

"""
@file epu/test/test_queuestat.py
@author David LaBissoniere
@brief Test queuestat behavior
"""


import os
import uuid

from twisted.internet import defer
from twisted.trial import unittest

from ion.core.process.process import Process
from ion.core.messaging.receiver import ServiceWorkerReceiver
import ion.util.procutils as pu
import ion.test.iontest
from ion.test.iontest import IonTestCase
from epu.ionproc.queuestat import QueueStatClient

from ion.core import ioninit
CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class TestQueueStatService(IonTestCase):

    @defer.inlineCallbacks
    @itv(CONF)
    def setUp(self):

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
    def test_queue_stat(self):
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

