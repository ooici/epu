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
import ion.util.procutils as pu
import ion.test.iontest
from ion.test.iontest import IonTestCase

from ion.core import ioninit
from epu.ionproc.torque import TorqueManagerClient

CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class TestTorqueManagerService(IonTestCase):

    @defer.inlineCallbacks
    @itv(CONF)
    def setUp(self):

        yield self._start_container()
        procs = [
            {'name':'torque','module':'epu.ionproc.torque',
                'class':'TorqueManagerService',
                'spawnargs' : {'interval_seconds' : 0.01}},
                ]
        self.sup = yield self._spawn_processes(procs)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()


    @defer.inlineCallbacks
    def test_watch_unwatch(self):
        subscriber = TestSubscriber()
        subId = yield self._spawn_process(subscriber)
        client = TorqueManagerClient(subscriber)

        yield client.watch_queue(subId, "stat")
        stat = yield subscriber.deferred

        #TODO assert the stat


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