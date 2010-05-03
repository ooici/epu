#!/usr/bin/env python

"""
@file ion/services/coi/test/test_resource_registry.py
@author Michael Meisinger
@brief test service for registering resources and client classes
"""

import logging, time
from twisted.internet import defer
from twisted.trial import unittest
from magnet.container import Container

from ion.core import bootstrap
from ion.core import base_process
from ion.core.worker import *
from ion.test.iontest import IonTestCase
import ion.util.procutils as pu

class WorkerTest(IonTestCase):
    """Testing service classes of resource registry
    """

    @defer.inlineCallbacks
    def setUp(self):
        yield self._startContainer()

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._stopContainer()

    @defer.inlineCallbacks
    def _test_basic(self):
        messaging = {}
        
        workers = [
            {'name':'hello','module':'ion.services.hello_service','class':'HelloService'},
            {'name':'hello1','module':'ion.services.hello_service','class':'HelloService'},
            {'name':'hello2','module':'ion.services.hello_service','class':'HelloService'},
        ]
        
        yield bootstrap.bootstrap(messaging, workers)
 
    @defer.inlineCallbacks
    def _test_worker_queue(self):
        messaging = {'worker1':{'name_type':'worker', 'args':{'scope':'local'}}}
        
        workers = [
            {'name':'workerProc1','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'worker1','scope':'local'}},
            {'name':'workerProc2','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'worker1','scope':'local'}},
        ]
        
        yield bootstrap.bootstrap(messaging, workers)
        
        sup = yield base_process.procRegistry.get("bootstrap")
        logging.info("Supervisor: "+repr(sup))

        wc = WorkerClient()
        wcId = yield spawn(wc.receiver)

        wq_name = Container.id + ".worker1"
        for i in range(1,11):
            yield wc.submit_work(wq_name, i, 1)
        
        yield pu.asleep(3)
        logging.info("Work results: "+str(wc.workresult))
        logging.info("Worker results: "+str(wc.worker))
        
        sum = 0
        for w,v in wc.worker.items():
            sum += v
        self.assertEqual(sum, 10)
    
    @defer.inlineCallbacks
    def test_fanout(self):
        messaging = {'fanout1':{'name_type':'fanout', 'args':{'scope':'local'}}}

        workers = [
            {'name':'fanoutProc1','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'fanout1','scope':'local'}},
            {'name':'fanoutProc2','module':'ion.core.worker','class':'WorkerProcess','spawnargs':{'service-name':'fanout1','scope':'local'}},
        ]

        yield bootstrap.bootstrap(messaging, workers)
        
        sup = yield base_process.procRegistry.get("bootstrap")
        logging.info("Supervisor: "+repr(sup))

        wc = WorkerClient()
        wcId = yield spawn(wc.receiver)

        wq_name = Container.id + ".fanout1"
        for i in range(1,3):
            yield wc.submit_work(wq_name, i, 1)
        
        yield pu.asleep(3)
        logging.info("Work results: "+str(wc.workresult))
        logging.info("Worker results: "+str(wc.worker))
        
        sum = 0
        for w,v in wc.worker.items():
            sum += v
        #self.assertEqual(sum, 20)
        