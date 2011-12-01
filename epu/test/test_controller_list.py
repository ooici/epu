#!/usr/bin/env python

"""
@file epu/test/test_controller_list.py
@brief Test controller list behavior
"""

import logging
import os
import tempfile

from twisted.internet import defer

import ion.test.iontest
from ion.test.iontest import IonTestCase
from ion.services.cei import EPUControllerListClient

log = logging.getLogger(__name__)

TEST_LIST_OK1 = """
# comments should be ignored

epu_controller_no1

epu_controller_no2

epu_controller_no3

"""

TEST_LIST_OK2 = """
epu_controller_no4\n
epu_controller_no5\n
# comments should be ignored
epu_controller_no6\n
epu_controller_no7\n
# comments should be ignored
epu_controller_no8\n
"""

class TestListServiceOK1(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.listfile = tempfile.mktemp(prefix="controllerlist-")
        f = open(self.listfile, 'w')
        f.write(TEST_LIST_OK1)
        f.close()
        yield self._start_container()
        procs = [
            {'name':'epu_controller_list','module':'epu.ionproc.controller_list',
                'class':'EPUControllerListService',
                'spawnargs' : {'controller_list_path' : self.listfile}},
                ]
        self.sup = yield self._spawn_processes(procs)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()
        if os.path.exists(self.listfile):
            os.remove(self.listfile)

    @defer.inlineCallbacks
    def test_controller_list_ok1(self):
        list_client = EPUControllerListClient()
        controller_list = yield list_client.list()
        assert len(controller_list) == 3
        expected = {'epu_controller_no1':False, 'epu_controller_no2':False, 'epu_controller_no3':False}
        for name in controller_list:
            assert expected.has_key(name)
            expected[name] = True
        for key in expected.keys():
            assert expected[key] is True

class TestListServiceOK2(IonTestCase):
    @defer.inlineCallbacks
    def setUp(self):
        self.listfile = tempfile.mktemp(prefix="controllerlist-")
        f = open(self.listfile, 'w')
        f.write(TEST_LIST_OK2)
        f.close()
        yield self._start_container()
        procs = [
            {'name':'epu_controller_list','module':'epu.ionproc.controller_list',
                'class':'EPUControllerListService',
                'spawnargs' : {'controller_list_path' : self.listfile}},
                ]
        self.sup = yield self._spawn_processes(procs)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()
        if os.path.exists(self.listfile):
            os.remove(self.listfile)

    @defer.inlineCallbacks
    def test_controller_list_ok2(self):
        list_client = EPUControllerListClient()
        controller_list = yield list_client.list()
        assert len(controller_list) == 5
        expected = {'epu_controller_no4':False,
                    'epu_controller_no5':False,
                    'epu_controller_no6':False,
                    'epu_controller_no7':False,
                    'epu_controller_no8':False}
        for name in controller_list:
            assert expected.has_key(name)
            expected[name] = True
        for key in expected.keys():
            assert expected[key] is True
