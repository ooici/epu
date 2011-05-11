#!/usr/bin/env python

"""
@file epu/ionproc/controller_list.py
@brief Provides list of active EPU Controller service names in the system
"""

import os

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.process.process import ProcessFactory, ProcessDesc
from ion.core.pack import app_supervisor

class EPUControllerListService(ServiceProcess):
    """Provides list of EPU Controller service names
    """

    declare = ServiceProcess.service_declare(name='epu_controller_list',
            version='0.1.0', dependencies=[])

    def slc_init(self):
        # Allow direct list for tests, etc.
        self.controller_list = self.spawn_args.get('controller_list_direct', None)

        # It's valid to have a zero length list, only check 'is None'
        if self.controller_list is None:
            controller_list_path = self.spawn_args.get('controller_list_path', None)
            if not controller_list_path:
                raise Exception("There is no 'controller_list_path' configuration")
            self.controller_list = self._intake_file(controller_list_path)

        if self.controller_list:
            log.debug("Initialized with controller list:\n%s\n" % self.controller_list)
        else:
            log.debug("Initialized with empty controller list")

    def _intake_file(self, controller_list_path):
        if not os.path.exists(controller_list_path):
            raise Exception("The 'controller_list_path' file does not exist: %s" % controller_list_path)
        controller_list = []
        f = open(controller_list_path)
        for line in f.readlines():
            name = line.strip()
            if not name:
                continue
            if name.startswith("#"):
                continue
            controller_list.append(name)
        return controller_list

    def plc_terminate(self):
        log.debug('EPU Controller List service: shutdown triggered')

    @defer.inlineCallbacks
    def op_list(self, content, headers, msg):
        """Return a list of zero to N controller names
        """
        yield self.reply_ok(msg, self.controller_list)


class EPUControllerListClient(ServiceClient):
    """Client for querying EPUControllerListService
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "epu_controller_list"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def list(self):
        """Query the EPUControllerListService
        """
        yield self._check_init()
        log.debug("Sending EPU controller list query")
        (content, headers, msg) = yield self.rpc_send('list', {})
        defer.returnValue(content)

# Direct start of the service as a process with its default name
factory = ProcessFactory(EPUControllerListService)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('EPU Controller List service starting, startup type "%s"' % starttype)

    proc = [{'name': 'epu_controller_list',
             'module': __name__,
             'class': EPUControllerListService.__name__,
             'spawnargs': { }
            },
    ]

    app_supv_desc = ProcessDesc(name='EPU Controller List supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':proc})

    supv_id = yield app_supv_desc.spawn()
    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('EPU Controller List stopping, state "%s"' % str(state))
    supdesc = state[0]
    return supdesc.terminate()
