#!/usr/bin/env python
from ion.core.process.process import ProcessDesc, ProcessFactory

from twisted.internet import defer
from twisted.internet.task import LoopingCall

import ion.util.ionlog
from ion.core.process.service_process import ServiceProcess
from ion.core.pack import app_supervisor

from epu.ionproc.provisioner import ProvisionerClient

log = ion.util.ionlog.getLogger(__name__)

DEFAULT_QUERY_INTERVAL = 10.0

class ProvisionerQueryService(ServiceProcess):
    """Provisioner querying service
    """

    declare = ServiceProcess.service_declare(name='provisioner_query',
                                             version='0.1.0',
                                             dependencies=[])

    def slc_init(self):
        interval = float(self.spawn_args.get("interval_seconds",
                                             DEFAULT_QUERY_INTERVAL))

        self.client = ProvisionerClient(self)

        log.debug('Starting provisioner query loop - %s second interval',
                  interval)
        self.loop = LoopingCall(self.query)
        self.loop.start(interval)

    def slc_terminate(self):
        if self.loop:
            self.loop.stop()

    @defer.inlineCallbacks
    def query(self):
        try:
            yield self._do_query()
        except Exception,e:
            log.error("Error sending provisioner query request: %s", e,
                      exc_info=True)

    @defer.inlineCallbacks
    def _do_query(self):
        log.debug("Sending query request to provisioner")
        yield self.client.query()

        # This is an unfortunate hack to work around a memory leak in ion.
        # Some caches are only cleared after a received message is handled.
        # Since this process sends messages "spontaneously" -- triggered by a
        # LoopingCall -- we must manually clear the cache.
        self.message_client.workbench.manage_workbench_cache('Default Context')

factory = ProcessFactory(ProvisionerQueryService)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    proc = [{'name': 'provisioner',
             'module': __name__,
             'class': ProvisionerQueryService.__name__,
             'spawnargs': {}}]

    app_supv_desc = ProcessDesc(name='Provisioner Query app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':proc})

    supv_id = yield app_supv_desc.spawn()

    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    supdesc = state[0]
    return supdesc.terminate()