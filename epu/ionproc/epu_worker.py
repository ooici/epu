#!/usr/bin/env python

import time
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor
from ion.core.messaging.receiver import WorkerReceiver
from ion.core.process.service_process import ServiceProcess
from ion.core.process.process import ProcessFactory
import ion.util.procutils as pu
from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc
from ion.core import ioninit

from epu import cei_events

class EPUWorkerService(ServiceProcess):
    """EPU Worker service.
    """
    declare = ServiceProcess.service_declare(name='epu_worker', version='0.1.0', dependencies=[])

    def slc_init(self):
        queue_name = self.spawn_args["queue_name_work"]
        self.workReceiver = WorkerReceiver(name=queue_name,
                                           label=__name__,
                                           scope=WorkerReceiver.SCOPE_SYSTEM,
                                           handler=self.receive)
        self.queue_name_work = self.workReceiver.xname
        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event("worker", "init_begin", log, extra=extradict)
        self.laterinitialized = False
        reactor.callLater(0, self.later_init)

    @defer.inlineCallbacks
    def later_init(self):
        spawnId = yield self.workReceiver.attach()
        log.debug("spawnId: %s" % spawnId)
        self.laterinitialized = True
        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event("worker", "init_end", log, extra=extradict)

    @defer.inlineCallbacks
    def op_work(self, content, headers, msg):
        if not self.laterinitialized:
            log.error("message got here without the later-init")
        sleepsecs = int(content['work_amount'])
        extradict = {"batchid":content['batchid'],
                     "jobid":content['jobid'],
                     "work_amount":sleepsecs}
        cei_events.event("worker", "job_begin", log, extra=extradict)
        log.info("WORK: sleeping for %d seconds ---" % sleepsecs)
        yield pu.asleep(sleepsecs)
        yield self.reply(msg, 'result', {'result':'work_complete'}, {})
        cei_events.event("worker", "job_end", log, extra=extradict)


# Direct start of the service as a process with its default name
factory = ProcessFactory(EPUWorkerService)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('EPU Worker starting, startup type "%s"' % starttype)

    conf = ioninit.config(__name__)
    spawnargs = {'queue_name_work' : conf['queue_name_work']}

    # Required services.
    proc = [{'name': 'epu_worker',
             'module': __name__,
             'class': EPUWorkerService.__name__,
             'spawnargs': spawnargs
            }]

    app_supv_desc = ProcessDesc(name='EPU Worker app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':proc})

    supv_id = yield app_supv_desc.spawn()

    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('EPU Worker stopping, state "%s"' % str(state))
    supdesc = state[0]
    # Return the deferred
    return supdesc.terminate()