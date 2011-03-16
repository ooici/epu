#!/usr/bin/env python
from epu.ionproc.queuestat import QueueStatClient

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor

from ion.core.process.service_process import ServiceProcess
from ion.core.process.process import ProcessFactory
from ion.core import bootstrap
from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc
from ion.core import ioninit

from epu.epucontroller import ControllerCore
from epu.ionproc.provisioner import ProvisionerClient
from epu import cei_events

import simplejson as json

DEFAULT_NAME = "epu_controller_hcoded"

class EPUControllerService(ServiceProcess):
    """EPU Controller service interface
    """

    declare = ServiceProcess.service_declare(name=DEFAULT_NAME, version='0.1.0', dependencies=[])

    def slc_init(self):
        self.queue_name_work = self.get_scoped_name("system", self.spawn_args["queue_name_work"])
        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event(self.svc_name, "init_begin", log, extra=extradict)
        self.worker_queue = {self.queue_name_work:{'name_type':'worker'}}
        self.laterinitialized = False
        reactor.callLater(0, self.later_init)

        engineclass = "epu.decisionengine.impls.DefaultEngine"
        if self.spawn_args.has_key("engine_class"):
            engineclass = self.spawn_args["engine_class"]
            log.info("Using configured decision engine: %s" % engineclass)
        else:
            log.info("Using default decision engine: %s" % engineclass)

        if self.spawn_args.has_key("engine_conf"):
            engine_conf = self.spawn_args["engine_conf"]
            if isinstance(engine_conf, str):
                engine_conf = json.loads(engine_conf)
        else:
            engine_conf = None

        scoped_name = self.get_scoped_name("system", self.svc_name)
        self.core = ControllerCore(ProvisionerClient(self), engineclass, scoped_name, conf=engine_conf)

        self.core.begin_controlling()

    @defer.inlineCallbacks
    def later_init(self):
        yield bootstrap.declare_messaging(self.worker_queue)
        self.laterinitialized = True
        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event(self.svc_name, "init_end", log, extra=extradict)
        queuestat_client = QueueStatClient(self)
        yield queuestat_client.watch_queue(self.queue_name_work, self.svc_name, 'sensor_info')
        cei_events.event(self.svc_name, "queue_watched", log)

    def op_heartbeat(self, content, headers, msg):
        self.core.new_heartbeat(content)

    def op_sensor_info(self, content, headers, msg):
        if not self.laterinitialized:
            log.error("message got here without the later-init")
        self.core.new_sensor_info(content)
        
    def op_reconfigure(self, content, headers, msg):
        log.info("EPU Controller: reconfigure: '%s'" % content)
        self.core.run_reconfigure(content)

    def op_cei_test(self, content, headers, msg):
        log.info('EPU Controller: CEI test'+ content)

# Direct start of the service as a process with its default name
factory = ProcessFactory(EPUControllerService)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('EPU Controller starting, startup type "%s"' % starttype)

    conf = ioninit.config(__name__)

    # Required configurations for app-based launch
    spawnargs = {'queue_name_work' : conf['queue_name_work'],
                 'servicename': conf['servicename'],
                 'engine_class' : conf.getValue('engine_class'),
                 'engine_conf' : conf.getValue('engine_conf')}

    # Required services.
    proc = [{'name': 'epu_controller',
             'module': __name__,
             'class': EPUControllerService.__name__,
             'spawnargs': spawnargs
            }]

    app_supv_desc = ProcessDesc(name='EPU Controller app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':proc})

    supv_id = yield app_supv_desc.spawn()

    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('EPU Controller stopping, state "%s"' % str(state))
    supdesc = state[0]
    # Return the deferred
    return supdesc.terminate()
