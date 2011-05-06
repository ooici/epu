#!/usr/bin/env python
from ion.core.messaging.receiver import ServiceWorkerReceiver
from epu.epucontroller.controller_core import CoreInstance
from epu.epucontroller.controller_store import CassandraControllerStore, ControllerStore
from epu.epucontroller.forengine import SensorItem
from epu.ionproc.queuestat import QueueStatClient

import ion.util.ionlog
import os

log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.service_process import ServiceProcess
from ion.core.process.process import ProcessFactory
from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc
from ion.core import ioninit

from epu.epucontroller import ControllerCore
from epu.ionproc.provisioner import ProvisionerClient
from epu import cei_events, cassandra

import simplejson as json

DEFAULT_NAME = "epu_controller_hcoded"

class EPUControllerService(ServiceProcess):
    """EPU Controller service interface
    """

    declare = ServiceProcess.service_declare(name=DEFAULT_NAME, version='0.1.0', dependencies=[])

    @defer.inlineCallbacks
    def slc_init(self):

        queue_name_work = self.spawn_args["queue_name_work"]
        self.queue_name_work = self.get_scoped_name("system", queue_name_work)

        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event(self.svc_name, "init_begin", log, extra=extradict)
        self._make_queue(queue_name_work)

        scoped_name = self.get_scoped_name("system", self.svc_name)
        self.scoped_name = scoped_name

        queuestat_client = QueueStatClient(self)
        yield queuestat_client.watch_queue(self.queue_name_work, self.scoped_name, 'sensor_info')
        cei_events.event(self.svc_name, "queue_watched", log)

        engineclass = "epu.decisionengine.impls.NpreservingEngine"
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

        if self.spawn_args.has_key("cassandra"):
            cassandra = self.spawn_args["cassandra"]
            host = cassandra['hostname']
            username = cassandra['username']
            password = cassandra['password']
            port = cassandra['port']
            keyspace = cassandra['keyspace']

            store = CassandraControllerStore(self.svc_name, host, port,
                                             username, password, keyspace,
                                             CoreInstance, SensorItem)
        else:
            store = ControllerStore()

        self.core = ControllerCore(ProvisionerClient(self), engineclass,
                                   scoped_name, conf=engine_conf, store=store)

        # run state recovery and engine initialization
        yield self.core.run_initialize()

        self.core.begin_controlling()
        cei_events.event(self.svc_name, "init_end", log, extra=extradict)

    @defer.inlineCallbacks
    def _make_queue(self, name):
        self.worker_queue_receiver = ServiceWorkerReceiver(
            label=name,
            name=name,
            scope='system')
        yield self.worker_queue_receiver.initialize()

    def op_heartbeat(self, content, headers, msg):
        log.debug("Got node heartbeat: %s", content)
        return self.core.new_heartbeat(content)

    def op_instance_state(self, content, headers, msg):
        return self.core.new_instance_state(content)

    def op_sensor_info(self, content, headers, msg):
        return self.core.new_sensor_info(content)
        
    def op_reconfigure(self, content, headers, msg):
        log.info("EPU Controller: reconfigure: '%s'" % content)
        return self.core.run_reconfigure(content)

    @defer.inlineCallbacks
    def op_reconfigure_rpc(self, content, headers, msg):
        log.info("EPU Controller: reconfigure_rpc: '%s'" % content)
        yield self.core.run_reconfigure(content)
        yield self.reply_ok(msg, "")

    @defer.inlineCallbacks
    def op_de_state(self, content, headers, msg):
        state = self.core.de_state()
        extradict = {"state":state}
        cei_events.event(self.svc_name, "de_state", log, extra=extradict)
        yield self.reply_ok(msg, state)

    @defer.inlineCallbacks
    def op_whole_state(self, content, headers, msg):
        state = yield self.core.whole_state()
        yield self.reply_ok(msg, state)

    @defer.inlineCallbacks
    def op_node_error(self, content, headers, msg):
        node_id = content
        state = yield self.core.node_error(node_id)
        yield self.reply_ok(msg, state)


# Direct start of the service as a process with its default name
factory = ProcessFactory(EPUControllerService)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('EPU Controller starting, startup type "%s"' % starttype)

    config_name = __name__
    if os.environ.has_key("ION_CONFIGURATION_SECTION"):
        config_name = os.environ["ION_CONFIGURATION_SECTION"]
    conf = ioninit.config(config_name)

    # Required configurations for app-based launch
    spawnargs = {'queue_name_work' : conf['queue_name_work'],
                 'servicename': conf['servicename'],
                 'engine_class' : conf.getValue('engine_class'),
                 'engine_conf' : conf.getValue('engine_conf')}

    use_cassandra = conf.getValue('cassandra', True)
    if use_cassandra:
        try:
            spawnargs['cassandra'] = cassandra.get_config()

        except cassandra.CassandraConfigurationError,e:
            log.error("Problem loading Cassandra config: %s", e)
            raise

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
