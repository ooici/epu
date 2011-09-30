from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.ionlog

from epu.processdispatcher.lightweight import ExecutionEngineRegistry, \
    ProcessDispatcherCore

log = ion.util.ionlog.getLogger(__name__)

class ProcessDispatcherService(ServiceProcess):
    """PD service interface
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='processdispatcher',
                                             version='0.1.0', dependencies=[])

    def slc_init(self):
        self.registry = ExecutionEngineRegistry()
        self.core = ProcessDispatcherCore(self.registry)

    @defer.inlineCallbacks
    def op_dispatch_process(self, content, headers, msg):
        epid = content['epid']
        engine_type = content['engine_type']
        description = content['description']
        subscribers = content['subscribers']
        constraints = content.get('constraints')
        immediate = bool(content.get('immediate'))

        result = yield self.core.dispatch_process(epid, engine_type,
                                                  description, subscribers,
                                                  constraints, immediate)
        yield self.reply_ok(msg, result)

    @defer.inlineCallbacks
    def op_terminate_process(self, content, headers, msg):
        epid = content['epid']

        result = yield self.core.terminate_process(epid)
        yield self.reply_ok(msg, result)

    def op_dt_state(self, content, headers, msg):
        node_id = content['node_id']
        deployable_type = content['deployable_type']
        state = content['state']

        return self.core.dt_state(node_id, deployable_type, state)

    def op_ee_heartbeat(self, content, headers, msg):
        log.debug('heartbeatheaders: %s', headers)
        sender = headers.get('sender')
        if sender is None:
            log.warn("Got EE heartbeat without a sender header! Ignoring: %s", content)
            return defer.succeed(None)
        return self.core.ee_heartbeart(sender, content)

    @defer.inlineCallbacks
    def op_dump(self, content, headers, msg):
        state = yield self.core.dump()
        yield self.reply_ok(msg, state)


class ProcessDispatcherClient(ServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "processdispatcher"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def dispatch_process(self, epid, engine_type, description, subscribers,
                         constraints=None, immediate=False):
        yield self._check_init()
        request = dict(epid=epid, engine_type=engine_type,
                       description=description, subscribers=subscribers,
                       constraints=constraints, immediate=immediate)
        process, headers, msg = yield self.rpc_send('dispatch_process', request)
        defer.returnValue(process)

    @defer.inlineCallbacks
    def terminate_process(self, epid):
        yield self._check_init()
        request = dict(epid=epid)
        process, headers, msg = yield self.rpc_send('terminate_process', request)
        defer.returnValue(process)

    @defer.inlineCallbacks
    def dt_state(self, node_id, deployable_type, state, properties=None):
        yield self._check_init()

        request = dict(node_id=node_id, deployable_type=deployable_type,
                       state=state)
        if properties is not None:
            request['properties'] = properties

        yield self.send('dt_state', request)

    @defer.inlineCallbacks
    def dump(self):
        yield self._check_init()
        state, headers, msg = yield self.rpc_send('dump', None)
        defer.returnValue(state)

factory = ProcessFactory(ProcessDispatcherService)
