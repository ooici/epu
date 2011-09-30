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
        self.eeagent_client = EEAgentClient(self)
        self.core = ProcessDispatcherCore(self.registry, self.eeagent_client)

    def _make_process_dict(self, proc):
        return dict(epid=proc.epid, state=proc.state, round=proc.round,
                    assigned=proc.assigned)

    @defer.inlineCallbacks
    def op_dispatch_process(self, content, headers, msg):
        epid = content['epid']
        spec = content['spec']
        subscribers = content['subscribers']
        constraints = content.get('constraints')
        immediate = bool(content.get('immediate'))

        result = yield self.core.dispatch_process(epid, spec, subscribers,
                                                  constraints, immediate)
        yield self.reply_ok(msg, self._make_process_dict(result))

    @defer.inlineCallbacks
    def op_terminate_process(self, content, headers, msg):
        epid = content['epid']

        result = yield self.core.terminate_process(epid)
        yield self.reply_ok(msg, self._make_process_dict(result))

    def op_dt_state(self, content, headers, msg):
        node_id = content['node_id']
        deployable_type = content['deployable_type']
        state = content['state']

        return self.core.dt_state(node_id, deployable_type, state)

    def op_ee_heartbeat(self, content, headers, msg):
        sender = headers.get('sender')
        if sender is None:
            log.warn("Got EE heartbeat without a sender header! Ignoring: %s", content)
            return defer.succeed(None)
        sender = self.get_scoped_name("system", sender)
        return self.core.ee_heartbeart(sender, content)

    @defer.inlineCallbacks
    def op_dump(self, content, headers, msg):
        state = yield self.core.dump()
        yield self.reply_ok(msg, state)


class EEAgentClient(object):
    """Client that uses ION to send messages to EEAgents
    """
    def __init__(self, process):
        self.process = process

    def dispatch_process(self, eeagent, epid, spec):
        request = dict(epid=epid, spec=spec)
        return self.process.send(eeagent, "dispatch", request)

    def terminate_process(self, eeagent, epid):
        request = dict(epid=epid)
        return self.process.send(eeagent, "terminate", request)


class ProcessDispatcherClient(ServiceClient):
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "processdispatcher"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def dispatch_process(self, epid, spec, subscribers, constraints=None,
                         immediate=False):
        yield self._check_init()
        request = dict(epid=epid, spec=spec, immediate=immediate,
                       subscribers=subscribers, constraints=constraints)
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
