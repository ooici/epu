import logging

from dashi import bootstrap

from epu.processdispatcher.lightweight import ExecutionEngineRegistry, \
    ProcessDispatcherCore
from epu.util import get_config_paths

log =  logging.getLogger(__name__)

class ProcessDispatcherService(object):
    """PD service interface
    """

    def __init__(self, amqp_uri=None, topic="processdispatcher"):

        configs = ["service", "processdispatcher"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)
        self.topic = self.CFG.processdispatcher.get('topic', topic)

        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG,
                                             amqp_uri=amqp_uri)

        self.registry = ExecutionEngineRegistry()
        self.eeagent_client = EEAgentClient(self.dashi)
        self.notifier = SubscriberNotifier(self.dashi)
        self.core = ProcessDispatcherCore(self.registry, self.eeagent_client,
                                          self.notifier)

    def start(self):
        self.dashi.handle(self.dispatch_process)
        self.dashi.handle(self.terminate_process)
        self.dashi.handle(self.dt_state)
        self.dashi.handle(self.heartbeat, sender_kwarg='sender')
        self.dashi.handle(self.dump)

        self.dashi.consume()

    def stop(self):
        self.dashi.cancel()

    def _make_process_dict(self, proc):
        return dict(upid=proc.upid, state=proc.state, round=proc.round,
                    assigned=proc.assigned)

    def dispatch_process(self, upid, spec, subscribers, constraints, immediate=False):
        result = self.core.dispatch_process(upid, spec, subscribers,
                                                  constraints, immediate)
        return self._make_process_dict(result)

    def terminate_process(self, upid):
        result = self.core.terminate_process(upid)
        return self._make_process_dict(result)

    def dt_state(self, node_id, deployable_type, state):
        self.core.dt_state(node_id, deployable_type, state)

    def heartbeat(self, sender, message):
        log.debug("got heartbeat from %s", sender)
        self.core.ee_heartbeart(sender, message)

    def dump(self):
        return self.core.dump()


class SubscriberNotifier(object):
    def __init__(self, dashi):
        self.dashi = dashi

    def notify_process(self, process):
        if not process:
            return

        subscribers = process.subscribers
        if not process.subscribers:
            return

        process_dict = dict(upid=process.upid, round=process.round,
                            state=process.state, assigned=process.assigned)

        for name, op in subscribers:
            yield self.dashi.fire(name, op, process_dict)


class EEAgentClient(object):
    """Client that uses ION to send messages to EEAgents
    """
    def __init__(self, dashi):
        self.dashi = dashi

    def launch_process(self, eeagent, upid, round, run_type, parameters):
        self.dashi.fire(eeagent, "launch_process", u_pid=upid, round=round,
                        run_type=run_type, parameters=parameters)

    def terminate_process(self, eeagent, upid, round):
        return self.dashi.fire(eeagent, "terminate_process", u_pid=upid,
                               round=round)

    def cleanup_process(self, eeagent, upid, round):
        return self.dashi.fire(eeagent, "cleanup", u_pid=upid, round=round)


class ProcessDispatcherClient(object):
    def __init__(self, dashi, topic):
        self.dashi = dashi
        self.topic = topic

    def dispatch_process(self, upid, spec, subscribers, constraints=None,
                         immediate=False):
        request = dict(upid=upid, spec=spec, immediate=immediate,
                       subscribers=subscribers, constraints=constraints)

        return self.dashi.call(self.topic, "dispatch_process", args=request)

    def terminate_process(self, upid):
        return self.dashi.call(self.topic, 'terminate_process', upid=upid)

    def dt_state(self, node_id, deployable_type, state, properties=None):

        request = dict(node_id=node_id, deployable_type=deployable_type,
                       state=state)
        if properties is not None:
            request['properties'] = properties

        self.dashi.call(self.topic, 'dt_state', args=request)

    def dump(self):
        return self.dashi.call(self.topic, 'dump')
