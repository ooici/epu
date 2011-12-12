import logging
import os

from dashi import bootstrap

from epu.processdispatcher.lightweight import ExecutionEngineRegistry, \
    ProcessDispatcherCore
from epu.util import determine_path

log =  logging.getLogger(__name__)

class ProcessDispatcherService(object):
    """PD service interface
    """

    def __init__(self):

        configs = ["service", "processdispatcher"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        self.dashi = bootstrap.dashi_connect(self.CFG.epumanagement.service_name, self.CFG)

        self.registry = ExecutionEngineRegistry()
        self.eeagent_client = EEAgentClient(self)
        self.notifier = SubscriberNotifier(self)
        self.core = ProcessDispatcherCore(self.registry, self.eeagent_client,
                                          self.notifier)

    def start(self):
        self.dashi.handle(self.dispatch_process)
        self.dashi.handle(self.terminate_process)
        self.dashi.handle(self.dt_state)
        self.dashi.handle(self.ee_heartbeat)
        self.dashi.handle(self.dump)

        self.dashi.consume()

    def _make_process_dict(self, proc):
        return dict(epid=proc.epid, state=proc.state, round=proc.round,
                    assigned=proc.assigned)

    def dispatch_process(self, epid, spec, subscribers, constraints, immediate=False):
        result = self.core.dispatch_process(epid, spec, subscribers,
                                                  constraints, immediate)
        return self._make_process_dict(result)

    def terminate_process(self, epid):
        result = self.core.terminate_process(epid)
        return self._make_process_dict(result)

    def dt_state(self, node_id, deployable_type, state):
        self.core.dt_state(node_id, deployable_type, state)

    def ee_heartbeat(self, sender, heartbeat):
        self.core.ee_heartbeart(sender, heartbeat)

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

        process_dict = dict(epid=process.epid, round=process.round,
                            state=process.state, assigned=process.assigned)

        for name, op in subscribers:
            yield self.dashi.fire(name, op, process_dict)


class EEAgentClient(object):
    """Client that uses ION to send messages to EEAgents
    """
    def __init__(self, dashi):
        self.dashi = dashi

    def dispatch_process(self, eeagent, epid, round, spec):
        self.dashi.fire(eeagent, "dispatch", epid=epid, round=round, spec=spec)

    def terminate_process(self, eeagent, epid):
        return self.dashi.send(eeagent, "terminate", epid=epid)

    def cleanup_process(self, eeagent, epid, round):
        return self.dashi.send(eeagent, "cleanup", epid=epid, round=round)


class ProcessDispatcherClient(object):
    def __init__(self, dashi, topic):
        self.dashi = dashi
        self.topic = topic

    def dispatch_process(self, epid, spec, subscribers, constraints=None,
                         immediate=False):
        request = dict(epid=epid, spec=spec, immediate=immediate,
                       subscribers=subscribers, constraints=constraints)

        return self.dashi.call(self.topic, "dispatch_process", args=request)

    def terminate_process(self, epid):
        return self.dashi.call(self.topic, 'terminate_process', epid=epid)

    def dt_state(self, node_id, deployable_type, state, properties=None):

        request = dict(node_id=node_id, deployable_type=deployable_type,
                       state=state)
        if properties is not None:
            request['properties'] = properties

        self.dashi.fire(self.topic, 'dt_state', args=request)

    def dump(self):
        return self.dashi.call('dump')
