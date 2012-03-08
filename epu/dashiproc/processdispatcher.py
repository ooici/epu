import logging

import gevent
from dashi import bootstrap

from epu.processdispatcher.core import ProcessDispatcherCore
from epu.processdispatcher.store import ProcessDispatcherStore
from epu.processdispatcher.engines import EngineRegistry
from epu.processdispatcher.matchmaker import PDMatchmaker
from epu.util import get_config_paths

log =  logging.getLogger(__name__)

class ProcessDispatcherService(object):
    """PD service interface
    """

    def __init__(self, amqp_uri=None, topic="processdispatcher", registry=None,
                 store=None):

        configs = ["service", "processdispatcher"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)
        self.topic = self.CFG.processdispatcher.get('topic', topic)

        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG,
                                             amqp_uri=amqp_uri)

        engine_conf = self.CFG.processdispatcher.get('engines', {})
        self.store =  store or ProcessDispatcherStore()
        self.registry = registry or EngineRegistry.from_config(engine_conf)
        self.eeagent_client = EEAgentClient(self.dashi)

        # allow disabling communication with EPUM for epuharness case
        if not self.CFG.processdispatcher.get('static_resources'):
            self.epum_client = EpuManagementClient(self.dashi,
                subscriber_name=self.topic, subscriber_op='dt_state')
        else:
            self.epum_client = None

        self.notifier = SubscriberNotifier(self.dashi)
        self.core = ProcessDispatcherCore(self.topic, self.store,
                                          self.registry,
                                          self.eeagent_client,
                                          self.notifier)

        self.matchmaker = PDMatchmaker(self.store, self.eeagent_client,
            self.registry, self.epum_client)

    def start(self):
        self.dashi.handle(self.dispatch_process)
        self.dashi.handle(self.describe_process)
        self.dashi.handle(self.describe_processes)
        self.dashi.handle(self.terminate_process)
        self.dashi.handle(self.dt_state)
        self.dashi.handle(self.heartbeat, sender_kwarg='sender')
        self.dashi.handle(self.dump)

        self.matchmaker.initialize()
        self.matchmaker_thread = gevent.spawn_link_exception(self.matchmaker.run)

        try:
            self.dashi.consume()
        except KeyboardInterrupt:
            log.warning("Caught terminate signal. Bye!")
        else:
            log.info("Exiting normally. Bye!")

    def stop(self):
        self.dashi.cancel()
        self.dashi.disconnect()

        if self.matchmaker_thread:
            self.matchmaker.cancel()
            self.matchmaker_thread.join()

    def _make_process_dict(self, proc):
        return dict(upid=proc.upid, state=proc.state, round=proc.round,
                    assigned=proc.assigned)

    def dispatch_process(self, upid, spec, subscribers, constraints, immediate=False):
        result = self.core.dispatch_process(None, upid, spec, subscribers,
                                                  constraints, immediate)
        return self._make_process_dict(result)

    def describe_process(self, upid):
        return self.core.describe_process(None, upid)

    def describe_processes(self):
        return self.core.describe_processes()

    def terminate_process(self, upid):
        result = self.core.terminate_process(None, upid)
        return self._make_process_dict(result)

    def dt_state(self, node_id, deployable_type, state, properties=None):
        self.core.dt_state(node_id, deployable_type, state,
            properties=properties)

    def heartbeat(self, sender, message):
        log.debug("got heartbeat from %s: %s", sender, message)
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


class EpuManagementClient(object):

    def __init__(self, dashi, topic="epu_management_service",
                 subscriber_name=None, subscriber_op=None):
        self.dashi = dashi
        self.topic = topic
        self.subscriber_name = subscriber_name
        self.subscriber_op = subscriber_op

    def register_need(self, dt_id, constraints, num_needed,
                      subscriber_name=None, subscriber_op=None):
        if not subscriber_name:
            subscriber_name = self.subscriber_name
        if not subscriber_op:
            subscriber_op = self.subscriber_op

        self.dashi.fire(self.topic, "register_need", dt_id=dt_id,
                        constraints=constraints, num_needed=num_needed,
                        subscriber_name=subscriber_name, subscriber_op=subscriber_op)

    def retire_node(self, node_id):
        self.dashi.fire(self.topic, "retire_node", node_id=node_id)


class ProcessDispatcherClient(object):
    def __init__(self, dashi, topic):
        self.dashi = dashi
        self.topic = topic

    def dispatch_process(self, upid, spec, subscribers, constraints=None,
                         immediate=False):
        request = dict(upid=upid, spec=spec, immediate=immediate,
                       subscribers=subscribers, constraints=constraints)

        return self.dashi.call(self.topic, "dispatch_process", args=request)

    def describe_process(self, upid):
        return self.dashi.call(self.topic, "describe_process", upid=upid)

    def describe_processes(self):
        return self.dashi.call(self.topic, "describe_processes")

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

def main():
    logging.basicConfig(level=logging.DEBUG)
    pd = ProcessDispatcherService()
    pd.start()