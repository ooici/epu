import logging

from dashi import bootstrap

from epu.processdispatcher.core import ProcessDispatcherCore
from epu.processdispatcher.store import ProcessDispatcherStore, ProcessDispatcherZooKeeperStore
from epu.processdispatcher.engines import EngineRegistry
from epu.processdispatcher.matchmaker import PDMatchmaker
from epu.dashiproc.epumanagement import EPUManagementClient
from epu.util import get_config_paths
import epu.dashiproc


log = logging.getLogger(__name__)


class ProcessDispatcherService(object):
    """PD service interface
    """



    def __init__(self, amqp_uri=None, topic="processdispatcher", registry=None,
                 store=None, epum_client=None, notifier=None, definition_id=None, domain_config=None):

        configs = ["service", "processdispatcher"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)
        self.topic = self.CFG.processdispatcher.get('service_name', topic)

        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG,
                                             amqp_uri=amqp_uri)

        engine_conf = self.CFG.processdispatcher.get('engines', {})
        default_engine = self.CFG.processdispatcher.get('default_engine')
        if default_engine is None and len(engine_conf.keys()) == 1:
            default_engine = engine_conf.keys()[0]
        self.store = store or self._get_processdispatcher_store()
        self.store.initialize()
        self.registry = registry or EngineRegistry.from_config(engine_conf, default=default_engine)
        self.eeagent_client = EEAgentClient(self.dashi)

        domain_definition_id = None
        base_domain_config = None
        # allow disabling communication with EPUM for epuharness case
        if epum_client:
            self.epum_client = epum_client
            domain_definition_id = definition_id
            base_domain_config = domain_config
        elif not self.CFG.processdispatcher.get('static_resources'):
            domain_definition_id = definition_id or self.CFG.processdispatcher.get('definition_id')
            base_domain_config = domain_config or self.CFG.processdispatcher.get('domain_config')
            self.epum_client = EPUManagementClient(self.dashi,
                "epu_management_service")

        else:
            self.epum_client = None

        if notifier:
            self.notifier = notifier
        else:
            self.notifier = SubscriberNotifier(self.dashi)

        self.core = ProcessDispatcherCore(self.store,
                                          self.registry,
                                          self.eeagent_client,
                                          self.notifier)

        launch_type = self.CFG.processdispatcher.get('launch_type', 'supd')

        self.matchmaker = PDMatchmaker(self.store, self.eeagent_client,
            self.registry, self.epum_client, self.notifier, self.topic,
            domain_definition_id, base_domain_config, launch_type)

    def start(self):
        self.dashi.handle(self.create_definition)
        self.dashi.handle(self.describe_definition)
        self.dashi.handle(self.update_definition)
        self.dashi.handle(self.remove_definition)
        self.dashi.handle(self.list_definitions)
        self.dashi.handle(self.schedule_process)
        self.dashi.handle(self.describe_process)
        self.dashi.handle(self.describe_processes)
        self.dashi.handle(self.restart_process)
        self.dashi.handle(self.terminate_process)
        self.dashi.handle(self.dt_state)
        self.dashi.handle(self.heartbeat, sender_kwarg='sender')
        self.dashi.handle(self.dump)

        self.matchmaker.start_election()

        try:
            self.dashi.consume()
        except KeyboardInterrupt:
            log.warning("Caught terminate signal. Bye!")
        else:
            log.info("Exiting normally. Bye!")
        self.dashi.disconnect()

    def stop(self):
        self.dashi.cancel()
        self.store.shutdown()

    def _make_process_dict(self, proc):
        return dict(upid=proc.upid, state=proc.state, round=proc.round,
                    assigned=proc.assigned)

    def create_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):
        self.core.create_definition(definition_id, definition_type, executable,
            name=name, description=description)

    def describe_definition(self, definition_id):
        return self.core.describe_definition(definition_id)

    def update_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):
        self.core.update_definition(definition_id, definition_type, executable,
            name=name, description=description)

    def remove_definition(self, definition_id):
        self.core.remove_definition(definition_id)

    def list_definitions(self):
        return self.core.list_definitions()

    def schedule_process(self, upid, definition_id, configuration=None,
                         subscribers=None, constraints=None,
                         queueing_mode=None, restart_mode=None,
                         execution_engine_id=None, node_exclusive=None):

        result = self.core.schedule_process(None, upid, definition_id,
            configuration, subscribers, constraints, queueing_mode=queueing_mode,
                restart_mode=restart_mode, node_exclusive=node_exclusive,
                execution_engine_id=execution_engine_id)
        return self._make_process_dict(result)

    def describe_process(self, upid):
        return self.core.describe_process(None, upid)

    def describe_processes(self):
        return self.core.describe_processes()

    def restart_process(self, upid):
        result = self.core.restart_process(None, upid)
        return self._make_process_dict(result)

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

    def _get_processdispatcher_store(self):

        zookeeper = self.CFG.get("zookeeper")
        if zookeeper:
            log.info("Using ZooKeeper ProcessDispatcher store")
            store = ProcessDispatcherZooKeeperStore(zookeeper['hosts'],
                zookeeper['processdispatcher_path'], zookeeper.get('timeout'))
        else:
            log.info("Using in-memory ProcessDispatcher store")
            store = ProcessDispatcherStore()
        return store


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
            self.dashi.fire(name, op, process=process_dict)


class EEAgentClient(object):
    """Client that uses ION to send messages to EEAgents
    """
    def __init__(self, dashi):
        self.dashi = dashi

    def launch_process(self, eeagent, upid, round, run_type, parameters):
        self.dashi.fire(eeagent, "launch_process", u_pid=upid, round=round,
                        run_type=run_type, parameters=parameters)

    def restart_process(self, eeagent, upid, round):
        return self.dashi.fire(eeagent, "restart_process", u_pid=upid,
                               round=round)

    def terminate_process(self, eeagent, upid, round):
        return self.dashi.fire(eeagent, "terminate_process", u_pid=upid,
                               round=round)

    def cleanup_process(self, eeagent, upid, round):
        return self.dashi.fire(eeagent, "cleanup", u_pid=upid, round=round)


class ProcessDispatcherClient(object):
    def __init__(self, dashi, topic):
        self.dashi = dashi
        self.topic = topic

    def create_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):
        args = dict(definition_id=definition_id, definition_type=definition_type,
            executable=executable, name=name, description=description)
        self.dashi.call(self.topic, "create_definition", args=args)

    def describe_definition(self, definition_id):
        return self.dashi.call(self.topic, "describe_definition",
            definition_id=definition_id)

    def update_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):
        args = dict(definition_id=definition_id, definition_type=definition_type,
            executable=executable, name=name, description=description)
        self.dashi.call(self.topic, "update_definition", args=args)

    def remove_definition(self, definition_id):
        self.dashi.call(self.topic, "remove_definition",
            definition_id=definition_id)

    def list_definitions(self):
        return self.dashi.call(self.topic, "list_definitions")

    def schedule_process(self, upid, definition_id, configuration=None,
                         subscribers=None, constraints=None,
                         queueing_mode=None, restart_mode=None,
                         execution_engine_id=None, node_exclusive=None):
        request = dict(upid=upid, definition_id=definition_id,
                       configuration=configuration,
                       subscribers=subscribers, constraints=constraints,
                       queueing_mode=queueing_mode, restart_mode=restart_mode,
                       execution_engine_id=execution_engine_id,
                       node_exclusive=node_exclusive)

        return self.dashi.call(self.topic, "schedule_process", args=request)

    def describe_process(self, upid):
        return self.dashi.call(self.topic, "describe_process", upid=upid)

    def describe_processes(self):
        return self.dashi.call(self.topic, "describe_processes")

    def restart_process(self, upid):
        return self.dashi.call(self.topic, 'restart_process', upid=upid)

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
    epu.dashiproc.epu_register_signal_stack_debug()
    pd = ProcessDispatcherService()
    pd.start()
