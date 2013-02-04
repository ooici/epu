import logging

from dashi import bootstrap

from epu.processdispatcher.core import ProcessDispatcherCore
from epu.processdispatcher.store import get_processdispatcher_store
from epu.processdispatcher.engines import EngineRegistry
from epu.processdispatcher.matchmaker import PDMatchmaker
from epu.processdispatcher.doctor import PDDoctor
from epu.dashiproc.epumanagement import EPUManagementClient
from epu.util import get_config_paths
import epu.dashiproc


log = logging.getLogger(__name__)


class ProcessDispatcherService(object):
    """PD service interface
    """

    def __init__(self, amqp_uri=None, topic="process_dispatcher", registry=None,
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
        self.store = store or get_processdispatcher_store(self.CFG)
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
            epum_service_name = self.CFG.processdispatcher.get('epum_service_name',
                    'epu_management_service')
            self.epum_client = EPUManagementClient(self.dashi, epum_service_name)

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

        self.doctor = PDDoctor(self.core, self.store)

    def start(self):

        # start the doctor before we do anything else
        log.debug("Starting doctor election")
        self.doctor.start_election()

        log.debug("Waiting for Doctor to initialize the Process Dispatcher")
        # wait for the store to be initialized before proceeding. The doctor
        # (maybe not OUR doctor, but whoever gets elected), will check the
        # state of the system and then mark it as initialized.
        self.store.wait_initialized()

        self.dashi.handle(self.set_system_boot)
        self.dashi.handle(self.create_definition)
        self.dashi.handle(self.describe_definition)
        self.dashi.handle(self.update_definition)
        self.dashi.handle(self.remove_definition)
        self.dashi.handle(self.list_definitions)
        self.dashi.handle(self.create_process)
        self.dashi.handle(self.schedule_process)
        self.dashi.handle(self.describe_process)
        self.dashi.handle(self.describe_processes)
        self.dashi.handle(self.restart_process)
        self.dashi.handle(self.terminate_process)
        self.dashi.handle(self.node_state)
        self.dashi.handle(self.heartbeat, sender_kwarg='sender')
        self.dashi.handle(self.dump)

        self.matchmaker.start_election()

        try:
            self.dashi.consume()
        except KeyboardInterrupt:
            log.warning("Caught terminate signal. Bye!")
        else:
            log.info("Exiting normally. Bye!")

    def stop(self):
        self.dashi.cancel()
        self.dashi.disconnect()
        self.store.shutdown()

    def _make_process_dict(self, proc):
        return dict(upid=proc.upid, state=proc.state, round=proc.round,
                    assigned=proc.assigned)

    def set_system_boot(self, system_boot):
        self.core.set_system_boot(system_boot)

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

    def create_process(self, upid, definition_id, name=None):
        result = self.core.create_process(None, upid, definition_id, name=name)
        return self._make_process_dict(result)

    def schedule_process(self, upid, definition_id=None, configuration=None,
                         subscribers=None, constraints=None,
                         queueing_mode=None, restart_mode=None,
                         execution_engine_id=None, node_exclusive=None,
                         name=None):

        result = self.core.schedule_process(None, upid=upid,
            definition_id=definition_id, configuration=configuration,
            subscribers=subscribers, constraints=constraints,
            queueing_mode=queueing_mode, restart_mode=restart_mode,
            node_exclusive=node_exclusive,
            execution_engine_id=execution_engine_id, name=name)
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

    def node_state(self, node_id, domain_id, state, properties=None):
        self.core.node_state(node_id, domain_id, state, properties=properties)

    def heartbeat(self, sender, message):
        log.debug("got heartbeat from %s: %s", sender, message)
        self.core.ee_heartbeat(sender, message)

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

    def set_system_boot(self, system_boot):
        self.dashi.call(self.topic, "set_system_boot", system_boot=system_boot)

    def create_definition(self, definition_id, definition_type, executable,
                          name=None, description=None):
        args = dict(definition_id=definition_id, definition_type=definition_type,
            executable=executable, name=name, description=description)
        log.debug("Creating def in client %s" % args)
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

    def create_process(self, upid, definition_id, name=None):
        request = dict(upid=upid, definition_id=definition_id, name=name)
        return self.dashi.call(self.topic, "create_process", args=request)

    def schedule_process(self, upid, definition_id=None, configuration=None,
                         subscribers=None, constraints=None,
                         queueing_mode=None, restart_mode=None,
                         execution_engine_id=None, node_exclusive=None,
                         name=None):
        request = dict(upid=upid, definition_id=definition_id,
                       configuration=configuration,
                       subscribers=subscribers, constraints=constraints,
                       queueing_mode=queueing_mode, restart_mode=restart_mode,
                       execution_engine_id=execution_engine_id,
                       node_exclusive=node_exclusive, name=name)

        return self.dashi.call(self.topic, "schedule_process", args=request)

    def describe_process(self, upid):
        return self.dashi.call(self.topic, "describe_process", upid=upid)

    def describe_processes(self):
        return self.dashi.call(self.topic, "describe_processes")

    def restart_process(self, upid):
        return self.dashi.call(self.topic, 'restart_process', upid=upid)

    def terminate_process(self, upid):
        return self.dashi.call(self.topic, 'terminate_process', upid=upid)

    def node_state(self, node_id, domain_id, state, properties=None):

        request = dict(node_id=node_id, domain_id=domain_id, state=state)
        if properties is not None:
            request['properties'] = properties

        self.dashi.call(self.topic, 'node_state', args=request)

    def dump(self):
        return self.dashi.call(self.topic, 'dump')


def main():
    logging.basicConfig(level=logging.DEBUG)
    epu.dashiproc.epu_register_signal_stack_debug()
    pd = ProcessDispatcherService()
    pd.start()
