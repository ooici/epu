from twisted.internet import defer
import os

from ion.core.process.process import ProcessFactory
from ion.core.process.service_process import ServiceProcess, ServiceClient
import ion.util.ionlog
from ion.core import ioninit
from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc

from epu.epumanagement import EPUManagement
from epu.ionproc.provisioner import ProvisionerClient

log = ion.util.ionlog.getLogger(__name__)

class EPUManagementService(ServiceProcess):
    """EPU management service interface

    See: https://confluence.oceanobservatories.org/display/syseng/CIAD+CEI+OV+Elastic+Computing
    """

    # Declaration of service
    declare = ServiceProcess.service_declare(name='epumanagement', version='0.1.0', dependencies=[])

    def slc_init(self):
        conf_dict = {} # TODO: get from filesystem and spawn_args
        
        self.epumanagement = EPUManagement(conf_dict, SubscriberNotifier(self), ProvisionerClient(self))

    def op_register_need(self, content, headers, msg):
        dt_id = content.get('dt_id')
        constraints = content.get('constraints')
        num_needed = content.get('num_needed')

        subscriber_name = content.get('subscriber_name') # Optional, does this work?
        subscriber_op = content.get('subscriber_op') # Optional, does this work?

        self.epumanagement.msg_register_need(None, dt_id, constraints, num_needed, subscriber_name, subscriber_op)

    def op_retire_node(self, content, headers, msg):
        node_id = content.get('node_id')
        self.epumanagement.msg_retire_node(None, node_id)

    def op_subscribe_dt(self, content, headers, msg):
        dt_id = content.get('dt_id')
        subscriber_name = content.get('subscriber_name')
        subscriber_op = content.get('subscriber_op')
        self.epumanagement.msg_subscribe_dt(None, dt_id, subscriber_name, subscriber_op)

    def op_unsubscribe_dt(self, content, headers, msg):
        dt_id = content.get('dt_id')
        subscriber_name = content.get('subscriber_name')
        self.epumanagement.msg_unsubscribe_dt(None, dt_id, subscriber_name)

    def op_add_epu(self, content, headers, msg):
        dt_id = content.get('dt_id')
        epu_config = content.get('epu_config')
        self.epumanagement.msg_add_epu(None, dt_id, epu_config)

    def op_remove_epu(self, content, headers, msg):
        dt_id = content.get('dt_id')
        self.epumanagement.msg_remove_epu(None, dt_id)

    def op_reconfigure_epu(self, content, headers, msg):
        dt_id = content.get('dt_id')
        epu_config = content.get('epu_config')
        self.epumanagement.msg_add_epu(None, dt_id, epu_config)
        
    def op_ou_heartbeat(self, content, headers, msg):
        self.epumanagement.msg_heartbeat(None, content) # epum parses

    def op_instance_info(self, content, headers, msg):
        self.epumanagement.msg_instance_info(None, content) # epum parses

    def op_sensor_info(self, content, headers, msg):
        self.epumanagement.msg_sensor_info(None, content) # epum parses

_unported_r1 = """
    @defer.inlineCallbacks
    def op_reconfigure_epu_rpc(self, content, headers, msg):
        yield self.epumanagement.msg_reconfigure_epu_rpc(None, content)
        yield self.reply_ok(msg, "")
        
    @defer.inlineCallbacks
    def op_de_state(self, content, headers, msg):
        state = self.core.de_state()
        extradict = {"state":state}
        cei_events.event(self.svc_name, "de_state", extra=extradict)
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
"""

class SubscriberNotifier(object):
    """See: ISubscriberNotifier
    """
    def __init__(self, ionprocess):
        self.ionprocess = ionprocess

    def notify_by_names(self, receiver_names):
        # TODO
        pass

    @defer.inlineCallbacks
    def notify(self, receiver):
        """backwards compatibility method, during refactor"""
        yield self.notify_by_object(receiver)

    @defer.inlineCallbacks
    def notify_by_object(self, receiver_object):

        # ION only:
        process = receiver_object
        if not process:
            defer.returnValue(None)

        subscribers = process.subscribers
        if not process.subscribers:
            defer.returnValue(None)

        process_dict = dict(epid=process.epid, round=process.round,
                            state=process.state, assigned=process.assigned)

        for name, op in subscribers:
            yield self.ionprocess.send(name, op, process_dict)


class EPUManagementClient(ServiceClient):
    """See: IEpuManagementClient
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "epumanagement"
        ServiceClient.__init__(self, proc, **kwargs)

    # TODO: make operations e.g. "msg_add_epu" with full argument list that translate into
    # the 'content' bag for ION that in turn calls corresponding e.g. "op_add_epu" via ION


factory = ProcessFactory(EPUManagementService)


@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('EPU Management Service starting, startup type "%s"' % starttype)

    config_name = __name__
    if os.environ.has_key("ION_CONFIGURATION_SECTION"):
        config_name = os.environ["ION_CONFIGURATION_SECTION"]
    conf = ioninit.config(config_name)

    # Required configurations for app-based launch
    spawnargs = {'servicename': conf['servicename'],
                 'engine_class' : conf.getValue('engine_class'),
                 'engine_conf' : conf.getValue('engine_conf')}

    # Required services.
    proc = [{'name': 'epu_controller',
             'module': __name__,
             'class': EPUManagementService.__name__,
             'spawnargs': spawnargs
            }]

    app_supv_desc = ProcessDesc(name='EPU Management Service app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':proc})

    supv_id = yield app_supv_desc.spawn()

    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('EPU Management Service stopping, state "%s"' % str(state))
    supdesc = state[0]
    # Return the deferred
    return supdesc.terminate()
