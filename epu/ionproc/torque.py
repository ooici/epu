from ion.core.process.process import ProcessFactory, ProcessDesc
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from twisted.internet import defer
from twisted.internet.task import LoopingCall
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.pack import app_supervisor

DEFAULT_INTERVAL_SECONDS = 3.0

class TorqueManagerService(ServiceProcess):
    """ION interface for torque server. Node management, queue monitoring.
    """
    declare = ServiceProcess.service_declare(name='torque',
            version='0.1.0', dependencies=[])

    def slc_init(self):
        self.interval = float(self.spawn_args.get('interval_seconds',
                DEFAULT_INTERVAL_SECONDS))
        self.subscriber = self.spawn_args.get('subscriber')

        if self.subscriber:
            self.loop = LoopingCall(self._do_poll)
            log.info("Starting Torque queue polling loop. subscriber=%s  "+
                     "interval=%s", self.subscriber, self.interval)
            self.loop.start(self.interval, now=False)

    def _do_poll(self):
        """Queries torque for current queue size, and sends to subscriber
        """
        #TODO

    def op_add_node(self, content, headers, msg):
        log.debug("Got add_node request: %s", content)
        try:
            hostname = content['hostname']
        except KeyError,e:
            log.error("Bad request. missing '%s'" % e)

        #TODO

    def op_remove_node(self, content, headers, msg):
        log.debug("Got remove_node request: %s", content)
        try:
            hostname = content['hostname']
        except KeyError,e:
            log.error("Bad request. missing '%s'" % e)
        #TODO

            
class TorqueManagerClient(ServiceClient):
    """Client for managing a Torque head node over ION
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "torque"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def add_node(self, hostname):
        """Adds a node to torque pool.

        The node must already be setup and running a torque mum
        """
        yield self._check_init()

        m = {'hostname' : hostname}
        self.send('add_node', m)

        #TODO what else is needed?

    @defer.inlineCallbacks
    def remove_node(self, hostname):
        """Removes a node from the torque pool.
        """
        yield self._check_init()

        m = {'hostname' : hostname}
        self.send('remove_node', m)

        #TODO what else is needed?

factory = ProcessFactory(TorqueManagerService)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('Torque manager starting, startup type "%s"' % starttype)

    conf = ioninit.config(__name__)

    proc = [{'name': 'torque',
             'module': __name__,
             'class': TorqueManagerService.__name__,
             'spawnargs': {
                 'interval_seconds' : conf.getValue('interval_seconds'),
                 'subscriber' : conf.getValue('subscriber'),
                 }
            },
    ]

    app_supv_desc = ProcessDesc(name='Torque manager app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':proc})

    supv_id = yield app_supv_desc.spawn()
    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('Torque manager stopping, state "%s"' % str(state))
    supdesc = state[0]
