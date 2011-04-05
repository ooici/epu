import pbs

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
        self.watched_queues = {}
        self.loop = LoopingCall(self._do_poll)

        self.pbs_server = pbs.pbs_default()
        self.pbs_con = pbs.pbs_connect(pbs_server)

    @defer.inlineCallbacks
    def _do_poll(self):
        if not self.watched_queues:
            log.debug('No queues are being watched, not querying Torque')
            defer.returnValue(None)
        log.debug('Querying Torque for queue information')

        torque_queues = pbs.pbs_statque(self.pbs_con, '', 'NULL', 'NULL')
        torque_queue_lengths = {}
        for queue_name in self.watch_queues.keys()
            torque_queue_lengths[queue_name] = 0
        for torque_queue in torque_queues:
            if torque_queue.name in self.watch_queues.keys():
                for attrib in torque_queue.attribs:
                    if attrib.name == 'total_jobs':
                        attrib_val = int(attrib.value)
                        torque_queue_lengths[torque_queue.name] += attrib_val
        for queue_name in self.watch_queues.keys():
            subscribers = self.watched_queues.get(queue_name, None)
            queue_length = torque_queue_lengths[queue_name]
            message = {'queue_name': queue_name, 'queue_length': queue_length}
            yield self._notify_subscribers(list(subscribers), message)

    @defer.inlineCallbacks
    def _notify_subscribers(self, subscribers, message):
        for name, op in subscribers:
            log.debug('Notifying subscriber %s (op: %s): %s', name, op, message)
            yield self.send(name, op, message)

    def op_watch_queue(self, content, headers, msg):
        """Start watching a queue for updates. If queue is already being
        watched by this subscriber, this operation does nothing.
        """
        log.debug("op_watch_queue content:"+str(content))
        queue_name = content.get('queue_name')
        subscriber_name = content.get('subscriber_name')
        subscriber_op = content.get('subscriber_op')

        sub_tuple = (subscriber_name, subscriber_op)

        queue_subs = self.watched_queues.get(queue_name, None)
        if queue_subs is None:
            queue_subs = set()
            self.watched_queues[queue_name] = queue_subs
        queue_subs.add(sub_tuple)

        if not self.loop.running:
            log.debug('starting LoopingCall, to poll queues')
            self.loop.start(self.interval)

    def op_unwatch_queue(self, content, headers, msg):
        """Stop watching a queue. If queue is not being watched by subscriber,
        this operation does nothing.
        """
        log.debug("op_unwatch_queue content:"+str(content))
        queue_name = content.get('queue_name')
        subscriber_name = content.get('subscriber_name')
        subscriber_op = content.get('subscriber_op')

        sub_tuple = (subscriber_name, subscriber_op)

        queue_subs = self.watched_queues.get(queue_name, None)
        if queue_subs:
            queue_subs.discard(sub_tuple)
            if not queue_subs:
                del self.watched_queues[queue_name]

        if not self.watched_queues and self.loop.running:
            log.debug('No queues are being watched, disabling LoopingCall')
            self.loop.stop()

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
    def watch_queue(self, subscriber, op, queue_name="default"):
        """Watch a queue.

        Updates will be sent to specified subscriber and operation
        """
        yield self._check_init()

        message = {'queue_name' : queue_name, 'subscriber_name' : subscriber,
                'subscriber_op' : op}
        log.debug("Sending Torque queue watch request: %s", message)
        yield self.send('watch_queue', message)

    @defer.inlineCallbacks
    def unwatch_queue(self, subscriber, op, queue_name="default"):
        """Stop watching a queue.
        """
        yield self._check_init()

        message = {'queue_name' : queue_name, 'subscriber_name' : subscriber,
                'subscriber_op' : op}
        log.debug("Sending Torque queue unwatch request: %s", message)
        yield self.send('unwatch_queue', message)

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
