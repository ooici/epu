from ion.core.process.process import ProcessFactory, ProcessDesc
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from ion.core import ioninit
from twisted.internet import defer
from twisted.internet.task import LoopingCall
from ion.core.process.service_process import ServiceProcess, ServiceClient
from ion.core.pack import app_supervisor

from epu import sensors


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
        self.loop = LoopingCall(self._wrapped_do_poll)

        # some stupidness to allow testing without having pbs lib present
        self.pbs = self.spawn_args.get("pbs")
        if self.pbs is None:
            import pbs
            self.pbs = pbs

        self.pbs_server = self.pbs.pbs_default()
        self.pbs_con = self.pbs.pbs_connect(self.pbs_server)

    @defer.inlineCallbacks
    def _wrapped_do_poll(self):
        try:
            yield self._do_poll()
        except Exception,e:
            log.error("Error in polling call: %s", str(e), exc_info=True)

    @defer.inlineCallbacks
    def _do_poll(self):
        if not self.watched_queues:
            log.debug('No queues are being watched, not querying Torque')
            defer.returnValue(None)
        log.debug('Querying Torque for queue information')
        log.debug('Watched queues: %s' % self.watched_queues)

        torque_queues = self.pbs.pbs_statque(self.pbs_con, '', 'NULL', 'NULL')
        errno, txt = self.pbs.error()
        if errno != 0:
            log.error("Problem getting Torque queue information: %s" % txt)
        else:
            log.debug("Successfully retreived Torque queue information")

        torque_workers = self.pbs.pbs_statnode(self.pbs_con, '', 'NULL', 'NULL')
        errno, txt = self.pbs.error()
        if errno != 0:
            log.error("Problem getting Torque node information: %s" % txt)
        else:
            log.debug("Successfully retreived Torque node information")
            log.debug("Nodes: %s" % [x.name for x in torque_workers])

        worker_status = self._get_worker_status(torque_workers)
        log.debug("Got worker status: %s" % worker_status)

        torque_queue_lengths = {}
        torque_worker_status = {}
        for queue_name in self.watched_queues:
            torque_queue_lengths[queue_name] = 0
            # for now, this will be the same regardless of the queue name
            torque_worker_status[queue_name] = worker_status

        for torque_queue in torque_queues:
            if torque_queue.name in self.watched_queues:
                for attrib in torque_queue.attribs:
                    if attrib.name == 'state_count':
                        states = attrib.value.split()
                        queued_state = states[1]
                        queued_jobs = int(queued_state.split(':')[1].strip())
                        torque_queue_lengths[torque_queue.name] += queued_jobs

        for queue_name in self.watched_queues.keys():
            subscribers = self.watched_queues.get(queue_name, None)
            queue_length = torque_queue_lengths[queue_name]
            message = {'queue_name': queue_name, 'queue_length': queue_length}
            log.debug("Sending queue_length message: %s" % message)
            sensor_message = sensors.sensor_message("queue-length", message)
            yield self._notify_subscribers(list(subscribers), sensor_message)

            worker_status = torque_worker_status[queue_name]
            message = {'queue_name': queue_name, 'worker_status': worker_status}
            sensor_message = sensors.sensor_message("worker-status", message)
            log.debug("Sending worker_status message: %s" % message)
            yield self._notify_subscribers(list(subscribers), sensor_message)

    def _get_worker_status(self, workers):
        status = ''
        for worker in workers:
            status += worker.name
            status += ':'
            state = ''
            for attrib in worker.attribs:
                if attrib.name == 'state':
                    state = attrib.value
            status += state
            status += ';'
        return status.rstrip(';')

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
        self.reply_ok(msg)

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
        self.reply_ok(msg)

    def op_add_node(self, content, headers, msg):
        log.debug("Got add_node request: %s", content)
        try:
            hostname = content['hostname']
        except KeyError,e:
            log.error("Bad request. missing '%s'" % e)

        self.pbs.pbs_manager(self.pbs_con, self.pbs.MGR_CMD_CREATE, self.pbs.MGR_OBJ_NODE,
          hostname, 'NULL', 'NULL')
        errno, errtxt = self.pbs.error()
        if errno != 0:
            err = "Error adding node (%s): %s" % (errno, errtxt)
            log.error(err)
            self.reply_err(msg, err)
        else:
            log.debug("Successfully added node: %s" % hostname)
            self.reply_ok(msg)

    def op_remove_node(self, content, headers, msg):
        log.debug("Got remove_node request: %s", content)
        try:
            hostname = content['hostname']
        except KeyError,e:
            log.error("Bad request. missing '%s'" % e)

        self.pbs.pbs_manager(self.pbs_con, self.pbs.MGR_CMD_DELETE, self.pbs.MGR_OBJ_NODE,
          hostname, 'NULL', 'NULL')
        errno, errtxt = self.pbs.error()
        if errno != 0:
            err = "Error removing node (%s): %s" % (errno, errtxt)
            log.error(err)
            self.reply_err(msg, err)
        else:
            log.debug("Successfully removed node: %s" % hostname)
            self.reply_ok(msg)

    def op_offline_node(self, content, headers, msg):
        log.debug("Got offline_node request: %s", content)
        try:
            hostname = content['hostname']
        except KeyError,e:
            log.error("Bad request. missing '%s'" % e)

        attribs = self.pbs.new_attropl(1)
        attribs[0].name = self.pbs.ATTR_NODE_state
        attribs[0].value = 'offline'
        attribs[0].op = self.pbs.SET

        self.pbs.pbs_manager(self.pbs_con, self.pbs.MGR_CMD_SET, self.pbs.MGR_OBJ_NODE,
          hostname, attribs, 'NULL')
        errno, errtxt = self.pbs.error()
        if errno != 0:
            err = "Error marking node offline (%s): %s" % (errno, errtxt)
            log.error(err)
            self.reply_err(msg, err)
        else:
            log.debug("Successfully marked node offline: %s" % hostname)
            self.reply_ok(msg)


            
class TorqueManagerClient(ServiceClient):
    """Client for managing a Torque head node over ION
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "torque"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def watch_queue(self, subscriber, op="sensor_info", queue_name="default"):
        """Watch a queue.

        Updates will be sent to specified subscriber and operation
        """
        yield self._check_init()

        message = {'queue_name' : queue_name, 'subscriber_name' : str(subscriber),
                'subscriber_op' : op}
        log.debug("Sending Torque queue watch request: %s", message)
        yield self.rpc_send('watch_queue', message)

    @defer.inlineCallbacks
    def unwatch_queue(self, subscriber, op="sensor_info", queue_name="default"):
        """Stop watching a queue.
        """
        yield self._check_init()

        message = {'queue_name' : queue_name, 'subscriber_name' : str(subscriber),
                'subscriber_op' : op}
        log.debug("Sending Torque queue unwatch request: %s", message)
        yield self.rpc_send('unwatch_queue', message)

    @defer.inlineCallbacks
    def add_node(self, hostname):
        """Adds a node to torque pool.

        The node must already be setup and running a torque mum
        """
        yield self._check_init()

        m = {'hostname' : hostname}
        self.rpc_send('add_node', m)

    @defer.inlineCallbacks
    def remove_node(self, hostname):
        """Removes a node from the torque pool.
        """
        yield self._check_init()

        m = {'hostname' : hostname}
        self.rpc_send('remove_node', m)

    @defer.inlineCallbacks
    def offline_node(self, hostname):
        """Offlines a node in the torque pool.
        """
        yield self._check_init()

        m = {'hostname' : hostname}
        self.rpc_send('offline_node', m)

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
