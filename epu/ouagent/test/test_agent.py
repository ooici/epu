import signal
import uuid
import tempfile
from ion.core.process.process import Process
import os

import twisted.internet.utils
from twisted.trial import unittest
from twisted.internet import defer

from epu.ionproc.ouagent import OUAgent
from epu.ouagent.agent import OUAgentCore

from epu.ouagent.supervisor import SupervisorError, \
    ProcessStates, Supervisor
from ion.test.iontest import IonTestCase
from ion.core import ioninit
from ion.util import procutils

CONF = ioninit.config(__name__)
from ion.util.itv_decorator import itv

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

NODE_ID = "the_node_id"

class OUAgentCoreTests(unittest.TestCase):
    def setUp(self):
        self.sup = FakeSupervisor()
        self.core = OUAgentCore(NODE_ID, supervisor=self.sup)

    def assertBasics(self, state, error=False):
        self.assertEqual(NODE_ID, state['node_id'])
        self.assertTrue(state['timestamp'])
        self.assertEqual("ERROR" if error else "OK", state['state'])

    @defer.inlineCallbacks
    def test_supervisor_error(self):
        self.sup.error = SupervisorError('faaaaaaaail')
        state = yield self.core.get_state()
        self.assertBasics(state, error=True)
        self.assertTrue('faaaaaaaail' in state['error'])

    @defer.inlineCallbacks
    def test_series(self):
        self.sup.processes = [_one_process(ProcessStates.RUNNING),
                              _one_process(ProcessStates.RUNNING)]
        state = yield self.core.get_state()
        self.assertBasics(state)

        # mark one of the processes as failed and give it a fake logfile
        # to read and send back
        fail = self.sup.processes[1]
        fail['state'] = ProcessStates.FATAL
        fail['exitstatus'] = -1

        stderr = "this is the errros!"
        err_path = _write_tempfile(stderr)
        fail['stderr_logfile'] = err_path
        try:
            state = yield self.core.get_state()
        finally:
            os.unlink(err_path)
            
        self.assertBasics(state, error=True)

        failed_processes = state['failed_processes']
        self.assertEqual(1, len(failed_processes))
        failed = failed_processes[0]
        self.assertEqual(stderr, failed['stderr'])

        # next time around process should still be failed but no stderr
        state = yield self.core.get_state()
        self.assertBasics(state, error=True)
        failed_processes = state['failed_processes']
        self.assertEqual(1, len(failed_processes))
        failed = failed_processes[0]
        self.assertFalse(failed.get('stderr'))

        # make it all ok again
        fail['state'] = ProcessStates.RUNNING
        state = yield self.core.get_state()
        self.assertBasics(state)

SUPERVISORD_CONF = """
[program:proc1]
command=/bin/cat
autorestart=false

[program:proc2]
command=/bin/cat
autorestart=false

[unix_http_server]
file=%(here)s/supervisor.sock

[supervisord]
logfile=%(here)s/supervisord.log
pidfile=%(here)s/supervisord.pid

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix://%(here)s/supervisor.sock
"""

class OUAgentIntegrationTests(IonTestCase):
    """Integration tests for OU Agent

    Uses real ION messaging, real supervisord, and real child processes.
    Requires supervisord command to be available in $PATH.
    (easy_install supervisor)
    """

    @itv(CONF)
    @defer.inlineCallbacks
    def setUp(self):
        yield self._start_container()
        self.subscriber = TestSubscriber()
        self.subscriber_id = yield self._spawn_process(self.subscriber)

        self.tmpdir = None
        self.supervisor = None

    @defer.inlineCallbacks
    def tearDown(self):
        yield self._shutdown_processes()
        yield self._stop_container()

        if self.supervisor:
            try:
                yield self.supervisor.shutdown()
            except:
                pass

            # shutting down takes awhile..
            sock = os.path.join(self.tmpdir, "supervisor.sock")
            i = 0
            while os.path.exists(sock) and i < 10:
                yield procutils.asleep(0.1)
                i += 1

        if self.tmpdir and os.path.exists(self.tmpdir):
            try:
                os.remove(os.path.join(self.tmpdir, "supervisord.conf"))
            except IOError:
                pass
            try:
                os.remove(os.path.join(self.tmpdir, "supervisord.log"))
            except IOError:
                pass
            try:
                os.rmdir(self.tmpdir)
            except IOError, e:
                log.warn("Failed to remove test temp dir %s: %s", self.tmpdir, e)

    @defer.inlineCallbacks
    def test_no_supervisor(self):

        # make up a nonexistent path
        sock = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
        agent = yield self._setup_agent(sock)

        yield agent.heartbeat()
        yield self.subscriber.deferred

        self.assertEqual(1, self.subscriber.beat_count)

        self.assertBasics(self.subscriber.last_beat, error=True)
        log.debug(self.subscriber.last_beat)

    
    @defer.inlineCallbacks
    def test_everything(self):

        yield self._setup_supervisord()

        sock = os.path.join(self.tmpdir, "supervisor.sock")
        self.supervisor = Supervisor(sock)

        agent = yield self._setup_agent(sock)

        for i in range(3):
            # automatic heartbeat is turned off so we don't deal with time
            yield agent.heartbeat()
            yield self.subscriber.deferred

            self.assertEqual(i+1, self.subscriber.beat_count)
            self.assertBasics(self.subscriber.last_beat)

        yield procutils.asleep(1.01)

        # now kill a process and see that it is reflected in heartbeat

        # use backdoor supervisor client to find PID
        procs = yield self.supervisor.query()
        self.assertEqual(2, len(procs))
        if procs[0]['name'] == 'proc1':
            proc1 = procs[0]
        elif procs[1]['name'] == 'proc1':
            proc1 = procs[1]
        else:
            proc1 = None #stifle pycharm warning
            self.fail("process proc1 not found")

        pid = proc1['pid']
        if not pid:
            self.fail('No PID for proc1')

        log.debug("Killing process %s", pid)
        os.kill(pid, signal.SIGTERM)

        yield agent.heartbeat()
        yield self.subscriber.deferred
        self.assertBasics(self.subscriber.last_beat, error=True)

        failed_processes = self.subscriber.last_beat['failed_processes']
        self.assertEqual(1, len(failed_processes))
        failed = failed_processes[0]
        self.assertEqual("proc1", failed['name'])

    @defer.inlineCallbacks
    def _setup_supervisord(self):
        supd_exe = which('supervisord')
        if not supd_exe:
            self.fail("supervisord executable not found in path!")

        self.tmpdir = tempfile.mkdtemp()

        conf = os.path.join(self.tmpdir, "supervisord.conf")

        f = None
        try:
            f = open(conf, 'w')
            f.write(SUPERVISORD_CONF)
        finally:
            if f:
                f.close()

        rc = yield twisted.internet.utils.getProcessValue(supd_exe,
                                                          args=('-c', conf))
        self.assertEqual(0, rc, "supervisord didn't start ok!")

    @defer.inlineCallbacks
    def _setup_agent(self, socket_path):
        spawnargs = {
            'heartbeat_dest': self.subscriber_id,
            'heartbeat_op': 'beat',
            'node_id': NODE_ID,
            'period_seconds': 2.0,
            'start_heartbeat': False,
            'supervisord': {
                'socket': socket_path
            }
        }
        agent = OUAgent(spawnargs=spawnargs)
        yield self._spawn_process(agent)
        defer.returnValue(agent)

    def assertBasics(self, state, error=False):
        self.assertEqual(NODE_ID, state['node_id'])
        self.assertTrue(state['timestamp'])
        self.assertEqual("ERROR" if error else "OK", state['state'])


class TestSubscriber(Process):
    def __init__(self, *args, **kwargs):
        Process.__init__(self, *args, **kwargs)
        self.last_beat = None
        self.beat_count = 0
        self.deferred = defer.Deferred()

    def op_beat(self, content, headers, msg):
        log.info('Got heartbeat: %s', content)
        self.last_beat = content
        self.beat_count += 1
        self.deferred.callback(content)
        self.deferred = defer.Deferred()

def _write_tempfile(text):
    fd,path = tempfile.mkstemp()
    f = None
    try:
        f = os.fdopen(fd, 'w')
        f.write(text)
    finally:
        if f:
            f.close()
    return path

def _one_process(state, exitstatus=0, spawnerr=''):
    return {'name' : str(uuid.uuid4()), 'state' : state,
            'exitstatus' :exitstatus, 'spawnerr' : spawnerr}

class FakeSupervisor(object):
    def __init__(self):
        self.error = None
        self.processes = None

    def query(self):
        if self.error:
            return defer.fail(self.error)
        return defer.succeed(self.processes)

def which(program):
    import os
    def is_exe(fpath):
        return os.path.exists(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None
