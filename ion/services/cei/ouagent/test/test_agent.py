import uuid
import tempfile
import os

from twisted.trial import unittest
from twisted.internet import defer

from ion.services.cei.ouagent.agent import OUAgentCore
from ion.services.cei.ouagent.supervisor import SupervisorError, ProcessStates

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
        self.assertTrue('faaaaaaaail' in state['supervisor_error'])

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
