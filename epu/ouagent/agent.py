import time

from twisted.internet import defer

from epu.ouagent.supervisor import RUNNING_STATES, SupervisorError

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class OUAgentCore(object):
    """Core state detection of Operational Unit Agent
    """
    def __init__(self, node_id, supervisor=None):
        self.node_id = node_id
        self.supervisor = supervisor

        # We only want to send log information at first sign of failure.
        # After that we just send basic information declaring that the
        # process is still dead. Cache it here.
        self.fail_cache = {}

    @defer.inlineCallbacks
    def get_state(self):
        state = self._base_state()

        if not self.supervisor:
            defer.returnValue(state)

        sup_errors = yield self._supervisor_errors()
        if sup_errors:
            state.update(sup_errors)
            state['state'] = 'ERROR'
        else:
            state['state'] = 'OK'
        defer.returnValue(state)

    def _base_state(self):
        return {'node_id' : self.node_id,
                'timestamp' : time.time(),
        }

    @defer.inlineCallbacks
    def _supervisor_errors(self):
        try:
            failed = yield self._failed_processes()

            if failed:
                ret = {'failed_processes' : failed}
            else:
                ret = None
            defer.returnValue(ret)

        except SupervisorError, e:
            log.error("Error querying supervisord: %s", e)
            ret = {'supervisor_error' : str(e)}
            defer.returnValue(ret)

    @defer.inlineCallbacks
    def _failed_processes(self):
        procs = yield self.supervisor.query()

        failed = None
        for proc in procs:
            state = proc['state']
            if state not in RUNNING_STATES:
                proc_fail = self._one_process_failure(proc)
                if failed is None:
                    failed = [proc_fail]
                else:
                    failed.append(proc_fail)

            else:
                # remove from failure list if present
                self.fail_cache.pop(proc['name'], None)

        defer.returnValue(failed)

    def _one_process_failure(self, proc):
        name = proc['name']
        prev = self.fail_cache.get(name)

        if (prev and prev.get('state') == proc.get('state') and
            prev.get('exitcode') == proc.get('exitstatus') and
            prev.get('stop_timestamp') == proc.get('stop')):
            return dict(prev)

        failure = {'name': proc.get('name'), 'state': proc.get('state'),
                   'statename': proc.get('statename'),
                   'exitcode': proc.get('exitstatus'),
                   'stop_timestamp': proc.get('stop') or None,
                   'error': proc.get('spawnerr')}

        # store in cache then make a copy and add detailed error info
        # only want that the first time

        self.fail_cache[name] = failure
        failure = dict(failure)

        stderr_path = proc.get('stderr_logfile')
        if stderr_path:
            failure['stderr'] = _get_file(stderr_path)

        return failure


def _get_file(path):
    if not path:
        return None
    f = None
    try:
        f = open(path)
        return f.read()
    except IOError, e:
        log.warn('Failed to read file: %s: %s', path, e)
        return None
    finally:
        if f:
            f.close()