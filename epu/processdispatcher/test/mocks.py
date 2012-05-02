from itertools import chain
import time
import logging
import threading
from collections import defaultdict

from epu.states import ProcessState
from epu.exceptions import NotFoundError

log = logging.getLogger(__name__)


class MockResourceClient(object):
    def __init__(self):
        self.launches = []

    @property
    def launch_count(self):
        return len(self.launches)

    def launch_process(self, eeagent, upid, round, run_type, parameters):
        self.launches.append((eeagent, upid, round, run_type, parameters))

    def check_process_launched(self, process, resource_id=None):
        for eeagent, upid, round, run_type, parameters in self.launches:
            if upid == process.upid and round == process.round and \
                resource_id is None or resource_id == eeagent:
                return True

        raise Exception("Process %s not launched: launches: %s",
                        process, self.launches)

    def terminate_process(self, eeagent, upid, round):
        pass

    def cleanup_process(self, eeagent, upid, round):
        pass

class MockEPUMClient(object):
    def __init__(self):
        self.reconfigures = defaultdict(list)

        self.condition = threading.Condition()
        self.domains = {}
        self.domain_subs = defaultdict(list)

    def describe_epu(self, domain_id):
        #TODO this doesn't return the real describe format
        got_domain = self.domains.get(domain_id)
        if not got_domain:
            raise NotFoundError("Couldn't find domain %s" % domain_id)
        else:
            return got_domain

    def add_epu(self, domain_id, config, subscriber_name=None,
                subscriber_op=None):
        assert domain_id not in self.domains
        self.domains[domain_id] = config
        if subscriber_name and subscriber_op:
            self.domain_subs[domain_id].append((subscriber_name, subscriber_op))

    def reconfigure_epu(self, domain_id, config):
        with self.condition:
            self.reconfigures[domain_id].append(config)

    def clear(self):
        self.reconfigures.clear()
        self.domains.clear()
        self.domain_subs.clear()

    def assert_needs(self, need_counts, domain_id=None):
        if domain_id:
            domain_reconfigures = self.reconfigures[domain_id]
        else:
            assert len(self.reconfigures) == 1
            domain_reconfigures = self.reconfigures.values()[0]

        assert len(need_counts) == len(domain_reconfigures)
        for reconfigure, expected in zip(domain_reconfigures, need_counts):
            assert reconfigure['engine_conf']['preserve_n'] == expected

class MockNotifier(object):
    def __init__(self):
        self.processes = {}
        self.condition = threading.Condition()

    def notify_process(self, process):
        process_dict = dict(upid=process.upid, round=process.round,
            state=process.state, assigned=process.assigned)

        with self.condition:
            self.processes[process.upid] = process_dict
            self.condition.notify_all()

    def wait_for_state(self, upid, state, timeout=3):
        start = time.time()
        while True:
            with self.condition:
                process = self.processes.get(upid)
                if process and process['state'] == state:
                    return

                elapsed = time.time() - start
                if elapsed >= timeout:
                    raise Exception("timeout waiting for state")

                self.condition.wait(timeout - elapsed)

    def assert_process_state(self, upid, state):
        assert upid in self.processes, "process unknown"
        actual = self.processes[upid]['state']
        assert actual == state, "expected state %s, actual %s" % (state, actual)


class FakeEEAgent(object):
    def __init__(self, dashi, heartbeat_dest, node_id, slot_count):
        self.dashi = dashi
        self.heartbeat_dest = heartbeat_dest
        self.node_id = node_id
        self.slot_count = int(slot_count)

        self.processes = {}

        # keep around old processes til they are cleaned up
        self.history = []

    def start(self):
        self.dashi.handle(self.launch_process)
        self.dashi.handle(self.terminate_process)
        self.dashi.handle(self.cleanup)

        self.dashi.consume()

    def stop(self):
        self.dashi.cancel()

    def launch_process(self, u_pid, round, run_type, parameters):

        process = dict(u_pid=u_pid, run_type=run_type, parameters=parameters,
                       state=ProcessState.RUNNING, round=round)

        log.debug("got launch_process request: %s", process)

        if u_pid not in self.processes:
            self.processes[u_pid] = process
        self.send_heartbeat()

    def terminate_process(self, u_pid, round):
        process = self.processes.pop(u_pid)
        if process:
            process['state'] = ProcessState.TERMINATED
            self.history.append(process)
        self.send_heartbeat()

    def cleanup(self, u_pid, round):
        if u_pid in self.history:
            del self.history[u_pid]

    def make_heartbeat(self, timestamp=None):
        now = time.time() if timestamp is None else timestamp

        processes = []
        for process in chain(self.processes.itervalues(), self.history):
            p = dict(upid=process['u_pid'], round=process['round'],
                     state=process['state'])
            processes.append(p)

        beat = dict(timestamp=now, processes=processes)
        return beat

    def send_heartbeat(self, timestamp=None):
        beat = self.make_heartbeat(timestamp)
        log.debug("sending heartbeat to %s: %s", self.heartbeat_dest, beat)
        self.dashi.fire(self.heartbeat_dest, "heartbeat", message=beat)

    def fail_process(self, u_pid):
        process = self.processes.pop(u_pid)
        process['state'] = ProcessState.FAILED
        self.history.append(process)
        self.send_heartbeat()

    def exit_process(self, u_pid):
        process = self.processes.pop(u_pid)
        process['state'] = ProcessState.EXITED
        self.history.append(process)
        self.send_heartbeat()

def get_domain_config():
    engine_class = "epu.decisionengine.impls.needy.NeedyEngine"
    general = {"engine_class": engine_class}
    health = {"health": False}
    engine = {}
    return {"general":general, "engine_conf": engine, "health": health}
