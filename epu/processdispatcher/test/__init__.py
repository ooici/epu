from itertools import chain
import time
import logging

from epu.states import ProcessState

log = logging.getLogger(__name__)

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

        # processes format is a list of (u_pid, round, state) tuples
        processes = []
        for process in chain(self.processes.itervalues(), self.history):
            p = dict(upid=process['u_pid'], round=process['round'],
                     state=process['state'])
            processes.append(p)

        available_slots = self.slot_count - len(self.processes)

        beat = dict(node_id=self.node_id, timestamp=now, processes=processes,
                    slot_count=available_slots)
        return beat

    def send_heartbeat(self, timestamp=None):
        beat = self.make_heartbeat(timestamp)
        log.debug("sending heartbeat to %s: %s", self.heartbeat_dest, beat)
        self.dashi.fire(self.heartbeat_dest, "heartbeat",
                        sender=self.dashi.name, message=beat)

    def fail_process(self, u_pid):
        process = self.processes.pop(u_pid)
        process['state'] = ProcessState.FAILED
        self.history.append(process)
        self.send_heartbeat()
