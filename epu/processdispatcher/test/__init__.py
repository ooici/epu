from itertools import chain
import time
import logging

from ion.core.process.process import Process, ProcessFactory
from twisted.internet import defer

from epu.states import ProcessState

log = logging.getLogger(__name__)

class FakeEEAgent(Process):

    def plc_init(self):
        self.engine_type = self.spawn_args['engine_type']
        self.heartbeat_dest = self.spawn_args['heartbeat_dest']
        self.heartbeat_op = self.spawn_args['heartbeat_op']
        self.node_id = self.spawn_args['node_id']
        self.slot_count = int(self.spawn_args['slot_count'])

        self.processes = {}

        # keep around old processes til they are cleaned up
        self.history = []

    @defer.inlineCallbacks
    def op_dispatch(self, content, headers, msg):
        epid = content['epid']
        round = content['round']
        spec = content['spec']

        process = dict(epid=epid, spec=spec, state=ProcessState.RUNNING,
                       round=round)

        if epid not in self.processes:
            self.processes[epid] = process
        yield self.send_heartbeat()

    def op_terminate(self, content, headers, msg):
        epid = content['epid']
        process = self.processes.pop(epid)
        if process:
            process['state'] = ProcessState.TERMINATED
            self.history.append(process)
        return self.send_heartbeat()

    def op_cleanup(self, content, headers, msg):
        epid = content['epid']
        if epid in self.history:
            del self.history[epid]
        return defer.succeed(None)

    def make_heartbeat(self, timestamp=None):
        now = time.time() if timestamp is None else timestamp

        # processes format is a list of (epid, round, state) tuples
        processes = []
        for process in chain(self.processes.itervalues(), self.history):
            p = (process['epid'], process['round'], process['state'])
            processes.append(p)

        available_slots = self.slot_count - len(self.processes)

        beat = dict(node_id=self.node_id, timestamp=now, processes=processes,
                    slot_count=available_slots, engine_type=self.engine_type)
        return beat

    def send_heartbeat(self, timestamp=None):
        beat = self.make_heartbeat(timestamp)
        log.debug("sending heartbeat to %s(%s): %s", self.heartbeat_dest,
                  self.heartbeat_op, beat)
        return self.send(self.heartbeat_dest, self.heartbeat_op, beat)

    def fail_process(self, epid):
        process = self.processes.pop(epid)
        process['state'] = ProcessState.FAILED
        self.history.append(process)
        return self.send_heartbeat()


factory = ProcessFactory(FakeEEAgent)