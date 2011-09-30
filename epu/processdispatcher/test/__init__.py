import time
import copy
import ion.util.ionlog

from ion.core.process.process import Process, ProcessFactory

log = ion.util.ionlog.getLogger(__name__)

class FakeEEAgent(Process):

    def plc_init(self):
        self.heartbeat_dest = self.spawn_args['heartbeat_dest']
        self.heartbeat_op = self.spawn_args['heartbeat_op']
        self.node_id = self.spawn_args['node_id']
        self.slot_count = int(self.spawn_args['slot_count'])

        self.processes = {}

    def op_dispatch(self, content, headers, msg):
        epid = content['epid']
        description = content['description']
        if epid not in self.processes:
            self.processes[epid] = description

    def op_terminate(self, content, headers, msg):
        epid = content['epid']
        self.processes.pop(epid)

    def make_heartbeat(self, timestamp=None):
        now = time.time() if timestamp is None else timestamp

        processes = copy.deepcopy(self.processes)
        available_slots = self.slot_count - len(processes)

        beat = dict(node_id=self.node_id, timestamp=now, processes=processes,
                    slot_count=available_slots)
        return beat

    def send_heartbeat(self, timestamp=None):
        beat = self.make_heartbeat(timestamp)
        log.debug("sending heartbeat to %s(%s): %s", self.heartbeat_dest,
                  self.heartbeat_op, beat)
        return self.send(self.heartbeat_dest, self.heartbeat_op, beat)


factory = ProcessFactory(FakeEEAgent)