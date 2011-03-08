from twisted.internet import defer
from twisted.internet.task import LoopingCall

from ion.core.process.process import Process
from epu.ouagent.supervisor import Supervisor
from epu.ouagent.agent import OUAgentCore

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

class OUAgent(Process):
    """"Operational Unit" Agent. Checks vitals in running VMs.
    """

    def plc_init(self):
        self.heartbeat_dest = self.spawn_args['heartbeat_dest']
        self.heartbeat_op = self.spawn_args['heartbeat_op']
        self.node_id = self.spawn_args['node_id']
        self.period = float(self.spawn_args['period_seconds'])

        # for testing, allow for not starting heartbeat automatically
        start_beat = self.spawn_args.get('start_heartbeat', True)

        supd = self.spawn_args.get('supervisord')
        if supd:
            self.supervisor = Supervisor(supd['socket'])
        else:
            self.supervisor = None

        self.core = OUAgentCore(self.node_id, supervisor=self.supervisor)

        self.loop = LoopingCall(self._loop)
        if start_beat:
            self.loop.start(self.period)

    def plc_terminate(self):
        self.loop.stop()

    def _loop(self):
        return self.heartbeat()

    @defer.inlineCallbacks
    def heartbeat(self):
        state = yield self.core.get_state()
        yield self.send(self.heartbeat_dest, self.heartbeat_op, state)
