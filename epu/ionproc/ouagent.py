from twisted.internet import defer
from twisted.internet.task import LoopingCall

from ion.core.process.process import Process, ProcessFactory
from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc
from ion.core import ioninit

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
            sock = supd['socket']
            log.debug("monitoring a process supervisor at: %s", sock)
            self.supervisor = Supervisor(sock)
        else:
            log.debug("not monitoring process supervisor")
            self.supervisor = None

        self.core = OUAgentCore(self.node_id, supervisor=self.supervisor)

        self.loop = LoopingCall(self._loop)
        if start_beat:
            log.debug('Starting heartbeat loop - %s second interval', self.period)
            self.loop.start(self.period)

    def plc_terminate(self):
        self.loop.stop()

    def _loop(self):
        return self.heartbeat()

    @defer.inlineCallbacks
    def heartbeat(self):
        state = yield self.core.get_state()
        yield self.send(self.heartbeat_dest, self.heartbeat_op, state)

factory = ProcessFactory(OUAgent)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('OUAgent starting, startup type "%s"' % starttype)

    conf = ioninit.config("epu.universal")

    proc = [{'name': 'ouagent', 'module': __name__, 'class': OUAgent.__name__,
             'spawnargs':
                 {'node_id': conf['node_id'],
                  'heartbeat_dest': conf['heartbeat_dest'],
                  'heartbeat_op': conf.getValue('heartbeat_op', 'heartbeat'),
                  'period_seconds': conf.getValue('heartbeat_period', 5.0)}
            }]

    app_supv_desc = ProcessDesc(name='OUAgent app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs': proc})

    supv_id = yield app_supv_desc.spawn()

    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('OUAgent stopping, state "%s"' % str(state))
    supdesc = state[0]
    return supdesc.terminate()