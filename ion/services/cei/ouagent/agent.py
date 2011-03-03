from twisted.internet.task import LoopingCall
from ion.core.process.process import Process
from ion.services.cei.ouagent.supervisor import Supervisor


class OUAgent(Process):
    """"Operational Unit" Agent. Checks vitals in running VMs.
    """

    def plc_init(self):
        self.heartbeat_dest = self.spawn_args['heartbeat_dest']
        self.node_id = self.spawn_args['node_id']
        self.period = float(self.spawn_args['period_seconds'])

        supd = self.spawn_args.get('supervisord')
        if supd:
            self.supervisor = Supervisor(supd['socket'])
        else:
            self.supervisor = None

        self.loop = LoopingCall(self._loop)
        self.loop.start(self.period)

    def plc_terminate(self):
        self.loop.stop()

    def _loop(self):
        pass
