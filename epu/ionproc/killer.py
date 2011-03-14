from epu.ionproc.provisioner import ProvisionerClient
import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer, reactor
from ion.core.process.service_process import ServiceProcess
from ion.core.process.process import ProcessFactory
from ion.core.pack import app_supervisor
from ion.core.process.process import ProcessDesc
from ion.core import ioninit

class SystemKiller(ServiceProcess):
    """
    This should be run before tearing down a running system with the launch tools (cloudinit.d).
    It will cause the provisioner to a) stop accepting new requests (to prevent race conditions)
    and b) terminate any instances that were launched via the provisioner.

    TODO: figure out a good way to fit one-off clients into the launch system (surely this can
    be done), but for now just masking the client call as a service so it drops right into what
    we have.
    """

    declare = ServiceProcess.service_declare(name='system_killer', version='6.6.6', dependencies=[])

    def slc_init(self, proc=None, **kwargs):
        self.client = ProvisionerClient()
        reactor.callLater(1, self.send_terminate_all)

    @defer.inlineCallbacks
    def send_terminate_all(self):
        log.critical("Instructing provisioner to tear down the system")
        self.client.terminate_all()

factory = ProcessFactory(SystemKiller)

@defer.inlineCallbacks
def start(container, starttype, *args, **kwargs):
    log.info('EPU Killer starting, startup type "%s"' % starttype)

    conf = ioninit.config(__name__)

    proc = [{'name': 'epu_killer',
             'module': __name__,
             'class': SystemKiller.__name__,
             'spawnargs': {}
            }]

    app_supv_desc = ProcessDesc(name='EPU killer app supervisor',
                                module=app_supervisor.__name__,
                                spawnargs={'spawn-procs':proc})

    supv_id = yield app_supv_desc.spawn()

    res = (supv_id.full, [app_supv_desc])
    defer.returnValue(res)

def stop(container, state):
    log.info('EPU Killer stopping, state "%s"' % str(state))
    supdesc = state[0]
    # Return the deferred
    return supdesc.terminate()