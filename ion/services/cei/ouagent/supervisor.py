import xmlrpclib

from twisted.internet import defer, reactor
from twisted.internet.error import ConnectError
from twisted.web.xmlrpc import Proxy

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

# this state information is copied from supervisord source, to avoid
# otherwise needless dependency
class ProcessStates:
    STOPPED = 0
    STARTING = 10
    RUNNING = 20
    BACKOFF = 30
    STOPPING = 40
    EXITED = 100
    FATAL = 200
    UNKNOWN = 1000

STOPPED_STATES = (ProcessStates.STOPPED,
                  ProcessStates.EXITED,
                  ProcessStates.FATAL,
                  ProcessStates.UNKNOWN)

RUNNING_STATES = (ProcessStates.RUNNING,
                  ProcessStates.BACKOFF,
                  ProcessStates.STARTING)


class Supervisor(object):
    """Interface to supervisord process via XML-RPC over UNIX socket

    @note There are many other operations available than what is being used.
    See http://supervisord.org/api.html for a list. It should also be possible
    to run the "listMethods" and "methodHelp" operations to get information
    directly from the service.
    """

    def __init__(self, address):
        self.address = address
        self.proxy = UnixProxy(address)

    def query(self):
        """Checks supervisord for process information
        """
        return self._call("getAllProcessInfo")

    def shutdown(self):
        """Gracefully terminates all processes and the supervisor itself
        """
        return self._call("shutdown")

    @defer.inlineCallbacks
    def _call(self, method, namespace='supervisor', *args):

        m = namespace and "%s.%s" % (namespace, method) or method

        try:
            log.debug("Remote call to supervisord: method=%s args=%s",
                      method, args)

            resp = yield self.proxy.callRemote(m, *args)
            defer.returnValue(resp)

        except ConnectError, e:
            raise SupervisorError("UNIX socket (%s) connection error: %s"
                                  % (self.address, e))

        except xmlrpclib.Fault, e:
            raise SupervisorError("Remote fault: %s" % e)

        except xmlrpclib.Error, e:
            raise SupervisorError("XMLRPC error: %s" % e)


class SupervisorError(Exception):
    """A problem communicating with the supervisor daemon
    """
    def __str__(self):
        s = self.__doc__ or self.__class__.__name__
        if self[0]:
            s = '%s: %s' % (s, self[0])
        return s


class UnixProxy(object):
    """XMLRPC proxy that uses via UNIX sockets

    Uses twisted.web.xmlrpc for everything except connection. Written
    specifically to work with supervisord, probably not applicable elsewhere.
    """

    # arguably too tightly coupled..
    queryFactory = Proxy.queryFactory

    def __init__(self, address, allowNone=False):
        """Create XML-RPC over Unix sockets proxy. No connection made until
        callRemote is used.

        @param address path to socket file
        @param allowNone allow the use of None values in parameters.
        """
        self.address = address
        self.allowNone = allowNone

    def callRemote(self, method, *args):
        """Call remote XML-RPC method with given arguments.

        @param method remote method to execute
        """
        def cancel(d):
            factory.deferred = None
            connector.disconnect()

        factory = Proxy.queryFactory(
            "/RPC2",         # hardcoded; i think this is always the same
            "localhost",     # hostname irrelevant but still sent here
            method,
            args=args,
            allowNone=self.allowNone,
            canceller=cancel)
        connector = reactor.connectUNIX(self.address, factory)

        return factory.deferred




__all__ = ['ProcessStates', 'STOPPED_STATES', 'RUNNING_STATES', 'Supervisor',
           'SupervisorError']