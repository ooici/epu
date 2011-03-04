import uuid
import os
import tempfile

from twisted.trial import unittest
from twisted.internet.error import ConnectError

from ion.services.cei.ouagent.supervisor import UnixProxy

class UnixProxyTests(unittest.TestCase):
    def test_error_nofile(self):
        noexist = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))

        proxy = UnixProxy(noexist)
        self.failUnlessFailure(proxy.callRemote("method.name"), ConnectError)



