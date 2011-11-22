import unittest
import tempfile
import os

import epu
from epu.util import KEY_MESSAGING, KEY_SYSNAME, KEY_BROKERHOST, KEY_BROKERCREDFILE

OK = """
[%s]
%s: blah.com
%s: /home/blah/credfile.txt
%s: unique_sysname
""" % (KEY_MESSAGING, KEY_BROKERHOST, KEY_BROKERCREDFILE, KEY_SYSNAME)

OK2 = """
[%s]
%s: blah.com
%s: unique_sysname
""" % (KEY_MESSAGING, KEY_BROKERHOST, KEY_SYSNAME)

BAD1 = """
[%s]
broker_host_lies: blah.com
%s: unique_sysname
""" % (KEY_MESSAGING, KEY_SYSNAME)

BAD2 = """
[badsection]
%s: blah.com
%s: unique_sysname
""" % (KEY_BROKERHOST, KEY_SYSNAME)

BAD3 = """
[%s]
%s: blah.com
%s: /home/blah/credfile.txt
asdasd: unique_sysname
""" % (KEY_MESSAGING, KEY_BROKERHOST, KEY_BROKERCREDFILE)

BAD4 = """
"""

BAD5 = """
[%s]
%s: blah.com
""" % (KEY_MESSAGING, KEY_BROKERHOST)

class CEIUtilTestCase(unittest.TestCase):

    def test_okconf1(self):
        path = tempfile.mktemp(prefix="epu-util-test-")
        f = open(path, 'w')
        f.write(OK)
        f.close()
        d = epu.util.parse_messaging_config(path)
        assert len(d) == 3
        assert len(d[KEY_SYSNAME]) > 0
        assert len(d[KEY_BROKERHOST]) > 0
        assert len(d[KEY_BROKERCREDFILE]) > 0
        os.remove(path)

    def test_okconf2(self):
        path = tempfile.mktemp(prefix="epu-util-test-")
        f = open(path, 'w')
        f.write(OK2)
        f.close()
        d = epu.util.parse_messaging_config(path)
        assert len(d) == 3
        assert len(d[KEY_SYSNAME]) > 0
        assert len(d[KEY_BROKERHOST]) > 0
        assert d.has_key(KEY_BROKERCREDFILE)
        assert d[KEY_BROKERCREDFILE] is None
        os.remove(path)
        
    def test_badconf1(self):
        path = tempfile.mktemp(prefix="epu-util-test-")
        f = open(path, 'w')
        f.write(BAD1)
        f.close()
        self.failUnlessRaises(Exception, epu.util.parse_messaging_config, path)
        os.remove(path)

    def test_badconf2(self):
        path = tempfile.mktemp(prefix="epu-util-test-")
        f = open(path, 'w')
        f.write(BAD2)
        f.close()
        self.failUnlessRaises(Exception, epu.util.parse_messaging_config, path)
        os.remove(path)

    def test_badconf3(self):
        path = tempfile.mktemp(prefix="epu-util-test-")
        f = open(path, 'w')
        f.write(BAD3)
        f.close()
        self.failUnlessRaises(Exception, epu.util.parse_messaging_config, path)
        os.remove(path)

    def test_badconf4(self):
        path = tempfile.mktemp(prefix="epu-util-test-")
        f = open(path, 'w')
        f.write(BAD4)
        f.close()
        self.failUnlessRaises(Exception, epu.util.parse_messaging_config, path)
        os.remove(path)

    def test_badconf5(self):
        path = tempfile.mktemp(prefix="epu-util-test-")
        f = open(path, 'w')
        f.write(BAD5)
        f.close()
        self.failUnlessRaises(Exception, epu.util.parse_messaging_config, path)
        os.remove(path)
