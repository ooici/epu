import os.path
import socket

from mock import Mock

from epu.ganglia import GangliaClient
from nose.plugins.skip import SkipTest


class TestGangliaClient(object):

    def setup(self):

        with open(os.path.join(get_my_directory(), "ganglia.xml")) as gangxml:
            gangliaxml = gangxml.read()

        mock_poll_ganglia = Mock()
        mock_poll_ganglia.return_value = gangliaxml

        self.client = GangliaClient()
        self.client._poll_ganglia = mock_poll_ganglia
        try:
            self.client._poll_ganglia()
        except socket.error:
            raise SkipTest("Can't connect to Ganglia. Skipping")

    def test_poll_ganglia(self):
        xml = self.client._poll_ganglia()
        assert 'xml' in xml
        assert '</GANGLIA_XML>' in xml

    def test_parse_ganglia(self):

        ip = '10.0.2.15'

        xml = self.client._poll_ganglia()
        parsed = self.client._parse_ganglia(xml)
        assert len(parsed) == 1
        assert parsed[ip]['IP'] == ip


class TestGangliaClientWithRealGanglia(TestGangliaClient):
    """Run tests with a real Ganglia installation

       Skip them when there's a problem connecting to Ganglia
    """

    def setup(self):

        self.client = GangliaClient()
        try:
            self.client._poll_ganglia()
        except socket.error:
            raise SkipTest("Can't connect to Ganglia. Skipping")


def get_my_directory():
    return os.path.dirname(__file__)
