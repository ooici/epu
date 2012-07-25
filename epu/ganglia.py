
import socket

from xml.etree import ElementTree
from StringIO import StringIO

BUFFER = 1024


class GangliaClient(object):

    def __init__(self, hostname='localhost', port=8649, buffer_size=BUFFER):

        self.hostname = hostname
        self.port = port
        self.buffer_size = buffer_size

        self.host_info = {}

    def _poll_ganglia(self):

        ganglia_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ganglia_socket.connect((self.hostname, self.port))

        ganglia_xml = ""

        while True:
            data = ganglia_socket.recv(self.buffer_size)
            if not data:
                break
            ganglia_xml += data

        ganglia_socket.close()

        return ganglia_xml

    def _parse_ganglia(self, ganglia_xml):

        info = {}

        parsed = ElementTree.iterparse(StringIO(ganglia_xml))
        for action, elem in parsed:
            if elem.tag == "HOST":
                host = dict(elem.attrib)
                metrics = {}
                for metric_element in elem.findall("METRIC"):
                    metric = dict(metric_element.attrib)
                    extra = {}
                    for extra_element in metric_element.findall(".//EXTRA_ELEMENT"):
                        extra[extra_element.get('NAME')] = extra_element.get('VAL')
                    metric['EXTRA_DATA'] = extra
                    metrics[metric['NAME']] = metric
                host['metrics'] = metrics

                info[host['NAME']] = host

        return info

    def query(self):
        return self._parse_ganglia(self._poll_ganglia)

