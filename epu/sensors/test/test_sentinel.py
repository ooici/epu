# Copyright 2013 University of Chicago

import os
import urllib2

from StringIO import StringIO
from mock import Mock
from nose.plugins.skip import SkipTest
from datetime import datetime, timedelta

from epu.sensors import Statistics
from epu.sensors.trafficsentinel import TrafficSentinel, _extract_app_attribute
from pprint import PrettyPrinter
pprint = PrettyPrinter(indent=2).pprint


class TestTrafficSentinel(object):

    def setup(self):

        host = 'fake'
        username = 'fake'
        password = 'fake'
        self.mock_traffic_sentinel = True

        self.traffic_sentinel = TrafficSentinel(host, username=username,
                password=password)
        self.original_urlopen = urllib2.urlopen

    def patch_urllib(self, return_string):
        self.traffic_sentinel_string = StringIO(return_string)
        urllib2.urlopen = Mock(return_value=self.traffic_sentinel_string)

    def teardown(self):
        urllib2.urlopen = self.original_urlopen

    def test_get_metric_statistics(self):

        # This is a tricky way to make sure this test passes with the real ts,
        # since a real TS will always have its own values
        test_host = os.environ.get("TRAFFIC_SENTINEL_HOST", "fake.ts.host.tld")
        loads = [0.010, 0.020]
        test_reply = "%s,%f\n" % (test_host, loads[0])
        test_reply += "%s,%f\n" % (test_host, loads[1])
        load_average = sum(loads) / float(len(loads))
        if self.mock_traffic_sentinel:
            self.patch_urllib(test_reply)

        period = 60
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()
        metric_name = "load_five"
        statistics = Statistics.AVERAGE

        result = self.traffic_sentinel.get_metric_statistics(period, start_time,
                end_time, metric_name, statistics)

        assert len(result) > 0
        assert result.get(test_host)
        assert result[test_host].get(Statistics.AVERAGE)

        if not self.mock_traffic_sentinel:
            return

        # assert result[test_host][Statistics.AVERAGE] ~= load_average
        assert abs(result[test_host][Statistics.AVERAGE] - load_average) < 0.0000001

    def test_get_metric_statistics_app_attributes(self):

        # test_host = os.environ.get("TRAFFIC_SENTINEL_HOST", "fake.ts.host.tld")
        test_process = os.environ.get("TRAFFIC_SENTINEL_PROCESS", "fake.process")
        queue_length = 1
        ml = 1
        app_attributes = ['pid=%s&ql=%s&ml=%s' % (test_process, queue_length, ml)]
        test_reply = "%s\n" % (app_attributes[0])
        if self.mock_traffic_sentinel:
            self.patch_urllib(test_reply)

        period = 60
        start_time = datetime.now() - timedelta(days=1)
        end_time = datetime.now()
        metric_name = "app_attributes:ml"
        statistics = Statistics.AVERAGE
        dimensions = {'pid': [test_process]}

        result = self.traffic_sentinel.get_metric_statistics(period, start_time,
                end_time, metric_name, statistics, dimensions)
        assert len(result) > 0
        assert result.get(test_process)
        assert result[test_process].get(Statistics.AVERAGE)

        if not self.mock_traffic_sentinel:
            return

        # assert result[test_host][Statistics.AVERAGE] ~= load_average
        assert abs(result[test_process][Statistics.AVERAGE] - ml) < 0.0000001

    def test_build_script(self):

        query_fields = ["first", "second"]
        query_type = 'host'
        group = 60
        interval = "201209190101.01-201209200101.01"
        dimensions = {'hostname': ['somevm.cloud.tld', 'someothervm.cloud.tld']}
        script = self.traffic_sentinel._build_script(query_fields, query_type, interval, group, dimensions)
        assert 'interval = "%s"' % interval in script
        assert 'select = "%s"' % ','.join(query_fields) in script
        assert 'where = "%s"' % '(hostname = somevm.cloud.tld | hostname = someothervm.cloud.tld)' in script


class TestRealTrafficSentinel(TestTrafficSentinel):

    def setup(self):

        host = os.environ.get("TRAFFIC_SENTINEL_HOST")
        username = os.environ.get("TRAFFIC_SENTINEL_USERNAME")
        password = os.environ.get("TRAFFIC_SENTINEL_PASSWORD")
        self.mock_traffic_sentinel = False

        if not (host and username and password):
            raise SkipTest("Traffic sentinel host, username or password aren't in env")

        self.traffic_sentinel = TrafficSentinel(host, username=username,
                password=password)

    def teardown(self):
        pass


def test_extract_app_attribute():
    key = "ql"
    app_attribute_empty = ""
    app_attribute_one_value = '2'
    app_attribute_one = "ql=%s" % app_attribute_one_value
    app_attribute_two_value = '4'
    app_attribute_two = "ql=%s&ml=2" % app_attribute_two_value

    got = _extract_app_attribute(app_attribute_empty, key)
    assert got == ''

    got = _extract_app_attribute(app_attribute_one, key)
    assert got == app_attribute_one_value

    got = _extract_app_attribute(app_attribute_two, key)
    assert got == app_attribute_two_value
