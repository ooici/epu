import os

from nose.plugins.skip import SkipTest
from datetime import datetime, timedelta

from epu.sensors import Statistics
from epu.sensors.opentsdb import OpenTSDB

class TestRealOpenTSDB(object):

    def setup(self):

        host = os.environ.get("OPENTSDB_HOST")
        port = os.environ.get("OPENTSDB_PORT")
        self.test_host = os.environ.get("OPENTSDB_TEST_HOST")

        if not (host and port and self.test_host):
            raise SkipTest("OpenTSDB host, port, and test host aren't in env")

        self.opentsdb = OpenTSDB(host, port)

    def teardown(self):
        pass

    
    def test_get_metric_statistics(self):

        period = 60
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=20)
        metric_name = "avg:iostat.disk.write_merged"
        statistics = Statistics.AVERAGE
        dimensions = {'host': [self.test_host,]}

        result = self.opentsdb.get_metric_statistics(period, start_time,
                end_time, metric_name, statistics, dimensions)

        assert len(result) > 0
        assert result.get(self.test_host)
        assert result[self.test_host].get(Statistics.AVERAGE)
