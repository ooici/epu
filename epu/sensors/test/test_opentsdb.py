import os

from nose.plugins.skip import SkipTest
from datetime import datetime, timedelta

from epu.sensors import Statistics
from epu.sensors.opentsdb import OpenTSDB

class TestRealOpenTSDB(object):

    def setup(self):

        host = os.environ.get("OPENTSDB_HOST")
        port = os.environ.get("OPENTSDB_PORT")
        #self.instance = os.environ.get("CLOUDWATCH_TEST_INSTANCE")
        self.instance = ""

        if not (host and port):
            raise SkipTest("OpenTSDB host and port aren't in env")

        self.opentsdb = OpenTSDB(host, port)

    def teardown(self):
        pass

    
    def test_get_metric_statistics(self):

        raise SkipTest("this is just a shell of an implementation at this point")

        period = 60
        end_time = datetime.now()
        start_time = end_time - timedelta(minutes=20)
        metric_name = "avg:iostat.disk.write_merged"
        statistics = Statistics.AVERAGE
        dimensions = {'InstanceId': [self.instance]}

        result = self.opentsdb.get_metric_statistics(period, start_time,
                end_time, metric_name, statistics, dimensions)

        assert len(result) > 0
        assert result.get(self.instance)
        assert result[self.instance].get(Statistics.AVERAGE)

