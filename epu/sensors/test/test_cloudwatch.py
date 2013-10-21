# Copyright 2013 University of Chicago


import os

from nose.plugins.skip import SkipTest
from datetime import datetime, timedelta

from epu.sensors import Statistics
from epu.sensors.cloudwatch import CloudWatch


class TestRealCloudWatch(object):

    def setup(self):

        access_key = os.environ.get("EC2_ACCESS_KEY")
        secret_key = os.environ.get("EC2_SECRET_KEY")
        self.instance = os.environ.get("CLOUDWATCH_TEST_INSTANCE")
        self.mock_traffic_sentinel = False

        if not (access_key and secret_key and self.instance):
            raise SkipTest("CloudWatch access key, secret key or instance aren't in env")

        self.cloudwatch = CloudWatch(access_key, secret_key)

    def teardown(self):
        pass

    def test_get_metric_statistics(self):

        period = 60
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=20)
        metric_name = "CPUUtilization"
        statistics = Statistics.AVERAGE
        dimensions = {'InstanceId': [self.instance]}

        result = self.cloudwatch.get_metric_statistics(period, start_time,
                end_time, metric_name, statistics, dimensions)

        assert len(result) > 0
        assert result.get(self.instance)
        assert result[self.instance].get(Statistics.AVERAGE)
