# Copyright 2013 University of Chicago


import boto

from operator import itemgetter

from epu.sensors import ISensorAggregator, Statistics
from epu.exceptions import ProgrammingError

CW_EC2_NAMESPACE = 'AWS/EC2'


class CloudWatch(ISensorAggregator):
    """Implementation of CloudWatch sensor aggregator client

    Limited at the moment (You can only query one thing at a time, and
    only by instance id)
    """

    def __init__(self, aws_access_key_id, aws_secret_access_key):

        self.cw = boto.connect_cloudwatch(aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key)

    def get_metric_statistics(self, period, start_time, end_time, metric_name,
            statistics, dimensions=None):
        """
        Get a metric from the Sensor Aggregator

        @param period(integer) The granularity of returned data in
            seconds. This will be rounded to the nearest second, with a minimum
            of 60 seconds.
        @param start_time(datetime) Time to use for the first datapoint returned
        @param end_time(datetime) Time to use for the last datapoint returned
        @param metric_name(string) The name of the metric to be returned
        @param statistics(list of Statistics Types) List of statistics to apply
            to metric. Accepts: Average, Sum, SampleCount, Maximum, Minimum
        @param dimensions(dict of filters) A way to filter on certain values.
            Keys are any metric supported by the sensor aggregator, and value
            is a value or list of values to filter on.
            For example:
            dimensions={'InstanceId'=['i-deadbeef']}
        """
        # round to nearest 60
        if period % 60 != 0:
            period = (period + 60)
            period = period - (period % 60)
        period = max(60, period)

        namespace = CW_EC2_NAMESPACE

        if not dimensions:
            raise ProgrammingError("You must supply dimensions")
        if not dimensions.get('InstanceId'):
            raise ProgrammingError("You must supply an instance id")
        if (not isinstance(dimensions.get('InstanceId'), basestring) and
                len(dimensions.get('InstanceId')) > 1):
            raise ProgrammingError("You may only supply one instance id")

        if not isinstance(statistics, basestring) and len(statistics) > 1:
            raise ProgrammingError("You may only supply one statistic")

        if not isinstance(statistics, basestring):
            statistics = statistics[0]

        stats = self.cw.get_metric_statistics(period, start_time, end_time,
                metric_name, namespace, statistics, dimensions)

        sorted_stats = sorted(stats, key=itemgetter('Timestamp'))
        if isinstance(dimensions.get('InstanceId'), basestring):
            instance = dimensions.get('InstanceId')
        else:
            instance = dimensions.get('InstanceId')[0]

        series = [item[statistics] for item in sorted_stats]
        instances = {instance: {Statistics.SERIES: series}}
        metric = instances[instance]

        if Statistics.AVERAGE == statistics:
            try:
                metric[Statistics.AVERAGE] = sum(map(float, series)) / float(len(series))
            except ZeroDivisionError:
                metric[Statistics.AVERAGE] = 0.0
        if Statistics.SUM == statistics:
            metric[Statistics.SUM] = sum(map(float, series))
        if Statistics.SAMPLE_COUNT == statistics:
            metric[Statistics.SAMPLE_COUNT] = len(series)
        if Statistics.MAXIMUM == statistics:
            metric[Statistics.MAXIMUM] = max(map(float, series))
        if Statistics.MINIMUM == statistics:
            metric[Statistics.MINIMUM] = min(map(float, series))

        return instances
