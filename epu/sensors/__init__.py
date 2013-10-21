# Copyright 2013 University of Chicago

import time

OPENTSDB_SENSOR_TYPE = 'opentsdb'
TRAFFIC_SENTINEL_SENSOR_TYPE = 'traffic_sentinel'
CLOUDWATCH_SENSOR_TYPE = 'cloudwatch'
MOCK_CLOUDWATCH_SENSOR_TYPE = 'mockcloudwatch'


def sensor_message(sensor_id, value, timestamp=None):
    """Builds a sensor message
    """
    if timestamp is None:
        timestamp = long(time.time() * 1e6)

    return dict(sensor_id=str(sensor_id), time=long(timestamp), value=value)


class Statistics(object):
    """Types of statistics to apply to results from get_metric_statistics
    """

    SERIES = "Series"
    AVERAGE = "Average"
    SUM = "Sum"
    SAMPLE_COUNT = "SampleCount"
    MAXIMUM = "Maximum"
    MINIMUM = "Minimum"

    ALL = (SERIES, AVERAGE, SUM, SAMPLE_COUNT, MAXIMUM, MINIMUM)


class ISensorAggregator(object):
    """Abstract Sensor Aggregator class. Sensor aggregator implementations.

    """

    def get_metric_statistics(self, period, start_time, end_time, metric_name,
            statistics, dimensions=None):
        """
        Get a metric from the Sensor Aggregator

        @param period(integer) The granularity of returned data in
            seconds. When used with CloudWatch, this will be rounded to the
            nearest second, with a minimum of 60 seconds.
        @param start_time(datetime) Time to use for the first datapoint returned
        @param end_time(datetime) Time to use for the last datapoint returned
        @param metric_name(string) The name of the metric to be returned
        @param statistics(list of Statistics Types) List of statistics to apply
            to metric. Accepts: Average, Sum, SampleCount, Maximum, Minimum
        @param dimensions(dict of filters) A way to filter on certain values.
            Keys are any metric supported by the sensor aggregator, and value
            is a value or list of values to filter on.
            For example:
            dimensions={'hostname'=['alpha.vms.cloud.tld', 'tango.vms.cloud.tld']}

        """
        raise NotImplementedError("%s doesn't implement get_metric_statistics" %
                self.__class__.__name__)
