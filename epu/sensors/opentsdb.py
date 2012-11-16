
import urllib
import httplib
import datetime

from epu.sensors import ISensorAggregator, Statistics


class OpenTSDB(ISensorAggregator):
    """Implementation of OpenTSDB sensor aggregator client
    """

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.opentsdb = httplib.HTTPConnection(self.host, self.port)

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

        start_time_formatted = start_time.strftime("%Y/%m/%d-%H:%M:%S")
        end_time_formatted = end_time.strftime("%Y/%m/%d-%H:%M:%S")

        print "start %s end %s" % (start_time_formatted, end_time_formatted)
        params = urllib.urlencode({
            'start': start_time_formatted,
            'end': end_time_formatted,
            'm': metric_name,
            'ascii': 'true'
        })
        self.opentsdb.request('GET', '/q?%s' % params)
        response = self.opentsdb.getresponse()
        print response.status
        print response.read()
