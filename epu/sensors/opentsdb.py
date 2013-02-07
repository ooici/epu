
import urllib
import httplib
import datetime

from epu.sensors import ISensorAggregator, Statistics

_stat_map = {
    Statistics.AVERAGE: 'avg',
    Statistics.MAXIMUM: 'max',
    Statistics.MINIMUM: 'min',
    Statistics.SUM: 'sum',
}

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
            dimensions={'host'=['my.real.host.tld']}
        """
        # round to nearest 60
        if period % 60 != 0:
            period = (period + 60)
            period = period - (period % 60)
        period = max(60, period)

        start_time_formatted = start_time.strftime("%Y/%m/%d-%H:%M:%S")
        end_time_formatted = end_time.strftime("%Y/%m/%d-%H:%M:%S")

        formatted_dimensions = format_dimensions(dimensions)

        params = urllib.urlencode({
            'start': start_time_formatted,
            'end': end_time_formatted,
            'm': "%s:%s%s" % (_stat_map.get(statistics, 'avg'), metric_name, formatted_dimensions),
            'ascii': 'true'
        })
        self.opentsdb.request('GET', '/q?%s' % params)
        response = self.opentsdb.getresponse()

        if response.status != 200:
            return {}

        #TODO: this could be process etc in future
        if 'domain' in dimensions:
            index = 'domain'
        else:
            index = 'host'

        parsed_stats = {}
        raw_stats =  response.read()

        for line in raw_stats.splitlines():
            metric_name, timestamp, raw_data, raw_params = line.split(' ', 3)

            if timestamp > end_time.strftime("%s") or timestamp < start_time.strftime("%s"):
                continue
            params = {}
            for pair in raw_params.split(' '):
                key, val = pair.split('=')
                params[key] = val

            if params.get(index) is None:
                return {}
            index_val = params[index]

            if parsed_stats.get(index_val) is None:
                parsed_stats[index_val] = {Statistics.SERIES: []}

            data = parse_data(raw_data)
            parsed_stats[index_val][Statistics.SERIES].append(data)

        for i, m in parsed_stats.iteritems():
            series = m[Statistics.SERIES]
            if Statistics.AVERAGE in statistics:
                try:
                    m[Statistics.AVERAGE] = sum(map(float, series)) / float(len(series))
                except ZeroDivisionError:
                    m[Statistics.AVERAGE] = 0.0
            if Statistics.SUM in statistics:
                m[Statistics.SUM] = sum(map(float,series))
            if Statistics.SAMPLE_COUNT in statistics:
                m[Statistics.SAMPLE_COUNT] = len(series)
            if Statistics.MAXIMUM in statistics:
                m[Statistics.MAXIMUM] = max(map(float, series))
            if Statistics.MINIMUM in statistics:
                m[Statistics.MINIMUM] = min(map(float, series))

        return parsed_stats

def parse_data(raw_data):
    """Try to get a numerical form of string data.

    First tries to get an integer, then a float, then a string
    """
    try:
        data = int(raw_data)
    except ValueError:
        try:
            data = float(raw_data)
        except ValueError:
            data = str(raw_data)
    return data

def format_dimensions(raw_dimensions):

    if raw_dimensions is None:
        return ''

    formatted_dimensions = []
    for key, vals in raw_dimensions.iteritems():
        if isinstance(vals, basestring):
            vals = [vals, ]
        dimension = "%s=%s" % (key, "|".join(vals))
        formatted_dimensions.append(dimension)
    formatted_dimensions = "{%s}" % ','.join(formatted_dimensions)
    return formatted_dimensions
