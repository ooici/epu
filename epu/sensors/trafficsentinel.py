import csv
import base64
import urllib
import urllib2

from datetime import datetime
from epu.sensors import ISensorAggregator, Statistics
from epu.exceptions import ProgrammingError

script = """
var view     = "host";
var select   = "hostname,mem_used,mem_free";
var where    = "";
var interval = "today";

Query.topN(view, select, where, interval).run().printCSV();
"""

class TrafficSentinel(ISensorAggregator):

    def __init__(self, host, username=None, password=None):

        self.host = host
        self.username = username
        self.password = password
        self.base_url = "https://%s/inmsf/Query" % self.host

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
        query_type = 'host'

        if not isinstance(start_time, datetime):
            raise TypeError("start_time must be a datetime object")
        if not isinstance(end_time, datetime):
            raise TypeError("end_time must be a datetime object")

        tformat = "%Y%m%d %H:%M"
        interval = "%s,%s" % (start_time.strftime(tformat), end_time.strftime(tformat))
        time_group = int(period)
        query_fields = ['hostname', metric_name,]
        
        script = self._build_script(query_fields, query_type, interval, time_group, dimensions)

        authenticate = 'basic' if self.username and self.password else None
        url = self._build_query_url(self.base_url, authenticate=authenticate,
                script=script)
        request = urllib2.Request(url)

        if self.username and self.password:
            auth_header = self._build_auth_header(self.username, self.password)
            request.add_header('Authorization', auth_header)

        reply = urllib2.urlopen(request)
        hosts = {}
        reader = csv.reader(reply)
        for metrics in reader:
            hostname = metrics.pop(0)
            host = hosts.get(hostname, {Statistics.SERIES: []})
            for i, metric in enumerate(metrics):
                host[Statistics.SERIES].append(metric)

            hosts[hostname] = host

        for hostname, metric in hosts.iteritems():
            series = metric[Statistics.SERIES]
            if Statistics.AVERAGE in statistics:
                try:
                    metric[Statistics.AVERAGE] = sum(map(float, series)) / float(len(series))
                except ZeroDivisionError:
                    metric[Statistics.AVERAGE] = 0.0
            if Statistics.SUM in statistics:
                metric[Statistics.SUM] = sum(map(float,series))
            if Statistics.SAMPLE_COUNT in statistics:
                metric[Statistics.SAMPLE_COUNT] = len(series)
            if Statistics.MAXIMUM in statistics:
                metric[Statistics.MAXIMUM] = max(map(float, series))
            if Statistics.MINIMUM in statistics:
                metric[Statistics.MINIMUM] = min(map(float, series))

        return hosts

    def _build_query_url(self, base_url, authenticate=None, script=None):

        params = {}
        params['resultFormat'] = 'txt'

        if authenticate is not None:
            params['authenticate'] = authenticate

        if script is not None:
            params['script'] = script

        url = "%s?%s" % (base_url, urllib.urlencode(params))

        return url

    def _build_auth_header(self, username, password):

        user_pass = "%s:%s" % (username, password)
        base64_user_pass = base64.encodestring(user_pass)
        auth_header = "Basic %s" % base64_user_pass

        return auth_header

    def _build_script(self, query_fields, query_type, interval, group, dimensions=None):

        if query_type not in ('application', 'host'):
            raise Exception("Unrecognized query_type %s" % query_type)

        formatted_query_fields = ','.join(query_fields)

        if dimensions is not None:
            where_items = []
            for metric, vals in dimensions.iteritems():
                if isinstance(vals, basestring):
                    vals = [vals, ]
                where_item = "(%s = %s)" % (metric, " | ".join(vals))
                where_items.append(where_item)

            where = " & ".join(where_items)
        else:
            where = ""

        script = """
        var view = "%s";
        var select = "%s";
        var where = "%s";
        var interval = "%s";
        var group = "%s";

        Query.trend(view, select, where, interval, group).run().printCSV();
        """ % (query_type, formatted_query_fields, where, interval, group)
        
        return script
