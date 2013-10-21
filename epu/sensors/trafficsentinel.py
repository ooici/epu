# Copyright 2013 University of Chicago

import re
import csv
import base64
import urllib
import urllib2

from datetime import datetime
from epu.sensors import ISensorAggregator, Statistics


class TrafficSentinel(ISensorAggregator):

    def __init__(self, host, username=None, password=None, protocol=None, port=None):

        self.host = host
        self.username = username
        self.password = password
        if protocol is None:
            self.protocol = 'https'
        else:
            self.protocol = protocol
        if port is None:
            self.port = 443
        else:
            self.port = port
        self.base_url = "%s://%s:%s/inmsf/Query" % (self.protocol, self.host, self.port)
        self.app_metrics = APP_METRICS
        self.host_metrics = HOST_METRICS

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
            Note: app_attributes is a special metric name. You can request a
            specific attribute from the list of attributes, for example:
            'app_attributes:ql' for queue_length,
            'app_attributes:ml' for message_latency,
            'app_attributes:ps' for process saturation
        @param statistics(list of Statistics Types) List of statistics to apply
            to metric. Accepts: Average, Sum, SampleCount, Maximum, Minimum
        @param dimensions(dict of filters) A way to filter on certain values.
            Keys are any metric supported by the sensor aggregator, and value
            is a value or list of values to filter on.
            For example:
            dimensions={'hostname'=['alpha.vms.cloud.tld', 'tango.vms.cloud.tld']}

            the dimension 'upid' is a special dimension that gets hashed and mapped to
            the equivalent value in TS (TODO)

        """
        # Ugly heuristic to determine where to query a metric from
        if dimensions and dimensions.get('pid') and metric_name in self.app_metrics:
            query_type = 'application'
            index_by = 'pid'
        elif dimensions and dimensions.get('pid') and metric_name in self.host_metrics:
            query_type = 'host'
            index_by = 'pid'
        elif dimensions and dimensions.get('hostname') and metric_name in self.app_metrics:
            query_type = 'application'
            index_by = 'hostname'
        elif 'app_attributes' in metric_name:
            query_type = 'application'
            index_by = 'pid'
        else:
            query_type = 'host'
            index_by = 'hostname'

        if not isinstance(start_time, datetime):
            raise TypeError("start_time must be a datetime object")
        if not isinstance(end_time, datetime):
            raise TypeError("end_time must be a datetime object")

        tformat = "%Y%m%d %H:%M"
        interval = "%s,%s" % (start_time.strftime(tformat), end_time.strftime(tformat))
        time_group = int(period)

        if metric_name.startswith('app_attributes:'):
            metric_name, app_attribute = metric_name.split(':')
        else:
            app_attribute = ''
        if index_by == 'pid':
            query_fields = [metric_name, ]
        else:
            query_fields = [index_by, metric_name, ]

        script = self._build_script(query_fields, query_type, interval, time_group, dimensions)

        authenticate = 'basic' if self.username and self.password else None
        url = self._build_query_url(self.base_url, authenticate=authenticate,
                script=script)
        request = urllib2.Request(url)

        if self.username and self.password:
            auth_header = self._build_auth_header(self.username, self.password)
            request.add_header('Authorization', auth_header)

        reply = urllib2.urlopen(request)
        results = {}
        reader = csv.reader(reply)
        for metrics in reader:
            if metrics == []:
                continue

            if index_by == 'pid':
                match = re.search('pid=(.*?)&', metrics[0])
                if match:
                    index = match.group(1)
                else:
                    continue
            else:
                index = metrics.pop(0)
                if index == '':
                    continue

            result = results.get(index, {Statistics.SERIES: []})
            for i, metric in enumerate(metrics):
                if metric_name == 'app_attributes' and app_attribute:
                    metric = _extract_app_attribute(metric, app_attribute)

                result[Statistics.SERIES].append(metric)

            results[index] = result

        for index, metric in results.iteritems():
            series = metric[Statistics.SERIES]
            if Statistics.AVERAGE in statistics:
                try:
                    metric[Statistics.AVERAGE] = sum(map(float, series)) / float(len(series))
                except (ZeroDivisionError, ValueError):
                    metric[Statistics.AVERAGE] = 0.0
            if Statistics.SUM in statistics:
                metric[Statistics.SUM] = sum(map(float, series))
            if Statistics.SAMPLE_COUNT in statistics:
                metric[Statistics.SAMPLE_COUNT] = len(series)
            if Statistics.MAXIMUM in statistics:
                metric[Statistics.MAXIMUM] = max(map(float, series))
            if Statistics.MINIMUM in statistics:
                metric[Statistics.MINIMUM] = min(map(float, series))

        return results

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

                if vals == []:
                    continue

                if metric == 'pid':
                    vals = ["%s ~ %s" % ("app_attributes", val) for val in vals]
                    where_item = "(%s)" % " | ".join(vals)
                    where_items.append(where_item)
                else:
                    vals = ["%s = %s" % (metric, val) for val in vals]
                    where_item = "(%s)" % " | ".join(vals)
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


def _extract_app_attribute(metric_result, app_attribute):
    if not metric_result:
        return ''

    for attribute in metric_result.split('&'):
        key, value = attribute.split('=')
        if key == app_attribute:
            return value
    return ''

APP_METRICS = ["agent", "app_attributes", "app_ctxt_attributes",
"app_ctxt_name", "app_ctxt_operation", "app_error", "app_initiator",
"app_name", "app_operation", "app_status", "app_target", "clientaddress",
"clientcountry", "cliententerprise", "clientgroup", "clientname", "clientpath",
"clientport", "clientsite", "clientsubnet", "clientzone", "destinationaddress",
"destinationagententerprise", "destinationagentgroup", "destinationagentsite",
"destinationagentzone", "destinationcountry", "destinationenterprise",
"destinationgroup", "destinationname", "destinationpath", "destinationport",
"destinationsite", "destinationsubnet", "destinationzone", "enterprise",
"fcclient", "fcserver", "group", "hostname", "httpauthuser", "httpdirection",
"httphost", "httpmethod", "httpmimetype", "httpprotocol", "httprefhost",
"httprefpath", "httpstatus", "httpurl", "httpuseragent", "httpxff", "ifindex",
"interface", "ip6client", "ip6server", "ipclient", "ipserver", "macclient",
"machine_type", "macserver", "memcachecommand", "memcachekey",
"memcachenumkeys", "memcacheprotocol", "memcachestatus", "op_bytes",
"op_count", "op_duration", "op_reqbytes", "op_respbytes", "op_type", "os_name",
"os_release", "protocol", "protocolgroup", "serveraddress", "servercountry",
"serverenterprise", "servername", "serverpath", "os_name", "os_release",
"serversubnet", "site", "sourceaddress", "sourceagententerprise",
"sourceagentsite", "sourceagentzone", "sourcecountry", "sourcegroup",
"sourcename", "sourcepath", "sourceport", "sourcesite", "sourcesubnet",
"sourcezone", "time", "uriextension", "urifile", "urifragment", "urihost",
"uripath", "uriport", "uriquery", "urischeme", "uuid", "uuidclient",
"uuiddestination", "uuidserver", "uuidsource", "zone"]

HOST_METRICS = ["agent", "app_connections_max", "app_connections_open",
"app_count", "app_cpusystem", "app_cpuuser", "app_duration", "app_err_badreq",
"app_err_forbidden", "app_err_internal", "app_err_notfound", "app_err_notimpl",
"app_err_other", "app_err_timeout", "app_err_toolarge", "app_err_unauth",
"app_err_unavail", "app_errors", "app_files_max", "app_files_open",
"app_mem_max", "app_mem_used", "app_name", "app_req_delayed",
"app_req_dropped", "app_success", "app_workers_active", "app_workers_idle",
"app_workers_max", "bytes_in", "bytes_out", "bytes_read", "bytes_written",
"contexts", "cpu_idle", "cpu_intr", "cpu_nice", "cpu_num", "cpu_sintr",
"cpu_system", "cpu_user", "cpu_util", "cpu_wio", "diskfree", "diskpartmax",
"disktotal", "drops_in", "drops_out", "enterprise", "errs_in", "errs_out",
"group", "has_app", "has_host", "has_http", "has_if", "has_java",
"has_memcache", "has_switch", "has_vm", "hostname", "http_method_connect",
"http_method_delete", "http_method_get", "http_method_head",
"http_method_option", "http_method_other", "http_method_post",
"http_method_put", "http_method_total", "http_method_trace", "http_status_1xx",
"http_status_2xx", "http_status_3xx", "http_status_4xx", "http_status_5xx",
"http_status_other", "ifindex", "interface", "interrupts",
"jvm_classes_loaded", "jvm_classes_total", "jvm_classes_unloaded",
"jvm_compile_ms", "jvm_fds_max", "jvm_fds_open", "jvm_gc_count", "jvm_gc_ms",
"jvm_heap_committed", "jvm_heap_initial", "jvm_heap_max", "jvm_heap_used",
"jvm_name", "jvm_non_heap_committed", "jvm_non_heap_initial",
"jvm_non_heap_max", "jvm_non_heap_used", "jvm_thread_daemon",
"jvm_thread_live", "jvm_thread_started", "jvm_vendor", "jvm_version",
"load_fifteen", "load_five", "load_one", "loadpercpu_fifteen",
"loadpercpu_five", "loadpercpu_one", "machine_type", "mem_buffers",
"mem_cached", "mem_cached_pc", "mem_free", "mem_shared", "mem_total",
"mem_used", "mem_used_pc", "memcache_accepting", "memcache_authcmds",
"memcache_autherrors", "memcache_bytes", "memcache_bytesread",
"memcache_byteswritten", "memcache_casbadval", "memcache_cashits",
"memcache_casmisses", "memcache_cmdflush", "memcache_cmdget",
"memcache_cmdset", "memcache_cmdtouch", "memcache_connections",
"memcache_connectionyields", "memcache_cpusystem", "memcache_cpuuser",
"memcache_currentconnections", "memcache_currentitems", "memcache_decrhits",
"memcache_decrmisses", "memcache_delhits", "memcache_delmisses",
"memcache_evictions", "memcache_gethits", "memcache_getmisses",
"memcache_hits", "memcache_hits_pc", "memcache_incrhits",
"memcache_incrmisses", "memcache_items", "memcache_limitbytes",
"memcache_listendisabled", "memcache_misses", "memcache_misses_pc",
"memcache_ops", "memcache_reclaimed", "memcache_rejected", "memcache_structs",
"memcache_threads", "memcache_uptime", "memcache_used_pc", "os_name",
"os_release", "page_in", "page_out", "pkts_in", "pkts_out", "proc_run",
"proc_total", "read_time", "read_time_mean", "reads", "site", "swap_free",
"swap_in", "swap_inout", "swap_out", "swap_total", "swap_util_pc", "time",
"uuid", "v_dsindex", "v_hostname", "v_machine_type", "v_os_name",
"v_os_release", "v_uuid", "vbytes_in", "vbytes_out", "vbytes_read",
"vbytes_written", "vcpu_num", "vcpu_pc", "vcpu_state", "vdiskallocation",
"vdiskavailable", "vdiskcapacity", "vdrops_in", "vdrops_out", "verrs_in",
"verrs_out", "vmem_free", "vmem_total", "vmem_used", "vmem_used_pc",
"vnode_cpu_mhz", "vnode_cpus", "vnode_domains", "vnode_mem_free",
"vnode_mem_total", "vpkts_in", "vpkts_out", "vreads", "vwrites", "write_time",
"write_time_mean", "writes", "zone"]
