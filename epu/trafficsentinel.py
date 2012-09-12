import csv
import base64
import urllib
import urllib2

script = """
var view     = "host";
var select   = "hostname,mem_used,mem_free";
var where    = "";
var interval = "today";

Query.topN(view, select, where, interval).run().printCSV();
"""

class TrafficSentinel(object):

    def __init__(self, host, username=None, password=None):

        self.host = host
        self.username = username
        self.password = password
        self.base_url = "https://%s/inmsf/Query" % self.host

    def query(self, metrics):
 
        query_fields = ['hostname',] + list(metrics)
        script = self._build_script(query_fields)

        authenticate = 'basic' if self.username and self.password else None
        url = self._build_query_url(self.base_url, authenticate=authenticate,
                script=script)
        request = urllib2.Request(url)

        if self.username and self.password:
            auth_header = self._build_auth_header(self.username, self.password)
            request.add_header('Authorization', auth_header)

        result = urllib2.urlopen(request)

        hosts = {}
        reader = csv.reader(result)
        for row in reader:
            hostname = row.pop(0)
            host = {}
            for i, metric in enumerate(metrics):
                host[metric] = row[i]

            hosts[hostname] = host

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
        #TODO: remove last character?
        base64_user_pass = base64.encodestring(user_pass)
        auth_header = "Basic %s" % base64_user_pass

        return auth_header

    def _build_script(self, query_fields):

        formatted_query_fields = ','.join(query_fields)

        script = """
        var view     = "host";
        var select   = "%s";
        var where    = "";
        var interval = "today";

        Query.topN(view, select, where, interval).run().printCSV();
        """ % formatted_query_fields
        
        return script
