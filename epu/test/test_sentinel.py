
import os

from nose.plugins.skip import SkipTest

from epu.trafficsentinel import TrafficSentinel

class TestTrafficSentinel(object):

    def setup(self):

        host = os.environ.get("TRAFFIC_SENTINEL_HOST")
        username = os.environ.get("TRAFFIC_SENTINEL_USERNAME")
        password = os.environ.get("TRAFFIC_SENTINEL_PASSWORD")

        if not (host and username and password):
            raise SkipTest("Traffic sentinel host, username or password aren't in env")

        self.traffic_sentinel = TrafficSentinel(host, username=username,
                password=password)

    def test_basic_request(self):

        from pprint import PrettyPrinter
        pprint = PrettyPrinter(indent=2).pprint

        metrics = ['mem_used'] #, 'mem_free']
        query_result = self.traffic_sentinel.query(metrics)
        pprint(query_result)

