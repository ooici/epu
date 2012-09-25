
import urllib2
import unittest

from datetime import datetime
from StringIO import StringIO
from mock import Mock

from epu.highavailability.policy import SensorPolicy
from epu.processdispatcher.store import ProcessRecord


class SensorPolicyTest(unittest.TestCase):

    def setUp(self):

        aggregator_config = {
            'type': 'trafficsentinel',
            'host': 'fake',
            'username': 'fake',
            'password': 'fake',
        }

        self.mock_schedule = Mock()
        self.mock_terminate = Mock()

        self.policy = SensorPolicy(schedule_process_callback=self.mock_schedule,
                terminate_process_callback=self.mock_terminate,
                aggregator_config=aggregator_config)

        self.original_urlopen = urllib2.urlopen

    def tearDown(self):
        urllib2.urlopen = self.original_urlopen

    def patch_urllib(self, return_string):
        self.traffic_sentinel_string = StringIO(return_string)
        urllib2.urlopen = Mock(return_value=self.traffic_sentinel_string)


    def test_get_hostnames(self):

        owner = 'fred'
        upids = ['myupid0', 'myupid1', 'myupid2']
        hostnames = ['my.hostname0.tld', 'my.hostname1.tld', 'my.hostname2.tld']
        definition = None
        state = None

        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state, hostname=hostnames[0]),
            ],
            'pd1': [
                ProcessRecord.new(owner, upids[1], definition, state, hostname=hostnames[1]),
                ProcessRecord.new(owner, upids[2], definition, state, hostname=hostnames[2]),
            ]
        }

        got = self.policy._get_hostnames(all_procs, upids)
        self.assertEqual(sorted(got), sorted(hostnames))

    def test_apply_policy(self):

        parameters = {
                'metric': 'load_five',
                'sample_period': 600,
                'sample_function': 'Average',
                'cooldown_period': 600,
                'scale_up_threshold': 2.0,
                'scale_up_n_processes': 1,
                'scale_down_threshold': 0.5,
                'scale_down_n_processes': 1,
                'maximum_processes': 5,
                'minimum_processes': 1,
        }

        self.policy.parameters = parameters

        owner = 'fred'
        upids = ['myupid0', 'myupid1', 'myupid2']
        hostnames = ['my.hostname0.tld', 'my.hostname1.tld', 'my.hostname2.tld']
        loads_no_scale = [1.0, 0.5, 1.1]
        loads_scale_up = [2.0, 2.5, 4.1]
        loads_scale_down = [0.0, 0.5, 0.1]
        definition = None
        state = None

        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state, hostname=hostnames[0]),
            ],
            'pd1': [
                ProcessRecord.new(owner, upids[1], definition, state, hostname=hostnames[1]),
                ProcessRecord.new(owner, upids[2], definition, state, hostname=hostnames[2]),
            ]
        }

         
        # Since average is below 2.0, but above 0.5, we shouldn't see any
        # scaling activity
        self.patch_urllib(make_ts_string(hostnames, loads_no_scale))
        self.policy.apply_policy(all_procs, upids)

        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        # This average is above 2.0, so we should see one process schedule
        self.patch_urllib(make_ts_string(hostnames, loads_scale_up))
        self.policy.apply_policy(all_procs, upids)

        self.assertEqual(self.mock_schedule.call_count, 1)
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        # Now that we've made a scaling adjustment, we can test the cooldown 
        # period. No scaling actions should happen, even though the metric
        # results warrant a scaling action
        self.patch_urllib(make_ts_string(hostnames, loads_scale_down))
        self.policy.apply_policy(all_procs, upids)

        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        # Change the last scale action timestamp to a long time ago
        self.policy.last_scale_action = datetime.min

        # This average is below 0.5, so we should see one process terminate
        self.patch_urllib(make_ts_string(hostnames, loads_scale_down))
        self.policy.apply_policy(all_procs, upids)

        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 1)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()
        upids.pop() # this is normally done in ha core

        # Change the last scale action timestamp to a long time ago
        self.policy.last_scale_action = datetime.min

        # Keep the same low load, we should see another terminate
        self.patch_urllib(make_ts_string(hostnames, loads_scale_down))
        self.policy.apply_policy(all_procs, upids)

        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 1)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()
        upids.pop() # this is normally done in ha core

        # Change the last scale action timestamp to a long time ago
        self.policy.last_scale_action = datetime.min

        # Keep the same low load, we should not see any action, as we
        # should be at the minimum number of processes
        self.patch_urllib(make_ts_string(hostnames, loads_scale_down))
        self.policy.apply_policy(all_procs, upids)

        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()


def make_ts_string(hosts, metrics):
    traffic_sentinel_string = ""
    
    assert len(hosts) == len(metrics)

    for i, host in enumerate(hosts):
        traffic_sentinel_string += "%s,%f\n" % (host, metrics[i])

    return traffic_sentinel_string
