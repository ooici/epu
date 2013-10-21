# Copyright 2013 University of Chicago

import urllib2
import unittest

from datetime import datetime
from StringIO import StringIO
from mock import Mock, ANY, call

from epu.highavailability.policy import SensorPolicy, NPreservingPolicy
from epu.processdispatcher.store import ProcessRecord
from epu.states import ProcessState, HAState
from epu.exceptions import PolicyError


class NPreservingPolicyTest(unittest.TestCase):

    def setUp(self):

        self.mock_schedule = Mock()
        self.mock_terminate = Mock()

        self.policy = NPreservingPolicy(schedule_process_callback=self.mock_schedule,
            terminate_process_callback=self.mock_terminate,)

    def test_apply_policy(self):

        parameters = {
            'preserve_n': 2,
            'execution_engine_id': 'some_ee',
            'node_exclusive': 'hats'
        }

        self.policy.parameters = parameters

        upids = []
        all_procs = {'pd0': []}

        self.policy.apply_policy(all_procs, upids)

        schedule_call = call(ANY, ANY, configuration=None,
            execution_engine_id='some_ee', node_exclusive='hats')

        self.assertEqual(self.mock_schedule.call_args_list, [schedule_call, schedule_call])
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        owner = 'fred'
        upids = ['myupid0', 'myupid1']
        definition = None
        state = None

        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state),
                ProcessRecord.new(owner, upids[1], definition, state),
            ],
        }

        parameters['preserve_n'] = 1
        self.policy.parameters = parameters
        self.policy.apply_policy(all_procs, upids)

        self.assertEqual(self.mock_terminate.call_count, 1)
        self.assertEqual(self.mock_schedule.call_count, 0)

    def test_rejected_procs(self):

        parameters = {
            'preserve_n': 2,
            'execution_engine_id': 'some_ee',
            'node_exclusive': 'hats'
        }

        self.policy.parameters = parameters

        upids = []
        all_procs = {'pd0': []}

        self.policy.apply_policy(all_procs, upids)

        schedule_call = call(ANY, ANY, configuration=None,
            execution_engine_id='some_ee', node_exclusive='hats')

        self.assertEqual(self.mock_schedule.call_args_list, [schedule_call, schedule_call])
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        owner = 'fred'
        upids = ['myupid0', 'myupid1']
        definition = None
        state_running = ProcessState.RUNNING
        state_rejected = ProcessState.REJECTED

        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state_running),
                ProcessRecord.new(owner, upids[1], definition, state_rejected),
            ],
        }

        self.policy.apply_policy(all_procs, upids)
        self.assertEqual(self.mock_schedule.call_count, 1)
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        upids = ['myupid0', 'myupid1', 'myupid2']
        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state_running),
                ProcessRecord.new(owner, upids[1], definition, state_rejected),
                ProcessRecord.new(owner, upids[2], definition, state_running),
            ],
        }

        self.policy.apply_policy(all_procs, upids)
        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        parameters['preserve_n'] = 1
        self.policy.parameters = parameters
        self.policy.apply_policy(all_procs, upids)

        self.assertEqual(self.mock_terminate.call_count, 1)
        self.assertEqual(self.mock_schedule.call_count, 0)

    def test_set_status(self):

        self.assertEqual(self.policy.status(), HAState.PENDING)

        owner = 'fred'
        upids = ['myupid0', 'myupid1']
        definition = None
        state_running = ProcessState.RUNNING
        state_pending = ProcessState.PENDING

        to_rebalance = 0

        parameters = {
            'preserve_n': 3,
        }
        self.policy.parameters = parameters

        self.assertEqual(self.policy.status(), HAState.PENDING)

        upids = ['myupid0']
        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state_pending),
            ],
        }

        self.policy._set_status(to_rebalance, upids, all_procs)
        self.assertEqual(self.policy.status(), HAState.PENDING)

        upids = ['myupid0']
        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state_running),
            ],
        }

        self.policy._set_status(to_rebalance, upids, all_procs)
        self.assertEqual(self.policy.status(), HAState.READY)

        upids = ['myupid0', 'myupid1']
        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state_running),
                ProcessRecord.new(owner, upids[1], definition, state_running),
            ],
        }

        self.policy._set_status(to_rebalance, upids, all_procs)
        self.assertEqual(self.policy.status(), HAState.READY)

        upids = ['myupid0', 'myupid1', 'myupid2']
        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state_running),
                ProcessRecord.new(owner, upids[1], definition, state_running),
                ProcessRecord.new(owner, upids[2], definition, state_pending),
            ],
        }

        self.policy._set_status(to_rebalance, upids, all_procs)
        self.assertEqual(self.policy.status(), HAState.READY)

        upids = ['myupid0', 'myupid1', 'myupid2']
        all_procs = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state_running),
                ProcessRecord.new(owner, upids[1], definition, state_running),
                ProcessRecord.new(owner, upids[2], definition, state_running),
            ],
        }

        self.policy._set_status(to_rebalance, upids, all_procs)
        self.assertEqual(self.policy.status(), HAState.STEADY)

        self.policy._status = HAState.FAILED
        self.policy._set_status(to_rebalance, upids, all_procs)
        self.assertEqual(self.policy.status(), HAState.FAILED)


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

    def test_parameters(self):

        self.policy.parameters = {
            'metric': 'ok',
            'sample_period': 5,
            'sample_function': 'Average',
            'cooldown_period': 5,
            'scale_up_threshold': 0.5,
            'scale_down_threshold': 0.5,
            'scale_up_n_processes': 1,
            'scale_down_n_processes': 1,
            'minimum_processes': 1,
            'maximum_processes': 5
        }

        def set_policy(params):
            self.policy.parameters = params

        params = {'metric': None}
        self.assertRaises(PolicyError, set_policy, params)

        self.policy.parameters = {'metric': 'test'}
        assert self.policy.parameters['metric'] == 'test'

        params = {'sample_period': 'hi'}
        self.assertRaises(PolicyError, set_policy, params)

        params = {'sample_period': -1}
        self.assertRaises(PolicyError, set_policy, params)

        self.policy.parameters = {'sample_period': 10000}
        assert self.policy.parameters['sample_period'] == 10000

        self.policy.parameters = {'sample_period': '10000'}
        assert self.policy.parameters['sample_period'] == 10000

        params = {'sample_function': "blorp"}
        self.assertRaises(PolicyError, set_policy, params)

        params = {'sample_function': None}
        self.assertRaises(PolicyError, set_policy, params)

        self.policy.parameters = {'sample_function': 'Sum'}
        assert self.policy.parameters['sample_function'] == 'Sum'

        params = {'cooldown_period': 'hi'}
        self.assertRaises(PolicyError, set_policy, params)

        params = {'cooldown_period': -1}
        self.assertRaises(PolicyError, set_policy, params)

        self.policy.parameters = {'cooldown_period': 66}
        assert self.policy.parameters['cooldown_period'] == 66

        self.policy.parameters = {'cooldown_period': '66'}
        assert self.policy.parameters['cooldown_period'] == 66

        params = {'scale_up_threshold': 'hi'}
        self.assertRaises(PolicyError, set_policy, params)

        self.policy.parameters = {'scale_up_threshold': 66}
        assert self.policy.parameters['scale_up_threshold'] == 66

        self.policy.parameters = {'scale_up_threshold': '66'}
        assert self.policy.parameters['scale_up_threshold'] == 66

        params = {'scale_up_n_processes': 'hi'}
        self.assertRaises(PolicyError, set_policy, params)

        self.policy.parameters = {'scale_up_n_processes': 66}
        assert self.policy.parameters['scale_up_n_processes'] == 66

        self.policy.parameters = {'scale_up_n_processes': '66'}
        assert self.policy.parameters['scale_up_n_processes'] == 66

        params = {'scale_down_n_processes': 'hi'}
        self.assertRaises(PolicyError, set_policy, params)

        self.policy.parameters = {'scale_down_n_processes': 66}
        assert self.policy.parameters['scale_down_n_processes'] == 66

        self.policy.parameters = {'scale_down_n_processes': '66'}
        assert self.policy.parameters['scale_down_n_processes'] == 66

        params = {'minimum_processes': 'hi'}
        self.assertRaises(PolicyError, set_policy, params)

        params = {'minimum_processes': -1}
        self.assertRaises(PolicyError, set_policy, params)

        self.policy.parameters = {'minimum_processes': 66}
        assert self.policy.parameters['minimum_processes'] == 66

        self.policy.parameters = {'minimum_processes': '66'}
        assert self.policy.parameters['minimum_processes'] == 66

        params = {'maximum_processes': 'hi'}
        self.assertRaises(PolicyError, set_policy, params)

        params = {'maximum_processes': -1}
        self.assertRaises(PolicyError, set_policy, params)

        self.policy.parameters = {'maximum_processes': 66}
        assert self.policy.parameters['maximum_processes'] == 66

        self.policy.parameters = {'maximum_processes': '66'}
        assert self.policy.parameters['maximum_processes'] == 66

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
            'maximum_processes': 3,
            'minimum_processes': 1,
            'execution_engine_id': 'some_ee'
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

        all_procs_0 = {
            'pd0': [
            ],
            'pd1': [
            ]
        }

        all_procs_1 = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state, hostname=hostnames[0]),
            ],
            'pd1': [
            ]
        }

        all_procs_2 = {
            'pd0': [
                ProcessRecord.new(owner, upids[0], definition, state, hostname=hostnames[0]),
            ],
            'pd1': [
                ProcessRecord.new(owner, upids[1], definition, state, hostname=hostnames[1]),
            ]
        }

        all_procs_3 = {
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
        self.policy.apply_policy(all_procs_0, upids[:])

        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        # This average is above 2.0, so we should see one process schedule
        self.patch_urllib(make_ts_string(hostnames, loads_scale_up))

        self.policy.apply_policy(all_procs_0, [])
        self.assertEqual(self.mock_schedule.call_count, 1)
        self.assertEqual(self.mock_terminate.call_count, 0)

        # make sure schedule kwargs were passed through
        self.mock_schedule.assert_called_once_with(ANY, ANY, execution_engine_id="some_ee")

        # and another
        self.policy.last_scale_action = datetime.min
        self.patch_urllib(make_ts_string(hostnames, loads_scale_up))
        self.policy.apply_policy(all_procs_1, upids[:1])
        self.assertEqual(self.mock_schedule.call_count, 2)
        self.assertEqual(self.mock_terminate.call_count, 0)

        # and another
        self.policy.last_scale_action = datetime.min
        self.patch_urllib(make_ts_string(hostnames, loads_scale_up))
        self.policy.apply_policy(all_procs_2, upids[:2])
        self.assertEqual(self.mock_schedule.call_count, 3)
        self.assertEqual(self.mock_terminate.call_count, 0)

        # and we hit the maximum, so don't scale up anymore
        self.policy.last_scale_action = datetime.min
        self.patch_urllib(make_ts_string(hostnames, loads_scale_up))
        self.policy.apply_policy(all_procs_3, upids[:])
        self.assertEqual(self.mock_schedule.call_count, 3)
        self.assertEqual(self.mock_terminate.call_count, 0)

        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        # Now that we've made a scaling adjustment, we can test the cooldown
        # period. No scaling actions should happen, even though the metric
        # results warrant a scaling action

        self.policy.last_scale_action = datetime.now()

        self.patch_urllib(make_ts_string(hostnames, loads_scale_down))
        self.policy.apply_policy(all_procs_3, upids[:])

        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 0)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()

        # Change the last scale action timestamp to a long time ago
        self.policy.last_scale_action = datetime.min

        # This average is below 0.5, so we should see one process terminate
        self.patch_urllib(make_ts_string(hostnames, loads_scale_down))
        self.policy.apply_policy(all_procs_3, upids[:])

        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 1)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()
        upids.pop()  # this is normally done in ha core

        # Change the last scale action timestamp to a long time ago
        self.policy.last_scale_action = datetime.min

        # Keep the same low load, we should see another terminate
        self.patch_urllib(make_ts_string(hostnames, loads_scale_down))
        self.policy.apply_policy(all_procs_3, upids[:])

        self.assertEqual(self.mock_schedule.call_count, 0)
        self.assertEqual(self.mock_terminate.call_count, 1)
        self.mock_schedule.reset_mock()
        self.mock_terminate.reset_mock()
        upids.pop()  # this is normally done in ha core

        # Change the last scale action timestamp to a long time ago
        self.policy.last_scale_action = datetime.min

        # Keep the same low load, we should not see any action, as we
        # should be at the minimum number of processes
        self.patch_urllib(make_ts_string(hostnames, loads_scale_down))
        self.policy.apply_policy(all_procs_3, upids[:])

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
