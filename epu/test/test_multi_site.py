# Copyright 2013 University of Chicago

import unittest
import random

from epu.decisionengine.impls.phantom_multi_site_overflow import PhantomMultiSiteOverflowEngine
from epu.states import InstanceState
from epu.epumanagement.test.mocks import MockControl, MockState
from epu.sensors import Statistics

HEALTHY_STATES = [InstanceState.REQUESTING, InstanceState.REQUESTED,
    InstanceState.PENDING, InstanceState.RUNNING, InstanceState.STARTED]
UNHEALTHY_STATES = [InstanceState.TERMINATING, InstanceState.TERMINATED,
    InstanceState.FAILED, InstanceState.RUNNING_FAILED]


def make_conf(clouds, n, dtname, instance_type):
    conf = {}
    conf['minimum_vms'] = n
    conf['maximum_vms'] = n
    conf['clouds'] = clouds
    conf['dtname'] = dtname
    conf['instance_type'] = instance_type

    return conf


def make_sensor_conf(clouds, minimum_vms, maximum_vms, metric, sample_function,
        scale_up_threshold, scale_up_n_vms, scale_down_threshold,
        scale_down_n_vms, dtname, instance_type, cooldown_period):
    cfg = dict(clouds=clouds, minimum_vms=minimum_vms, maximum_vms=maximum_vms,
            metric=metric, sample_function=sample_function,
            scale_up_threshold=scale_up_threshold,
            scale_up_n_vms=scale_up_n_vms,
            scale_down_threshold=scale_down_threshold,
            scale_down_n_vms=scale_down_n_vms,
            dtname=dtname, instance_type=instance_type,
            cooldown_period=cooldown_period)
    return cfg


def make_cloud_conf(name, size, rank):
    cloud_conf = {}
    cloud_conf['site_name'] = name
    cloud_conf['size'] = size
    cloud_conf['rank'] = rank
    return cloud_conf


class TestMultiSiteDE(unittest.TestCase):

    def test_basic_start(self):
        control = MockControl()
        state = MockState()

        n = 4
        cloud = make_cloud_conf('hotel', n, 1)
        conf = make_conf([cloud, ], n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, state)

        self.assertEqual(control._launch_calls, n)

    def test_basic_sensor(self):
        control = MockControl()
        state = MockState()

        minimum_vms = 4
        maximum_vms = 6
        metric = "something"
        sample_function = "Average"
        scale_up_threshold = 2
        scale_down_threshold = 0.5
        scale_up_n_vms = 1
        scale_down_n_vms = 1
        cooldown_period = 0
        cloud = make_cloud_conf('hotel', maximum_vms, 1)
        conf = make_sensor_conf([cloud, ], minimum_vms, maximum_vms, metric,
                sample_function, scale_up_threshold, scale_up_n_vms,
                scale_down_threshold, scale_down_n_vms, 'testdt', 'm1.small',
                cooldown_period)

        sensor_series_up = [1, 3, 5]
        sensor_average_up = sum(sensor_series_up) / len(sensor_series_up)
        sensor_data_up = {metric: {Statistics.SERIES: sensor_series_up, Statistics.AVERAGE: sensor_average_up}}

        sensor_series_down = [0, 1, 0]
        sensor_average_down = float(sum(sensor_series_down)) / len(sensor_series_down)
        sensor_data_down = {metric: {Statistics.SERIES: sensor_series_down, Statistics.AVERAGE: sensor_average_down}}

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        # after a start, we should see the DE start the minimum number of VMs
        state = control.get_state()
        de.decide(control, state)

        self.assertEqual(control._launch_calls, minimum_vms)
        previous_launch_calls = control._launch_calls

        # Now we add some sensor data, and we should see a scale up until we get to max VMs (n=5)
        control.set_instance_sensor_data(sensor_data_up)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls, previous_launch_calls + scale_up_n_vms)
        previous_launch_calls = control._launch_calls

        # We should see one more (n=6)
        control.set_instance_sensor_data(sensor_data_up)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls, previous_launch_calls + scale_up_n_vms)

        # Now we should be at max vms, and see no more starts
        control.set_instance_sensor_data(sensor_data_up)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls, maximum_vms)

        # Now we scale down with some vms die
        control.set_instance_sensor_data(sensor_data_down)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls, maximum_vms)
        self.assertEqual(control._destroy_calls, 1)

        # and another one
        control.set_instance_sensor_data(sensor_data_down)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls, maximum_vms)
        self.assertEqual(control._destroy_calls, 2)

        # and we should now be at minimum vms
        control.set_instance_sensor_data(sensor_data_down)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls - control._destroy_calls, minimum_vms)

    def test_start_on_2_clouds(self):
        control = MockControl()
        state = MockState()

        n = 4
        cloud1 = make_cloud_conf('hotel', n / 2, 1)
        cloud2 = make_cloud_conf('sierra', n / 2, 2)
        conf = make_conf([cloud1, cloud2], n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, state)

        self.assertEqual(control.site_launch_calls['hotel'], n / 2)
        self.assertEqual(control.site_launch_calls['sierra'], n / 2)
        self.assertEqual(control._launch_calls, n)

    def test_basic_too_many_clouds(self):
        control = MockControl()
        state = MockState()

        n = 4
        cloud1 = make_cloud_conf('hotel', n, 1)
        cloud2 = make_cloud_conf('sierra', n, 2)
        cloud3 = make_cloud_conf('foxtrot', n, 3)
        conf = make_conf([cloud1, cloud2, cloud3], n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, state)

        self.assertEqual(control.site_launch_calls['hotel'], n)
        self.assertFalse('sierra' in control.site_launch_calls)
        self.assertFalse('foxtrot' in control.site_launch_calls)
        self.assertEqual(control._launch_calls, n)

    def test_basic_negative_one(self):
        control = MockControl()
        state = MockState()

        n = 4
        cloud1 = make_cloud_conf('hotel', -1, 1)
        cloud2 = make_cloud_conf('sierra', -1, 2)
        cloud3 = make_cloud_conf('foxtrot', -1, 3)
        conf = make_conf([cloud1, cloud2, cloud3], n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, state)

        self.assertEqual(control.site_launch_calls['hotel'], n)
        self.assertFalse('sierra' in control.site_launch_calls)
        self.assertFalse('foxtrot' in control.site_launch_calls)
        self.assertEqual(control._launch_calls, n)

    def test_basic_move_to_pending(self):
        control = MockControl()
        state = MockState()

        n = 2
        cloud_names = [('hotel', n), ]
        clouds = []

        rank = 1
        for (cn, count) in cloud_names:
            cloud = make_cloud_conf(cn, count, rank)
            rank = rank + 1
            clouds.append(cloud)

        conf = make_conf(clouds, n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())

        self.assertEqual(control._launch_calls, n)

        for i in control.instances:
            i.state = InstanceState.PENDING

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls, n)

    def test_basic_node_failed(self):
        control = MockControl()
        state = MockState()

        n = 4
        cloud = make_cloud_conf('hotel', n, 1)
        conf = make_conf([cloud, ], n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls, n)

        i = random.choice(control.instances)
        i.state = InstanceState.FAILED

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls, n + 1)

    def test_basic_reconf_one_cloud_increase(self):
        control = MockControl()
        state = MockState()

        n = 4
        cloud = make_cloud_conf('hotel', n, 1)
        conf = make_conf([cloud, ], n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls, n)

        n = 6
        cloud = make_cloud_conf('hotel', n, 1)
        newconf = make_conf([cloud, ], n, 'testdt', 'm1.small')
        de.reconfigure(control, newconf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls, n)

    def test_basic_reconf_one_cloud_n_and_overall_n_decrease(self):
        control = MockControl()
        state = MockState()

        n = 4
        cloud = make_cloud_conf('hotel', n, 1)
        conf = make_conf([cloud, ], n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls, n)

        n = 2
        cloud = make_cloud_conf('hotel', n, 1)
        newconf = make_conf([cloud, ], n, 'testdt', 'm1.small')
        de.reconfigure(control, newconf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls - control._destroy_calls, n)

    def test_basic_reconf_one_cloud_n_decrease(self):
        control = MockControl()
        state = MockState()

        cloud_n = 4
        overall_n = 4
        cloud = make_cloud_conf('hotel', cloud_n, 1)
        conf = make_conf([cloud, ], overall_n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls, overall_n)

        cloud_n = cloud_n / 2
        cloud = make_cloud_conf('hotel', cloud_n, 1)
        newconf = make_conf([cloud, ], overall_n, 'testdt', 'm1.small')
        de.reconfigure(control, newconf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls - control._destroy_calls, cloud_n)

    def test_basic_reconf_one_cloud_overall_n_decrease(self):
        control = MockControl()
        state = MockState()

        cloud_n = 4
        overall_n = 4
        cloud = make_cloud_conf('hotel', cloud_n, 1)
        conf = make_conf([cloud, ], overall_n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls, overall_n)

        overall_n = overall_n / 2
        cloud = make_cloud_conf('hotel', cloud_n, 1)
        newconf = make_conf([cloud, ], overall_n, 'testdt', 'm1.small')
        de.reconfigure(control, newconf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls - control._destroy_calls, overall_n)

    def test_reconf_two_clouds_n_increase(self):
        control = MockControl()
        state = MockState()

        overall_n = 3
        hotel_n = 4
        sierra_n = 2

        hotel_cloud = make_cloud_conf('hotel', hotel_n, 1)
        sierra_cloud = make_cloud_conf('sierra', sierra_n, 2)

        conf = make_conf([hotel_cloud, sierra_cloud, ], overall_n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())

        self.assertEqual(control._launch_calls, overall_n)
        self.assertEqual(control.site_launch_calls['hotel'], 3)
        self.assertFalse('sierra' in control.site_launch_calls)

        overall_n = 5
        hotel_cloud = make_cloud_conf('hotel', hotel_n, 1)
        sierra_cloud = make_cloud_conf('sierra', sierra_n, 2)
        newconf = make_conf([hotel_cloud, sierra_cloud, ], overall_n, 'testdt', 'm1.small')
        de.reconfigure(control, newconf)
        de.decide(control, control.get_state())

        self.assertEqual(control._launch_calls, overall_n)
        self.assertEqual(control.site_launch_calls['hotel'], hotel_n)
        self.assertEqual(control.site_launch_calls['sierra'], 1)

        overall_n = hotel_n + sierra_n + 10
        hotel_cloud = make_cloud_conf('hotel', hotel_n, 1)
        sierra_cloud = make_cloud_conf('sierra', sierra_n, 2)
        newconf = make_conf([hotel_cloud, sierra_cloud, ], overall_n, 'testdt', 'm1.small')
        de.reconfigure(control, newconf)
        de.decide(control, control.get_state())

        self.assertEqual(control._launch_calls, hotel_n + sierra_n)
        self.assertEqual(control.site_launch_calls['hotel'], hotel_n)
        self.assertEqual(control.site_launch_calls['sierra'], sierra_n)

    def test_reconf_two_clouds_n_stays_the_same(self):
        control = MockControl()
        state = MockState()

        hotel_n = 4
        sierra_n = 2
        overall_n = hotel_n + sierra_n

        hotel_cloud = make_cloud_conf('hotel', hotel_n, 1)
        sierra_cloud = make_cloud_conf('sierra', sierra_n, 2)

        conf = make_conf([hotel_cloud, sierra_cloud, ], overall_n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())

        self.assertEqual(control._launch_calls, overall_n)
        self.assertEqual(control.site_launch_calls['hotel'], hotel_n)
        self.assertEqual(control.site_launch_calls['sierra'], sierra_n)

        hotel_cloud = make_cloud_conf('hotel', hotel_n + 5, 1)
        sierra_cloud = make_cloud_conf('sierra', sierra_n, 2)
        newconf = make_conf([hotel_cloud, sierra_cloud, ], overall_n, 'testdt', 'm1.small')
        de.reconfigure(control, newconf)
        de.decide(control, control.get_state())

        # since we only rebalance optimistically nothing should change
        self.assertEqual(control._launch_calls, overall_n)
        self.assertEqual(control.site_launch_calls['hotel'], hotel_n)
        self.assertEqual(control.site_launch_calls['sierra'], sierra_n)

    def test_reconf_two_clouds_n_stays_the_same_but_node_dies(self):
        control = MockControl()
        state = MockState()

        hotel_n = 4
        sierra_n = 2
        overall_n = hotel_n + sierra_n

        hotel_cloud = make_cloud_conf('hotel', hotel_n, 1)
        sierra_cloud = make_cloud_conf('sierra', sierra_n, 2)

        conf = make_conf([hotel_cloud, sierra_cloud, ], overall_n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())

        self.assertEqual(control._launch_calls, overall_n)
        self.assertEqual(control.site_launch_calls['hotel'], hotel_n)
        self.assertEqual(control.site_launch_calls['sierra'], sierra_n)

        hotel_cloud = make_cloud_conf('hotel', hotel_n + 5, 1)
        sierra_cloud = make_cloud_conf('sierra', sierra_n, 2)
        newconf = make_conf([hotel_cloud, sierra_cloud, ], overall_n, 'testdt', 'm1.small')
        de.reconfigure(control, newconf)
        de.decide(control, control.get_state())

        # since we only rebalance optimistically nothing should change
        self.assertEqual(control._launch_calls, overall_n)
        self.assertEqual(control.site_launch_calls['hotel'], hotel_n)
        self.assertEqual(control.site_launch_calls['sierra'], sierra_n)

        for i in control.instances:
            if i.site == 'sierra':
                i.state = InstanceState.FAILED
                break

        # check to verify that we optimistically rebalanced
        de.decide(control, control.get_state())
        healthy_instances = control.get_instances(states=HEALTHY_STATES)
        sierra_instances = control.get_instances(site='sierra', states=HEALTHY_STATES)
        hotel_instances = control.get_instances(site='hotel', states=HEALTHY_STATES)

        self.assertEqual(len(healthy_instances), overall_n)
        self.assertEqual(len(hotel_instances), hotel_n + 1)
        self.assertEqual(len(sierra_instances), sierra_n - 1)

    def test_conf_errors(self):
        control = MockControl()
        state = MockState()

        conf = None
        de = PhantomMultiSiteOverflowEngine()
        try:
            de.initialize(control, state, conf)
            self.fail("An exception should have been thrown")
        except ValueError:
            pass

        conf = {}
        de = PhantomMultiSiteOverflowEngine()
        try:
            de.initialize(control, state, conf)
            self.fail("An exception should have been thrown")
        except ValueError:
            pass

        needed_keys = [('minimum_vms', 1), ('dtname', 'hello'), ('instance_type', 'm1.huge')]

        for k in needed_keys:
            conf[k[0]] = k[1]
            de = PhantomMultiSiteOverflowEngine()
            try:
                de.initialize(control, state, conf)
                self.fail("An exception should have been thrown")
            except ValueError:
                pass

        conf['clouds'] = make_cloud_conf('hotel', 10, 3)
        de = PhantomMultiSiteOverflowEngine()
        try:
            de.initialize(control, state, conf)
            self.fail("An exception should have been thrown")
        except ValueError:
            pass
        except Exception:
            pass

        cloud1 = make_cloud_conf('hotel', 10, 1)
        cloud2 = make_cloud_conf('sierra', 10, 3)
        conf['clouds'] = [cloud1, cloud2]
        de = PhantomMultiSiteOverflowEngine()
        try:
            de.initialize(control, state, conf)
            self.fail("An exception should have been thrown")
        except ValueError:
            pass
        except Exception:
            pass

        cloud1 = make_cloud_conf('hotel', 10, 1)
        cloud2 = make_cloud_conf('sierra', 10, 2)
        conf['clouds'] = [cloud1, cloud2]
        de.initialize(control, state, conf)

        cloud1 = make_cloud_conf('hotel', 10, 1)
        cloud2 = make_cloud_conf('sierra', 10, 5)
        conf['clouds'] = [cloud1, cloud2]

        try:
            de.reconfigure(control, conf)
            self.fail("An exception should have been thrown")
        except ValueError:
            pass
        except Exception:
            pass

        cloud2 = make_cloud_conf('foxtrot', 10, 1)
        conf['clouds'] = [cloud2, ]
        try:
            de.reconfigure(control, conf)
            self.fail("An exception should have been thrown")
        except ValueError:
            pass
        except Exception:
            pass

        cloud2 = make_cloud_conf('foxtrot', 10, 3)
        conf['clouds'] = [cloud2, ]
        de.reconfigure(control, conf)

        try:
            de.reconfigure(control, {})
            self.fail("An exception should have been thrown")
        except ValueError:
            pass
        except Exception:
            pass
        de.dying()

    def test_basic_capacity(self):
        control = MockControl()
        state = MockState()

        desired_n = 10
        capacity = 5

        hotel_cloud = make_cloud_conf('hotel', desired_n, 1)

        conf = make_conf([hotel_cloud, ], desired_n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())

        healthy_instances = control.get_instances(states=HEALTHY_STATES)
        self.assertEqual(len(healthy_instances), desired_n)

        for i in range(100):
            # may need to add a sleep once the time metric is added
            de.decide(control, control.get_state())

            # mark them all failed, go back and mark up to capacity as running
            # this is how we fake a capacity of n
            for i in control.instances:
                i.state = InstanceState.FAILED
            for i in range(0, capacity):
                control.instances[i].state = InstanceState.RUNNING

        healthy_instances = control.get_instances(states=HEALTHY_STATES)
        self.assertEqual(len(healthy_instances), capacity)
        # at this point the DE should have figured out that we are at capacity so it should
        # not decide to try to add more
        de.decide(control, control.get_state())

        healthy_instances = control.get_instances(states=HEALTHY_STATES)
        self.assertEqual(len(healthy_instances), capacity)

        # now reduce the desired n down to the capacity. nothing should be killed
        conf = make_conf([hotel_cloud, ], capacity, 'testdt', 'm1.small')
        de.reconfigure(control, conf)

        de.decide(control, control.get_state())

        healthy_instances = control.get_instances(states=HEALTHY_STATES)
        self.assertEqual(len(healthy_instances), capacity)

        self.assertEqual(control._destroy_calls, 0)

    def test_capacity_overflow(self):
        control = MockControl()
        state = MockState()

        desired_n = 10
        capacity = 5

        hotel_cloud = make_cloud_conf('hotel', desired_n * 2, 1)  # set hotel to have plenty of room
        sierra_cloud = make_cloud_conf('sierra', desired_n, 2)  # set hotel to have enough for the overflow

        conf = make_conf([hotel_cloud, sierra_cloud, ], desired_n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())

        healthy_instances = control.get_instances(states=HEALTHY_STATES)
        self.assertEqual(len(healthy_instances), desired_n)

        for i in range(100):
            de.decide(control, control.get_state())

            skip_count = 0
            for i in control.instances:
                if i.site == 'hotel':
                    if skip_count >= capacity:
                        i.state = InstanceState.FAILED
                    skip_count = skip_count + 1

        healthy_instances = control.get_instances(states=HEALTHY_STATES, site='hotel')
        self.assertEqual(len(healthy_instances), capacity)
        # at this point the DE should have figured out that we are at capacity so it should
        # not decide to try to add more
        de.decide(control, control.get_state())

        healthy_instances = control.get_instances(states=HEALTHY_STATES, site='hotel')
        self.assertEqual(len(healthy_instances), capacity)
        sierra_healthy_instances = control.get_instances(states=HEALTHY_STATES, site='sierra')
        self.assertEqual(len(sierra_healthy_instances), desired_n - capacity)
        healthy_instances = control.get_instances(states=HEALTHY_STATES)
        self.assertEqual(len(healthy_instances), desired_n)

    def test_multi_cloud_n_decrease_multi_site(self):
        control = MockControl()
        state = MockState()

        n = 14
        cloud1 = make_cloud_conf('hotel', 8, 1)
        cloud2 = make_cloud_conf('sierra', 4, 2)
        cloud3 = make_cloud_conf('foxtrot', 2, 3)

        conf = make_conf([cloud1, cloud2, cloud3], n, 'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls, n)

        n = 10
        newconf = make_conf([cloud1, cloud2, cloud3], n, 'testdt', 'm1.small')
        de.reconfigure(control, newconf)

        de.decide(control, control.get_state())
        self.assertEqual(control._launch_calls - control._destroy_calls, n)

        healthy_instances = control.get_instances(states=HEALTHY_STATES, site='foxtrot')
        self.assertEqual(len(healthy_instances), 0)
        healthy_instances = control.get_instances(states=HEALTHY_STATES, site='sierra')
        self.assertEqual(len(healthy_instances), 2)
        healthy_instances = control.get_instances(states=HEALTHY_STATES, site='hotel')
        self.assertEqual(len(healthy_instances), 8)
