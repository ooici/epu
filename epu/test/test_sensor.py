# Copyright 2013 University of Chicago

import unittest

from epu.epumanagement.test.mocks import MockControl
from epu.decisionengine.impls.sensor import SensorEngine
from epu.states import InstanceState
from epu.sensors import Statistics


class TestSensorDE(unittest.TestCase):

    def _get_config(self, minimum_vms, maximum_vms, metric, sample_function,
            scale_up_threshold, scale_up_n_vms, scale_down_threshold,
            scale_down_n_vms, deployable_type, site="chicago", allocation="small",
            unique_key=None, unique_values=None):
        cfg = dict(minimum_vms=minimum_vms, maximum_vms=maximum_vms,
                metric=metric, sample_function=sample_function,
                scale_up_threshold=scale_up_threshold,
                scale_up_n_vms=scale_up_n_vms,
                scale_down_threshold=scale_down_threshold,
                scale_down_n_vms=scale_down_n_vms,
                deployable_type=deployable_type,
                iaas_site=site, iaas_allocation=allocation)
        if unique_key:
            cfg['unique_key'] = unique_key
        if unique_values:
            cfg['unique_values'] = unique_values
        return cfg

    def test_basic(self):
        control = MockControl()
        state = control.get_state()

        minimum_n = 1
        maximum_n = 3
        metric = 'fake'
        sample_function = Statistics.AVERAGE
        scale_up_threshold = 2.0
        scale_down_threshold = 0.5
        scale_up_n_vms = 1
        scale_down_n_vms = 1
        config = self._get_config(minimum_n, maximum_n, metric, sample_function,
                scale_up_threshold, scale_up_n_vms, scale_down_threshold,
                scale_down_n_vms, "dt1")

        sensor_series_up = [1, 3, 5]
        sensor_average_up = sum(sensor_series_up) / len(sensor_series_up)
        sensor_data_up = {metric: {Statistics.SERIES: sensor_series_up, Statistics.AVERAGE: sensor_average_up}}

        sensor_series_down = [0, 1, 0]
        sensor_average_down = float(sum(sensor_series_down)) / len(sensor_series_down)
        sensor_data_down = {metric: {Statistics.SERIES: sensor_series_down, Statistics.AVERAGE: sensor_average_down}}

        de = SensorEngine()
        de.initialize(control, state, config)

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 1)
        state = control.get_state()

        control.set_instance_sensor_data(sensor_data_up)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 2)

        control.set_instance_sensor_data(sensor_data_up)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 3)

        # We should see this stay at 3, since we hit max_instances
        control.set_instance_sensor_data(sensor_data_up)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 3)

        # Now, change the sensor data to scale us down
        control.set_instance_sensor_data(sensor_data_down)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 3)
        self.assertEqual(control._destroy_calls, 1)

        control.set_instance_sensor_data(sensor_data_down)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._destroy_calls, 2)

        # We are now at min, so we shouldn't see any more scaling
        control.set_instance_sensor_data(sensor_data_down)
        state = control.get_state()

        de.decide(control, state)
        self.assertEqual(control._destroy_calls, 2)

    def test_uniques(self):
        control = MockControl()
        state = control.get_state()

        minimum_n = 2
        maximum_n = 3
        metric = 'fake'
        sample_function = Statistics.AVERAGE
        scale_up_threshold = 2.0
        scale_down_threshold = 0.5
        scale_up_n_vms = 1
        scale_down_n_vms = 1
        config = self._get_config(minimum_n, maximum_n, metric, sample_function,
                scale_up_threshold, scale_up_n_vms, scale_down_threshold,
                scale_down_n_vms, "dt1", unique_key="somekey", unique_values=[1, 2, 3])

        de = SensorEngine()
        de.initialize(control, state, config)

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 2)
        self.assertEqual(control.instances[0].extravars, {"somekey": 1})
        self.assertEqual(control.instances[1].extravars, {"somekey": 2})

        # kill first. replacement should get same unique
        control.instances[0].state = InstanceState.FAILED

        state = control.get_state()
        de.decide(control, state)
        self.assertEqual(control._launch_calls, 3)
        self.assertEqual(control.instances[2].extravars, {"somekey": 1})
        self.assertEqual(control.instances[1].extravars, {"somekey": 2})
