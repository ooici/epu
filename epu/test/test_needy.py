# Copyright 2013 University of Chicago

import unittest

from epu.epumanagement.test.mocks import MockControl
from epu.decisionengine.impls.needy import NeedyEngine
from epu.states import InstanceState


class TestNeedyDE(unittest.TestCase):

    def _get_config(self, preserve_n, deployable_type, site="chicago",
                    allocation="small", unique_key=None, unique_values=None):
        cfg = dict(preserve_n=preserve_n, deployable_type=deployable_type,
            iaas_site=site, iaas_allocation=allocation)
        if unique_key:
            cfg['unique_key'] = unique_key
        if unique_values:
            cfg['unique_values'] = unique_values
        return cfg

    def test_basic(self):
        control = MockControl()
        state = control.get_state()

        de = NeedyEngine()
        de.initialize(control, state, self._get_config(1, "dt1"))

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 1)
        state = control.get_state()

        de.reconfigure(control, dict(preserve_n=2))

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 2)

    def test_uniques(self):
        control = MockControl()
        state = control.get_state()

        de = NeedyEngine()
        de.initialize(control, state, self._get_config(2, "dt1",
            unique_key="somekey", unique_values=[1, 2, 3]))

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

    def test_ran_out_of_uniques(self):
        control = MockControl()
        state = control.get_state()

        de = NeedyEngine()
        de.initialize(control, state, self._get_config(2, "dt1",
            unique_key="somekey", unique_values=[1, ]))

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 1)
        self.assertEqual(control.instances[0].extravars, {"somekey": 1})

        # kill first. replacement should get same unique
        control.instances[0].state = InstanceState.FAILED

        state = control.get_state()
        de.decide(control, state)
        self.assertEqual(control._launch_calls, 2)
        self.assertEqual(control.instances[1].extravars, {"somekey": 1})

    def test_uniques_string_values(self):
        control = MockControl()
        state = control.get_state()

        de = NeedyEngine()
        de.initialize(control, state, self._get_config(2, "dt1",
            unique_key="somekey", unique_values="a, b,c "))

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 2)
        self.assertEqual(control.instances[0].extravars, {"somekey": "a"})
        self.assertEqual(control.instances[1].extravars, {"somekey": "b"})

        # kill first. replacement should get same unique
        control.instances[0].state = InstanceState.FAILED

        state = control.get_state()
        de.decide(control, state)
        self.assertEqual(control._launch_calls, 3)
        self.assertEqual(control.instances[2].extravars, {"somekey": "a"})
        self.assertEqual(control.instances[1].extravars, {"somekey": "b"})

    def test_uniques_empty_values_1(self):
        control = MockControl()
        state = control.get_state()

        de = NeedyEngine()
        de.initialize(control, state, self._get_config(1, "dt1",
            unique_key="somekey", unique_values=""))

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 1)
        self.assertEqual(control.instances[0].extravars, None)

    def test_uniques_empty_values_2(self):
        control = MockControl()
        state = control.get_state()

        de = NeedyEngine()
        de.initialize(control, state, self._get_config(1, "dt1",
            unique_key="somekey", unique_values=" "))

        de.decide(control, state)
        self.assertEqual(control._launch_calls, 1)
        self.assertEqual(control.instances[0].extravars, None)
