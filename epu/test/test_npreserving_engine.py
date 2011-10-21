from ion.test import iontest

import ion.util.ionlog

from epu.decisionengine.test.mockcontroller import DeeControl
from epu.decisionengine.test.mockcontroller import DeeState
from epu.decisionengine import EngineLoader

import epu.states as InstanceStates
BAD_STATES = [InstanceStates.TERMINATING, InstanceStates.TERMINATED, InstanceStates.FAILED]

from epu.epucontroller import PROVISIONER_VARS_KEY

log = ion.util.ionlog.getLogger(__name__)

ENGINE="epu.decisionengine.impls.NpreservingEngine"

class NPreservingEngineTestCase(iontest.IonTestCase):

    def setUp(self):
        self.engine = EngineLoader().load(ENGINE)
        self.state = DeeState()
        self.state.new_qlen(0)
        self.control = DeeControl(self.state)

    def tearDown(self):
        pass


    # -----------------------------------------------------------------------
    # Basics
    # -----------------------------------------------------------------------
    
    def test_preserve_0(self):
        conf = {'preserve_n':'0'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 0

    def test_preserve_1(self):
        conf = {'preserve_n':'1'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1

    def test_preserve_N(self):
        conf = {'preserve_n':'5'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 5


    # -----------------------------------------------------------------------
    # PreserveN Reconfiguration
    # -----------------------------------------------------------------------
        
    def test_reconfigure1(self):
        conf = {'preserve_n':'0'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 0
        
        newconf = {'preserve_n':'1'}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        
        newconf2 = {'preserve_n':'0'}
        self.engine.reconfigure(self.control, newconf2)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 0
        
    def test_reconfigure2(self):
        for n in range(20):
            newconf = {'preserve_n':n}
            self.engine.reconfigure(self.control, newconf)
            self.engine.decide(self.control, self.state)
            assert self.control.num_launched == n
        for n in reversed(range(20)):
            newconf = {'preserve_n':n}
            self.engine.reconfigure(self.control, newconf)
            self.engine.decide(self.control, self.state)
            assert self.control.num_launched == n

    def test_bad_reconfigure1(self):
        conf = {'preserve_n':'asd'}
        try:
            self.engine.initialize(self.control, self.state, conf)
        except ValueError:
            return
        assert False
        
    def test_bad_reconfigure2(self):
        conf = {'preserve_n':'-1'}
        try:
            self.engine.initialize(self.control, self.state, conf)
        except ValueError:
            return
        assert False


    # -----------------------------------------------------------------------
    # Provisioner Vars Reconfiguration
    # -----------------------------------------------------------------------
        
    def test_provreconfigure1(self):
        newconf = {'preserve_n':'1'}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        assert self.control.prov_vars is None

    def test_provreconfigure2(self):
        pvars = {'workerid':'abcdefg'}
        conf = {'preserve_n':'0', PROVISIONER_VARS_KEY:pvars}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 0
        
        # Provisioner vars are not configured initially by the engine itself 
        assert self.control.prov_vars is None

    def test_provreconfigure3(self):
        conf = {'preserve_n':'1'}
        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        assert self.control.prov_vars is None

        pvars = {'workerid':'abcdefg'}
        newconf = {'preserve_n':'1', PROVISIONER_VARS_KEY:pvars}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        assert self.control.prov_vars is not None
        assert self.control.prov_vars.has_key("workerid")
        

    # -----------------------------------------------------------------------
    # Unique Instance Support
    # -----------------------------------------------------------------------
    
    def _get_iaas_id(self, uniq_id):
        """Return the IaaS ID that is serving as the unique ID in
        question."""
        # This is NOT a standard engine API method
        return self.engine._iaas_id_from_uniq_id(uniq_id)

    def _is_iaas_id_active(self, iaas_id):
        """Return True if the (mock) controller thinks this ID is active,
        return False if it is in a BAD_STATE or if it is not present.
        """
        instance = self.state.instances.get(iaas_id)
        if instance:
            log.debug("instance in %s state", instance.state)
        else:
            log.debug("no instance found")
        return instance and instance.state not in BAD_STATES

    # -----------------------------------------------------------------------
    # Unique Instances
    # -----------------------------------------------------------------------
    
    def test_uniques1(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)

    def test_uniques2(self):
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        iaas_id = self._get_iaas_id("2")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)

    def test_bad_unique1(self):
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        try:
            self.engine.reconfigure(self.control, newconf)
        except Exception:
            return
        assert False

    def test_uniques3(self):
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        iaas_id = self._get_iaas_id("2")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        try:
            self.engine.reconfigure(self.control, newconf)
        except Exception:
            return
        assert False

    def test_uniques4(self):
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        
        # Start two
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        iaas_id = self._get_iaas_id("2")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        
        # Remove one
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)

    def test_uniques5(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        
        same_iaas_id = iaas_id
        
        # Replace the variables of same unique instance
        uniq1 = {"akey":"uniq1value2"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'1', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        
        # The variable replacement only should not cause a new VM instance
        # to be launched.
        assert iaas_id == same_iaas_id


    # -----------------------------------------------------------------------
    # Generic and Unique Instances combined
    # -----------------------------------------------------------------------
    
    def test_generic_and_unique1(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        
        
    def test_generic_and_unique2(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'5', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 5
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)

        newconf2 = {'preserve_n':'1'}
        self.engine.reconfigure(self.control, newconf2)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        
        newconf3 = {'preserve_n':'2'}
        self.engine.reconfigure(self.control, newconf3)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        
        newconf4 = {'preserve_n':'1'}
        self.engine.reconfigure(self.control, newconf4)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 1
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        

    def test_generic_and_unique3(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        
        original_iaas_id = iaas_id
        
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"someotherkey":"uniq2value"}
        uniqs = {"1":uniq1, "2":uniq2}
        newconf = {'preserve_n':'3', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 3
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        assert original_iaas_id == iaas_id
        iaas_id = self._get_iaas_id("2")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)

        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        iaas_id = self._get_iaas_id("1")
        assert iaas_id is not None
        assert self._is_iaas_id_active(iaas_id)
        assert original_iaas_id == iaas_id

    def test_unique_recovery1(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        conf = {'preserve_n':'2', "unique_instances":uniqs}

        self.state.new_launch("instance1")
        self.state.new_launch("instance2", extravars=uniq1.copy())

        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        self.assertEqual(self.control.num_launched, 0)

    def test_unique_recovery2(self):
        uniq1 = {"akey":"uniq1value"}
        uniq2 = {"akey":"uniq2value"}

        uniqs = {"1":uniq1, "2":uniq2}
        conf = {'preserve_n':'3', "unique_instances":uniqs}

        self.state.new_launch("instance1", extravars=uniq1.copy())
        self.state.new_launch("instance2")

        # we are missing uniq2

        self.engine.initialize(self.control, self.state, conf)
        self.engine.decide(self.control, self.state)
        self.assertEqual(self.control.num_launched, 1)
        self.assertEqual(len(self.state.instances), 3)

        instance_ids = set(self.state.instances.keys())
        instance_ids.remove("instance1")
        instance_ids.remove("instance2")

        #this should be the launched one
        new_instance_id = instance_ids.pop()
        self.assertEqual(self.state.instances[new_instance_id].extravars, uniq2)

    def test_unhealthy(self):
        uniq1 = {"akey":"uniq1value"}
        uniqs = {"1":uniq1}
        newconf = {'preserve_n':'2', "unique_instances":uniqs}
        self.engine.reconfigure(self.control, newconf)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2

        unique_id = self._get_iaas_id("1")
        generic_id = None
        for iaas_id in self.state.instances:
            if iaas_id != unique_id:
                generic_id = iaas_id
        assert generic_id

        # all in good health, should be no change
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        assert self.control.total_launched == 2
        assert self.control.total_killed == 0

        assert self.state.instances[unique_id].state == InstanceStates.RUNNING
        assert self.state.instances[generic_id].state == InstanceStates.RUNNING

        self.state.new_health(unique_id, False)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        assert self.control.total_launched == 3
        assert self.control.total_killed == 1
        assert self.state.instances[unique_id].state == InstanceStates.TERMINATING
        assert self.state.instances[generic_id].state == InstanceStates.RUNNING

        # unique one should have been replaced
        unique_id = self._get_iaas_id("1")
        assert self.state.instances[unique_id].state == InstanceStates.RUNNING

        self.state.new_health(generic_id, False)
        self.engine.decide(self.control, self.state)
        assert self.control.num_launched == 2
        assert self.control.total_launched == 4
        assert self.control.total_killed == 2
        
        assert self.state.instances[generic_id].state == InstanceStates.TERMINATING
        assert self.state.instances[unique_id].state == InstanceStates.RUNNING






