import copy
import unittest

from epu.decisionengine.impls.simplest import CONF_PRESERVE_N
from epu.epumanagement import EPUManagement
from epu.epumanagement.test.mocks import MockSubscriberNotifier, MockProvisionerClient, MockOUAgentClient
from epu.epumanagement.conf import *

import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)

class NeedinessTests(unittest.TestCase):
    """
    Tests that cover the new_need and retire_node interface that a higher level system
    (e.g. the Process Dispatcher) can use to provide deliberate sensor input.
    """

    def setUp(self):
        # Mock mode:
        initial_conf = {EPUM_INITIALCONF_PERSISTENCE: "memory",
                        EPUM_INITIALCONF_EXTERNAL_DECIDE: True}
        self.notifier = MockSubscriberNotifier()
        self.provisioner_client = MockProvisionerClient()
        self.ou_client = MockOUAgentClient()
        self.epum = EPUManagement(initial_conf, self.notifier, self.provisioner_client, self.ou_client)

        # For instance-state changes "from the provisioner"
        self.provisioner_client._set_epum(self.epum)
        
        # For heartbeats "from the OU instance"
        self.ou_client._set_epum(self.epum)

    def test_one_need(self):
        """Use "register_need" with a newly intialized EPUManagement and test if one VM launches
        """
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        constraints = {CONF_IAAS_SITE: "00_iaas_site", CONF_IAAS_ALLOCATION: "00_iaas_alloc"}
        self.epum.msg_register_need(None, "00_dt_id", constraints, 1, None, None)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.provisioner_client.deployable_types_launched[0], "00_dt_id")

    def test_one_need_one_retire(self):
        """Use "register_need" with a newly intialized EPUManagement and test if five VMs launch.
        Use "retire_node" twice, and see if the three remaining VMs are the other nodes.
        """
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        constraints = {CONF_IAAS_SITE: "00_iaas_site", CONF_IAAS_ALLOCATION: "00_iaas_alloc"}
        self.epum.msg_register_need(None, "00_dt_id", constraints, 5, None, None)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 5)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 5)

        # pick two to retire
        launched = self.provisioner_client.launched_instance_ids
        nodeid_1 = launched[2]
        nodeid_2 = launched[4]
        retired = [nodeid_1, nodeid_2]
        others = [launched[0], launched[1], launched[3]]
        self.epum.msg_retire_node(None, nodeid_1)
        self.epum.msg_retire_node(None, nodeid_2)

        # Reset the num_needed to 3
        self.epum.msg_register_need(None, "00_dt_id", constraints, 3, None, None)

        # This should launch two kills
        self.epum._run_decisions()
        self.assertEqual(len(self.provisioner_client.terminated_instance_ids), 2)

        # And the two killed should be the ones we retired
        killed = self.provisioner_client.terminated_instance_ids
        killed.sort()
        retired.sort()
        self.assertEqual(killed, retired)

    def test_changing_needs(self):
        """Use "register_need" and "retire_node" many times, keeping the DT and constraints steady
        """
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        constraints = {CONF_IAAS_SITE: "00_iaas_site", CONF_IAAS_ALLOCATION: "00_iaas_alloc"}

        self.epum.msg_register_need(None, "00_dt_id", constraints, 5, None, None)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 5)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 5)

        # retire four out of the five
        launched = copy.copy(self.provisioner_client.launched_instance_ids)
        for node_id in launched[:-1]:
            self.epum.msg_retire_node(None, node_id)
        keep_id = launched[-1]
        log.debug("keep_id: %s" % keep_id)

        # not a list copy, just a shorter ref name
        terminated = self.provisioner_client.terminated_instance_ids

        # Reset the num_needed to 2 (but 4 are retirable)
        self.epum.msg_register_need(None, "00_dt_id", constraints, 2, None, None)

        # This should launch three kills, not four
        self.epum._run_decisions()
        self.assertEqual(len(terminated), 3)

        # The non-retired node should be among the ones left, i.e. NOT in terminated list
        self.assertFalse(keep_id in terminated)

        # Reset the num_needed to 0
        self.epum.msg_register_need(None, "00_dt_id", constraints, 0, None, None)

        # This should launch two more kills and the one we didn't retire (keep_id) should be
        # killed anyhow.  The num_needed (or some other forceful engine policy) trumps the
        # retirable "suggestions"
        self.epum._run_decisions()

        self.assertEqual(len(terminated), 5)
        self.assertTrue(keep_id in terminated)
        for node in launched:
            self.assertTrue(node in terminated)

        # Back to N=0 now, but engine should remain
        # Need 6
        self.assertEqual(self.provisioner_client.provision_count, 5)
        self.epum.msg_register_need(None, "00_dt_id", constraints, 6, None, None)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 11)

        # Done
        self.assertEqual(len(terminated), 5)
        self.epum.msg_register_need(None, "00_dt_id", constraints, 0, None, None)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 11)
        self.assertEqual(len(terminated), 11)

    def test_with_different_constraints(self):
        """Exercise needs-based launching with differing key constraints.  There should be
         separate engines for each permutation
        """
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)

        # Same DT and IaaS site, but different allocation
        dt_id = "01_dt_id"
        constraints1 = {CONF_IAAS_SITE: "01_iaas_site", CONF_IAAS_ALLOCATION: "01_iaas_alloc"}
        constraints2 = {CONF_IAAS_SITE: "01_iaas_site", CONF_IAAS_ALLOCATION: "02_iaas_alloc"}

        self.epum.msg_register_need(None, dt_id, constraints1, 5, None, None)
        self.epum.msg_register_need(None, dt_id, constraints2, 5, None, None)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 10)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 10)

        # digging into internal structure to verify
        self.assertEquals(len(self.epum.decider.engines), 2)
        engine_to_reconfigure = None
        for name,engine in self.epum.decider.engines.iteritems():
            log.debug("engine: %s" % name)
            if "02_iaas_alloc" in name:
                engine_to_reconfigure = name
            self.assertNotEqual(engine, None)
            self.assertEqual(engine.initialize_count, 1)
            self.assertEqual(engine.initialize_conf[CONF_PRESERVE_N], 5)
            self.assertEqual(engine.decide_count, 1)
            self.assertEqual(engine.preserve_n, 5)

        # reconfigure the second one
        self.epum.msg_register_need(None, dt_id, constraints2, 3, None, None)
        self.epum._run_decisions()

        # digging into internal structure to verify
        self.assertEquals(len(self.epum.decider.engines), 2)
        for name,engine in self.epum.decider.engines.iteritems():
            self.assertEqual(engine.initialize_count, 1)
            self.assertEqual(engine.decide_count, 2)
            if name == engine_to_reconfigure:
                self.assertEqual(engine.reconfigure_count, 1)
                self.assertEqual(engine.preserve_n, 3)
            else:
                self.assertEqual(engine.reconfigure_count, 0)
                self.assertEqual(engine.preserve_n, 5)

        # register a need for a new DT
        dt_id_2 = "02_dt_id"
        self.epum.msg_register_need(None, dt_id_2, constraints1, 5, None, None)
        self.epum._run_decisions()

        # digging into internal structure to verify
        dtone_count = 0
        dttwo_count = 0
        self.assertEquals(len(self.epum.decider.engines), 3)
        for name,engine in self.epum.decider.engines.iteritems():
            if engine.deployable_type == dt_id:
                dtone_count += 1
                self.assertEqual(engine.decide_count, 3)
            elif engine.deployable_type == dt_id_2:
                dttwo_count += 1
                self.assertEqual(engine.decide_count, 1)
            else:
                self.fail("unexpected dt config: %s" % engine.deployable_type)
        self.assertEquals(dtone_count, 2)
        self.assertEquals(dttwo_count, 1)

    def test_with_normal_epus(self):
        """Exercise some simple needs-based launching while a 'normal' EPU is also configured.
        """
        self.epum.initialize()

        # The "normal" EPU
        engine_class = "epu.decisionengine.impls.simplest.SimplestEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        engine = {CONF_PRESERVE_N:2}
        epu_conf = {EPUM_CONF_GENERAL:general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}
        epu_name = "testing123"
        self.epum.msg_add_epu(None, epu_name, epu_conf)

        self.assertEqual(self.provisioner_client.provision_count, 0)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)

        # The needs-based EPU
        constraints = {CONF_IAAS_SITE: "00_iaas_site", CONF_IAAS_ALLOCATION: "00_iaas_alloc"}
        self.epum.msg_register_need(None, "00_dt_id", constraints, 5, None, None)

        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 7)

        # Reconfigure the "normal" EPU
        epu_conf2 = {EPUM_CONF_ENGINE: {CONF_PRESERVE_N:4}}
        self.epum.msg_reconfigure_epu(None, epu_name, epu_conf2)

        self.assertEqual(self.provisioner_client.provision_count, 7)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 9)

        # Adjust needs for needs-based EPU
        self.epum.msg_register_need(None, "00_dt_id", constraints, 10, None, None)
        
        self.assertEqual(self.provisioner_client.provision_count, 9)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 14)
