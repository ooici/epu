import unittest
import logging

from epu.decisionengine.impls.simplest import CONF_PRESERVE_N
from epu.epumanagement import EPUManagement
from epu.epumanagement.test.mocks import MockSubscriberNotifier, MockProvisionerClient, MockOUAgentClient
from epu.epumanagement.conf import *
from epu.exceptions import NotFoundError

log = logging.getLogger(__name__)

MOCK_PKG = "epu.epumanagement.test.mocks"

class EPUManagementBasicTests(unittest.TestCase):
    """
    Tests that cover basic things like running a decision engine cycle and making sure a VM
    is requested, etc.
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

    def _config_mock1(self):
        """Keeps increment count
        """
        general = {EPUM_CONF_ENGINE_CLASS: MOCK_PKG + ".MockDecisionEngine01"}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        engine = {CONF_PRESERVE_N:1}
        return {EPUM_CONF_GENERAL:general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}

    def _config_mock2(self):
        """decide and reconfigure fail
        """
        conf = self._config_mock1()
        conf[EPUM_CONF_GENERAL] = {EPUM_CONF_ENGINE_CLASS: MOCK_PKG + ".MockDecisionEngine02"}
        return conf

    def _config_mock3(self):
        """uses Deferred
        """
        conf = self._config_mock1()
        conf[EPUM_CONF_GENERAL] = {EPUM_CONF_ENGINE_CLASS: MOCK_PKG + ".MockDecisionEngine03"}
        return conf

    def _config_simplest_domainconf(self, n_preserving):
        """Get 'simplest' domain conf with specified NPreserving policy
        """
        engine_class = "epu.decisionengine.impls.simplest.SimplestEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        engine = {CONF_PRESERVE_N:n_preserving}
        return {EPUM_CONF_GENERAL:general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}

    def test_engine_decide(self):
        """
        Verify decide is called at expected time
        """
        self.epum.initialize()
        config = self._config_mock1()
        owner = "owner1"
        domain_id = "testing123"
        self.epum.msg_add_domain(owner, domain_id, config)
        self.epum._run_decisions()

        # digging into internal structure to get engine instances
        engine = self.epum.decider.engines[(owner, domain_id)]
        self.assertNotEqual(engine, None)
        self.assertEqual(engine.initialize_count, 1)
        self.assertEqual(engine.initialize_conf[CONF_PRESERVE_N], 1)
        self.assertEqual(engine.decide_count, 1)
        self.epum._run_decisions()
        self.assertEqual(engine.decide_count, 2)

    def _compare_configs(self, c1, c2):
        self.assertEqual(set(c1.keys()), set(c2.keys()))
        self.assertEqual(c1[EPUM_CONF_GENERAL], c2[EPUM_CONF_GENERAL])
        self.assertEqual(c1[EPUM_CONF_HEALTH], c2[EPUM_CONF_HEALTH])
        self.assertEqual(c1[EPUM_CONF_ENGINE], c2[EPUM_CONF_ENGINE])

    def test_domain_query(self):
        """Verify domain query operations work
        """
        self.epum.initialize()
        caller = "asterix"
        domain1_config = self._config_mock1()
        domain1_name = "onedomain"
        domain2_config = self._config_simplest_domainconf(1)
        domain2_name = "twodomain"

        domains = self.epum.msg_list_domains(caller)
        self.assertEqual(domains, [])

        self.epum.msg_add_domain(caller, domain1_name, domain1_config)
        domains = self.epum.msg_list_domains(caller)
        self.assertEqual(domains, [domain1_name])

        domain1_desc = self.epum.msg_describe_domain(caller, domain1_name)
        self.assertEqual(domain1_desc['name'], domain1_name)
        log.debug("domain1 desc: %s", domain1_desc)
        self._compare_configs(domain1_config, domain1_desc['config'])
        self.assertEqual(domain1_desc['instances'], [])

        self.epum.msg_add_domain(caller, domain2_name, domain2_config)
        domains = self.epum.msg_list_domains(caller)
        self.assertEqual(set(domains), set([domain1_name, domain2_name]))

        # this will cause domain2 to launch an instance
        self.epum._run_decisions()

        domain2_desc = self.epum.msg_describe_domain(caller, domain2_name)
        self.assertEqual(domain2_desc['name'], domain2_name)
        self._compare_configs(domain2_config, domain2_desc['config'])
        self.assertEqual(len(domain2_desc['instances']), 1)

        # just make sure it looks roughly like a real instance
        instance = domain2_desc['instances'][0]
        self.assertIn("instance_id", instance)
        self.assertIn("state", instance)

    def test_engine_reconfigure(self):
        """
        Verify reconfigure is called after a 'worker' alters the domain config
        """
        self.epum.initialize()
        domain_config = self._config_mock1()
        owner = "emily"
        domain_name1 = "testing123"
        domain_name2 = "testing789"
        self.epum.msg_add_domain(owner, domain_name1, domain_config)
        self.epum.msg_add_domain(owner, domain_name2, domain_config)
        self.epum._run_decisions()

        # digging into internal structure to get engine instances
        domain_engine1 = self.epum.decider.engines[(owner, domain_name1)]
        domain_engine2 = self.epum.decider.engines[(owner, domain_name2)]
        self.assertEqual(domain_engine1.decide_count, 1)
        self.assertEqual(domain_engine2.decide_count, 1)

        # reconfigure test
        self.assertEqual(domain_engine1.reconfigure_count, 0)
        self.assertEqual(domain_engine2.reconfigure_count, 0)
        domain_config2 = {EPUM_CONF_ENGINE: {CONF_PRESERVE_N:2}}
        self.epum.msg_reconfigure_domain(owner, domain_name1, domain_config2)

        # should not take effect immediately, a reconfigure is external msg handled by reactor worker
        self.assertEqual(domain_engine1.reconfigure_count, 0)
        self.assertEqual(domain_engine2.reconfigure_count, 0)

        self.epum._run_decisions()

        # now it should have happened, after a decision cycle, but only to domain_name1
        self.assertEqual(domain_engine1.reconfigure_count, 1)
        self.assertEqual(domain_engine2.reconfigure_count, 0)

    def test_basic_npreserving(self):
        """
        Create one domain with NPreserving=2 policy.
        Verify two instances are launched on the first decision cycle.
        """
        self.epum.initialize()
        domain_config = self._config_simplest_domainconf(2)
        self.epum.msg_add_domain("owner1", "testing123", domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)

    def test_reconfigure_npreserving(self):
        """
        Create one domain with NPreserving=2 policy.
        Verify two instances are launched on the first decision cycle.
        Reconfigure with NPreserving=4 policy.
        Verify two more instances are launched on next decision cycle.
        Reconfigure with NPreserving=0 policy.
        Verify four instances are terminated on next decision cycle.
        """
        self.epum.initialize()
        owner = "opwner1"
        domain_name = "testing123"
        domain_config = self._config_simplest_domainconf(2)

        self.epum.msg_add_domain(owner, domain_name, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(self.provisioner_client.terminate_node_count, 0)

        domain_config = self._config_simplest_domainconf(4)
        self.epum.msg_reconfigure_domain(owner, domain_name, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 4)
        self.assertEqual(self.provisioner_client.terminate_node_count, 0)

        domain_config = self._config_simplest_domainconf(0)
        self.epum.msg_reconfigure_domain(owner, domain_name, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 4)
        self.assertEqual(self.provisioner_client.terminate_node_count, 4)

    def test_decider_leader_disable(self):
        """
        Create one domain with NPreserving=2 policy.
        Verify two instances are launched on the first decision cycle.
        Change to NPreserving=1, verify that one is terminated on second decision cycle
        Disable leader via epum internals
        Change to NPreserving=4, verify that nothing happened.
        Enable leader via epum internals
        Previous reconfiguration will be recognized

        This will only work in this in-memory situation, otherwise another EPUM worker becomes
        the decider and will respond to reconfigurations.
        """
        self.epum.initialize()
        owner = "opwner1"
        domain_name = "testing123"
        domain_config = self._config_simplest_domainconf(2)

        self.epum.msg_add_domain(owner, domain_name, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(self.provisioner_client.terminate_node_count, 0)

        domain_config = self._config_simplest_domainconf(1)
        self.epum.msg_reconfigure_domain(owner, domain_name, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(self.provisioner_client.terminate_node_count, 1)

        # digging into internal structure to disable leader
        self.epum.epum_store._change_decider(False)

        # nothing should happen now, should stay provision=2, terminate=1
        domain_config = self._config_simplest_domainconf(4)
        self.epum.msg_reconfigure_domain(owner, domain_name, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(self.provisioner_client.terminate_node_count, 1)

        # digging into internal structure to enable leader
        self.epum.epum_store._change_decider(True)

        # previous reconfiguration (preserve 4) should be recognized if decision cycle runs
        self.epum._run_decisions()

        # 3 more provisions to take from N=1 to N=4 (making 5 total provisions)
        self.assertEqual(self.provisioner_client.provision_count, 5)
        self.assertEqual(self.provisioner_client.terminate_node_count, 1)

    def test_instance_lookup(self):
        """
        Create two domains, run NPreserving=1 in each of them.  Lookup by instance_id and make sure
        the right domain is returned to the caller.  Some incoming service messages, like heartbeats,
        only have the  instance_id to go on (not which domain it belongs to).
        """
        self.epum.initialize()
        domain_config = self._config_simplest_domainconf(1)
        owner = "owner1"
        domain_name1 = "domain1"
        domain_name2 = "domain2"
        self.epum.msg_add_domain(owner, domain_name1, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        via_domain1 = self.provisioner_client.launched_instance_ids[0]

        self.epum.msg_add_domain(owner, domain_name2, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 2)
        via_domain2 = self.provisioner_client.launched_instance_ids[1]

        domain1 = self.epum.epum_store.get_domain_for_instance_id(via_domain1)
        domain2 = self.epum.epum_store.get_domain_for_instance_id(via_domain2)

        self.assertEqual(domain1.domain_id, domain_name1)
        self.assertEqual(domain2.domain_id, domain_name2)

    def test_failing_engine_decide(self):
        """Exceptions during decide cycle should not affect EPUM.
        """
        self.epum.initialize()
        fail_config = self._config_mock2()
        self.epum.msg_add_domain("joeowner", "fail_domain", fail_config)
        self.epum._run_decisions()
        # digging into internal structure to get engine instance
        domain_engine = self.epum.decider.engines[("joeowner","fail_domain")]
        self.assertEqual(domain_engine.decide_count, 1)

    def test_failing_engine_reconfigure(self):
        """Exceptions during engine reconfigure should not affect EPUM.
        """
        self.epum.initialize()
        fail_config = self._config_mock2()
        self.epum.msg_add_domain("owner", "fail_domain", fail_config)
        self.epum._run_decisions()

        # digging into internal structure to get engine instance
        domain_engine = self.epum.decider.engines[("owner","fail_domain")]
        self.assertEqual(domain_engine.decide_count, 1)
        self.assertEqual(domain_engine.reconfigure_count, 0)

        config2 = {EPUM_CONF_ENGINE: {CONF_PRESERVE_N:2}}
        self.epum.msg_reconfigure_domain("owner", "fail_domain", config2)
        self.epum._run_decisions()
        self.assertEqual(domain_engine.decide_count, 2)
        self.assertEqual(domain_engine.reconfigure_count, 1)

    def test_remove_domain(self):
        """
        Ensure instances are killed when domain is removed
        """
        self.epum.initialize()
        domain_config = self._config_simplest_domainconf(2)
        self.epum.msg_add_domain("owner1", "testing123", domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)

        self.epum.msg_remove_domain("owner1", "testing123")
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.terminate_node_count, 2)

    def test_multiuser(self):
        """Ensure that multiuser checks are working
        """
        permitted_user = "asterix"
        disallowed_user = "cacaphonix"

        self.epum.initialize()

        # TODO: test adding with a dt that user doesn't own
        domain_config = self._config_mock1()
        domain_name = "testing123"
        self.epum.msg_add_domain(permitted_user, domain_name, domain_config)

        # Test describe
        not_found_error = False
        try:
            got_domain = self.epum.msg_describe_domain(disallowed_user, domain_name)
        except NotFoundError:
            not_found_error = True
        msg = "Non-permitted user was able to describe an domain he didn't own!"
        self.assertTrue(not_found_error, msg)

        self.epum.msg_describe_domain(permitted_user, domain_name)

        # Test list
        disallowed_domains = self.epum.msg_list_domains(disallowed_user)
        self.assertEqual(len(disallowed_domains), 0)

        permitted_domains = self.epum.msg_list_domains(permitted_user)
        self.assertEqual(len(permitted_domains), 1)


        # Test reconfigure
        new_config = {}
        not_found_error = False
        try:
            self.epum.msg_reconfigure_domain(disallowed_user, domain_name, new_config)
        except NotFoundError:
            not_found_error = True
        msg = "Non-permitted user was able to reconfigure an domain he didn't own!"
        self.assertTrue(not_found_error, msg)

        self.epum.msg_reconfigure_domain(permitted_user, domain_name, new_config)
        # TODO: test adding with a dt that user doesn't own


        # Test Remove
        not_found_error = False
        try:
            self.epum.msg_remove_domain(disallowed_user, domain_name)
        except NotFoundError:
            not_found_error = True
        msg = "Non-permitted user was able to remove an domain he didn't own!"
        self.assertTrue(not_found_error, msg)

        self.epum.msg_remove_domain(permitted_user, domain_name)


