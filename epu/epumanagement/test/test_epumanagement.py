# Copyright 2013 University of Chicago

import copy
import unittest
import logging
import time
import threading

from epu.decisionengine.impls.simplest import CONF_PRESERVE_N
from epu.epumanagement import EPUManagement
from epu.epumanagement.test.mocks import FakeDomainStore, MockSubscriberNotifier, \
    MockProvisionerClient, MockOUAgentClient, MockDTRSClient
from epu.epumanagement.store import LocalEPUMStore
from epu.epumanagement.conf import *  # noqa
from epu.exceptions import NotFoundError, WriteConflictError
from epu.states import InstanceState
from epu.sensors import Statistics
from epu.decisionengine.impls.sensor import CONF_SENSOR_TYPE

log = logging.getLogger(__name__)

MOCK_PKG = "epu.epumanagement.test.mocks"


class EPUManagementBasicTests(unittest.TestCase):
    """
    Tests that cover basic things like running a decision engine cycle and making sure a VM
    is requested, etc.
    """

    def setUp(self):
        # Mock mode:
        initial_conf = {EPUM_INITIALCONF_EXTERNAL_DECIDE: True}
        self.notifier = MockSubscriberNotifier()
        self.provisioner_client = MockProvisionerClient()
        self.ou_client = MockOUAgentClient()
        self.dtrs_client = MockDTRSClient()
        self.epum_store = LocalEPUMStore(EPUM_DEFAULT_SERVICE_NAME)
        self.epum_store.initialize()
        self.epum = EPUManagement(
            initial_conf, self.notifier, self.provisioner_client, self.ou_client,
            self.dtrs_client, store=self.epum_store)

        # For instance-state changes "from the provisioner"
        self.provisioner_client._set_epum(self.epum)

        # For heartbeats "from the OU instance"
        self.ou_client._set_epum(self.epum)

    def _config_mock1(self):
        """Keeps increment count
        """
        engine = {CONF_PRESERVE_N: 1}
        return {EPUM_CONF_ENGINE: engine}

    def _definition_mock1(self):
        general = {EPUM_CONF_ENGINE_CLASS: MOCK_PKG + ".MockDecisionEngine01"}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        return {EPUM_CONF_GENERAL: general, EPUM_CONF_HEALTH: health}

    def _definition_mock2(self):
        """decide and reconfigure fail
        """
        definition = self._definition_mock1()
        definition[EPUM_CONF_GENERAL] = {EPUM_CONF_ENGINE_CLASS: MOCK_PKG + ".MockDecisionEngine02"}
        return definition

    def _definition_mock3(self):
        """uses Deferred
        """
        definition = self._definition_mock1()
        definition[EPUM_CONF_GENERAL] = {EPUM_CONF_ENGINE_CLASS: MOCK_PKG + ".MockDecisionEngine03"}
        return definition

    def _get_simplest_domain_definition(self):
        engine_class = "epu.decisionengine.impls.simplest.SimplestEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        return {EPUM_CONF_GENERAL: general, EPUM_CONF_HEALTH: health}

    def _config_simplest_domainconf(self, n_preserving):
        """Get 'simplest' domain conf with specified NPreserving policy
        """
        engine = {CONF_PRESERVE_N: n_preserving}
        return {EPUM_CONF_ENGINE: engine}

    def _config_simplest_chef_domainconf(self, n_preserving, chef_credential):
        """Get 'simplest' domain conf with specified NPreserving policy
        """
        engine = {CONF_PRESERVE_N: n_preserving}
        general = {EPUM_CONF_CHEF_CREDENTIAL: chef_credential}
        return {EPUM_CONF_ENGINE: engine, EPUM_CONF_GENERAL: general}

    def _get_sensor_domain_definition(self):
        engine_class = "epu.decisionengine.impls.sensor.SensorEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        return {EPUM_CONF_GENERAL: general, EPUM_CONF_HEALTH: health}

    def _config_sensor_domainconf(self, minimum_n):
        """Get 'sensor' domain conf with mock aggregator
        """
        engine = {CONF_SENSOR_TYPE: 'mockcloudwatch',
                  CONF_IAAS_SITE: 'fake',
                  CONF_IAAS_ALLOCATION: 'also.fake',
                  'deployable_type': 'fake',
                 'minimum_vms': minimum_n,
                 'metric': 'load',
                 'monitor_sensors': ['load', ],
                 'monitor_domain_sensors': ['queuelen', ],
                 'sample_function': 'Average'}
        return {EPUM_CONF_ENGINE: engine}

    def test_engine_decide(self):
        """
        Verify decide is called at expected time
        """
        self.epum.initialize()
        definition = self._definition_mock1()
        config = self._config_mock1()
        owner = "owner1"
        domain_id = "testing123"
        definition_id = "def123"
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain(owner, domain_id, definition_id, config)
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
        domain1_definition_name = "onedomaindef"
        domain1_definition = self._definition_mock1()
        domain1_config = self._config_mock1()
        domain1_name = "onedomain"
        domain2_definition_name = "twodomaindef"
        domain2_definition = self._get_simplest_domain_definition()
        domain2_config = self._config_simplest_domainconf(1)
        domain2_name = "twodomain"

        domains = self.epum.msg_list_domains(caller)
        self.assertEqual(domains, [])

        self.epum.msg_add_domain_definition(domain1_definition_name, domain1_definition)
        self.epum.msg_add_domain(caller, domain1_name, domain1_definition_name, domain1_config)
        domains = self.epum.msg_list_domains(caller)
        self.assertEqual(domains, [domain1_name])

        domain1_desc = self.epum.msg_describe_domain(caller, domain1_name)
        self.assertEqual(domain1_desc['name'], domain1_name)
        log.debug("domain1 desc: %s", domain1_desc)
        merged_config = copy.copy(domain1_definition)
        merged_config.update(domain1_config)
        self._compare_configs(merged_config, domain1_desc['config'])
        self.assertEqual(domain1_desc['instances'], [])

        self.epum.msg_add_domain_definition(domain2_definition_name, domain2_definition)
        self.epum.msg_add_domain(caller, domain2_name, domain2_definition_name, domain2_config)
        domains = self.epum.msg_list_domains(caller)
        self.assertEqual(set(domains), set([domain1_name, domain2_name]))

        # this will cause domain2 to launch an instance
        self.epum._run_decisions()

        domain2_desc = self.epum.msg_describe_domain(caller, domain2_name)
        self.assertEqual(domain2_desc['name'], domain2_name)
        merged_config = copy.copy(domain2_definition)
        merged_config.update(domain2_config)
        self._compare_configs(merged_config, domain2_desc['config'])
        self.assertEqual(len(domain2_desc['instances']), 1)

        # just make sure it looks roughly like a real instance
        instance = domain2_desc['instances'][0]
        self.assertIn("instance_id", instance)
        self.assertIn("state", instance)

    def test_sensor_data(self):
        self.epum.initialize()
        caller = "asterix"
        domain_definition_name = "twodomaindef"
        domain_definition = self._get_sensor_domain_definition()
        domain_config = self._config_sensor_domainconf(1)
        domain_name = "twodomain"

        domains = self.epum.msg_list_domains(caller)
        self.assertEqual(domains, [])

        self.epum.msg_add_domain_definition(domain_definition_name, domain_definition)
        self.epum.msg_add_domain(caller, domain_name, domain_definition_name, domain_config)
        domains = self.epum.msg_list_domains(caller)
        self.assertEqual(domains, [domain_name])

        domain_desc = self.epum.msg_describe_domain(caller, domain_name)
        self.assertEqual(domain_desc['name'], domain_name)
        log.debug("domain desc: %s", domain_desc)
        merged_config = copy.copy(domain_definition)
        merged_config.update(domain_config)
        self._compare_configs(merged_config, domain_desc['config'])
        self.assertEqual(domain_desc['instances'], [])

        # this will cause domain to launch an instance
        self.epum._run_decisions()

        domain_desc = self.epum.msg_describe_domain(caller, domain_name)
        self.assertEqual(domain_desc['name'], domain_name)
        merged_config = copy.copy(domain_definition)
        merged_config.update(domain_config)
        self._compare_configs(merged_config, domain_desc['config'])
        self.assertEqual(len(domain_desc['instances']), 1)

        # just make sure it looks roughly like a real instance
        instance = domain_desc['instances'][0]
        self.assertIn("instance_id", instance)
        self.assertIn("state", instance)
        self.assertNotIn("sensor_data", instance)
        self.epum._run_decisions()

        domain_desc = self.epum.msg_describe_domain(caller, domain_name)
        self.assertEqual(domain_desc['name'], domain_name)
        merged_config = copy.copy(domain_definition)
        merged_config.update(domain_config)
        self._compare_configs(merged_config, domain_desc['config'])
        self.assertEqual(len(domain_desc['instances']), 1)

        # just make sure it now has sensor_data
        self.assertIn("sensor_data", domain_desc)
        self.assertIn("queuelen", domain_desc['sensor_data'])
        self.assertIn(Statistics.SERIES, domain_desc['sensor_data']['queuelen'])

        instance = domain_desc['instances'][0]
        self.assertIn("instance_id", instance)
        self.assertIn("state", instance)
        self.assertIn("sensor_data", instance)
        self.assertIn("load", instance['sensor_data'])
        self.assertIn(Statistics.SERIES, instance['sensor_data']['load'])

    def test_engine_reconfigure(self):
        """
        Verify reconfigure is called after a 'worker' alters the domain config
        """
        self.epum.initialize()
        domain_definition = self._definition_mock1()
        domain_config = self._config_mock1()
        owner = "emily"
        definition_id = "def123"
        domain_name1 = "testing123"
        domain_name2 = "testing789"
        self.epum.msg_add_domain_definition(definition_id, domain_definition)
        self.epum.msg_add_domain(owner, domain_name1, definition_id, domain_config)
        self.epum.msg_add_domain(owner, domain_name2, definition_id, domain_config)
        self.epum._run_decisions()

        # digging into internal structure to get engine instances
        domain_engine1 = self.epum.decider.engines[(owner, domain_name1)]
        domain_engine2 = self.epum.decider.engines[(owner, domain_name2)]
        self.assertEqual(domain_engine1.decide_count, 1)
        self.assertEqual(domain_engine2.decide_count, 1)

        # reconfigure test
        self.assertEqual(domain_engine1.reconfigure_count, 0)
        self.assertEqual(domain_engine2.reconfigure_count, 0)
        domain_config2 = {EPUM_CONF_ENGINE: {CONF_PRESERVE_N: 2}}
        self.epum.msg_reconfigure_domain(owner, domain_name1, domain_config2)

        # should not take effect immediately, a reconfigure is external msg handled by reactor worker
        self.assertEqual(domain_engine1.reconfigure_count, 0)
        self.assertEqual(domain_engine2.reconfigure_count, 0)

        self.epum._run_decisions()

        # now it should have happened, after a decision cycle, but only to domain_name1
        self.assertEqual(domain_engine1.reconfigure_count, 1)
        self.assertEqual(domain_engine2.reconfigure_count, 0)

        # should not happen again
        self.epum._run_decisions()
        self.assertEqual(domain_engine1.reconfigure_count, 1)
        self.assertEqual(domain_engine2.reconfigure_count, 0)

    def test_basic_npreserving(self):
        """
        Create one domain with NPreserving=2 policy.
        Verify two instances are launched on the first decision cycle.
        """
        self.epum.initialize()
        domain_config = self._config_simplest_domainconf(2)
        definition = {}
        self.epum.msg_add_domain_definition("definition1", definition)
        self.epum.msg_add_domain("owner1", "testing123", "definition1", domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)

    def test_basic_chef_domain(self):
        self.epum.initialize()
        domain_config = self._config_simplest_chef_domainconf(2, "chef1")
        definition = {}
        self.epum.msg_add_domain_definition("definition1", definition)
        self.epum.msg_add_domain("owner1", "testing123", "definition1", domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        # ensure chef credential name is passed through in provisioner vars
        self.assertEqual(self.provisioner_client.launches[0]['vars']['chef_credential'], 'chef1')
        self.assertEqual(self.provisioner_client.launches[1]['vars']['chef_credential'], 'chef1')

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
        definition_id = "def123"
        definition = self._get_simplest_domain_definition()
        domain_name = "testing123"
        domain_config = self._config_simplest_domainconf(2)

        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain(owner, domain_name, definition_id, domain_config)
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
        definition_name = "def123"
        domain_definition = self._get_simplest_domain_definition()
        owner = "opwner1"
        domain_name = "testing123"
        domain_config = self._config_simplest_domainconf(2)

        self.epum.msg_add_domain_definition(definition_name, domain_definition)
        self.epum.msg_add_domain(owner, domain_name, definition_name, domain_config)
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
        definition_id = "definition1"
        definition = self._get_simplest_domain_definition()
        domain_config = self._config_simplest_domainconf(1)
        owner = "owner1"
        domain_name1 = "domain1"
        domain_name2 = "domain2"
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain(owner, domain_name1, definition_id, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        via_domain1 = self.provisioner_client.launched_instance_ids[0]

        self.epum.msg_add_domain(owner, domain_name2, definition_id, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 2)
        via_domain2 = self.provisioner_client.launched_instance_ids[1]

        domain1 = self.epum.epum_store.get_domain_for_instance_id(via_domain1)
        domain2 = self.epum.epum_store.get_domain_for_instance_id(via_domain2)

        self.assertEqual(domain1.domain_id, domain_name1)
        self.assertEqual(domain2.domain_id, domain_name2)

    def test_decider_retries(self):
        self.epum.initialize()
        definition_id = "definition1"
        definition = self._get_simplest_domain_definition()
        domain_config = self._config_simplest_domainconf(2)
        owner = "owner1"
        domain_name = "domain1"
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain(owner, domain_name, definition_id, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 2)

        # sneak into decider internals and patch out retry interval, to speed test
        for controls in self.epum.decider.controls.values():
            controls._retry_seconds = 0.5

        # rerun decisions. no retries should happen
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 2)

        # provide REQUESTED state for first instance. should not retried
        self.provisioner_client.report_node_state(
            InstanceState.REQUESTED,
            self.provisioner_client.launched_instance_ids[0])

        # wait until a retry should be expected
        time.sleep(0.6)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 3)
        self.assertEqual(len(set(self.provisioner_client.launched_instance_ids)), 2)
        self.assertEqual(self.provisioner_client.launched_instance_ids[1],
            self.provisioner_client.launched_instance_ids[2])

        # now kill the instances.
        domain_config = self._config_simplest_domainconf(0)
        self.epum.msg_reconfigure_domain(owner, domain_name, domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 3)
        self.assertEqual(self.provisioner_client.terminate_node_count, 2)
        self.assertEqual(len(self.provisioner_client.terminated_instance_ids), 2)

        # should be no retries immediately
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 3)
        self.assertEqual(self.provisioner_client.terminate_node_count, 2)
        self.assertEqual(len(self.provisioner_client.terminated_instance_ids), 2)

        # provide TERMINATED state for first instance. should not retried
        self.provisioner_client.report_node_state(
            InstanceState.TERMINATED,
            self.provisioner_client.terminated_instance_ids[0])

        # wait until a retry should be expected
        time.sleep(0.6)
        self.epum._run_decisions()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 3)
        self.assertEqual(self.provisioner_client.terminate_node_count, 3)
        self.assertEqual(len(self.provisioner_client.terminated_instance_ids), 3)
        self.assertEqual(self.provisioner_client.terminated_instance_ids[1],
            self.provisioner_client.terminated_instance_ids[2])

    def test_failing_engine_decide(self):
        """Exceptions during decide cycle should not affect EPUM.
        """
        self.epum.initialize()
        fail_definition = self._definition_mock2()
        fail_definition_id = "fail_definition"
        config = self._config_mock1()
        self.epum.msg_add_domain_definition(fail_definition_id, fail_definition)
        self.epum.msg_add_domain("joeowner", "fail_domain", fail_definition_id, config)
        self.epum._run_decisions()
        # digging into internal structure to get engine instance
        domain_engine = self.epum.decider.engines[("joeowner", "fail_domain")]
        self.assertEqual(domain_engine.decide_count, 1)

    def test_failing_engine_reconfigure(self):
        """Exceptions during engine reconfigure should not affect EPUM.
        """
        self.epum.initialize()
        fail_definition = self._definition_mock2()
        fail_definition_id = "fail_definition"
        config = self._config_mock1()
        self.epum.msg_add_domain_definition(fail_definition_id, fail_definition)
        self.epum.msg_add_domain("owner", "fail_domain", fail_definition_id, config)
        self.epum._run_decisions()

        # digging into internal structure to get engine instance
        domain_engine = self.epum.decider.engines[("owner", "fail_domain")]
        self.assertEqual(domain_engine.decide_count, 1)
        self.assertEqual(domain_engine.reconfigure_count, 0)

        config2 = {EPUM_CONF_ENGINE: {CONF_PRESERVE_N: 2}}
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
        definition_id = "def123"
        definition = self._get_simplest_domain_definition()
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain("owner1", "testing123", definition_id, domain_config)
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
        definition_id = "def123"
        definition = self._definition_mock1()
        domain_config = self._config_mock1()
        domain_name = "testing123"
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain(permitted_user, domain_name, definition_id, domain_config)

        # Test describe
        not_found_error = False
        try:
            self.epum.msg_describe_domain(disallowed_user, domain_name)
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

    def test_definitions(self):
        self.epum.initialize()

        definition1_name = "definition1"
        definition1 = self._definition_mock1()
        definition2_name = "definition2"
        definition2 = self._definition_mock2()

        self.epum.msg_add_domain_definition(definition1_name, definition1)

        # Trying to add a domain definition with the same name should raise an
        # exception
        try:
            self.epum.msg_add_domain_definition(definition1_name, definition2)
        except WriteConflictError:
            pass
        else:
            self.fail("expected WriteConflictError")

        self.epum.msg_add_domain_definition(definition2_name, definition2)

        definition_one = self.epum.msg_describe_domain_definition(definition1_name)
        self.assertEqual(definition_one['name'], definition1_name)
        self.assertEqual(definition_one['definition'], definition1)

        definition_two = self.epum.msg_describe_domain_definition(definition2_name)
        self.assertEqual(definition_two['name'], definition2_name)
        self.assertEqual(definition_two['definition'], definition2)

        definitions = self.epum.msg_list_domain_definitions()
        self.assertEqual(len(definitions), 2)
        self.assertIn(definition1_name, definitions)
        self.assertIn(definition2_name, definitions)

        self.epum.msg_remove_domain_definition(definition1_name)
        try:
            self.epum.msg_describe_domain_definition(definition1_name)
        except NotFoundError:
            pass
        else:
            self.fail("expected NotFoundError")

        try:
            self.epum.msg_remove_domain_definition(definition1_name)
        except NotFoundError:
            pass
        else:
            self.fail("expected NotFoundError")

        self.epum.msg_update_domain_definition(definition2_name, definition1)
        definition_two = self.epum.msg_describe_domain_definition(definition2_name)
        self.assertEqual(definition_two['name'], definition2_name)
        self.assertEqual(definition_two['definition'], definition1)

    def test_config_validation(self):
        caller = "asterix"
        self.epum.initialize()

        definition_name = "def123"
        definition = self._get_simplest_domain_definition()

        wrong_config = {EPUM_CONF_ENGINE: {}}
        ok_config = self._config_simplest_domainconf(1)

        self.epum.msg_add_domain_definition(definition_name, definition)

        # Trying to add a domain using a config with missing parameters should
        # raise an exception
        try:
            self.epum.msg_add_domain(caller, "domain", definition_name, wrong_config)
        except ValueError:
            pass
        else:
            self.fail("expected ValueError")

        self.epum.msg_add_domain(caller, "domain", definition_name, ok_config)

    def test_engine_config_doc(self):
        self.epum.initialize()

        definition_name = "def123"
        definition = self._get_simplest_domain_definition()

        self.epum.msg_add_domain_definition(definition_name, definition)
        desc = self.epum.msg_describe_domain_definition(definition_name)
        self.assertTrue("documentation" in desc)

    def test_reaper(self):
        self.epum.initialize()
        config = self._config_mock1()
        owner = "owner1"
        domain_id = "testing123"

        # inject the FakeState instance directly instead of using msg_add_domain()
        self.state = FakeDomainStore(owner, domain_id, config)
        self.epum.epum_store.domains[(owner, domain_id)] = self.state

        now = time.time()

        # One running
        self.state.new_fake_instance_state("n1", InstanceState.RUNNING, now - EPUM_RECORD_REAPING_DEFAULT_MAX_AGE - 1)

        # Three in terminal state and outdated
        self.state.new_fake_instance_state(
            "n2", InstanceState.TERMINATED, now - EPUM_RECORD_REAPING_DEFAULT_MAX_AGE - 1)
        self.state.new_fake_instance_state("n3", InstanceState.REJECTED, now - EPUM_RECORD_REAPING_DEFAULT_MAX_AGE - 1)
        self.state.new_fake_instance_state("n4", InstanceState.FAILED, now - EPUM_RECORD_REAPING_DEFAULT_MAX_AGE - 1)

        # Three in terminal state and not yet outdated
        self.state.new_fake_instance_state(
            "n5", InstanceState.TERMINATED, now - EPUM_RECORD_REAPING_DEFAULT_MAX_AGE + 60)
        self.state.new_fake_instance_state("n6", InstanceState.REJECTED, now - EPUM_RECORD_REAPING_DEFAULT_MAX_AGE + 60)
        self.state.new_fake_instance_state("n7", InstanceState.FAILED, now - EPUM_RECORD_REAPING_DEFAULT_MAX_AGE + 60)

        self.epum._run_reaper_loop()
        instances = self.state.get_instance_ids()
        self.assertEqual(len(instances), 4)
        self.assertIn("n1", instances)
        self.assertIn("n5", instances)
        self.assertIn("n6", instances)
        self.assertIn("n7", instances)

    def test_instance_update_conflict_1(self):

        self.epum.initialize()
        domain_config = self._config_simplest_domainconf(1)
        definition = {}
        self.epum.msg_add_domain_definition("definition1", definition)
        self.epum.msg_add_domain("owner1", "testing123", "definition1", domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)

        domain = self.epum_store.get_domain("owner1", "testing123")

        instance_id = self.provisioner_client.launched_instance_ids[0]
        self.provisioner_client.launches[0]['launch_id']

        sneaky_msg = dict(node_id=instance_id, state=InstanceState.PENDING)

        # patch in a function that sneaks in an instance record update just
        # before a requested update. This simulates the case where two EPUM
        # workers are competing to update the same instance.
        original_new_instance_state = domain.new_instance_state

        patch_called = threading.Event()

        def patched_new_instance_state(content, timestamp=None, previous=None):
            patch_called.set()

            # unpatch ourself first so we don't recurse forever
            domain.new_instance_state = original_new_instance_state

            domain.new_instance_state(sneaky_msg, previous=previous)
            return domain.new_instance_state(content, timestamp=timestamp, previous=previous)
        domain.new_instance_state = patched_new_instance_state

        # send our "real" update. should get a conflict
        msg = dict(node_id=instance_id, state=InstanceState.STARTED)

        self.epum.msg_instance_info("owner1", msg)

        assert patch_called.is_set()

        instance = domain.get_instance(instance_id)
        self.assertEqual(instance.state, InstanceState.STARTED)

    def test_instance_update_conflict_2(self):

        self.epum.initialize()
        domain_config = self._config_simplest_domainconf(1)
        definition = {}
        self.epum.msg_add_domain_definition("definition1", definition)
        self.epum.msg_add_domain("owner1", "testing123", "definition1", domain_config)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)

        domain = self.epum_store.get_domain("owner1", "testing123")

        instance_id = self.provisioner_client.launched_instance_ids[0]
        self.provisioner_client.launches[0]['launch_id']

        sneaky_msg = dict(node_id=instance_id, state=InstanceState.STARTED)

        # patch in a function that sneaks in an instance record update just
        # before a requested update. This simulates the case where two EPUM
        # workers are competing to update the same instance.
        original_new_instance_state = domain.new_instance_state

        patch_called = threading.Event()

        def patched_new_instance_state(content, timestamp=None, previous=None):
            patch_called.set()

            # unpatch ourself first so we don't recurse forever
            domain.new_instance_state = original_new_instance_state

            domain.new_instance_state(sneaky_msg, previous=previous)
            return domain.new_instance_state(content, timestamp=timestamp, previous=previous)
        domain.new_instance_state = patched_new_instance_state

        # send our "real" update. should get a conflict
        msg = dict(node_id=instance_id, state=InstanceState.PENDING)

        self.epum.msg_instance_info(None, msg)

        assert patch_called.is_set()

        # in this case the sneaky message (STARTED) should win because it is
        # the later state
        instance = domain.get_instance(instance_id)
        self.assertEqual(instance.state, InstanceState.STARTED)
