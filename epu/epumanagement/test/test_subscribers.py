# Copyright 2013 University of Chicago

import unittest
import logging

from epu.epumanagement import EPUManagement
from epu.epumanagement.test.mocks import MockSubscriberNotifier,\
    MockProvisionerClient, MockOUAgentClient, MockDTRSClient
from epu.epumanagement.conf import EPUM_INITIALCONF_EXTERNAL_DECIDE,\
    EPUM_DEFAULT_SERVICE_NAME, EPUM_CONF_ENGINE_CLASS, EPUM_CONF_HEALTH_MONITOR,\
    EPUM_CONF_GENERAL, EPUM_CONF_HEALTH, EPUM_CONF_ENGINE
from epu.epumanagement.store import LocalEPUMStore
from epu.states import InstanceState
from epu.decisionengine.impls.simplest import CONF_PRESERVE_N


log = logging.getLogger(__name__)


class SubscriberTests(unittest.TestCase):

    def setUp(self):
        # Mock mode:
        initial_conf = {EPUM_INITIALCONF_EXTERNAL_DECIDE: True}
        self.notifier = MockSubscriberNotifier()
        self.provisioner_client = MockProvisionerClient()
        self.dtrs_client = MockDTRSClient()
        self.ou_client = MockOUAgentClient()
        self.epum_store = LocalEPUMStore(EPUM_DEFAULT_SERVICE_NAME)
        self.epum_store.initialize()
        self.epum = EPUManagement(
            initial_conf, self.notifier, self.provisioner_client, self.ou_client,
            self.dtrs_client, store=self.epum_store)

        # For instance-state changes "from the provisioner"
        self.provisioner_client._set_epum(self.epum)

        # For heartbeats "from the OU instance"
        self.ou_client._set_epum(self.epum)

    def _get_simplest_domain_definition(self):
        engine_class = "epu.decisionengine.impls.simplest.SimplestEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        return {EPUM_CONF_GENERAL: general, EPUM_CONF_HEALTH: health}

    def _config_simplest_domainconf(self, n_preserving, dt="00_dt_id"):
        """Get 'simplest' domain conf with specified NPreserving policy
        """
        engine = {CONF_PRESERVE_N: n_preserving, "epuworker_type": dt}
        return {EPUM_CONF_ENGINE: engine}

    def _reset(self):
        self.notifier.notify_by_name_called = 0
        self.notifier.receiver_names = []
        self.notifier.operations = []
        self.notifier.messages = []

    def _mock_checks(self, num_called, idx_check, subscriber_name, subscriber_op, expected_state, expected_domain):
        self.assertEqual(self.notifier.notify_by_name_called, num_called)
        self.assertEqual(len(self.notifier.receiver_names), num_called)
        self.assertEqual(len(self.notifier.operations), num_called)
        self.assertEqual(len(self.notifier.messages), num_called)
        self.assertEqual(self.notifier.receiver_names[idx_check], subscriber_name)
        self.assertEqual(self.notifier.operations[idx_check], subscriber_op)
        self.assertTrue("state" in self.notifier.messages[idx_check])
        self.assertEqual(self.notifier.messages[idx_check]["state"], expected_state)
        self.assertEqual(self.notifier.messages[idx_check]["domain_id"], expected_domain)

    def test_ignore_subscriber(self):

        self._reset()
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        definition_id = "definition1"
        definition = self._get_simplest_domain_definition()
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain("owner", "domain1", definition_id, self._config_simplest_domainconf(1))
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.provisioner_client.deployable_types_launched[0], "00_dt_id")
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Simulate provisioner
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.RUNNING}
        self.epum.msg_instance_info(None, content)

        self.assertEqual(self.notifier.notify_by_name_called, 0)

    def test_one_subscriber(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"

        self._reset()
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        self.assertEqual(self.provisioner_client.provision_count, 0)
        definition_id = "definition1"
        definition = self._get_simplest_domain_definition()
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain("owner", "domain1", definition_id, self._config_simplest_domainconf(1))
        self.epum.msg_subscribe_domain("owner", "domain1", subscriber_name, subscriber_op)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Simulate provisioner
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.STARTED}
        self.epum.msg_instance_info(None, content)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Running signal should be first notification
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.RUNNING}
        self.epum.msg_instance_info(None, content)

        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")

    def test_multiple_subscribers(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        subscriber2_name = "subscriber02_name"
        subscriber2_op = "subscriber02_op"
        subscriber3_name = "subscriber03_name"
        subscriber3_op = "subscriber03_op"

        self._reset()
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)

        definition_id = "definition1"
        definition = self._get_simplest_domain_definition()
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain("owner", "domain1", definition_id, self._config_simplest_domainconf(1))
        self.epum.msg_subscribe_domain("owner", "domain1", subscriber_name, subscriber_op)
        self.epum.msg_subscribe_domain("owner", "domain1", subscriber2_name, subscriber2_op)
        self.epum.msg_subscribe_domain("owner", "domain1", subscriber3_name, subscriber3_op)

        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.provisioner_client.deployable_types_launched[0], "00_dt_id")
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Simulate provisioner
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.STARTED}
        self.epum.msg_instance_info(None, content)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Running signal should be first notification
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.RUNNING}
        self.epum.msg_instance_info(None, content)

        self._mock_checks(3, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")
        self._mock_checks(3, 1, subscriber2_name, subscriber2_op, InstanceState.RUNNING, "domain1")
        self._mock_checks(3, 2, subscriber3_name, subscriber3_op, InstanceState.RUNNING, "domain1")

    def test_multiple_subscribers_multiple_domains(self):
        """Three subscribers, two for one domain, one for another.  One VM for each domain.
        """

        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        subscriber2_name = "subscriber02_name"
        subscriber2_op = "subscriber02_op"
        subscriber3_name = "subscriber03_name"
        subscriber3_op = "subscriber03_op"

        self._reset()
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)

        definition_id = "definition1"
        definition = self._get_simplest_domain_definition()
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain("owner", "domain1", definition_id, self._config_simplest_domainconf(1))
        self.epum.msg_subscribe_domain("owner", "domain1", subscriber_name, subscriber_op)
        self.epum.msg_subscribe_domain("owner", "domain1", subscriber2_name, subscriber2_op)

        # Subscriber 3 is for a different domain
        self.epum.msg_add_domain("owner", "domain2", definition_id, self._config_simplest_domainconf(1, dt="01_dt_id"))
        self.epum.msg_subscribe_domain("owner", "domain2", subscriber3_name, subscriber3_op)

        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 2)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 2)

        # Find out which order these were launched ...
        subscriber3_index = -1
        for i, dt_id in enumerate(self.provisioner_client.deployable_types_launched):
            if dt_id == "01_dt_id":
                subscriber3_index = i
        self.assertNotEqual(subscriber3_index, -1)

        # Now we know which was provisioned first... give opposite index to other one
        if subscriber3_index:
            subscriber1and2_index = 0
        else:
            subscriber1and2_index = 1

        self.assertEqual(self.provisioner_client.deployable_types_launched[subscriber1and2_index], "00_dt_id")
        self.assertEqual(self.provisioner_client.deployable_types_launched[subscriber3_index], "01_dt_id")

        # No notifications until RUNNING
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Simulate provisioner update for BOTH VMs launched
        content = {"node_id": self.provisioner_client.launched_instance_ids[subscriber1and2_index],
                   "state": InstanceState.STARTED}
        self.epum.msg_instance_info(None, content)
        content = {"node_id": self.provisioner_client.launched_instance_ids[subscriber3_index],
                   "state": InstanceState.STARTED}
        self.epum.msg_instance_info(None, content)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Running signal should be first notification, send RUNNING just for 01_dt_id instance (subscriber 3)
        content = {"node_id": self.provisioner_client.launched_instance_ids[subscriber3_index],
                   "state": InstanceState.RUNNING}
        self.epum.msg_instance_info(None, content)
        self._mock_checks(1, 0, subscriber3_name, subscriber3_op, InstanceState.RUNNING, "domain2")

        # Now for 00_dt_id instance (subscribers 1 and 2)
        content = {"node_id": self.provisioner_client.launched_instance_ids[subscriber1and2_index],
                   "state": InstanceState.RUNNING}
        self.epum.msg_instance_info(None, content)
        self._mock_checks(3, 1, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")
        self._mock_checks(3, 2, subscriber2_name, subscriber2_op, InstanceState.RUNNING, "domain1")

    def _fail_setup(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._reset()
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        definition_id = "definition1"
        definition = self._get_simplest_domain_definition()
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain("owner", "domain1", definition_id, self._config_simplest_domainconf(1))
        self.epum.msg_subscribe_domain("owner", "domain1", subscriber_name, subscriber_op)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.provisioner_client.deployable_types_launched[0], "00_dt_id")
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Simulate provisioner
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.STARTED}
        self.epum.msg_instance_info(None, content)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Running signal should be first notification
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.RUNNING}
        self.epum.msg_instance_info(None, content)

    # The "test_fail*" methods are for checking on notifications after RUNNING.  If the provisioner
    # doesn't 'increase' states, EPUM throws them out, no need to test that scenario.

    def test_fail_650(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._fail_setup()
        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")

        # Failing
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.RUNNING_FAILED}
        self.epum.msg_instance_info(None, content)

        # All non-RUNNING notifications should be FAILED
        self._mock_checks(2, 1, subscriber_name, subscriber_op, InstanceState.FAILED, "domain1")

    def test_fail_700(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._fail_setup()
        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")

        # Failing
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.TERMINATING}
        self.epum.msg_instance_info(None, content)

        # All non-RUNNING notifications should be FAILED
        self._mock_checks(2, 1, subscriber_name, subscriber_op, InstanceState.FAILED, "domain1")

    def test_fail_800(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._fail_setup()
        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")

        # Failing
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.TERMINATED}
        self.epum.msg_instance_info(None, content)

        # All non-RUNNING notifications should be FAILED
        self._mock_checks(2, 1, subscriber_name, subscriber_op, InstanceState.FAILED, "domain1")

    def test_fail_900(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._fail_setup()
        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")

        # Failing
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.FAILED}
        self.epum.msg_instance_info(None, content)

        # All non-RUNNING notifications should be FAILED
        self._mock_checks(2, 1, subscriber_name, subscriber_op, InstanceState.FAILED, "domain1")

    def test_updated_node_ip(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"

        self._reset()
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        definition_id = "definition1"
        definition = self._get_simplest_domain_definition()
        self.epum.msg_add_domain_definition(definition_id, definition)
        self.epum.msg_add_domain("owner", "domain1", definition_id, self._config_simplest_domainconf(1))
        self.epum.msg_subscribe_domain("owner", "domain1", subscriber_name, subscriber_op)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        domain = self.epum_store.get_domain("owner", "domain1")

        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.STARTED,
                   "update_counter": 1}
        self.epum.msg_instance_info(None, content)

        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.RUNNING,
                   "public_ip": "vm-1234",
                   "update_counter": 2}
        self.epum.msg_instance_info(None, content)

        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")
        self.assertEqual(domain.get_instance(self.provisioner_client.launched_instance_ids[0]).public_ip, "vm-1234")

        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.RUNNING,
                   "public_ip": "1.2.3.4",
                   "update_counter": 3}
        self.epum.msg_instance_info(None, content)

        self._mock_checks(2, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")
        self.assertEqual(domain.get_instance(self.provisioner_client.launched_instance_ids[0]).public_ip, "1.2.3.4")

        # Check that sequential update_counter is respected
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.RUNNING,
                   "public_ip": "localhost",
                   "update_counter": 2}
        self.epum.msg_instance_info(None, content)

        self._mock_checks(2, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")
        self.assertEqual(domain.get_instance(self.provisioner_client.launched_instance_ids[0]).public_ip, "1.2.3.4")

        # A state going backwards should not happen, but double-check
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceState.STARTED,
                   "public_ip": "localhost",
                   "update_counter": 4}
        self.epum.msg_instance_info(None, content)

        self._mock_checks(2, 0, subscriber_name, subscriber_op, InstanceState.RUNNING, "domain1")
        self.assertEqual(domain.get_instance(self.provisioner_client.launched_instance_ids[0]).public_ip, "1.2.3.4")
