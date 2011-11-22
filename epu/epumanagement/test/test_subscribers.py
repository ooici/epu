import unittest

from epu.epumanagement import EPUManagement
from epu.epumanagement.test.mocks import MockSubscriberNotifier, MockProvisionerClient, MockOUAgentClient
from epu.epumanagement.conf import *
import epu.states as InstanceStates

import ion.util.ionlog

log = ion.util.ionlog.getLogger(__name__)

class SubscriberTests(unittest.TestCase):

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

    def _reset(self):
        self.notifier.notify_by_name_called = 0
        self.notifier.receiver_names = []
        self.notifier.operations = []
        self.notifier.messages = []

    def _mock_checks(self, num_called, idx_check, subscriber_name, subscriber_op, expected_state):
        self.assertEqual(self.notifier.notify_by_name_called, num_called)
        self.assertEqual(len(self.notifier.receiver_names), num_called)
        self.assertEqual(len(self.notifier.operations), num_called)
        self.assertEqual(len(self.notifier.messages), num_called)
        self.assertEqual(self.notifier.receiver_names[idx_check], subscriber_name)
        self.assertEqual(self.notifier.operations[idx_check], subscriber_op)
        self.assertTrue(self.notifier.messages[idx_check].has_key("state"))
        self.assertEqual(self.notifier.messages[idx_check]["state"], expected_state)

    def test_ignore_subscriber(self):
        subscriber_name = None
        subscriber_op = None

        self._reset()
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        constraints = {CONF_IAAS_SITE: "00_iaas_site", CONF_IAAS_ALLOCATION: "00_iaas_alloc"}
        self.epum.msg_register_need(None, "00_dt_id", constraints, 1, subscriber_name, subscriber_op)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.provisioner_client.deployable_types_launched[0], "00_dt_id")
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Simulate provisioner
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.RUNNING}
        self.epum.msg_instance_info(None, content)

        self.assertEqual(self.notifier.notify_by_name_called, 0)

    def test_one_subscriber(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"

        self._reset()
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        constraints = {CONF_IAAS_SITE: "00_iaas_site", CONF_IAAS_ALLOCATION: "00_iaas_alloc"}
        self.epum.msg_register_need(None, "00_dt_id", constraints, 1, subscriber_name, subscriber_op)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.provisioner_client.deployable_types_launched[0], "00_dt_id")
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Simulate provisioner
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.STARTED}
        self.epum.msg_instance_info(None, content)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Running signal should be first notification
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.RUNNING}
        self.epum.msg_instance_info(None, content)

        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceStates.RUNNING)

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
        constraints = {CONF_IAAS_SITE: "00_iaas_site", CONF_IAAS_ALLOCATION: "00_iaas_alloc"}

        self.epum.msg_register_need(None, "00_dt_id", constraints, 1, subscriber_name, subscriber_op)
        self.epum.msg_subscribe_dt(None, "00_dt_id", subscriber2_name, subscriber2_op)
        self.epum.msg_subscribe_dt(None, "00_dt_id", subscriber3_name, subscriber3_op)

        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.provisioner_client.deployable_types_launched[0], "00_dt_id")
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Simulate provisioner
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.STARTED}
        self.epum.msg_instance_info(None, content)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Running signal should be first notification
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.RUNNING}
        self.epum.msg_instance_info(None, content)

        self._mock_checks(3, 0, subscriber_name, subscriber_op, InstanceStates.RUNNING)
        self._mock_checks(3, 1, subscriber2_name, subscriber2_op, InstanceStates.RUNNING)
        self._mock_checks(3, 2, subscriber3_name, subscriber3_op, InstanceStates.RUNNING)

    def test_multiple_subscribers_multiple_dts(self):
        """Three subscribers, two for one DT, one for another.  One VM for each DT.
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
        constraints = {CONF_IAAS_SITE: "00_iaas_site", CONF_IAAS_ALLOCATION: "00_iaas_alloc"}

        self.epum.msg_register_need(None, "00_dt_id", constraints, 1, subscriber_name, subscriber_op)
        self.epum.msg_subscribe_dt(None, "00_dt_id", subscriber2_name, subscriber2_op)

        # Subscriber 3 is for a different DT
        self.epum.msg_register_need(None, "01_dt_id", constraints, 1, subscriber3_name, subscriber3_op)

        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 2)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 2)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 2)

        # Find out which order these were launched ...
        subscriber3_index = -1
        for i,dt_id in enumerate(self.provisioner_client.deployable_types_launched):
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
                   "state": InstanceStates.STARTED}
        self.epum.msg_instance_info(None, content)
        content = {"node_id": self.provisioner_client.launched_instance_ids[subscriber3_index],
                   "state": InstanceStates.STARTED}
        self.epum.msg_instance_info(None, content)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Running signal should be first notification, send RUNNING just for 01_dt_id instance (subscriber 3)
        content = {"node_id": self.provisioner_client.launched_instance_ids[subscriber3_index],
                   "state": InstanceStates.RUNNING}
        self.epum.msg_instance_info(None, content)
        self._mock_checks(1, 0, subscriber3_name, subscriber3_op, InstanceStates.RUNNING)

        # Now for 00_dt_id instance (subscribers 1 and 2)
        content = {"node_id": self.provisioner_client.launched_instance_ids[subscriber1and2_index],
                   "state": InstanceStates.RUNNING}
        self.epum.msg_instance_info(None, content)
        self._mock_checks(3, 1, subscriber_name, subscriber_op, InstanceStates.RUNNING)
        self._mock_checks(3, 2, subscriber2_name, subscriber2_op, InstanceStates.RUNNING)

    def _fail_setup(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._reset()
        self.epum.initialize()
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 0)
        constraints = {CONF_IAAS_SITE: "00_iaas_site", CONF_IAAS_ALLOCATION: "00_iaas_alloc"}
        self.epum.msg_register_need(None, "00_dt_id", constraints, 1, subscriber_name, subscriber_op)
        self.epum._run_decisions()
        self.assertEqual(self.provisioner_client.provision_count, 1)
        self.assertEqual(len(self.provisioner_client.launched_instance_ids), 1)
        self.assertEqual(len(self.provisioner_client.deployable_types_launched), 1)
        self.assertEqual(self.provisioner_client.deployable_types_launched[0], "00_dt_id")
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Simulate provisioner
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.STARTED}
        self.epum.msg_instance_info(None, content)
        self.assertEqual(self.notifier.notify_by_name_called, 0)

        # Running signal should be first notification
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.RUNNING}
        self.epum.msg_instance_info(None, content)

    # The "test_fail*" methods are for checking on notifications after RUNNING.  If the provisioner
    # doesn't 'increase' states, EPUM throws them out, no need to test that scenario.

    def test_fail_650(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._fail_setup()
        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceStates.RUNNING)

        # Failing
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.RUNNING_FAILED}
        self.epum.msg_instance_info(None, content)

        # All non-RUNNING notifications should be FAILED
        self._mock_checks(2, 1, subscriber_name, subscriber_op, InstanceStates.FAILED)

    def test_fail_700(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._fail_setup()
        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceStates.RUNNING)

        # Failing
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.TERMINATING}
        self.epum.msg_instance_info(None, content)

        # All non-RUNNING notifications should be FAILED
        self._mock_checks(2, 1, subscriber_name, subscriber_op, InstanceStates.FAILED)

    def test_fail_800(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._fail_setup()
        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceStates.RUNNING)

        # Failing
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.TERMINATED}
        self.epum.msg_instance_info(None, content)

        # All non-RUNNING notifications should be FAILED
        self._mock_checks(2, 1, subscriber_name, subscriber_op, InstanceStates.FAILED)

    def test_fail_900(self):
        subscriber_name = "subscriber01_name"
        subscriber_op = "subscriber01_op"
        self._fail_setup()
        self._mock_checks(1, 0, subscriber_name, subscriber_op, InstanceStates.RUNNING)

        # Failing
        content = {"node_id": self.provisioner_client.launched_instance_ids[0],
                   "state": InstanceStates.FAILED}
        self.epum.msg_instance_info(None, content)

        # All non-RUNNING notifications should be FAILED
        self._mock_checks(2, 1, subscriber_name, subscriber_op, InstanceStates.FAILED)
