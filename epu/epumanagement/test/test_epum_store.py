import uuid
import unittest
import logging

from epu.decisionengine.impls.simplest import CONF_PRESERVE_N
from epu.epumanagement.core import CoreInstance
from epu.epumanagement.store import LocalEPUMStore, ZooKeeperEPUMStore
from epu.epumanagement.conf import *
from epu.exceptions import WriteConflictError
from epu.test import ZooKeeperTestMixin

log = logging.getLogger(__name__)

class BaseEPUMStoreTests(unittest.TestCase):

    def setUp(self):
        self.store = LocalEPUMStore(service_name="EPUM")

    def test_simple_add(self):
        config = {}
        self.store.add_domain("caller01", "testing01", config)
        domain = self.store.get_domain("caller01", "testing01")
        self.assertEqual("testing01", domain.domain_id)
        self.assertEqual("caller01", domain.owner)

        # try to create again, should be name clash
        self.assertRaises(WriteConflictError, self.store.add_domain,
                          "caller01", "testing01", config)

        # but another caller should be able to create the same name
        self.store.add_domain("caller02", "testing01", config)
        domain = self.store.get_domain("caller02", "testing01")
        self.assertEqual("testing01", domain.domain_id)
        self.assertEqual("caller02", domain.owner)

        # and first should still exist
        domain = self.store.get_domain("caller01", "testing01")
        self.assertEqual("testing01", domain.domain_id)
        self.assertEqual("caller01", domain.owner)

    def test_add_remove_definitions(self):
        definition01 = {
            "name": "definition01",
            "description": "Domain definition 01",
            "engine_class": "epu.decisionengine.impls.simplest.SimplestEngine",
            "health": {
                "monitor_health": True
            }
        }

        definition02 = {
            "name": "definition02",
            "description": "Domain definition 02",
            "engine_class": "epu.decisionengine.impls.needy.NeedyEngine",
            "health": {
                "monitor_health": False
            }
        }

        self.store.add_domain_definition("definition01", definition01)
        self.store.add_domain_definition("definition02", definition02)

        definitions = self.store.list_domain_definitions()
        self.assertEqual(2, len(self.store.list_domain_definitions()))
        self.assertIn("definition01", definitions)
        self.assertIn("definition02", definitions)

        domain_definition = self.store.get_domain_definition("definition01")
        self.assertEqual("definition01", domain_definition.definition_id)
        self.assertEqual(definition01, domain_definition.definition)

        domain_definition = self.store.get_domain_definition("definition02")
        self.assertEqual("definition02", domain_definition.definition_id)
        self.assertEqual(definition02, domain_definition.definition)

        self.store.remove_domain_definition("definition01")
        definitions = self.store.list_domain_definitions()
        self.assertEqual(1, len(self.store.list_domain_definitions()))
        self.assertIn("definition02", definitions)

    def test_domain_configs(self):
        """
        Create one domain with a certain configuration.  Test that initial conf and
        later conf additions work properly.
        """
        owner = "David"
        engine_class = "epu.decisionengine.impls.simplest.SimplestEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        engine = {CONF_PRESERVE_N:2, }
        config = {EPUM_CONF_GENERAL:general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}
        self.store.add_domain(owner, "testing02", config)
        domain = self.store.get_domain(owner, "testing02")

        general_out = domain.get_general_config()
        self.assertTrue(isinstance(general_out, dict))
        self.assertTrue(general_out.has_key(EPUM_CONF_ENGINE_CLASS))
        self.assertEqual(engine_class, general_out[EPUM_CONF_ENGINE_CLASS])

        engine_out = domain.get_engine_config()
        self.assertTrue(isinstance(engine_out, dict))
        self.assertTrue(engine_out.has_key(CONF_PRESERVE_N))
        self.assertEqual(2, engine_out[CONF_PRESERVE_N])

        health_out = domain.get_health_config()
        self.assertTrue(isinstance(health_out, dict))
        self.assertTrue(health_out.has_key(EPUM_CONF_HEALTH_MONITOR))
        self.assertEqual(False, health_out[EPUM_CONF_HEALTH_MONITOR])
        health_enabled = domain.is_health_enabled()
        self.assertFalse(health_enabled)

    def test_active_removed_epums_simple(self):
        owner = "David"
        engine_class = "epu.decisionengine.impls.simplest.SimplestEngine"
        general = {EPUM_CONF_ENGINE_CLASS: engine_class}
        health = {EPUM_CONF_HEALTH_MONITOR: False}
        engine = {CONF_PRESERVE_N:2, }
        config = {EPUM_CONF_GENERAL:general, EPUM_CONF_ENGINE: engine, EPUM_CONF_HEALTH: health}
        self.store.add_domain(owner, "active01", config)
        self.store.add_domain(owner, "removed02", config)

        r = self.store.get_domain(owner, "removed02")

        # make sure they both come out of the store list
        all_domains = self.store.get_all_domains()
        self.assertEqual(len(all_domains), 2)

        # mark for removal. This doesn't actually remove it from the
        # store because the instances may need to be shut down first.
        r.remove()
        self.assertTrue(r.is_removed())

        # make sure they both come out of the store lists
        all_domains = self.store.get_all_domains()
        self.assertEqual(len(all_domains), 2)

        self.store.remove_domain(owner, "removed02")

        # make sure they both come out of the store lists
        all_domains = self.store.get_all_domains()
        self.assertEqual(len(all_domains), 1)

    def test_config(self):

        domain = self.store.add_domain("David", "dom0", {})

        empty = domain.get_engine_config()
        self.assertIsInstance(empty, dict)
        self.assertFalse(empty)

        empty = domain.get_engine_config(keys=('not','real', 'keys'))
        self.assertIsInstance(empty, dict)
        self.assertFalse(empty)

        domain.add_engine_config({'a_string' : 'thisisastring',
                                     'a_list' : [1,2,3], 'a_number' : 1.23})
        cfg = domain.get_engine_config(keys=['a_string'])
        self.assertEqual(cfg, {'a_string' : 'thisisastring'})

        cfg = domain.get_engine_config()
        self.assertEqual(cfg, {'a_string' : 'thisisastring',
                                     'a_list' : [1,2,3], 'a_number' : 1.23})

        domain.add_engine_config({'a_dict' : {"akey": {'fpp' : 'bar'}, "blah" : 5},
                                     "a_list" : [4,5,6]})

        cfg = domain.get_engine_config()
        self.assertEqual(cfg, {'a_string' : 'thisisastring',
                                     'a_list' : [4,5,6], 'a_number' : 1.23,
                                     'a_dict' : {"akey": {'fpp' : 'bar'}, "blah" : 5}})

        cfg = domain.get_engine_config(keys=('a_list', 'a_number'))
        self.assertEqual(cfg, {'a_list' : [4,5,6], 'a_number' : 1.23})

    def test_instances_put_get_3(self):
        self._instances_put_get(3)

    def test_instances_put_get_100(self):
        self._instances_put_get(100)

    def test_instances_put_get_301(self):
        self._instances_put_get(301)

    def _instances_put_get(self, count):

        domain = self.store.add_domain("David", "dom0", {})

        instances = []
        instance_ids = set()
        for i in range(count):
            instance = CoreInstance(instance_id=str(uuid.uuid4()), launch_id=str(uuid.uuid4()),
                                    site="Chicago", allocation="small", state="Illinois")
            instances.append(instance)
            instance_ids.add(instance.instance_id)
            domain.add_instance(instance)

        found_ids = domain.get_instance_ids()
        found_ids = set(found_ids)
        log.debug("Put %d instances, got %d instance IDs", count, len(found_ids))
        self.assertEqual(len(found_ids), len(instance_ids))
        self.assertEqual(found_ids, instance_ids)

        # could go on to verify each instance record

class EPUMZooKeeperStoreTests(BaseEPUMStoreTests, ZooKeeperTestMixin):

    # this runs all of the BaseProvisionerStoreTests tests plus any
    # ZK-specific ones

    def setUp(self):
        self.setup_zookeeper("/epum_store_tests_")
        self.store = ZooKeeperEPUMStore("epum", self.zk_hosts, self.zk_base_path)

        self.store.initialize()

    def tearDown(self):
        self.teardown_zookeeper()