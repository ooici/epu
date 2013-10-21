# Copyright 2013 University of Chicago

import unittest

from epu.processdispatcher.engines import EngineRegistry


ENGINE_CONF1 = {'engine1': {'slots': 4, 'base_need': 1},
                'engine2': {'slots': 4}, 'engine3': {'slots': 4}}


class EngineRegistryTests(unittest.TestCase):

    def test_bad_default(self):
        with self.assertRaises(KeyError):
            EngineRegistry.from_config(ENGINE_CONF1, default="engineX")

    def test_process_engines_no_mapping(self):
        registry = EngineRegistry.from_config(ENGINE_CONF1, default="engine1")

        definition = dict(executable={"module": "some.module", "class": "SomeClass"})
        self.assertIsNone(registry.get_process_definition_engine_id(definition))

        registry = EngineRegistry.from_config(ENGINE_CONF1, default="engine1",
            process_engines={})
        self.assertIsNone(registry.get_process_definition_engine_id(definition))

    def test_process_engines(self):

        process_engines = {'a.b.c.D': 'engine1', 'a.b.c': 'engine2', 'a': 'engine3',
                           'e.f': 'engine3', 'e.f.G': 'engine1'}
        registry = EngineRegistry.from_config(ENGINE_CONF1, default="engine1",
                                              process_engines=process_engines)

        definition = dict(executable={"module": "a.b.c", "class": "D"})
        self.assertEqual(registry.get_process_definition_engine_id(definition), 'engine1')

        definition = dict(executable={"module": "a.b.c", "class": "E"})
        self.assertEqual(registry.get_process_definition_engine_id(definition), 'engine2')

        definition = dict(executable={"module": "a.b.x", "class": "D"})
        self.assertEqual(registry.get_process_definition_engine_id(definition), 'engine3')

        definition = dict(executable={"module": "e", "class": "B"})
        self.assertEqual(registry.get_process_definition_engine_id(definition), None)

        definition = dict(executable={"module": "e.f", "class": "B"})
        self.assertEqual(registry.get_process_definition_engine_id(definition), "engine3")

        definition = dict(executable={"module": "e.f", "class": "G"})
        self.assertEqual(registry.get_process_definition_engine_id(definition), "engine1")
