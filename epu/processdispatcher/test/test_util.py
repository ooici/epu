import unittest

from epu.processdispatcher.util import node_id_to_eeagent_name, \
    node_id_from_eeagent_name

class UtilTests(unittest.TestCase):

    def test_eeagent_names(self):
        node_id = "hats"
        eeagent_name = node_id_to_eeagent_name(node_id)
        self.assertNotEqual(node_id, eeagent_name)

        node_id2 = node_id_from_eeagent_name(eeagent_name)
        self.assertEqual(node_id, node_id2)

