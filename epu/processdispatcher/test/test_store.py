import unittest

from epu.processdispatcher.store import ResourceRecord

class RecordTests(unittest.TestCase):

    def test_resource_record(self):
        r = ResourceRecord.new("r1", "n1", 1)
        self.assertEqual(r.available_slots, 1)
        self.assertEqual(r.properties, {})
        r.assigned.append('proc1')
        self.assertEqual(r.available_slots, 0)
