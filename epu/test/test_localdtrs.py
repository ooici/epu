import os
import unittest

from epu.localdtrs import LocalVagrantDTRS, LocalDTRSException
from epu.ionproc.dtrs import DeployableTypeLookupError

class LocalVagrantDTRSTests(unittest.TestCase):
    def test_init(self):
        
        exception_caught = False
        try:
            LocalVagrantDTRS()
        except LocalDTRSException:
            exception_caught = True
        assert exception_caught

    def test_lookup(self):
        test_dir = os.path.dirname(os.path.realpath(__file__))
        cookbooks_path = os.path.join(test_dir, "dt-data", "cookbooks")
        dt_directory = os.path.join(test_dir, "dt-data", "dt")

        dt = "simple"
        dt_path = os.path.join(dt_directory, "%s.json" % dt)

        dtrs = LocalVagrantDTRS(dt_directory, cookbooks_path)

        result = yield dtrs.lookup(dt)
        print result, dt_path
        assert result['chef_json'] == dt_path

        baddt = "notreal"
        exception_caught = False
        try:
            result = yield dtrs.lookup(baddt)
        except:
            exception_caught = True
        assert exception_caught


        baddt_path = "/path/to/dt"
        dtrs._add_lookup(baddt, baddt_path)
        result = yield dtrs.lookup(baddt)
        assert result['chef_json'] == baddt_path

