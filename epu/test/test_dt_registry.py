import unittest
import logging

import simplejson as json

from epu.test import FileFixtures
from epu.dt_registry import DeployableTypeRegistry, DeployableTypeValidationError

log = logging.getLogger(__name__)

class TestDeployableTypeRegistry(unittest.TestCase):

    def test_ok1(self):
        dir = FileFixtures('dtrs').path('ok1')

        registry = DeployableTypeRegistry(dir)
        registry.load()

        self.assertEqual(2, len(registry.dt))

        dt = registry.get('common_doc')
        self.assertOneDt(dt, doc_content='common document')

        dt = registry.get('implicit_doc')
        self.assertOneDt(dt, doc_content='implicit document')

    def test_err1(self):
        self._test_err('err1')

    def test_err2(self):
        self._test_err('err2')

    def test_err3(self):
        self._test_err('err3')

    def _test_err(self, err):
        dir = FileFixtures('dtrs').path(err)

        registry = DeployableTypeRegistry(dir)
        try:
            registry.load()
        except DeployableTypeValidationError, e:
            log.debug('Got expected DT validation error: ' + str(e))
        else:
            self.fail("Expected DT validation error!")

    def assertOneDt(self, dt, has_vars=True, doc_content=None):
        self.assertTrue(dt.get('sites'))
        self.assertTrue(dt.get('document'))

        if has_vars:
            vars = dt['vars']

            a_list = vars['a_list']
            self.assertTrue(isinstance(a_list, (str,unicode)))
            self.assertEqual([1, 2, 3], json.loads(a_list))

            a_dict = vars['a_dict']
            self.assertTrue(isinstance(a_dict, (str,unicode)))
            self.assertEqual({'a' : 1}, json.loads(a_dict))

        if doc_content is not None:
            self.assertEqual(dt['document'].strip(), doc_content)


