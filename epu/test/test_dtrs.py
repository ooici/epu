#!/usr/bin/env python

"""
@file epu/test/test_dtrs.py
@author David LaBissoniere
@brief Test provisioner behavior
"""

import unittest
import logging
from epu.dt_registry import DeployableTypeValidationError

from epu.localdtrs import LocalDTRS, DeployableTypeLookupError

log = logging.getLogger(__name__)

_BASE_DOC = """
<cluster>
  <workspace>
    <name>worker-node</name>
    <quantity>1</quantity>
    <ctx><requires><data name="sandwich">${worker_node_sandwich}</data></requires></ctx>
  </workspace>
</cluster>
"""

_BASE_SITES = {
    'nimbus-test' : {
        'image' : 'base-image'}
    }

_DT_ALL_DEFAULT = {
        'document' : _BASE_DOC,
        'sites' : _BASE_SITES,
        'vars' : {
            'worker_node_sandwich' : 'cheese'}}
_DT_NO_DEFAULT = {
        'document' : _BASE_DOC,
        'sites' : _BASE_SITES,}


class TestDeployableTypeRegistryService(unittest.TestCase):
    """Testing deployable type lookups
    """

    def setUp(self):
        self.registry = {}
        self.dtrs = LocalDTRS(registry=self.registry)

    def test_dtrs_lookup(self):
        self.registry['base-cluster-1'] = _DT_ALL_DEFAULT

        req_node = {'site' : 'nimbus-test'}

        result = self.dtrs.lookup('base-cluster-1', node=req_node)
        doc = result['document']
        node = result['node']
        self.assertTrue('iaas_image' in node)

        try:
            self.dtrs.lookup('this-dt-doesnt-exist', node)
        except DeployableTypeLookupError, e:
            log.info('Got expected error: ' + str(e))
        else:
            self.fail("Expected lookup error")

        req_node['site'] = 'this-site-doesnt-exist'
        try:
            self.dtrs.lookup('base-cluster-1', req_node)
        except DeployableTypeLookupError, e:
            log.info('Got expected error: ' + str(e))
        else:
            self.fail("Expected lookup error")

    def test_vars(self):
        # test with
        self.registry['no-default'] = _DT_NO_DEFAULT
        self.registry['all-default'] = _DT_ALL_DEFAULT

        req_node = {'site' : 'nimbus-test'}

        try:
            self.dtrs.lookup('no-default', req_node)
        except DeployableTypeValidationError, e:
            log.info('Got expected error: ' + str(e))
        else:
            self.fail("Expected lookup error")

        vars = {'worker_node_sandwich' : 'steak'}
        response = self.dtrs.lookup('all-default', req_node, vars)
        # ensure default is overridden
        self.assertTrue(response['document'].find(vars['worker_node_sandwich']) != -1)

        self.dtrs.lookup('no-default', req_node, vars)

