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

_BASE_CLUSTER_DOC = """
<cluster>
  <workspace>
    <name>head-node</name>
    <quantity>1</quantity>
    <ctx><requires><data name="sandwich">${head_node_sandwich}</data></requires></ctx>
  </workspace>
  <workspace>
    <name>worker-node</name>
    <quantity>3</quantity>
    <ctx><requires><data name="sandwich">${worker_node_sandwich}</data></requires></ctx>
  </workspace>
</cluster>
"""

_BASE_CLUSTER_SITES = {
        'nimbus-test' : {
            'head-node' : {
                'image' : 'base-cluster',
            },
            'worker-node' : {
                'image' : 'base-cluster',
                }
            }
        }

_DT_ALL_DEFAULT = {
        'document' : _BASE_CLUSTER_DOC,
        'sites' : _BASE_CLUSTER_SITES,
        'vars' : {
            'worker_node_sandwich' : 'cheese',
            'head_node_sandwich' : 'ice cream'}}
_DT_NO_DEFAULT = {
        'document' : _BASE_CLUSTER_DOC,
        'sites' : _BASE_CLUSTER_SITES,}
_DT_WORKER_DEFAULT = {
        'document' : _BASE_CLUSTER_DOC,
        'sites' : _BASE_CLUSTER_SITES,
        'vars' : {
            'worker_node_sandwich' : 'ice cream'}}


class TestDeployableTypeRegistryService(unittest.TestCase):
    """Testing deployable type lookups
    """

    def setUp(self):
        self.registry = {}
        self.dtrs = LocalDTRS(registry=self.registry)

    def test_dtrs_lookup(self):
        self.registry['base-cluster-1'] = _DT_ALL_DEFAULT

        req_nodes = {
            'head-node' : {'site' : 'nimbus-test'},
            'worker-node' : {'site' : 'nimbus-test'}}

        result = self.dtrs.lookup('base-cluster-1', nodes=req_nodes)
        doc = result['document']
        nodes = result['nodes']
        for node in nodes.itervalues():
            self.assertTrue('iaas_image' in node)

        got_error = False
        try:
            self.dtrs.lookup('this-dt-doesnt-exist', nodes)
        except DeployableTypeLookupError, e:
            log.info('Got expected error: ' + str(e))
            got_error = True
        self.assertTrue(got_error)

        req_nodes['head-node']['site'] = 'this-site-doesnt-exist'
        got_error = False
        try:
            self.dtrs.lookup('base-cluster-1', req_nodes)
        except DeployableTypeLookupError, e:
            log.info('Got expected error: ' + str(e))
            got_error = True

        self.assertTrue(got_error)

    def test_vars(self):
        # test with
        self.registry['no-default'] = _DT_NO_DEFAULT
        self.registry['all-default'] = _DT_ALL_DEFAULT
        self.registry['worker-default'] = _DT_WORKER_DEFAULT

        req_nodes = {
            'head-node' : {'site' : 'nimbus-test'},
            'worker-node' : {'site' : 'nimbus-test'}}

        got_error = False
        try:
            self.dtrs.lookup('no-default', req_nodes)
        except DeployableTypeValidationError, e:
            log.info('Got expected error: ' + str(e))
            got_error = True
        self.assertTrue(got_error)

        vars = {'head_node_sandwich' : 'steak'}
        self.dtrs.lookup('worker-default', req_nodes, vars)

        vars['worker_node_sandwich'] = 'peanut butter'
        response = self.dtrs.lookup('worker-default', req_nodes, vars)
        # ensure default is overridden
        self.assertTrue(response['document'].find(vars['worker_node_sandwich']) != -1)

        self.dtrs.lookup('no-default', req_nodes, vars)

