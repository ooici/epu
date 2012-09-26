import logging
import unittest
import time
import json

import epu.tevent as tevent

from epu.dashiproc.dtrs import DTRS, DTRSClient
from epu.exceptions import DeployableTypeLookupError, DeployableTypeValidationError

log = logging.getLogger(__name__)


class DTRSTests(unittest.TestCase):

    def setUp(self):
        self.amqp_uri = "memory://hello"
        self.threads = []
        self.dtrs = DTRS(amqp_uri=self.amqp_uri)

        self._spawn_process(self.dtrs.start)

        # this sucks. sometimes service doesn't bind its queue before client
        # sends a message to it.
        time.sleep(0.05)

        self.dtrs_client = DTRSClient(dashi=self.dtrs.dashi)

        self.caller = "asterix"
        site_definition = {
            'name': 'nimbus-test',
            'description': 'Nimbus test',
            'driver_class': 'epu.provisioner.test.util.FakeNodeDriver'
        }
        self.dtrs.add_site("nimbus-test", site_definition)

        credentials_definition = {
            'access_key': 'myec2access',
            'secret_key': 'myec2secret',
            'key_name': 'nimbus'
        }
        self.dtrs.add_credentials(self.caller, "nimbus-test", credentials_definition)


    def _spawn_process(self, process):
        thread = tevent.spawn(process)
        self.threads.append(thread)

    def shutdown_procs(self):
        self._shutdown_processes(self.threads)

    def _shutdown_processes(self, threads):
        self.dtrs.dashi.cancel()
        tevent.joinall(threads)

    def tearDown(self):
        self.shutdown_procs()

    def test_dtrs_lookup(self):
        dt_definition = {
            'mappings': {
                'nimbus-test': {
                    'iaas_image': 'fake-image',
                    'iaas_allocation': 'm1.small'
                }
            }
        }
        self.dtrs.add_dt(self.caller, "base-cluster-1", dt_definition)

        req_node = {'site' : 'nimbus-test'}
        result = self.dtrs_client.lookup(self.caller, 'base-cluster-1', req_node)
        doc = result['document']
        node = result['node']
        self.assertTrue('iaas_image' in node)

        try:
            self.dtrs_client.lookup(self.caller, 'this-dt-doesnt-exist', node)
        except DeployableTypeLookupError, e:
            log.info('Got expected error: ' + str(e))
        else:
            self.fail("Expected lookup error")

        req_node['site'] = 'this-site-doesnt-exist'
        try:
            self.dtrs_client.lookup(self.caller, 'base-cluster-1', req_node)
        except DeployableTypeLookupError, e:
            log.info('Got expected error: ' + str(e))
        else:
            self.fail("Expected lookup error")

    def test_vars(self):
        dt_definition_with_vars = {
            'mappings': {
                'nimbus-test': {
                    'iaas_image': '${worker_node_image}',
                    'iaas_allocation': 'm1.small'
                }
            },
        }
        self.dtrs.add_dt(self.caller, "with-vars", dt_definition_with_vars)

        req_node = {'site' : 'nimbus-test'}

        try:
            self.dtrs_client.lookup(self.caller, 'with-vars', req_node)
        except DeployableTypeValidationError, e:
            log.info('Got expected error: ' + str(e))
        else:
            self.fail("Expected validation error")

        vars = {'worker_node_image' : 'fake-image-from-var'}
        response = self.dtrs_client.lookup(self.caller, 'with-vars', req_node,
                vars=vars)
        self.assertTrue(response['document'].find('worker_node_image') == -1)
        self.assertTrue(response['document'].find(vars['worker_node_image']) != -1)

    def test_chef_contextualization(self):
        dt_definition = {
            'mappings': {
                'nimbus-test': {
                    'iaas_image': 'fake-image',
                    'iaas_allocation': 'm1.small'
                }
            },
            'contextualization': {
                'method': 'chef-solo',
                'chef_config': {
                    "run_list": [ "recipe[r2app]", "recipe[user]" ]
                }
            }
        }
        self.dtrs.add_dt(self.caller, "with-chef", dt_definition)

        req_node = {'site' : 'nimbus-test'}

        response = self.dtrs_client.lookup(self.caller, 'with-chef', req_node)
        self.assertTrue(response['document'].find('dt-chef-solo') != -1)
        self.assertFalse('iaas_userdata' in response['node'])

    def test_userdata(self):
        userdata = "Hello Cloudy World"

        dt_definition = {
            'mappings': {
                'nimbus-test': {
                    'iaas_image': 'fake-image',
                    'iaas_allocation': 'm1.small'
                }
            },
            'contextualization': {
                'method': 'userdata',
                'userdata': userdata
            }
        }
        self.dtrs.add_dt(self.caller, "with-userdata", dt_definition)

        req_node = {'site' : 'nimbus-test'}

        response = self.dtrs_client.lookup(self.caller, 'with-userdata', req_node)
        self.assertFalse(response['document'].find('dt-chef-solo') != -1)
        self.assertTrue('iaas_userdata' in response['node'])
        self.assertEqual(userdata, response['node']['iaas_userdata'])
