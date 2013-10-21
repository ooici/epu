# Copyright 2013 University of Chicago

import logging
import unittest
import time

import epu.tevent as tevent
import dashi.exceptions

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
            "type": "fake"
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

        allocation_override = "m1.xlarge"
        req_node = {'site': 'nimbus-test', 'allocation': allocation_override}
        result = self.dtrs_client.lookup(self.caller, 'base-cluster-1', req_node)
        node = result['node']
        self.assertTrue('iaas_image' in node)
        self.assertEqual(node['iaas_allocation'], allocation_override)

        try:
            self.dtrs_client.lookup(self.caller, 'this-dt-doesnt-exist', node)
        except DeployableTypeLookupError, e:
            log.info("Got expected error: " + str(e))
        else:
            self.fail("Expected lookup error")

        req_node['site'] = 'this-site-doesnt-exist'
        try:
            self.dtrs_client.lookup(self.caller, 'base-cluster-1', req_node)
        except DeployableTypeLookupError, e:
            log.info("Got expected error: " + str(e))
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

        req_node = {'site': 'nimbus-test'}

        try:
            self.dtrs_client.lookup(self.caller, 'with-vars', req_node)
        except DeployableTypeValidationError, e:
            log.info("Got expected error: " + str(e))
        else:
            self.fail("Expected validation error")

        vars = {'worker_node_image': 'fake-image-from-var'}
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
                'method': 'chef',
                'run_list': ['hats', 'jackets'],
                'attributes': {"a": 4}
            }
        }
        self.dtrs.add_dt(self.caller, "with-chef", dt_definition)

        chef_credential = {
            'url': "http://fake",
            'client_key': 'aewlkfaejfalfk',
            'validator_key': 'aejfalkefjaklef'
        }
        self.dtrs.add_credentials(self.caller, "hats", chef_credential,
            credential_type="chef")
        self.dtrs.add_credentials(self.caller, "chef", chef_credential,
            credential_type="chef")

        req_node = {'site': 'nimbus-test'}

        lookup_vars = {'chef_credential': 'hats'}

        response = self.dtrs_client.lookup(self.caller, 'with-chef', req_node, vars=lookup_vars)
        self.assertEqual(response['node']['ctx_method'], 'chef')
        self.assertIs(response['node']['needs_nimbus_ctx'], False)
        self.assertEqual(response['node']['chef_runlist'], ['hats', 'jackets'])
        self.assertEqual(response['node']['chef_attributes'], {"a": 4})
        self.assertEqual(response['node']['chef_credential'], "hats")

        # lookup without chef_credential in vars results in default of "chef"
        response = self.dtrs_client.lookup(self.caller, 'with-chef', req_node)
        self.assertEqual(response['node']['chef_credential'], "chef")

    def test_chef_solo_contextualization(self):
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
                    "run_list": ["recipe[r2app]", "recipe[user]"]
                }
            }
        }
        self.dtrs.add_dt(self.caller, "with-chef-solo", dt_definition)

        req_node = {'site': 'nimbus-test'}

        response = self.dtrs_client.lookup(self.caller, 'with-chef-solo', req_node)
        self.assertEqual(response['node']['ctx_method'], 'chef-solo')
        self.assertIs(response['node']['needs_nimbus_ctx'], True)
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

        req_node = {'site': 'nimbus-test'}

        response = self.dtrs_client.lookup(self.caller, 'with-userdata', req_node)
        self.assertEqual(response['node']['ctx_method'], 'userdata')
        self.assertIs(response['node']['needs_nimbus_ctx'], False)
        self.assertFalse(response['document'].find('dt-chef-solo') != -1)
        self.assertTrue('iaas_userdata' in response['node'])
        self.assertEqual(userdata, response['node']['iaas_userdata'])

    def _test_wrong_site(self, wrong_site_definition):
        with self.assertRaises(dashi.exceptions.BadRequestError):
            self.dtrs_client.add_site("wrong_site", wrong_site_definition)

        good_site_definition = {
            "type": "nimbus",
            "host": "svc.uc.futuregrid.org",
            "port": 8444,
            "secure": True
        }
        self.dtrs_client.add_site("good_site", good_site_definition)

        with self.assertRaises(dashi.exceptions.BadRequestError):
            self.dtrs_client.update_site("good_site", wrong_site_definition)

    def test_site_missing_type(self):
        wrong_site_definition = {
            "region": "us-east-1"
        }
        self._test_wrong_site(wrong_site_definition)

    def test_site_missing_port(self):
        wrong_site_definition = {
            "type": "nimbus",
            "host": "svc.uc.futuregrid.org"
        }
        self._test_wrong_site(wrong_site_definition)

    def test_site_missing_host(self):
        wrong_site_definition = {
            "type": "nimbus",
            "port": 8444
        }
        self._test_wrong_site(wrong_site_definition)

    def test_site_wrong_port(self):
        wrong_site_definition = {
            "type": "nimbus",
            "host": "svc.uc.futuregrid.org",
            "port": "forty-two"
        }
        self._test_wrong_site(wrong_site_definition)

    def test_site_wrong_secure(self):
        wrong_site_definition = {
            "type": "nimbus",
            "host": "svc.uc.futuregrid.org",
            "port": 8444,
            "secure": "true"
        }
        self._test_wrong_site(wrong_site_definition)

    def test_site_wrong_ec2_region(self):
        wrong_site_definition = {
            "type": "ec2",
            "region": "51st-state"
        }
        self._test_wrong_site(wrong_site_definition)

    def test_good_site(self):
        good_site_definition = {
            "type": "nimbus",
            "host": "svc.uc.futuregrid.org",
            "port": 8444,
            "secure": True
        }
        self.dtrs_client.add_site("good_site", good_site_definition)

    def test_credentials(self):
        site_definition = {
            "type": "nimbus",
            "host": "svc.uc.futuregrid.org",
            "port": 8444,
            "secure": True
        }
        self.dtrs_client.add_site("site1", site_definition)
        self.dtrs_client.add_site("site2", site_definition)

        credentials_1 = {
            "access_key": "EC2_ACCESS_KEY",
            "secret_key": "EC2_SECRET_KEY",
            "key_name": "EC2_KEYPAIR"
        }

        credentials_2 = {
            "access_key": "EC2_ACCESS_KEY2",
            "secret_key": "EC2_SECRET_KEY2",
            "key_name": "EC2_KEYPAIR2"
        }

        self.dtrs_client.add_credentials(self.caller, "site1", credentials_1)

        with self.assertRaises(dashi.exceptions.WriteConflictError):
            self.dtrs_client.add_credentials(self.caller, "site1", credentials_1)

        self.dtrs_client.add_credentials(self.caller, "site2", credentials_1,
            credential_type='site')

        # another credential type with the same name doesn't conflict
        self.dtrs_client.add_credentials(self.caller, "site1", {"some": "thing"},
            credential_type="chef")
        chef_creds = self.dtrs_client.describe_credentials(self.caller, "site1",
            credential_type="chef")
        self.assertEqual(chef_creds, {"some": "thing"})

        site_credentials = self.dtrs_client.list_credentials(self.caller)
        self.assertEqual(set(site_credentials), set(['site1', 'site2', 'nimbus-test']))
        site_credentials = self.dtrs_client.list_credentials(self.caller, credential_type='site')
        self.assertEqual(set(site_credentials), set(['site1', 'site2', 'nimbus-test']))
        self.assertEqual(self.dtrs_client.list_credentials(self.caller, credential_type='chef'), ['site1'])

        self.dtrs_client.update_credentials(self.caller, 'site1', credentials_2)
        found_creds = self.dtrs_client.describe_credentials(self.caller, 'site1')
        self.assertEqual(found_creds, credentials_2)

    def test_bad_credential_type(self):
        with self.assertRaises(dashi.exceptions.BadRequestError):
            self.dtrs_client.add_credentials(self.caller, "hats", {}, credential_type="NOTAREALTYPE")
