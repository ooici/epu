import logging
import string

import simplejson as json
from xml.dom.minidom import Document

from epu.exceptions import DeployableTypeLookupError, DeployableTypeValidationError, \
    NotFoundError, BadRequestError
from epu.dtrs.store import sanitize_record
from epu.provisioner.sites import validate_site
from epu.util import is_valid_identifier

log = logging.getLogger(__name__)


class CredentialType(object):
    SITE = "site"
    CHEF = "chef"

    VALID_CREDENTIAL_TYPES = (SITE, CHEF)


class DTRSCore(object):
    """Core of the Deployable Type Registry Service"""

    def __init__(self, store):
        """Create DTRSCore
        """
        self.store = store

    def add_site(self, caller, site_name, site_definition):
        validate_site(site_definition)
        return self.store.add_site(caller, site_name, site_definition)

    def update_site(self, caller, site_name, site_definition):
        validate_site(site_definition)
        return self.store.update_site(caller, site_name, site_definition)

    def add_credentials(self, caller, credential_type, name, credentials):
        validate_credentials(credential_type, name, credentials)

        if credential_type == CredentialType.SITE:
            site = self.store.describe_site(caller, name)
            if site is None:
                raise NotFoundError("Cannot add credentials for unknown site %s" % name)

        log.info("Adding %s credentials '%s' for user %s", credential_type, name, caller)
        return self.store.add_credentials(caller, credential_type, name, credentials)

    def describe_credentials(self, caller, credential_type, name):
        ret = self.store.describe_credentials(caller, credential_type, name)
        return sanitize_record(ret)

    def describe_dt(self, caller, dt_name):
        ret = self.store.describe_dt(caller, dt_name)
        return sanitize_record(ret)

    def describe_site(self, caller, site_name):
        ret = self.store.describe_site(caller, site_name)
        return sanitize_record(ret)

    def lookup(self, caller, dt_name, dtrs_request_node, vars):

        log.debug("lookup dt %s for user %s" % (dt_name, caller))
        dt = self.store.describe_dt(caller, dt_name)
        if dt is None:
            raise DeployableTypeLookupError("Unknown deployable type name: %s" % dt_name)

        # dtrs_request_node contains:
        # - instance count (assuming one for now)
        # - site
        # - allocation
        try:
            site = dtrs_request_node['site']
        except KeyError:
            raise DeployableTypeLookupError('Node request missing site')

        dt_mappings = dt['mappings']
        try:
            site_mapping = dt_mappings[site]
        except KeyError:
            raise DeployableTypeLookupError('Site %s missing in mappings of DT %s', site, dt_name)

        try:
            iaas_image = site_mapping['iaas_image']
        except KeyError:
            raise DeployableTypeLookupError('iaas_image missing in mappings of DT %s and site %s', dt_name, site)

        try:
            iaas_allocation = site_mapping['iaas_allocation']
        except KeyError:
            raise DeployableTypeLookupError('iaas_allocation missing in mappings of DT %s and site %s', dt_name, site)

        allocation = dtrs_request_node.get("allocation")
        if allocation is not None:
            iaas_allocation = allocation

        site_credentials = self.store.describe_credentials(caller, CredentialType.SITE, site)
        if site_credentials is None:
            raise DeployableTypeLookupError('Credentials missing for caller %s and site %s', caller, site)

        try:
            iaas_sshkeyname = site_credentials['key_name']
        except KeyError:
            raise DeployableTypeLookupError('key_name missing from credentials of caller %s and site %s', caller, site)

        response_node = {
            'iaas_image': iaas_image,
            'iaas_allocation': iaas_allocation,
            'iaas_sshkeyname': iaas_sshkeyname,
            'needs_elastic_ip': bool(site_mapping.get('needs_elastic_ip', False)),
        }

        ctx_method = 'chef-solo'

        # this value controls whether the Provisioner will attempt to create a Nimbus context
        # for the VM.
        needs_nimbus_ctx = True
        contextualization = dt.get('contextualization')
        if contextualization and contextualization.get('method'):
            ctx_method = contextualization['method']
            if ctx_method == 'chef-solo':
                try:
                    chef_json = contextualization['chef_config']
                except KeyError:
                    raise DeployableTypeValidationError(dt_name, 'Missing chef_config in DT definition')
                document = generate_cluster_document(iaas_image, chef_json=chef_json)

            elif ctx_method == 'chef':
                needs_nimbus_ctx = False
                response_node['chef_runlist'] = contextualization.get('run_list', [])
                response_node['chef_attributes'] = contextualization.get('attributes', {})
                document = generate_cluster_document(iaas_image)

                # A chef credential name should be present in the vars, otherwise assume default "chef"
                if vars:
                    chef_credential_name = vars.get('chef_credential', 'chef')
                else:
                    chef_credential_name = 'chef'
                chef_credentials = self.store.describe_credentials(caller,
                    CredentialType.CHEF, chef_credential_name)
                if chef_credentials is None:
                    raise DeployableTypeLookupError('Chef credentials %s missing for caller %s' %
                        (chef_credential_name, caller))
                response_node['chef_credential'] = chef_credential_name

            elif ctx_method == 'userdata':
                needs_nimbus_ctx = False
                try:
                    userdata = str(contextualization['userdata'])
                except KeyError:
                    raise DeployableTypeValidationError(dt_name, 'Missing userdata in DT definition')
                document = generate_cluster_document(iaas_image)
                response_node['iaas_userdata'] = userdata
            else:
                raise DeployableTypeValidationError(dt_name, 'Unknown contextualization method %s' % ctx_method)
        else:
            document = generate_cluster_document(iaas_image)

        response_node['ctx_method'] = ctx_method
        response_node['needs_nimbus_ctx'] = needs_nimbus_ctx

        all_vars = {}
        if vars:
            process_vars(vars, dt_name)
            all_vars.update(vars)

        template = string.Template(document)
        try:
            document = template.substitute(all_vars)
        except KeyError, e:
            raise DeployableTypeValidationError(dt_name, 'DT doc has variable not present in request: %s' % str(e))
        except ValueError, e:
            raise DeployableTypeValidationError(dt_name, 'Deployable type document has bad variable: %s' % str(e))

        result = {'document': document, 'node': response_node}
        return result


def validate_credentials(credentials_type, name, credentials):
    if credentials_type not in CredentialType.VALID_CREDENTIAL_TYPES:
        raise BadRequestError("Invalid credentials type '%s'" % (credentials_type,))

    if not is_valid_identifier(name):
        raise BadRequestError("Invalid credentials name '%s'" % (name,))

    if not (credentials and isinstance(credentials, dict)):
        raise BadRequestError("Invalid credentials block")


def process_vars(vars, dt_name):
    """Process and validate node variables.

    Replaces list and dict values with JSON-encoded strings
    """
    if vars is None:
        # not required
        return None

    if not isinstance(vars, dict):
        raise DeployableTypeValidationError(dt_name, 'vars must be a dict')

    for key, value in vars.iteritems():

        # special handling of list and dict types: push these through JSON
        # encoder and make them strings. Allows object trees in variables
        # which can be placed inline with other JSON.
        if isinstance(value, (dict, list)):
            vars[key] = json.dumps(value)

    return vars


def generate_cluster_document(image, name="domain_instance", quantity=1,
                              nic="public", wantlogin="true", chef_json=None):

    doc = Document()

    root_el = doc.createElement("cluster")
    doc.appendChild(root_el)

    workspace_el = doc.createElement("workspace")
    root_el.appendChild(workspace_el)

    name_el = doc.createElement("name")
    workspace_el.appendChild(name_el)
    name_el_text = doc.createTextNode(name)
    name_el.appendChild(name_el_text)

    image_el = doc.createElement("image")
    workspace_el.appendChild(image_el)
    image_el_text = doc.createTextNode(image)
    image_el.appendChild(image_el_text)

    quantity_el = doc.createElement("quantity")
    workspace_el.appendChild(quantity_el)
    quantity_el_text = doc.createTextNode(str(quantity))
    quantity_el.appendChild(quantity_el_text)

    nic_el = doc.createElement("nic")
    nic_el.setAttribute("wantlogin", wantlogin)
    workspace_el.appendChild(nic_el)
    nic_el_text = doc.createTextNode(nic)
    nic_el.appendChild(nic_el_text)

    ctx_el = doc.createElement("ctx")
    workspace_el.appendChild(ctx_el)

    provides_el = doc.createElement("provides")
    ctx_el.appendChild(provides_el)

    provides_identity_el = doc.createElement("identity")
    provides_el.appendChild(provides_identity_el)

    requires_el = doc.createElement("requires")
    ctx_el.appendChild(requires_el)

    requires_identity_el = doc.createElement("identity")
    requires_el.appendChild(requires_identity_el)

    if chef_json:
        chef_config_string = json.dumps(chef_json)
        data_el = doc.createElement("data")
        data_el.setAttribute("name", "dt-chef-solo")
        requires_el.appendChild(data_el)
        cdata = doc.createCDATASection(chef_config_string)
        data_el.appendChild(cdata)

    return doc.toxml()
