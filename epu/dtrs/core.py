import logging
import string

import simplejson as json
from xml.dom.minidom import Document

from epu.exceptions import DeployableTypeLookupError, DeployableTypeValidationError, NotFoundError
from epu.dtrs.store import sanitize_record

log = logging.getLogger(__name__)


class DTRSCore(object):
    """Core of the Deployable Type Registry Service"""

    def __init__(self, store):
        """Create DTRSCore
        """
        self.store = store

    def add_credentials(self, caller, site_name, site_credentials):
        site = self.store.describe_site(site_name)
        if not site:
            raise NotFoundError

        return self.store.add_credentials(caller, site_name, site_credentials)

    def describe_credentials(self, caller, site_name):
        ret = self.store.describe_credentials(caller, site_name)
        return sanitize_record(ret)

    def describe_dt(self, caller, dt_name):
        ret = self.store.describe_dt(caller, dt_name)
        return sanitize_record(ret)

    def describe_site(self, site_name):
        ret = self.store.describe_site(site_name)
        return sanitize_record(ret)

    def lookup(self, caller, dt_name, dtrs_request_node, vars):
        # TODO Implement contextualization with deep merging of variables

        dt = self.store.describe_dt(caller, dt_name)
        if not dt:
            raise DeployableTypeLookupError("Unknown deployable type name: %s" % dt_name)

        # dtrs_request_node contains:
        # - instance count (assuming one for now)
        # - site
        # - allocation (ignored for now)
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

        site_credentials = self.store.describe_credentials(caller, site)
        if not site_credentials:
            raise DeployableTypeLookupError('Credentials missing for caller %s and site %s', caller, site)

        try:
            iaas_sshkeyname = site_credentials['key_name']
        except KeyError:
            raise DeployableTypeLookupError('key_name missing from credentials of caller %s and site %s', caller, site)

        contextualization = dt.get('contextualization')
        if contextualization:
            ctx_method = contextualization.get('method')
            if ctx_method == 'chef-solo':
                try:
                    chef_json = contextualization['chef_config']
                except KeyError:
                    raise DeployableTypeValidationError(dt_name, 'Missing chef_config in DT definition')
                document = generate_cluster_document(iaas_image, chef_json=chef_json)
            else:
                raise DeployableTypeValidationError(dt_name, 'Unknown contextualization method %s' % ctx_method)
        else:
            document = generate_cluster_document(iaas_image)

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

        response_node = {
                'iaas_image' : iaas_image,
                'iaas_allocation' : iaas_allocation,
                'iaas_sshkeyname' : iaas_sshkeyname,
        }
        result = {'document': document, 'node': response_node}
        return result


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
        chef_config_string= json.dumps(chef_json)
        data_el = doc.createElement("data")
        data_el.setAttribute("name", "dt-chef-solo")
        requires_el.appendChild(data_el)
        cdata = doc.createCDATASection(chef_config_string)
        data_el.appendChild(cdata)

    return doc.toxml()
