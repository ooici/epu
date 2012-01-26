import string
import os
import simplejson as json

from epu.dt_registry import DeployableTypeRegistry, DeployableTypeValidationError, process_vars

class LocalDTRS(object):
    """
    A simple DTRS implementation that works from a local directory
    and communicates without messaging.
    """

    def __init__(self, dt=None, registry=None, **kwargs):

        if registry is not None:
            # for tests
            self.registry = registry
        elif dt:
            if dt[0] != '/':
              dt = os.path.join(os.getcwd(), dt)

            self.registry = DeployableTypeRegistry(dt)
            self.registry.load()
        else:
            raise Exception("must specify either dt or registry")

    def lookup(self, dtid, node, vars=None):

        dt = self.registry.get(dtid)

        if not dt:
            raise DeployableTypeLookupError("Unknown deployable type name: %s" % dtid)

        doc_tpl = dt['document']
        defaults = dt.get('vars')
        all_vars = {}
        if defaults:
            all_vars.update(defaults)
        if vars:
            process_vars(vars, dtid)
            all_vars.update(vars)

        template = string.Template(doc_tpl)
        try:
            document = template.substitute(all_vars)
        except KeyError,e:
            raise DeployableTypeValidationError(dtid,
                    'DT doc has variable not present in request or defaults: %s'
                    % str(e))
        except ValueError,e:
            raise DeployableTypeValidationError(dtid,
                    'Deployable type document has bad variable: %s'
                    % str(e))

        sites = dt['sites']

        try:
            node_site = node['site']
        except KeyError:
            raise DeployableTypeLookupError('Node request missing site: "%s"' % node_name)

        try:
            site_node = sites[node_site]
        except KeyError:
            raise DeployableTypeLookupError(
                'Invalid deployable type site specified: "%s"' % node_site)

        response_node = {
                'iaas_image' : site_node.get('image'),
                'iaas_allocation' : site_node.get('allocation'),
                'iaas_sshkeyname' : site_node.get('sshkeyname'),
                }
        result = {'document' : document, 'node' : response_node}
        return result

class LocalVagrantDTRS(object):

    def __init__(self, dt=None, cookbooks="/opt/venv/dt-data/cookbooks"):
        
        if not dt:
            raise LocalDTRSException("You must supply a json_dt_directory to DirectoryDTRS")

        self.json_dt_directory = os.path.expanduser(dt)
        self.cookbook_dir = os.path.expanduser(cookbooks)

        self._additional_lookups = {}


    def lookup(self, dt, *args, **kwargs):

        try:
            return {'chef_json': self._additional_lookups[dt],
                    'cookbook_dir': self.cookbook_dir}
        except Exception, e:
            # Not in additional lookups, try file mapping
            pass


        chef_json = os.path.join(self.json_dt_directory, "%s.json" % dt)
        # Confirm we can read from the file
        try:
            with open(chef_json) as dt_file:
                dt_file.read()
        except Exception, e:
            raise DeployableTypeLookupError("Couldn't lookup dt. Got error %s" % str(e))

        return {'chef_json': chef_json, 'cookbook_dir': self.cookbook_dir}


    def _add_lookup(self, lookup_key, lookup_value):
        """mostly for debugging, _add_lookup allows you to add a mapping
           is not in the json_dt_directory. 

        """
        self._additional_lookups[lookup_key] = lookup_value

class DeployableTypeLookupError(Exception):
    pass

class LocalDTRSException(Exception):
    pass
