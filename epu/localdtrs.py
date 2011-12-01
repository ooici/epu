import os
import simplejson as json

from epu.dt_registry import DeployableTypeRegistry

class LocalDTRS(object):
    """
    A simple DTRS implementation that works from a local directory
    and communicates without messaging.
    """

    def __init__(self, dt=None, **kwargs):

        if dt[0] != '/':
          dt = os.path.join(os.getcwd(), dt)

        self.registry = DeployableTypeRegistry(dt)
        self.registry.load()

    def lookup(self, dtid, nodes, vars):

        dt = self.registry.get(dtid)

        if not dt:
            raise DeployableTypeLookupError("Unknown deployable type name: %s" % dtid)

        response_nodes = {}
        result = {'document' : dt.get('document'), 'nodes' : response_nodes}
        sites = dt['sites']

        for node_name, node in nodes.iteritems():

            try:
                node_site = node['site']
            except KeyError:
                raise DeployableTypeLookupError('Node request missing site: "%s"' % node_name)

            try:
                site_node = sites[node_site][node_name]
            except KeyError:
                raise DeployableTypeLookupError(
                    'Invalid deployable type site specified: "%s":"%s" ' % (node_site, node_name))

            response_nodes[node_name] = {
                    'iaas_image' : site_node.get('image'),
                    'iaas_allocation' : site_node.get('allocation'),
                    'iaas_sshkeyname' : site_node.get('sshkeyname'),
                    }
        return result

class LocalVagrantDTRS(object):

    def __init__(self, dt=None, cookbooks="/opt/venv/dt-data/cookbooks"):
        
        if not dt:
            raise LocalDTRSException("You must supply a json_dt_directory to DirectoryDTRS")

        self.json_dt_directory = dt
        self.cookbook_dir = cookbooks

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

class DeployableTypeLookupError(BaseException):
    pass

class LocalDTRSException(BaseException):
    pass
