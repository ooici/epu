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

class DeployableTypeLookupError(BaseException):
    pass

