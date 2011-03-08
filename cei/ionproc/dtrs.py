#!/usr/bin/env python

"""
@file cei/ion/dtrs.py
@author Alex Clemesha
@author David LaBissoniere
@brief Deployable Type Registry Service. Used to look up Deployable type data/metadata.
"""
import string

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from twisted.internet import defer

from ion.core.process.process import ProcessFactory
from ion.core.exception import ReceivedError
from ion.core.process.service_process import ServiceProcess, ServiceClient

from cei.dt_registry import DeployableTypeRegistry

__all__ = ['DeployableTypeRegistryService', 'DeployableTypeRegistryClient']

class DeployableTypeRegistryService(ServiceProcess):
    """Deployable Type Registry service interface
    """
    declare = ServiceProcess.service_declare(name='dtrs', version='0.1.0', dependencies=[])

    def slc_init(self):
        registry = self.spawn_args.get('registry')
        registry_dir = self.spawn_args.get('registry_dir')

        if registry is None and registry_dir is None:
            raise ValueError("DTRS needs either 'registry' or 'registry_dir' in spawnargs")

        if registry is not None:
            self.registry = registry

        else:
            self.registry = DeployableTypeRegistry(registry_dir)
            self.registry.load()

    def op_lookup(self, content, headers, msg):
        """Resolve a deployable type
        """

        log.debug('Recieved DTRS lookup. content: ' + str(content))
        # just using a file for this right now, to keep it simple
        dt_id = content['deployable_type']
        nodes = content.get('nodes')
        vars = content.get('vars')

        dt = self.registry.get(dt_id)
        if not dt:
            return self._dtrs_error(msg, 'Unknown deployable type name: '+ dt_id)

        doc_tpl = dt['document']
        defaults = dt.get('vars')
        all_vars = {}
        if defaults:
            all_vars.update(defaults)
        if vars:
            all_vars.update(vars)

        template = string.Template(doc_tpl)
        try:
            document = template.substitute(all_vars)
        except KeyError,e:
            return self._dtrs_error(msg,
                    'DT doc has variable not present in request or defaults: %s'
                    % str(e))
        except ValueError,e:
            return self._dtrs_error(msg, 'Deployable type document has bad variable: %s'
                    % str(e))

        response_nodes = {}
        result = {'document' : document, 'nodes' : response_nodes}
        sites = dt['sites']

        for node_name, node in nodes.iteritems():

            try:
                node_site = node['site']
            except KeyError:
                return self._dtrs_error(msg,'Node request missing site: "%s"' % node_name)

            try:
                site_node = sites[node_site][node_name]
            except KeyError:
                return self._dtrs_error(msg,
                    'Invalid deployable type site specified: "%s":"%s" ' % (node_site, node_name))

            response_nodes[node_name] = {
                    'iaas_image' : site_node.get('image'),
                    'iaas_allocation' : site_node.get('allocation'),
                    'iaas_sshkeyname' : site_node.get('sshkeyname'),
                    }

        log.debug('Sending DTRS response: ' + str(result))

        return self.reply_ok(msg, result)

    def _dtrs_error(self, msg, error):
        log.debug('Sending DTRS error reply: ' + error)
        return self.reply_err(msg, error)

class DeployableTypeRegistryClient(ServiceClient):
    """Client for accessing DTRS
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "dtrs"
        ServiceClient.__init__(self, proc, **kwargs)

    @defer.inlineCallbacks
    def lookup(self, dt, nodes=None, vars=None):
        """Lookup a deployable type
        """
        yield self._check_init()
        log.debug("Sending DTRS lookup request")
        try:
            (content, headers, msg) = yield self.rpc_send('lookup', {
                'deployable_type' : dt,
                'nodes' : nodes,
                'vars' : vars
            })
        except ReceivedError, re:
            raise DeployableTypeLookupError(re.msg_content)

        defer.returnValue({
            'document' : content.get('document'),
            'nodes' : content.get('nodes')
            })

class DeployableTypeLookupError(Exception):
    """Error resolving or interpolating deployable type
    """
    pass

# Direct start of the service as a process with its default name
factory = ProcessFactory(DeployableTypeRegistryService)
