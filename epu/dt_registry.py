#!/usr/bin/env python

import os
import copy
import logging

import simplejson as json
import yaml
from xml.dom.minidom import Document

log = logging.getLogger(__name__)


_DT_DEF_EXTENSIONS = (".json", ".yml")
_DT_DOC_EXTENSION = ".xml"

class DeployableTypeRegistry(object):
    """File-backed registry of deployable types
    """
    def __init__(self, directory, ignore_failures=False):
        self.directory = directory

        self.dt = None
        self.documents = None

        self.ignore_failures = ignore_failures

    def get(self, key):
        """Retrieve a deployable type by name
        """
        dt = self.dt.get(key)
        return copy.deepcopy(dt)

    def load(self):
        self.documents = {}
        self.dt = {}

        if self.ignore_failures:
            self.failures = {}

        self._load_directory()

    def _load_directory(self):
        for f in os.listdir(self.directory):
            name, ext = os.path.splitext(f)
            path = os.path.join(self.directory, f)
            if name and ext in _DT_DEF_EXTENSIONS:
                try:
                    dt = self._load_one_dt(name, path)
                    self.dt[name] = dt

                except DeployableTypeValidationError, e:
                    log.warn(e)
                    if self.ignore_failures:
                        self.failures[name] = e
                    else:
                        raise
        if self.ignore_failures and self.failures:
            log.info('Loaded %d deployable types (%d failed to load)',
                     len(self.dt), len(self.failures))
        else:
            log.debug('Loaded %d deployable types', len(self.dt))

    def _load_one_dt(self, name, path):
        f = None

        if path.endswith(".yml"):
            load = yaml.load
        elif path.endswith(".json"):
            load = json.load
        else:
            raise DeployableTypeValidationError(name,
                    "Don't know how to load dt file '%s'" % path)

        try:
            f = open(path)
            dt = load(f)
        except (IOError, json.JSONDecodeError, yaml.YAMLError), e:
            log.debug("Error loading deployable type: '%s'", name, exc_info=True)
            raise DeployableTypeValidationError(name,
                    "Failed to load dt file '%s': %s" % (path, str(e)))
        finally:
            if f:
                f.close()

        document = dt.get('document')
        chef_config = dt.get('chef_config')
        image = dt.get('image')

        # The context document can be provided in 3 ways:

        # 1. as a path in specification
        if document:
            if os.path.isabs(document):
                raise DeployableTypeValidationError(
                        name,
                        "absolute path for document '%s'" % document)
            if os.path.basename(document) != document:
                raise DeployableTypeValidationError(
                    name, "document path may not have a directory component")
            dt_doc = self._get_document(document, name)

        # 2. Generated from the specification
        elif chef_config is not None and image:
            chef_config_json = json.dumps(chef_config)
            dt_doc = generate_cluster_document(chef_config_json, image)

        # 3. implicitly as a file with the same name as the spec but xml extension
        else:
            document_path = name + _DT_DOC_EXTENSION
            dt_doc = self._get_document(document_path, name)

        sites = dt.get('sites')
        if not sites:
            raise DeployableTypeValidationError(name, 'DT has no sites')
        _validate_sites(sites, name)

        vars = dt.get('vars')
        process_vars(vars, name)

        return {'name' : name,
                'document' : dt_doc,
                'sites' : sites,
                'vars' : vars}

    def _get_document(self, path, dt_name):
        doc = self.documents.get(path)

        if doc:
            return doc

        f = None
        try:
            real_path = os.path.join(self.directory, path)
            f = open(real_path)
            doc = f.read()
        except IOError, e:
            log.debug("Error loading document '%s' for dt '%s'",
                          path, dt_name, exc_info=True)
            raise DeployableTypeValidationError(dt_name,
                    "Failed to load document '%s': %s" %
                    (real_path, str(e)))
        finally:
            if f:
                f.close()

        if not doc:
            raise DeployableTypeValidationError(dt_name,
                    "document '%s' is empty" % path)

        self.documents[path] = doc
        return doc

def _validate_sites(sites, dt_name):
    if not (sites and isinstance(sites, dict)):
        raise DeployableTypeValidationError(dt_name, 'sites empty or invalid')

    nodenames = None
    for site, nodes in sites.iteritems():
        if not (nodes and isinstance(nodes, dict)):
            raise DeployableTypeValidationError(
                    dt_name,
                    'nodes empty or invalid for site ' + str(site))

        if not nodenames:
            nodenames = set(nodes.iterkeys())
        else:
            thisset = set(nodes.iterkeys())
            diff = nodenames.symmetric_difference(thisset)
            if diff:
                raise DeployableTypeValidationError(
                        dt_name,
                        'node set must be consistent across all sites in DT')


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


def generate_cluster_document(chef_json, image, name="work_consumer",
                              quantity=1, nic="public", wantlogin="true"):

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
    nic_el.setAttribute("wantlogin", "true")
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

    data_el = doc.createElement("data")
    data_el.setAttribute("name", "dt-chef-solo")
    requires_el.appendChild(data_el)

    cdata = doc.createCDATASection(chef_json)
    data_el.appendChild(cdata)

    return doc.toxml()

class DeployableTypeValidationError(Exception):
    """Problem validating a deployable type
    """
    def __init__(self, dt_name, *args, **kwargs):
        self.dt_name = dt_name
        Exception.__init__(self, *args, **kwargs)

    def __str__(self):
        return "Deployable Type '%s': %s" % (self.dt_name,
                                             Exception.__str__(self))

if __name__ == '__main__':
    import sys
    def die_usage():
        print >>sys.stderr, "Usage: %s dt_dir" % sys.argv[0] if sys.argv else 'exe'
        sys.exit(1)

    if len(sys.argv) != 2 or sys.argv[1] in ('-h', '--help'):
        die_usage()

    dt_dir = sys.argv[1]

    registry = DeployableTypeRegistry(dt_dir, ignore_failures=True)
    registry.load()

    if registry.failures:
        print "\nBad deployable type definitions!\n"
        print "\n".join(str(e) for e in registry.failures.itervalues())
    else:
        print "\nOK"

    sys.exit(len(registry.failures))

