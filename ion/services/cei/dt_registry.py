#!/usr/bin/env python

import os
import copy

import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

try:
    import json
except ImportError:
    import simplejson as json

_DT_DEF_EXTENSION = ".json"
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
            if name and ext == _DT_DEF_EXTENSION:
                try:
                    dt = self._load_one_dt(name, path)
                    self.dt[name] = dt

                except DeployableTypeValidationError, e:
                    log.warn("Failed to load deployable type '%s': %s",
                             name, str(e))
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
        try:
            f = open(path)
            dt = json.load(f)
        except (IOError, json.JSONDecodeError), e:
            log.warn("Error loading deployable type: '%s'", name, exc_info=True)
            raise DeployableTypeValidationError(name,
                    "Failed to load dt file '%s': %s" % (path, str(e)))
        finally:
            if f:
                f.close()

        document = dt.get('document')
        document_path = dt.get('document_path')

        if document and document_path:
            raise DeployableTypeValidationError(name, 'DT has both "document"' +
                                                 'and "document_path"')

        # 3 ways to specify document:

        # inline in json spec (useful for tiny docs, testing)
        if document:
            dt_doc = document

        # as a path in json_spec
        elif document_path:
            if os.path.isabs(document_path):
                raise DeployableTypeValidationError(
                        name,
                        "absolute path for document '%s'" % document_path)
            if os.path.basename(document_path) != document_path:
                raise DeployableTypeValidationError(
                    name, "document_path may not have a directory component")
            dt_doc = self._get_document(document_path, name)

        # implicitly: a file with the same name as json spec but xml extension
        else:
            document_path = name + _DT_DOC_EXTENSION
            dt_doc = self._get_document(document_path, name)

        sites = dt.get('sites')
        if not sites:
            raise DeployableTypeValidationError(name, 'DT has no sites')

        vars = dt.get('vars')

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
            log.warn("Error loading document '%s' for dt '%s'",
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


class DeployableTypeValidationError(Exception):
    """Problem validating a deployable type
    """
    def __init__(self, dt_name, *args, **kwargs):
        self.dt_name = dt_name
        Exception.__init__(self, *args, **kwargs)

    def __str__(self):
        return "Deployable Type '%s': %s" % (self.dt_name,
                                             Exception.__str__(self))

