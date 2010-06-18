"""
@file ion/data/datastore/registry.py
"""

from zope import interface

from twisted.internet import defer

from ion.data import dataobject
from ion.data.datastore import objstore



LCStateNames = ['new',
                'active',
                'inactive',
                'decomm',
                'retired',
                'developed',
                'commissioned',
                ]

class LCState(object):

    def __init__(self, state):
        assert state in LCStateNames
        self._state = state

    def __repr__(self):
        return self._state

    def __eq__(self, other):
        assert isinstance(other, LCState)
        return str(self) == str(other)

LCStates = dict([('LCState', LCState)] + [(name, LCState(name)) for name in LCStateNames])


class ResourceDescription(dataobject.DataObject):
    """
    @brief Base for all OOI resource objects
    @note OOIResource or OOIRegistryObject or OOIObject???
    @note could build in explicit link back to ResourceRegistryClient so
    user can make changes through this object.
    """
    _types = LCStates

    name = dataobject.TypedAttribute(str)
    lifecycle = dataobject.TypedAttribute(LCState, default=LCStates['new'])

class Generic(ResourceDescription):
    """
    """


class IResourceRegistry(interface.Interface):
    """
    @brief General API of any registry
    """

    def register(uuid, resource):
        """
        @brief Register resource description.
        @param uuid unique name of resource instance.
        @param resource instance of OOIResource.
        @note Does the resource instance define its own name/uuid?
        """

    def get_description(uuid):
        """
        @param uuid name of resource.
        """

    def set_resource_lcstate(uuid, state):
        """
        """

class ResourceRegistryClient(objstore.ObjectChassis):
    """
    """
    objectClass = ResourceDescription

class ResourceRegistry(objstore.ObjectStore):
    """
    """

    objectChassis = ResourceRegistryClient

    @defer.inlineCallbacks
    def register(self, uuid, resource):
        """
        @brief Add a new resource description to the registry. Implemented
        by creating a new (unique) resource object to the store.
        @note Is the way objectClass is referenced awkward?
        """
        assert isinstance(resource, self.objectChassis.objectClass)
        
        try:
            res_client = yield self.create(uuid, self.objectChassis.objectClass)
        except objstore.ObjectStoreError:
            res_client = yield self.clone(uuid)
            
        yield res_client.checkout()
        res_client.index = resource
        c_id = yield res_client.commit()
        defer.returnValue(c_id)


    @defer.inlineCallbacks
    def get_description(self, uuid):
        """
        @brief Get resource description object
        """
        resource_client = yield self.clone(uuid)
        if resource_client:
            resource_description = yield resource_client.checkout()
        else:
            resource_description=None
        defer.returnValue(resource_description)
    

@defer.inlineCallbacks
def test(ns):
    from ion.data import store
    s = yield store.Store.create_store()
    ns.update(locals())
    reg = ResourceRegistry(s)
    res1 = ResourceDescription()
    ns.update(locals())
    res1.name = 'foo'
    commit_id = yield reg.register('foo', res1)
    ns.update(locals())


