#!/usr/bin/env python

"""
@file ion/services/sa/instrument_registry.py
@author Michael Meisinger
@brief service for registering instruments and platforms
"""

import logging
logging = logging.getLogger(__name__)
from twisted.internet import defer
from magnet.spawnable import Receiver

import ion.util.procutils as pu
from ion.core.base_process import ProtocolFactory
from ion.services.base_service import BaseService, BaseServiceClient
from ion.data.datastore import registry


from ion.resources import sa_resource_descriptions

'''
class InstrumentRegistryService(BaseService):
    """Data acquisition service interface
    """

    # Declaration of service
    declare = BaseService.service_declare(name='instrument_registry', version='0.1.0', dependencies=[])
 
    def op_define_instrument(self, content, headers, msg):
        """Service operation: Create or update an instrument registration
        """

    def op_define_agent(self, content, headers, msg):
        """Service operation: Create or update instrument or platform agent
        and register with an instrument or platform.
        """

    def op_register_agent_instance(self, content, headers, msg):
        """Service operation: .
        """

    def op_define_platform(self, content, headers, msg):
        """Service operation: Create or update a platform registration
        """

# Spawn of the process using the module name
factory = ProtocolFactory(InstrumentRegistryService)
'''
class InstrumentRegistryService(registry.BaseRegistryService):
    """
    Resource registry service interface
    The Resource Registry Service uses an IStore interface to a backend Key
    Value Store to store to track version controlled objects. The store will
    share a name space and share objects depending on configuration when it is
    created. The resource are retrieved as complete objects from the store. The
    built-in encode method is used to store and transmit them using the COI
    messaging.
    """

    # Declaration of service
    declare = BaseService.service_declare(name='instrument_registry', version='0.1.0', dependencies=[])

    
    op_clear_instrument = registry.BaseRegistryService.base_clear_registry #changed
    
    op_register_instrument_instance = registry.BaseRegistryService.base_register_resource #changed
    """
    Service operation: Register a resource instance with the registry.
    """
    op_register_instrument_type = registry.BaseRegistryService.base_register_resource #changed
    """
    Service operation: Create or update a resource type with the registry.
    """
    op_get_instrument_instance = registry.BaseRegistryService.base_get_resource #changed
    """
    Service operation: Get a resource instance.
    """

    op_get_instrument_by_id = registry.BaseRegistryService.base_get_resource_by_id

    op_get_instrument_type = registry.BaseRegistryService.base_get_resource #changed
    """
    Service operation: Get a resource type.
    """
    #op_set_instrument_lcstate = registry.BaseRegistryService.base_set_resource_lcstate #changed
    #"""
    #Service operation: Set a resource life cycle state
    #"""
    op_find_instrument_type = registry.BaseRegistryService.base_find_resource #changed
    """
    Service operation: find and instrument type
    """
    op_find_instrument_instance = registry.BaseRegistryService.base_find_resource #changed
    """
    Service operation: find an instrument instance
    """

class InstrumentRegistryClient(registry.BaseRegistryClient, registry.LCStateMixin):
    """
    Class for the client accessing the resource registry.
    """
    def __init__(self, proc=None, **kwargs):
        if not 'targetname' in kwargs:
            kwargs['targetname'] = "instrument_registry"
        BaseServiceClient.__init__(self, proc, **kwargs)

    
    def clear_registry(self):
        return self.base_clear_registry('clear_instrument')

    def register_instrument_instance(self,instrument_instance):
        """
        Client method to Register a Resource instance
        """
        return self.base_register_resource('register_instrument_instance', instrument_instance)
    
    def register_instrument_type(self,instrument_type):
        """
        Client method to register the Definition of a Resource Type
        """
        return self.base_register_resource('register_instrument_type',instrument_type)

    def get_instrument_type(self,instrument_type_reference):
        """
        Get a instrument type
        """
        return self.base_get_resource('get_instrument_type',instrument_type_reference)

    def get_instrument_instance(self,instrument_instance_reference):
        """
        Get a resource instance
        """
        return self.base_get_resource('get_instrument_instance',resource_reference)

    def get_instrument_by_id(self, id):
        return self.base_get_resource_by_id('get_instrument_by_id', id)
        
    #def set_resource_lcstate(self, resource_reference, lcstate):
    #    return self.base_set_resource_lcstate(resource_reference, lcstate, 'set_instrument_lcstate')

    def find_instrument_type(self, description,regex=True,ignore_defaults=True):
        return self.base_find_resource('find_instrument_type',description,regex,ignore_defaults)
        
    def find_instrument_instance(self, description,regex=True,ignore_defaults=True):
        return self.base_find_resource('find_instrument_instance',description,regex,ignore_defaults)


# Spawn of the process using the module name
factory = ProtocolFactory(InstrumentRegistryService)


"""
from ion.services.coi.resource_registry import *
rd1 = ResourceDesc(name='res1',res_type=ResourceTypes.RESTYPE_GENERIC)
c = ResourceRegistryClient()
c.registerResource(rd1)
"""

        

