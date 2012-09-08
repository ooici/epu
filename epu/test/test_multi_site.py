import unittest
import tempfile
import os
import uuid
from epu.decisionengine.impls.phantom_multi_site_overflow import PhantomMultiSiteOverflowEngine

class MockDomain(object):

    def __init__(self, owner):
        self.owner = owner

class MockControl(object):

    def __init__(self):
        self._launch_calls = 0
        self._destroy_calls = 0
        self.domain = MockDomain("user")

    def launch(self, dt_name, sites_name, instance_type, caller=None):
        self._launch_calls  = self._launch_calls + 1

        launch_id = str(uuid.uuid4())
        instance_ids = [str(uuid.uuid4()),]
        return (launch_id, instance_ids)

    def destroy_instances(self, instanceids, caller=None):
        self._destroy_calls  = self._destroy_calls + 1


class MockState(object):

    def __init__(self):
        self.instances = {}


def make_conf(clouds, n, dtname, instance_type):
    conf = {}
    conf['n_preserve'] = n
    conf['clouds'] = clouds
    conf['dtname'] = dtname
    conf['instance_type'] = instance_type
    
    return conf

def make_cloud_conf(name, size, rank):
    cloud_conf = {}
    cloud_conf['site_name'] = name
    cloud_conf['size'] = size
    cloud_conf['rank'] = rank
    return cloud_conf

class TestMultiSiteDE(unittest.TestCase):

    def test_basic_start(self):
        control = MockControl()
        state = MockState()

        print "sup"
        n = 4 
        cloud = make_cloud_conf('hotel', n, 1)
        conf = make_conf([cloud,], n,  'testdt', 'm1.small')

        de = PhantomMultiSiteOverflowEngine()
        de.initialize(control, state, conf)

        de.decide(control, state)

        self.assertEqual(control._launch_calls, n)


