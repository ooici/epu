
import os
from twisted.trial import unittest

from epu import cassandra

FIXTURES_ROOT = 'fixtures'

class FileFixtures(object):
    def __init__(self, subdir=None):
        test_root = os.path.abspath(os.path.dirname(__file__))
        self.root = os.path.join(test_root, FIXTURES_ROOT)
        if subdir:
            self.root = os.path.join(self.root, subdir)
        assert os.path.exists(self.root), "No test fixtures?: " + self.root

    def path(self, name):
        return os.path.join(self.root, name)


class Mock(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __repr__(self):
        return self.__str__()
    def __str__(self):
        return "Mock(" + ",".join("%s=%s" %(k,v) for k,v in self.__dict__.iteritems()) + ")"


def cassandra_test(func):
    """Decorator that skips cassandra integration tests when config is not present
    """
    skip = None
    try:
        if not cassandra.has_tests_enabled():
            skip = unittest.SkipTest(
                "Cassandra integration tests are disabled. To enable, add "+
                "'run_tests:True' and a cassandra config to the "+
                "'%s' config section." % cassandra.CONF_NAME)
            
    except cassandra.CassandraConfigurationError, e:
        skip = unittest.SkipTest("Cassandra configuration problem: %s" % e)

    if skip:
        def f(*args, **kwargs):
            raise skip
        return f
    
    return func
