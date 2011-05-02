
import os

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
