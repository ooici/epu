import os
from twisted.internet import defer, threads

class DirectoryDTRS(object):

    def __init__(self, json_dt_directory=None, 
                       cookbook_dir="/opt/venv/dt-data/cookbooks"):
        
        if not json_dt_directory:
            raise DirectoryDTRSException("You must supply a json_dt_directory to DirectoryDTRS")

        self.json_dt_directory = json_dt_directory
        self.cookbook_dir = cookbook_dir

        self._additional_lookups = {}


    @defer.inlineCallbacks
    def lookup(self, dt, **kwargs):

        try:
            defer.returnValue({'chef_json': self._additional_lookups[dt],
                    'cookbook_dir': self.cookbook_dir})
        except Exception, e:
            # Not in additional lookups, try file mapping
            pass


        chef_json = os.path.join(self.json_dt_directory, "%s.json" % dt)
        # Confirm we can read from the file
        try:
            with open(chef_json) as dt_file:
                yield threads.deferToThread(dt_file.read)
        except Exception, e:
            raise DeployableTypeLookupError("Couldn't lookup dt. Got error %s" % str(e))

        defer.returnValue({'chef_json': chef_json, 'cookbook_dir': self.cookbook_dir})


    def _add_lookup(self, lookup_key, lookup_value):
        """mostly for debugging, _add_lookup allows you to add a mapping
           is not in the json_dt_directory. 

        """
        self._additional_lookups[lookup_key] = lookup_value

class DeployableTypeLookupError(BaseException):
    pass

class DirectoryDTRSException(BaseException):
    pass
