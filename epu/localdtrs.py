import os

from epu.exceptions import DeployableTypeLookupError


class LocalVagrantDTRS(object):

    def __init__(self, dt=None, cookbooks="/opt/venv/dt-data/cookbooks"):

        if not dt:
            raise LocalDTRSException("You must supply a json_dt_directory to DirectoryDTRS")

        self.json_dt_directory = os.path.expanduser(dt)
        self.cookbook_dir = os.path.expanduser(cookbooks)

        self._additional_lookups = {}

    def lookup(self, dt, *args, **kwargs):

        try:
            return {'chef_json': self._additional_lookups[dt],
                    'cookbook_dir': self.cookbook_dir}
        except Exception, e:
            # Not in additional lookups, try file mapping
            pass

        chef_json = os.path.join(self.json_dt_directory, "%s.json" % dt)
        # Confirm we can read from the file
        try:
            with open(chef_json) as dt_file:
                dt_file.read()
        except Exception, e:
            raise DeployableTypeLookupError("Couldn't lookup dt. Got error %s" % str(e))

        return {'chef_json': chef_json, 'cookbook_dir': self.cookbook_dir}

    def _add_lookup(self, lookup_key, lookup_value):
        """mostly for debugging, _add_lookup allows you to add a mapping
           is not in the json_dt_directory.

        """
        self._additional_lookups[lookup_key] = lookup_value


class LocalDTRSException(Exception):
    pass
