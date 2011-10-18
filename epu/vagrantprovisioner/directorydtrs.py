import os

class DirectoryDTRS(object):

    def __init__(self, json_dt_directory=None, 
                       cookbook_dir="/opt/venv/dt-data/cookbooks"):
        
        if not json_dt_directory:
            raise DirectoryDTRSException("You must supply a json_dt_directory to DirectoryDTRS")

        self.json_dt_directory = json_dt_directory
        self.cookbook_dir = cookbook_dir


    def lookup(self, dt):
        chef_json = os.path.join(self.json_dt_directory, "%s.json" % dt)
        return {'chef_json': chef_json, 'cookbook_dir': self.cookbook_dir}


class DirectoryDTRSException(BaseException):
    pass
