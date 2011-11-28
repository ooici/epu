"""
Utiliity functions that are common to all dashiproc services.

TODO: Some of these might need to be moved to dashi.bootstrap
"""

import os

from epu.util import determine_path

def get_config_files(config_name):
    """return a list of embedded config files, based on the name passed
    in. For example, if you pass in "provisioner", this function will return:

    ["/path/to/epu/config/provisioner.yml", "/path/to/epu/config/provisioner.local.yml"]
    """

    config_files = []
    config_files.append(os.path.join(determine_path(),
                                     "config", "%s.yml" % config_name))
    config_files.append(os.path.join(determine_path(),
                                     "config", "%s.local.yml" % config_name))
    return config_files
