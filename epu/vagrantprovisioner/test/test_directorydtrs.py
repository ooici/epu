import os
from epu.vagrantprovisioner.directorydtrs import DirectoryDTRS, DirectoryDTRSException

def test_init():
    
    exception_caught = False
    try:
        DirectoryDTRS()
    except DirectoryDTRSException:
        exception_caught = True
    assert exception_caught

def test_lookup():

    cookbook_path = "/path/to/cookbooks"
    dt = "mydt"
    dt_directory = "/path/to/dts"
    dt_path = os.path.join(dt_directory, "%s.json" % dt)

    dtrs = DirectoryDTRS(dt_directory, cookbook_path)

    result = dtrs.lookup(dt)
    print result, dt_path
    assert result['chef_json'] == dt_path


