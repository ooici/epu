
import os
import tempfile
import nose.tools

import epu.vagrantprovisioner.vagrant as vagrant

class TestVagrant(object):

    def test_init(self):
        
        exception_raised = False
        try:
            vgrnt = vagrant.Vagrant(vagrant_bin="badbadvagrant")
        except vagrant.VagrantException:
            exception_raised = True
        assert exception_raised

        exception_raised = False
        try:
            vgrnt = vagrant.Vagrant()
        except vagrant.VagrantException:
            exception_raised = True
            raise
        assert not exception_raised


    def test_up(self):

        config = """
        Vagrant::Config.run do |config|
          config.vm.box = "base"
          config.vm.box_url = "http://files.vagrantup.com/lucid32.box"
        end
        """
        vgrnt = vagrant.Vagrant(config=config)

        status = vgrnt.status()
        assert status == "not created"
        vgrnt.up()
        status = vgrnt.status()
        assert status == "running"
        vgrnt.destroy()
        status = vgrnt.status()
        assert status == "not created"

    def test_chef(self):

        test_dir = os.path.dirname(os.path.realpath(__file__))
        cookbooks_path = os.path.join(test_dir, "dt-data", "cookbooks")
        user = "foo"
        chef_json = """
        {
        "username":"%s",
        "groupname":"users",
        "recipes":["user"]
        }
        """ % user
        (_, chef_json_file) = tempfile.mkstemp()
        with open(chef_json_file, "w") as chef_json_fh:
            chef_json_fh.write(chef_json)
        
        
        config = """
        Vagrant::Config.run do |config|
          config.vm.box = "epu"
          config.vm.box_url = "https://particle.phys.uvic.ca/~patricka/epu.box"
        end
        """
        vgrnt = vagrant.Vagrant(config=config, cookbooks_path=cookbooks_path, chef_json=chef_json_file)

        status = vgrnt.status()
        assert status == "not created"
        vgrnt.up()
        status = vgrnt.status()
        assert status == "running"

        (stdout, stderr, retcode) = vgrnt.ssh("ls -d ~%s" % user)

        print "'%s'" % stdout
        assert stdout == "/home/%s\n" % user

        print stdout
        vgrnt.destroy()
        status = vgrnt.status()
        assert status == "not created"

def test_vagrant_manager():
    manager = vagrant.VagrantManager()

    vm = manager.new_vm()
    print "status: %s" % vm.status()
    assert vm.status() == "not created"
    vm.up()
    assert vm.status() == "running"
    vm.destroy()
    assert vm.status() == "not created"

