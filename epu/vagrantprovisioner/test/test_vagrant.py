
import epu.vagrantprovisioner.vagrant as vagrant
import tempfile
import nose.tools

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

        cookbooks_path = "/opt/venv/dt-data/cookbooks"
        chef_json = """
        {
        "username":"controllers",
        "groupname":"users",
        "recipes":["user"]
        }
        """
        (_, chef_json_file) = tempfile.mkstemp()
        with open(chef_json_file, "w") as chef_json_fh:
            chef_json_fh.write(chef_json)
        
        
        config = """
        Vagrant::Config.run do |config|
          config.vm.box = "epu"
          config.vm.box_url = "~/epu.box"
        end
        """
        vgrnt = vagrant.Vagrant(config=config, cookbooks_path=cookbooks_path, chef_json=chef_json_file)

        status = vgrnt.status()
        assert status == "not created"
        vgrnt.up()
        status = vgrnt.status()
        assert status == "running"

        #(stdout, stderr, retcode) = vgrnt.ssh("ls -d ~epu")

        #print stdout
        #vgrnt.destroy()
        status = vgrnt.status()
        assert status == "not created"
        assert False

def test_vagrant_manager():
    manager = vagrant.VagrantManager()

    vm = manager.new_vm()
    print "status: %s" % vm.status()
    assert vm.status() == "not created"
    vm.up()
    assert vm.status() == "running"
    vm.destroy()
    assert vm.status() == "not created"

