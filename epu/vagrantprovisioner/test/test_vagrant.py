
import epu.vagrantprovisioner.vagrant as vagrant
import nose.tools

class TestVagrant(object):

    def test_init(self):
        
        exception_raised = False
        try:
            vgrnt = vagrant.Vagrant(vagrant_path="badbadvagrant")
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

        vgrnt = vagrant.Vagrant()
        config = """
        Vagrant::Config.run do |config|
          config.vm.box = "base"
          config.vm.box_url = "http://files.vagrantup.com/lucid32.box"
        end
        """

        status = vgrnt.status()
        assert status == "not created"
        vgrnt.up()
        status = vgrnt.status()
        assert status == "running"
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

