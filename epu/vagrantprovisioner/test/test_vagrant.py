import os
import tempfile
from nose.plugins.skip import SkipTest

import epu
import epu.vagrantprovisioner.vagrant as vagrant


class TestVagrant(object):

    def setUp(self):
        if not os.environ.get('VAGRANTINT'):
            raise SkipTest('Slow vagrant integration test')
        from subprocess import call
        with open(os.devnull, "w") as devnull:
            try:
                call("vagrant", stdout=devnull)
            except:
                raise SkipTest("Skipping test, since vagrant not available")

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

        epu_dir = os.path.dirname(os.path.realpath(epu.__file__))
        cookbooks_path = os.path.join(epu_dir, "test", "dt-data", "cookbooks")
        user = "foo"
        chef_json = """
        {
        "username":"%s",
        "groupname":"users",
        "recipes":["user"]
        }
        """ % user
        (fh, chef_json_file) = tempfile.mkstemp()
        os.close(fh)
        with open(chef_json_file, "w") as chef_json_fh:
            chef_json_fh.write(chef_json)

        config = """
        Vagrant::Config.run do |config|
          config.vm.box = "base"
          config.vm.box_url = "http://files.vagrantup.com/lucid32.box"
        end
        """
        vgrnt = vagrant.Vagrant(config=config, cookbooks_path=cookbooks_path, chef_json=chef_json_file)

        status = vgrnt.status()
        print "not created"
        assert status == "not created"
        print "booting vagrant"
        vgrnt.up()
        status = vgrnt.status()
        print "running"
        assert status == "running"

        (stdout, stderr, retcode) = vgrnt.ssh("ls -d ~%s" % user)

        print "'%s'" % stdout
        assert stdout == "/home/%s\n" % user

        print stdout
        vgrnt.destroy()
        exception_caught = False
        try:
            status = vgrnt.status()
        except:
            exception_caught = True
        assert exception_caught

    def test_vagrant_manager(self):
        manager = vagrant.VagrantManager()
        assert manager.vms == []
        assert manager.ips == []

        vm = manager.new_vm()
        print "status: %s" % vm.status()
        assert vm.status() == "not created"
        vm.up()

        ip = vm.ip
        vm_directory = vm.directory

        vm_copy = vagrant.Vagrant(vagrant_directory=vm_directory)
        assert ip == vm_copy.ip

        vm2 = manager.new_vm()
        assert vm2.status() == "not created"
        vm2.up()

        assert ip != vm2.ip
        assert vm.status() == "running"

        manager.remove_vm(vm.directory)
        assert vm.status() == "not created"

        manager.remove_vm(vm2.directory)
        assert vm2.status() == "not created"

        print manager.ips
        assert manager.ips == []
        assert manager.vms == []
