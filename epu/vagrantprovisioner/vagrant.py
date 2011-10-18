
import os
import re
import uuid
import tempfile
import subprocess

from subprocess import PIPE, Popen

DEFAULT_CONFIG = """
Vagrant::Config.run do |config|
  config.vm.box = "base"
  config.vm.customize do |vm|
    vm.memory_size = 128
  end
end
"""

class Vagrant(object):

    def __init__(self, vagrant_bin="vagrant", config=DEFAULT_CONFIG, 
                 vagrant_directory=None, ip=None, cookbooks_path=None, chef_json=None):
        """create a vagrant object has a vagrantfile associated with it.

        config is just a string with a vagrant config file in it
        if vagrant_directory is defined, reuse an existing configuration
        """

        self.vagrant_bin = vagrant_bin
        self.ip = ip
        self.cookbooks_path = cookbooks_path
        self.chef_json = chef_json

        if self.ip and "config.vm.network" not in config:
            config_option = 'config.vm.network("%s")' % self.ip
            config = _append_to_vagrant_config(config_option, config)
        elif not self.ip and "config.vm.network" in config:
            self.ip = _extract_ip_from_config(config)

        if cookbooks_path and chef_json and "cookbooks_path" not in config:
            config_option = """
            config.vm.provision :chef_solo do |chef|
                chef_json = JSON.parse(File.read("%s"))
                chef.run_list = chef_json.delete("recipes")
                chef.cookbooks_path = "%s"
                chef.json = chef_json
            end""" % (chef_json, cookbooks_path)
            config = _append_to_vagrant_config(config_option, config)

        self.validate()

        if vagrant_directory:
            self.directory = vagrant_directory
        else:
            self.directory = tempfile.mkdtemp()

        self.vagrantfile = config

        vagrantfile_path = _get_vagrantfile(self.directory)
        with open(vagrantfile_path, "w") as vagrantfile_handle:
            vagrantfile_handle.write(self.vagrantfile)

        print "path: %s" % vagrantfile_path

    def validate(self):
        """confirm that vagrant is installed and we can execute it"""

        try:
            process = Popen([self.vagrant_bin, "help"],stdout=PIPE, stderr=PIPE)
        except Exception, e:
            raise VagrantException("Couldn't validate vagrant. Got error: %s" % str(e))

        stderr = process.communicate()[1]
        retcode = process.returncode
        if retcode != 0:
            raise VagrantException("Couldn't validate vagrant. Got error: %s" % stderr)
        

    def up(self):
        """Bring vagrant VM to a running state"""
        
        try:
            process = Popen([self.vagrant_bin, "up"], cwd=self.directory,
                            stderr=PIPE, stdout=PIPE)
        except Exception, e:
            raise VagrantException("Couldn't start vagrant vm. Got error: %s" % str(e))
        (stdout, stderr) = process.communicate()
        retcode = process.returncode
        if retcode != 0:
            raise VagrantException("Couldn't start vagrant vm. Got error: %s" %
                                   stdout+stderr)



    def ssh(self, command):
        """runs a single command via ssh, and returns stdout, stderr, retcode  as a tuple

        command is a string with the command string to run
        """

        if self.status() != VagrantState.RUNNING:
            raise VagrantException("vagrant must be in the running state for ssh")

        try:
            process = Popen([self.vagrant_bin, "ssh", "-c", command],
                            cwd=self.directory, stderr=PIPE, stdout=PIPE)
        except Exception, e:
            raise VagrantException("Couldn't run vagrant ssh. Got error: %s" % str(e))

        (stdout, stderr) = process.communicate()
        retcode = process.returncode

        return stdout, stderr, retcode

    
    def status(self):
        """returns the status of a Vagrant VM as a string
        """

        try:
            process = Popen([self.vagrant_bin, "status"], cwd=self.directory,
                                            stderr=PIPE, stdout=PIPE)
            (stdout, stderr) = process.communicate()
        except Exception, e:
            raise VagrantException("Couldn't get vagrant status. Got error: %s" % str(e))

        status = ""
        for line in stdout.splitlines():
            if "default" in line:
                try:
                    status = " ".join(line.split()[1:])
                except:
                    pass

        return status

    def destroy(self):

        stderr = ""
        try:
            process = Popen([self.vagrant_bin, "destroy"],
                                            cwd=self.directory,
                                            stderr=PIPE, stdout=PIPE)
        except Exception, e:
            raise VagrantException("Couldn't destroy vagrant vm. Got error: %s" % e)
        stderr = process.communicate()[1]
        retcode = process.returncode
        if retcode != 0:
            raise VagrantException("Couldn't destroy vagrant vm. Got error: %s" % stderr)

class FakeVagrant(object):

    def __init__(self, vagrant_bin="vagrant", config=DEFAULT_CONFIG, vagrant_directory=None, ip=None, fail=False):
        if vagrant_directory:
            self.directory = vagrant_directory
        else:
            self.directory = tempfile.mkdtemp()

        self.ip = ip
        self.fail = fail

        got_status = self.status()
        if not got_status:
            self._set_status(VagrantState.NOT_CREATED)

    def up(self):
        if self.fail:
            self._set_status(VagrantState.STUCK)
        else:
            self._set_status(VagrantState.RUNNING)

    def destroy(self):
        self._set_status(VagrantState.NOT_CREATED)

    def status(self):

        try:
            with open(os.path.join(self.directory, "status")) as status_file:
                return status_file.read()
        except:
            return None
        

    def _set_status(self, newstate):

        with open(os.path.join(self.directory, "status"), "w") as status_file:
            status_file.write(newstate)

 
class VagrantManager(object):
    """manages a list of Vagrant VMs. Mostly used to ease coordination of multiple Vagrant VMs
    """

    NETWORK_PREFIX = "33.33.33"

    def __init__(self, vagrant=Vagrant):
        self.vms = []
        self.terminated_vms = []
        self.vagrant = vagrant # provide opportunity to pass in FakeVagrant
        self.ips = []


    def new_vm(self, vagrant_bin="vagrant", config=DEFAULT_CONFIG, vagrant_directory=None):


        ip = self._get_ip()
        vm = self.vagrant(vagrant_bin=vagrant_bin, config=config, vagrant_directory=vagrant_directory, ip=ip)
        self.vms.append(vm.directory)
        return vm

    def remove_vm(self, vagrant_directory=None):
        """ remove reference to VM, and destroy it if not yet destroyed
        """
        if not vagrant_directory:
            raise VagrantException("You must specify a directory to remove a vagrant vm")

        try:
            with open(_get_vagrantfile(vagrant_directory)) as config:
                ip = _extract_ip_from_config(config)
        except:
            ip = None

        vm = self.vagrant(vagrant_directory=vagrant_directory, ip=ip)
        if vm.status() != VagrantState.NOT_CREATED:
            vm.destroy()

        if ip in self.ips:
            self.ips.remove(ip)

        if vagrant_directory in self.vms:
            self.vms.remove(vagrant_directory)
        self.terminated_vms.append(vagrant_directory)

    def get_vm(self, vagrant_directory=None):
        if not vagrant_directory:
            raise VagrantException("You must specify a directory to remove a vagrant vm")

        vm = self.vagrant(vagrant_directory=vagrant_directory)
        return vm


    def _get_ip(self):

        for host_number in range(0, 255):
            if host_number not in self.ips:
                self.ips.append(host_number)
                return "%s.%s" % (self.NETWORK_PREFIX, host_number)

        raise VagrantException("No more IPs available for Vagrant VMs")

        



class VagrantState(object):

    ABORTED = "aborted"
    INACCESSIBLE = "inaccessible"
    NOT_CREATED = "not created"
    POWERED_OFF = "powered off"
    RUNNING = "running"
    SAVED = "saved"
    STUCK = "stuck"
    LISTING = "listing"

class VagrantException(Exception):
    pass


def _append_to_vagrant_config(config_option, config):

    cropped_config = config[:config.rindex("end")]
    appended_config = "%s\n%s\nend" % (cropped_config, config_option)

    return appended_config

def _extract_ip_from_config(config):

    match = re.search('config.vm.network\((\d*\.\d*\.\d*\.\d*)\)', config)

    if not match:
        return None
    else:
        return match.group(0)

def _get_vagrantfile(vagrant_directory):
    return os.path.join(vagrant_directory, "Vagrantfile")
