
import os
import re
import tempfile
import subprocess

from subprocess import PIPE

DEFAULT_CONFIG = """
Vagrant::Config.run do |config|
  config.vm.box = "base"
  config.vm.customize do |vm|
    vm.memory_size = 128
  end
end
"""

class Vagrant(object):

    def __init__(self, vagrant_path="vagrant", config=DEFAULT_CONFIG, vagrant_directory=None, ip=None):
        """create a vagrant object has a vagrantfile associated with it.

        config is just a string with a vagrant config file in it
        if vagrant_directory is defined, reuse an existing configuration
        """

        self.vagrant_path = vagrant_path
        self.ip = ip
        if self.ip and "config.vm.network" not in config:
            config_option = 'config.vm.network("%s")' % self.ip
            config = _append_to_vagrant_config(config_option, config)
        elif not self.ip and "config.vm.network" in config:
            self.ip = _extract_ip_from_config(config)
        self.validate()

        if vagrant_directory:
            self.directory = vagrant_directory
        else:
            self.directory = tempfile.mkdtemp()

            self.vagrantfile = config

            vagrantfile_path = os.path.join(self.directory, "Vagrantfile")
            with open(vagrantfile_path, "w") as vagrantfile_handle:
                vagrantfile_handle.write(self.vagrantfile)

            print "path: %s" % vagrantfile_path

    def validate(self):
        """confirm that vagrant is installed and we can execute it"""

        try:
            process = subprocess.Popen([self.vagrant_path, "help"],stdout=PIPE, stderr=PIPE)
        except Exception, e:
            raise VagrantException("Couldn't validate vagrant. Got error: %s" % str(e))

        stderr = process.communicate()[1]
        retcode = process.returncode
        if retcode != 0:
            raise VagrantException("Couldn't validate vagrant. Got error: %s" % stderr)
        

    def up(self):
        
        stderr = ""

        try:
            process = subprocess.Popen([self.vagrant_path, "up"], cwd=self.directory,
                                       stderr=PIPE, stdout=PIPE)
        except Exception, e:
            raise VagrantException("Couldn't start vagrant vm. Got error: %s" % str(e))
        (stdout, stderr) = process.communicate()
        retcode = process.returncode
        if retcode != 0:
            raise VagrantException("Couldn't start vagrant vm. Got error: %s" % stdout+stderr)

    def status(self):
        
        stderr = ""
        try:
            process = subprocess.Popen([self.vagrant_path, "status"], cwd=self.directory,
                                            stderr=PIPE, stdout=PIPE)
            (stdout, stderr) = process.communicate()
        except Exception, e:
            raise VagrantException("Couldn't get vagrant status. Got error: %s" % str(e))

        print stdout
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
            process = subprocess.Popen([self.vagrant_path, "destroy"],
                                            cwd=self.directory,
                                            stderr=PIPE, stdout=PIPE)
        except Exception, e:
            raise VagrantException("Couldn't destroy vagrant vm. Got error: %s" % e)
        stderr = process.communicate()[1]
        retcode = process.returncode
        if retcode != 0:
            raise VagrantException("Couldn't destroy vagrant vm. Got error: %s" % stderr)

class FakeVagrant(object):

    def __init__(self, vagrant_path="vagrant", config=DEFAULT_CONFIG, vagrant_directory=None):
        self.state = VagrantState.NOT_CREATED

    def up(self):
        self.state = VagrantState.RUNNING

    def destroy(self):
        self.state = VagrantState.NOT_CREATED

    def status(self):
        return self.state
 
class VagrantManager(object):
    """manages a list of Vagrant VMs. Mostly used to ease coordination of multiple Vagrant VMs
    """

    NETWORK_PREFIX = "33.33.33"

    def __init__(self, vagrant=Vagrant):
        self.vms = []
        self.vagrant = vagrant # provide opportunity to pass in FakeVagrant
        self.ips = []


    def new_vm(self, vagrant_path="vagrant", config=DEFAULT_CONFIG, vagrant_directory=None):


        ip = self._get_ip()
        vm = self.vagrant(vagrant_path=vagrant_path, config=config, vagrant_directory=vagrant_directory, ip=ip)
        self.vms += vm.directory
        return vm

    def remove_vm(self, vagrant_directory=None):
        """ remove VM from list of VMs, and remove ip from list of ips
        """
        if not vagrant_directory:
            raise VagrantException("You must specify a directory to remove a vagrant vm")

        vm = self.vagrant(vagrant_directory=vagrant_directory, ip=ip)
        if vm.status() != VagrantState.NOT_CREATED:
            vm.destroy()
        with open(vm.vagrantfile) as config:
            ip = _extract_ip_from_config(config)

        if ip in self.ips:
            self.ips.remove(ip)

        self.vms.remove(vagrant_directory)

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

