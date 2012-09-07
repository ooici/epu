import os
import epu
import mock
import time
import unittest
import tempfile

import epu.tevent as tevent

from epu.vagrantprovisioner.core import VagrantProvisionerCore
from epu.localdtrs import LocalVagrantDTRS
from epu.dashiproc.provisioner import ProvisionerService
from dashi.bootstrap import DEFAULT_EXCHANGE
from dashi import DashiConnection

class TestVagrantProvisionerIntegration(object):

    def setup(self):

        with open(os.devnull, "w") as devnull:
            try:
                call("vagrant", stdout=devnull)
            except:
                raise unittest.SkipTest("vagrant not available")
        if not os.environ('VAGRANTINT'):
                raise unittest.SkipTest("VAGRANTINT env var not set")

        try:
            from ceiclient.client import SERVICES
            from ceiclient.connection import DashiCeiConnection
        except ImportError:
            raise unittest.SkipTest("ceiclient isn't available")

        class InMemoryDashiCeiConnection(DashiCeiConnection):
            """a DashiCeiConnection that uses in memory kombu
            """
            def __init__(self, uri, amqp_exchange):
                self.dashi_connection = DashiConnection(self._name,
                    uri, amqp_exchange)
            
        testdir = os.path.dirname(__file__)
        dt_dir = os.path.join(testdir, 'vagrantdt')
        cookbook_dir = os.path.join(testdir, 'cookbooks')

        core = VagrantProvisionerCore
        dtrs = LocalVagrantDTRS(dt=dt_dir, cookbooks=cookbook_dir)
        self.amqp_uri = "memory://testprovisioner"

        self.provisioner = ProvisionerService(core=core, dtrs=dtrs, amqp_uri=self.amqp_uri)
        self.provisioner_thread = tevent.spawn(self.provisioner.start)


        provisioner_service = SERVICES['provisioner']
        conn = InMemoryDashiCeiConnection(self.amqp_uri, DEFAULT_EXCHANGE)

        self.provisioner_client = provisioner_service(conn)


    def teardown(self):
        self.provisioner_thread.kill(block=True)

    def test_start_vagrant(self):

        provision_config = """{
        }"""
        var_file = tempfile.NamedTemporaryFile(delete=False)
        var_file.write(provision_config)
        var_file.close()

        provision_command = self.provisioner_client.commands['provision']
        vars = mock.MagicMock()
        vars.provisioning_var_file = var_file.name
        vars.deployable_type = 'sleeper'
        vars.site = 'nothing'
        vars.nodes = 'nothing'
        time.sleep(5)
        out = provision_command.execute(self.provisioner_client, vars)

        describe_command = self.provisioner_client.commands['describe']
        while True:
            nodes = describe_command.execute(self.provisioner_client, vars)
            node = nodes[0]
            (state_code, state) = node['state'].split('-')
            state_code = int(state_code)
            time.sleep(2)
            if state_code >= 500:
                break

        assert state == 'STARTED'

        terminate_command = self.provisioner_client.commands['terminate_all']
        term_thread = tevent.spawn(terminate_command.execute, self.provisioner_client, vars)
        while True:
            nodes = describe_command.execute(self.provisioner_client, vars)
            node = nodes[0]
            (state_code, state) = node['state'].split('-')
            state_code = int(state_code)
            time.sleep(2)
            if state_code >= 700:
                break
            elif not nodes:
                break

