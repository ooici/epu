# Copyright 2013 University of Chicago

import logging
import uuid
from socket import timeout

import dashi.bootstrap as bootstrap
from dashi.util import LoopingCall

from epu.highavailability.policy import policy_map
from epu.highavailability.core import HighAvailabilityCore
from epu.dashiproc.processdispatcher import ProcessDispatcherClient
from epu.util import get_config_paths

log = logging.getLogger(__name__)

DEFAULT_TOPIC = "highavailability"


class DashiHAProcessControl(object):

    def __init__(self, dashi, process_dispatchers):
        self.dashi = dashi
        self.process_dispatchers = list(process_dispatchers)

    def _get_pd_client(self, name):
        return ProcessDispatcherClient(self.dashi, name)

    def schedule_process(self, pd_name, process_definition_id, **kwargs):
        """Launches a new process on the specified process dispatcher

        Returns upid of process
        """
        upid = uuid.uuid4().hex
        pd_client = self._get_pd_client(pd_name)
        proc = pd_client.schedule_process(
            upid, definition_id=process_definition_id, **kwargs)
        log.info("Launched HA process %s: %s", proc.get('upid'), proc.get('state'))
        return upid

    def terminate_process(self, upid):
        """Terminates a process in the system
        """
        all_procs = self.get_all_processes()
        for pd_name, procs in all_procs.iteritems():
            for proc in procs:
                if proc.get('upid') == upid:
                    pd_client = self._get_pd_client(pd_name)
                    pd_client.terminate_process(upid)
                    return upid
        return None

    def get_all_processes(self):
        """Gets a dictionary of lists of {"upid": "XXXX", "state": "XXXXX"} dicts
        """
        all_procs = {}
        for pd_name in self.process_dispatchers:
            pd_client = self._get_pd_client(pd_name)
            try:
                procs = pd_client.describe_processes()
                all_procs[pd_name] = procs
            except timeout:
                log.warning("%s timed out when calling describe_processes", pd_name)
            except Exception:
                log.exception("Problem querying %s", pd_name)

        return all_procs


class HighAvailabilityService(object):

    def __init__(self, *args, **kwargs):

        configs = ["service", "highavailability"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        exchange = kwargs.get('exchange')
        if exchange:
            self.CFG.server.amqp.exchange = exchange

        self.topic = kwargs.get('service_name') or self.CFG.highavailability.get('service_name') or DEFAULT_TOPIC

        self.amqp_uri = kwargs.get('amqp_uri') or None
        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG, self.amqp_uri, sysname=kwargs.get('sysname'))

        process_dispatchers = (kwargs.get('process_dispatchers') or
                self.CFG.highavailability.processdispatchers)

        policy_name = self.CFG.highavailability.policy.name
        try:
            policy_map[policy_name.lower()]
            self.policy = policy_name.lower()
        except KeyError:
            raise Exception("HA Service doesn't support '%s' policy" % policy_name)

        policy_parameters = (kwargs.get('policy_parameters') or
                self.CFG.highavailability.policy.parameters)

        process_definition_id = (kwargs.get('process_definition_id') or
                self.CFG.highavailability.process_definition_id)

        self.policy_interval = (kwargs.get('policy_interval') or
                self.CFG.highavailability.policy.interval)

        self.control = DashiHAProcessControl(self.dashi, process_dispatchers)

        core = HighAvailabilityCore
        self.core = core(self.CFG.highavailability, self.control,
            process_dispatchers, self.policy, parameters=policy_parameters,
            process_definition_id=process_definition_id)

    def start(self):

        log.info("starting high availability instance %s" % self)

        # Set up operations
        self.dashi.handle(self.reconfigure_policy)
        self.dashi.handle(self.dump)

        self.apply_policy_loop = LoopingCall(self.core.apply_policy)
        self.apply_policy_loop.start(self.policy_interval)

        try:
            self.dashi.consume()
        except KeyboardInterrupt:
            self.apply_policy_loop.stop()
            log.warning("Caught terminate signal. Bye!")
        else:
            self.apply_policy_loop.stop()
            log.info("Exiting normally. Bye!")

    def stop(self):
        self.dashi.cancel()
        self.dashi.disconnect()

    def reconfigure_policy(self, new_policy):
        """Service operation: Change the parameters of the policy used for service

        @param new_policy: parameters of policy
        @return:
        """
        self.core.reconfigure_policy(new_policy)

    def status(self):
        """Service operation: Get the status of the HA Service

        @return: {PENDING, READY, STEADY, BROKEN}
        """
        return self.core.status()

    def dump(self):
        """Dump state of ha core
        """
        return self.core.dump()


class HighAvailabilityServiceClient(object):

    def __init__(self, dashi, topic=None):

        self.dashi = dashi
        self.topic = topic or DEFAULT_TOPIC

    def reconfigure_policy(self, new_policy):
        """Service operation: Change policy
        """
        self.dashi.call(self.topic, "reconfigure_policy", new_policy=new_policy)

    def status(self):
        """Service operation: Change policy
        """
        return self.dashi.call(self.topic, "status")

    def dump(self):
        return self.dashi.call(self.topic, "dump")


def main():
    haservice = HighAvailabilityService()
    haservice.start()
