# Copyright 2013 University of Chicago

import logging
import uuid
import copy

from epu.decisionengine.engineapi import Engine
from epu.epumanagement.core import CoreInstance
from epu.epumanagement.store import LocalDomainStore
from epu.states import InstanceState, InstanceHealthState
from epu.sensors import Statistics

log = logging.getLogger(__name__)


class FakeDomainStore(LocalDomainStore):
    def new_fake_instance_state(self, instance_id, state, state_time,
                                health=None, errors=None):
        instance = self.get_instance(instance_id)
        if health is None:
            if instance:
                health = instance.health
            else:
                health = InstanceHealthState.UNKNOWN

        if errors is None and instance and instance.errors:
            errors = instance.errors

        newinstance = CoreInstance(instance_id=instance_id, launch_id="thelaunch",
                                site="chicago", allocation="big", state=state,
                                state_time=state_time, health=health, errors=errors,
                                iaas_id=instance_id)
        if instance:
            self.update_instance(newinstance, previous=instance)
        else:
            self.add_instance(newinstance)


class MockDomain(object):

    def __init__(self, owner):
        self.owner = owner


class MockInstances(object):

    def __init__(self, site, deployable_type, extravars=None, state=InstanceState.REQUESTING, sensor_data=None):
        self.site = site
        self.deployable_type = deployable_type
        self.extravars = extravars
        self.state = state
        self.instance_id = 'ami-' + str(uuid.uuid4()).split('-')[0]
        self.iaas_id = 'i-' + str(uuid.uuid4()).split('-')[0]
        self.sensor_data = sensor_data


class MockState(object):

    def __init__(self, instances=None):
        self.instances = {}
        if instances is not None:
            for i in instances:
                self.instances[i.instance_id] = i

    def get_unhealthy_instances(self):
        return []


class MockControl(object):

    def __init__(self):
        self._launch_calls = 0
        self._destroy_calls = 0
        self.domain = MockDomain("user")

        self.health_not_checked = True

        self.site_launch_calls = {}
        self.site_destroy_calls = {}
        self.instances = []

    def set_instance_sensor_data(self, sensor_data):
        for instance in self.instances:
            instance.sensor_data = sensor_data

    def get_state(self):
        return MockState(self.instances)

    def launch(self, dt_name, sites_name, instance_type, extravars=None, caller=None):
        self._launch_calls = self._launch_calls + 1

        if sites_name not in self.site_launch_calls:
            self.site_launch_calls[sites_name] = 0
        self.site_launch_calls[sites_name] = self.site_launch_calls[sites_name] + 1

        if extravars is not None:
            extravars = copy.deepcopy(extravars)
        instance = MockInstances(sites_name, dt_name, extravars=extravars)
        self.instances.append(instance)

        launch_id = str(uuid.uuid4())
        instance_ids = [instance.instance_id, ]
        return (launch_id, instance_ids)

    def destroy_instances(self, instanceids, caller=None):
        found_insts = []
        for i in self.instances:
            if i.instance_id in instanceids:
                found_insts.append(i)

        if len(found_insts) != len(instanceids):
            raise Exception("Some instances ids were not found")

        for i in found_insts:
            if i.site not in self.site_destroy_calls:
                self.site_launch_calls[i.site] = 0
            self.site_launch_calls[i.site] = self.site_launch_calls[i.site] + 1
            i.state = InstanceState.TERMINATING

        self._destroy_calls = self._destroy_calls + len(instanceids)

    def get_instances(self, site=None, states=None):
        instances = self.instances[:]

        if site:
            instances = [i for i in instances if i.site == site]
        if states:
            instances = [i for i in instances if i.state in states]

        return instances


class MockProvisionerClient(object):
    """See the IProvisionerClient class.
    """
    def __init__(self):
        self.provision_count = 0
        self.terminate_node_count = 0
        self.launched_instance_ids = []
        self.terminated_instance_ids = []
        self.deployable_types_launched = []
        self.launches = []
        self.epum = None

    def _set_epum(self, epum):
        # circular ref, only in this mock/unit test situation
        self.epum = epum

    def provision(self, launch_id, instance_ids, deployable_type,
                  site, allocation=None, vars=None, caller=None):
        self.provision_count += 1
        log.debug("provision() count %d", self.provision_count)
        self.launched_instance_ids.extend(instance_ids)
        for _ in instance_ids:
            self.deployable_types_launched.append(deployable_type)

        record = dict(launch_id=launch_id, dt=deployable_type,
            instance_ids=instance_ids, site=site, allocation=allocation,
            vars=vars)
        self.launches.append(record)

    def terminate_nodes(self, nodes, caller=None):
        self.terminate_node_count += len(nodes)
        self.terminated_instance_ids.extend(nodes)

    def report_node_state(self, state, node_id):
        assert self.epum
        content = {"node_id": node_id, "state": state}
        self.epum.msg_instance_info(None, content)

    def dump_state(self, nodes, force_subscribe=None):
        log.debug("dump_state()")


class MockDTRSClient(object):
    """This is only used for sensor data so is bare bones
    """

    def describe_credentials(self, caller, site):
        pass


class MockSubscriberNotifier(object):
    """See the ISubscriberNotifier class
    """
    def __init__(self):
        self.notify_by_name_called = 0
        self.receiver_names = []
        self.operations = []
        self.messages = []

    def notify_by_name(self, receiver_name, operation, message):
        """The name is translated into the appropriate messaging-layer object.
        @param receiver_name Message layer name
        @param operation The operation to call on that name
        @param message dict to send
        """
        log.debug("EPUM notification for %s:%s: %s", receiver_name, operation,
                  message)
        self.notify_by_name_called += 1
        self.receiver_names.append(receiver_name)
        self.operations.append(operation)
        self.messages.append(message)


class MockOUAgentClient(object):
    """See the IOUAgentClient class
    """
    def __init__(self):
        self.epum = None
        self.dump_state_called = 0
        self.heartbeats_sent = 0
        self.respond_to_dump_state = False

    def dump_state(self, target_address, mock_timestamp=None):
        self.dump_state_called += 1
        if self.epum and self.respond_to_dump_state:
            # In Mock mode, we know that node_id and the OUAgent address are equal things, by convention
            content = {'node_id': target_address, 'state': InstanceHealthState.OK}
            self.epum.msg_heartbeat(None, content, timestamp=mock_timestamp)
            self.heartbeats_sent += 1

    def _set_epum(self, epum):
        # circular ref, only in this mock/unit test situation
        self.epum = epum


class MockDecisionEngine01(Engine):
    """
    Counts only
    """

    def __init__(self):
        Engine.__init__(self)
        self.initialize_count = 0
        self.initialize_conf = None
        self.decide_count = 0
        self.reconfigure_count = 0

    def initialize(self, control, state, conf=None):
        self.initialize_count += 1
        self.initialize_conf = conf

    def decide(self, *args):
        self.decide_count += 1

    def reconfigure(self, *args):
        self.reconfigure_count += 1


class MockDecisionEngine02(Engine):
    """
    Fails only
    """

    def __init__(self):
        Engine.__init__(self)
        self.initialize_count = 0
        self.initialize_conf = None
        self.decide_count = 0
        self.reconfigure_count = 0

    def initialize(self, control, state, conf=None):
        self.initialize_count += 1
        self.initialize_conf = conf

    def decide(self, *args):
        self.decide_count += 1
        raise Exception("decide disturbance")

    def reconfigure(self, *args):
        self.reconfigure_count += 1
        raise Exception("reconfigure disturbance")


class MockCloudWatch(object):

    series_data = [0, ]

    def __init__(self, series_data=None):
        if series_data:
            self.series_data = series_data

    def get_metric_statistics(self, period, start_time, end_time, metric_name,
            statistics, dimensions=None):

        metrics = {}
        instanceid = dimensions.get('InstanceId')
        domainid = dimensions.get('DomainId')
        if instanceid is None and domainid is None:
            index = None
        elif isinstance(instanceid, basestring):
            index = instanceid
        elif isinstance(domainid, basestring):
            index = domainid
        else:
            index = instanceid[0]
        try:
            average = sum(self.series_data) / len(self.series_data)
        except ZeroDivisionError:
            average = 0
        metrics[index] = {Statistics.SERIES: self.series_data, Statistics.AVERAGE: average,
                Statistics.MAXIMUM: max(self.series_data), Statistics.MINIMUM: min(self.series_data),
                Statistics.SUM: sum(self.series_data), Statistics.SAMPLE_COUNT: len(self.series_data)}
        return metrics
