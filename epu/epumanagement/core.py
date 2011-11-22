import itertools
import ion.util.ionlog
from epu.epumanagement.forengine import Instance, SensorItem, State

log = ion.util.ionlog.getLogger(__name__)

import time
import epu.states as InstanceStates
from epu.epumanagement.health import InstanceHealthState


REQUIRED_INSTANCE_FIELDS = ('instance_id', 'launch_id', 'site', 'allocation', 'state')
class CoreInstance(Instance):
    @classmethod
    def from_existing(cls, previous, **kwargs):
        dct = previous.__dict__.copy()
        dct.update(kwargs)
        return cls(**kwargs)

    @classmethod
    def from_dict(cls, dct):
        return cls(**dct)

    def __init__(self, **kwargs):
        for f in REQUIRED_INSTANCE_FIELDS:
            if not f in kwargs:
                raise TypeError("Missing required instance field: " + f)
        self.__dict__.update(kwargs)

    def __getattr__(self, item):
        # only called when regular attribute resolution fails
        return None

    def __setattr__(self, key, value):
        # obviously not foolproof, more of a warning
        raise KeyError("Instance attribute setting disabled")

    def __getitem__(self, item):
        return self.__dict__[item]

    def __iter__(self):
        return iter(self.__dict__)

    def get(self, key, default=None):
        """Get a single instance property
        """
        return self.__dict__.get(key, default)

    def iteritems(self):
        """Iterator for (key,value) pairs of instance properties
        """
        return self.__dict__.iteritems()

    def iterkeys(self):
        """Iterator for instance property keys
        """
        return  self.__dict__.iterkeys()

    def items(self):
        """List of (key,value) pairs of instance properties
        """
        return self.__dict__.items()

    def keys(self):
        """List of available instance property keys
        """
        return self.__dict__.keys()

# OUT_OF_CONTACT is healthy because it is not marked truly missing yet
_HEALTHY_STATES = (InstanceHealthState.OK, InstanceHealthState.UNKNOWN, InstanceHealthState.OUT_OF_CONTACT)
class EngineState(State):
    """State object given to decision engine
    """

    def __init__(self):
        State.__init__(self)
        
        # the last value of each sensor input.
        # for example `queue_size = state.sensors['queuestat']`
        self.sensors = None

        # a list of values received for each sensor input, since the last decide() call
        # DEs can use this to easily inspect each value and maybe feed them into a model
        # for example: `for qs in state.sensor_changes['queuestat']`
        self.sensor_changes = None

        # the current Instance objects
        self.instances = None
        self.instance_changes = None

    def get_sensor(self, sensor_id):
        """Returns latest value for the specified sensor

        @param sensor_id Sensor ID to filter on
        """
        return self.sensors.get(sensor_id)

    def get_sensor_changes(self, sensor_id=None):
        """Returns list of sensor values received since last decide() call

        @param sensor_id Optional sensor ID to filter on
        """
        if sensor_id:
            changes = self.sensor_changes.get(sensor_id)
            if changes is None:
                return []
            return changes
        return list(itertools.chain(*self.sensor_changes.itervalues()))

    def get_sensor_history(self, sensor_id, count=None, reverse=True):
        """Queries datastore for historical values of the specified sensor
        """
        raise NotImplemented("History unavailable")

    def get_instance(self, instance_id):
        """
        Returns latest state object for the specified instance
        """
        return self.instances.get(instance_id)

    def get_instance_changes(self, instance_id=None):
        """
        Returns list of instance records received since the last decide() call

        Records are ordered by node and state and duplicates are omitted
        """
        if instance_id:
            changes = self.instance_changes.get(instance_id)
            if changes is None:
                return []
            return changes

        return list(itertools.chain(*self.instance_changes.itervalues()))

    def get_instance_history(self, instance_id, count):
        """Queries datastore for historical values of the specified instance

        @retval Deferred
        """
        raise NotImplemented("History unavailable")

    # below are instance-specific queries. There is room to add a lot more here
    # to query for launches, sites, IPs, etc.

    def get_instances_by_state(self, state, maxstate=None):
        """Returns a list of instances in the specified state or state range

        @param state instance state to search for, or inclusive lower bound in range
        @param maxstate Optional inclusive upper bound of range search
        """

        if maxstate:
            f = lambda i: i.state >= state and i.state <= maxstate
        else:
            f = lambda i: i.state == state
        return [instance for instance in self.instances.itervalues()
                if f(instance)]

    def get_healthy_instances(self):
        """Returns instances in a healthy state (OK, UNKNOWN, OUT_OF_CONTACT)
        """
        return [instance for instance in self.instances.itervalues()
                if instance.health in _HEALTHY_STATES and
                   instance.state < InstanceStates.RUNNING_FAILED]

    def get_pending_instances(self):
        """Returns instances that are in the process of starting.

        REQUESTED <= state < RUNNING
        """
        return [instance for instance in self.instances.itervalues()
                if instance.state >= InstanceStates.REQUESTED and
                   instance.state < InstanceStates.RUNNING]

    def get_unhealthy_instances(self):
        """Returns instances in an unhealthy state (MISSING, ERROR, ZOMBIE, etc)

        Most likely the DE will want to terminate these and replace them

        Includes RUNNING_FAILED (contextualization issue)
        """
        unhealthy = []
        for instance in self.instances.itervalues():
            if instance.state == InstanceStates.RUNNING_FAILED:
                unhealthy.append(instance)
                continue # health report from epuagent (or absence of it) is irrelevant

            if instance.health not in _HEALTHY_STATES:

                # only allow the zombie state for instances that are
                # terminated
                if (instance.state < InstanceStates.TERMINATED or
                    instance.health == InstanceHealthState.ZOMBIE):
                    unhealthy.append(instance)

        return unhealthy


class SensorItemParser(object):
    """Loads an incoming sensor item message
    """
    def parse(self, content):
        if not content:
            log.warn("Received empty sensor item: %s", content)
            return None

        if not isinstance(content, dict):
            log.warn("Received non-dict sensor item: %s", content)
            return None

        try:
            item = SensorItem(content['sensor_id'],
                              long(content['time']),
                              content['value'])
        except KeyError,e:
            log.warn('Received invalid sensor item. Missing "%s": %s', e,
                     content)
            return None
        except ValueError,e:
            log.warn('Received invalid sensor item. Bad "%s": %s', e, content)
            return None

        return item


class InstanceParser(object):
    """Loads an incoming instance state message
    """

    def parse_instance_id(self, content):
        try:
            instance_id = content.get('node_id')
        except KeyError:
            log.warn("Instance state message missing 'node_id' field: %s",
                     content)
            return None
        return instance_id

    def parse(self, content, previous, timestamp=None):
        now = time.time() if timestamp is None else timestamp

        try:
            instance_id = content.pop('node_id')
            state = content['state']
        except KeyError, e:
            log.warn("Instance state message missing required field '%s': %s",
                     e, content)
            return None

        if not previous:
            log.warn("Instance %s: got state update but instance is unknown."+
            " It will be dropped: %s", instance_id, content)
            return None

        # this has gotten messy because of need to preserve health
        # info from previous record

        d = dict(instance_id=instance_id, state_time=now)

        # hack to allow engine to distinguish between unique instances. always
        # copy this value to new instance state records.
        d['extravars'] = previous.extravars

        # in a special case FAILED records can come in without all fields present.
        # copy them over: should be safe since these values can't change.
        for k in REQUIRED_INSTANCE_FIELDS:
            d[k] = previous[k]

        d.update(content)

        # special handling for instances going to TERMINATED state:
        # we clear the health state so the instance will not reemerge as
        # "unhealthy" if its last health state was, say, MISSING
        if state >= InstanceStates.TERMINATING:
            d['health'] = InstanceHealthState.UNKNOWN
        else:
            d['health'] = previous.health
        d['errors'] = list(previous.errors) if previous.errors else None
        d['error_time'] = previous.error_time if previous.error_time else None
        new = CoreInstance(**d)

        if new.state <= previous.state:
            log.warn("Instance %s: got out of order or duplicate state message!"+
            " It will be dropped: %s", instance_id, content)
            return None
        return new
