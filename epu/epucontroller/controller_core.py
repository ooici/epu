import itertools
import ion.util.ionlog
from epu.epucontroller.forengine import Instance, SensorItem, Control, State

log = ion.util.ionlog.getLogger(__name__)

import time
import uuid
from collections import defaultdict
from epu.decisionengine import EngineLoader
import epu.states as InstanceStates
from epu import cei_events
from twisted.internet.task import LoopingCall
from twisted.internet import defer
from epu.epucontroller.health import HealthMonitor, InstanceHealthState
from epu.epucontroller.controller_store import ControllerStore
from epu.epucontroller import de_states

PROVISIONER_VARS_KEY = 'provisioner_vars'
MONITOR_HEALTH_KEY = 'monitor_health'
HEALTH_BOOT_KEY = 'health_boot_timeout'
HEALTH_MISSING_KEY = 'health_missing_timeout'
HEALTH_ZOMBIE_KEY = 'health_zombie_timeout'

_HEALTHY_STATES = (InstanceHealthState.OK, InstanceHealthState.UNKNOWN)

class ControllerCore(object):
    """Controller functionality that is not specific to the messaging layer.
    """

    def __init__(self, provisioner_client, engineclass, controller_name,
                 conf=None, state=None, store=None):

        if state:
            self.state = state
        else:
            self.state = ControllerCoreState(store or ControllerStore())

        prov_vars = None
        health_kwargs = None
        if conf:
            if conf.has_key(PROVISIONER_VARS_KEY):
                prov_vars = conf[PROVISIONER_VARS_KEY]

            if conf.get(MONITOR_HEALTH_KEY):
                health_kwargs = {}
                if HEALTH_BOOT_KEY in conf:
                    health_kwargs['boot_seconds'] = conf[HEALTH_BOOT_KEY]
                if HEALTH_MISSING_KEY in conf:
                    health_kwargs['missing_seconds'] = conf[HEALTH_MISSING_KEY]
                if HEALTH_ZOMBIE_KEY in conf:
                    health_kwargs['zombie_seconds'] = conf[HEALTH_ZOMBIE_KEY]
        self.conf = conf

        if health_kwargs is not None:
            self.health_monitor = HealthMonitor(self.state, **health_kwargs)
        else:
            self.health_monitor = None

        # There can only ever be one 'reconfigure' or 'decide' engine call run
        # at ANY time.  The 'decide' call is triggered via timed looping call
        # and 'reconfigure' is triggered asynchronously at any moment.  
        self.busy = defer.DeferredSemaphore(1)
        
        self.control = ControllerCoreControl(provisioner_client, self.state,
                                             prov_vars, controller_name)
        self.engine = EngineLoader().load(engineclass)

    def new_sensor_info(self, content):
        """Handle an incoming sensor message

        @param content Raw sensor content
        @retval Deferred
        """
        return self.state.new_sensor_item(content)

    def new_instance_state(self, content):
        """Handle an incoming instance state message

        @param content Raw instance state content
        @retval Deferred
        """
        return self.state.new_instance_state(content)

    def new_heartbeat(self, content):
        """Handle an incoming heartbeat message

        @param content Raw heartbeat content
        @retval Deferred
        """
        if self.health_monitor:
            return self.health_monitor.new_heartbeat(content)
        else:
            return defer.succeed(None)

    def begin_controlling(self):
        """Call the decision engine at the appropriate times.
        """
        log.debug('Starting engine decision loop - %s second interval',
                self.control.sleep_seconds)
        self.control_loop = LoopingCall(self.run_decide)
        self.control_loop.start(self.control.sleep_seconds, now=False)

    @defer.inlineCallbacks
    def run_initialize(self):
        """Performs initialization routines that may require async processing
        """

        yield self.state.recover()

        engine_state = self.state.get_engine_state()

        # DE routines can optionally return a Deferred
        yield defer.maybeDeferred(self.engine.initialize,
                                  self.control, engine_state, self.conf)
        
    @defer.inlineCallbacks
    def run_decide(self):

        # allow health monitor to update any MISSING etc instance states
        if self.health_monitor:
            yield self.health_monitor.update()

        engine_state = self.state.get_engine_state()
        try:
            yield self.busy.run(self.engine.decide, self.control, engine_state)
        except Exception,e:
            log.error("Error in engine decide call: %s", str(e), exc_info=True)

    @defer.inlineCallbacks
    def run_reconfigure(self, conf):
        log.debug("reconfigure()")
        yield self.busy.run(self.engine.reconfigure, self.control, conf)

    def de_state(self):
        log.debug("de_state()")
        if hasattr(self.engine, "de_state"):
            return self.engine.de_state
        else:
            return de_states.UNKNOWN

    def de_conf_report(self):
        log.debug("de_conf_report()")
        if hasattr(self.engine, "de_conf_report"):
            return self.engine.de_conf_report()
        else:
            return None

    @defer.inlineCallbacks
    def whole_state(self):
        log.debug("whole_state()")
        whole_state = yield self.busy.run(self._whole_state)
        # Cannot log this event until the event DB handles complex dicts
        # cei_events.event("controller", "whole_state", log, extra=whole_state)
        defer.returnValue(whole_state)

    @defer.inlineCallbacks
    def node_error(self, node_id):
        log.debug("node_error(): node_id '%s'" % node_id)
        whole_state = yield self.busy.run(self._node_error, node_id)
        defer.returnValue(whole_state)

    def _latest_qlen(self):
        """Return (last_queuelen_size, last_queuelen_time) """
        return -1, -1

    def _node_error(self, node_id):
        """Return a string (potentially long) for an error reported off the node via heartbeat.
        Return empty or None if there is nothing or if the node is not known."""
        instance = self.state.instances.get(node_id)
        if instance:
            return instance.errors
        return None

    def _whole_state(self):
        """
        Return dictionary of the state of each instance this controller is aware of.

        Here is the dictionary in pseudocode:

        {
            "de_state" : STABLE OR NOT - (a decision engine is not required to implement this)
            "de_conf_report" : CONFIGURATION REPORT - (a decision engine is not required to implement this)
            "last_queuelen_size" : INTEGER (or -1)
            "last_queuelen_time" : SECONDS SINCE EPOCH (or -1),
            "instances" : {
                    "$instance_id_01" : { "iaas_state" : LATEST INSTANCE STATE - epu.states.*
                                          "iaas_state_time" : SECONDS SINCE EPOCH (or -1)
                                          "heartbeat_time" : SECONDS SINCE EPOCH (or -1)
                                          "heartbeat_state" : HEALTH STATE - epu.epucontroller.health.NodeHealthState.*
                                        },
                    "$instance_id_02" : { ... },
                    "$instance_id_03" : { ... },
            }
        }
        """

        de_state = self.de_state()
        last_queuelen_size, last_queuelen_time = self._latest_qlen()

        instances = {}

        for instance_id, instance in self.state.instances.iteritems():
            hearbeat_time = -1
            if self.health_monitor:
                hearbeat_time = self.health_monitor.last_heartbeat_time(instance_id)
            instances[instance_id] = {"iaas_state": instance.state,
                                      "iaas_state_time": instance.state_time,
                                      "heartbeat_time": hearbeat_time,
                                      "heartbeat_state": instance.health}
        sensors = {}
        for sensor_id, sensor_item in self.state.sensors:
            sensors[sensor_id] = str(sensor_item.value)

        return { "de_state": de_state,
                 "de_conf_report": self.de_conf_report(),
                 # queuelen sizes don't make sense anymore
                 "last_queuelen_size": last_queuelen_size,
                 "last_queuelen_time": last_queuelen_time,
                 "instances": instances,
                 "sensors" : sensors}


class ControllerCoreState(State):
    """Provides state and persistence management facilities for EPU Controller

    Note that this is no longer the object given to Decision Engine decide().
    """

    def __init__(self, store):
        self.store = store
        self.engine_state = EngineState()

        self.instance_parser = InstanceParser()
        self.sensor_parser = SensorItemParser()

        self.instances = {}
        self.sensors = {}
        self.pending_instances = defaultdict(list)
        self.pending_sensors = defaultdict(list)

    @defer.inlineCallbacks
    def recover(self):
        """Attempt to recover any state from the store.

        Can safely be called during a fresh start.
        """
        log.debug("Attempting recovery of controller state")
        instance_ids = yield self.store.get_instance_ids()
        for instance_id in instance_ids:
            instance = yield self.store.get_instance(instance_id)
            if instance:
                log.info("Recovering instance %s in state %s", instance_id,
                         instance.state)
                self.instances[instance_id] = instance

        sensor_ids = yield self.store.get_sensor_ids()
        for sensor_id in sensor_ids:
            sensor = yield self.store.get_sensor(sensor_id)
            if sensor:
                log.info("Recovering sensor %s with value %s", sensor_id,
                         sensor.value)

    def new_instance_state(self, content, timestamp=None):
        """Introduce a new instance state from an incoming message

        @retval Deferred
        """
        instance_id = self.instance_parser.parse_instance_id(content)
        if instance_id:
            previous = self.instances.get(instance_id)
            instance = self.instance_parser.parse(content, previous,
                                                  timestamp=timestamp)
            if instance:
                return self._add_instance(instance)
        return defer.succeed(False)

    def new_instance_launch(self, instance_id, launch_id, site, allocation,
                            timestamp=None):
        """Record a new instance launch

        @param instance_id Unique id for the new instance
        @param launch_id Unique id for the new launch group
        @param site Site instance is being launched at
        @param allocation Size of new instance
        @retval Deferred
        """
        now = time.time() if timestamp is None else timestamp

        if instance_id in self.instances:
            raise KeyError("instance %s already exists" % instance_id)

        instance = CoreInstance(instance_id=instance_id, launch_id=launch_id,
                            site=site, allocation=allocation,
                            state=InstanceStates.REQUESTING,
                            state_time=now,
                            health=InstanceHealthState.UNKNOWN)
        return self._add_instance(instance)

    def new_instance_health(self, instance_id, health_state, errors=None):
        """Record new instance health information

        Expected to be called by the health monitor.

        @param instance_id Id of instance
        @param health_state The state
        @param errors Instance errors provided in the heartbeat
        @retval Deferred
        """
        instance = self.instances[instance_id]
        d = dict(instance.iteritems())
        d['health'] = health_state
        d['errors'] = errors
        newinstance = CoreInstance(**d)
        return self._add_instance(newinstance)

    def new_sensor_item(self, content):
        """Introduce new sensor item from an incoming message

        @retval Deferred
        """
        item = self.sensor_parser.parse(content)
        if item:
            return self._add_sensor(item)
        return defer.succeed(False)

    def get_engine_state(self):
        """Get an object to provide to engine decide() and reset pending state

        Beware that the object provided may be changed and reused by the
        next invocation of this method.
        """
        s = self.engine_state
        s.sensors = dict(self.sensors.iteritems())
        s.sensor_changes = dict(self.pending_sensors.iteritems())
        s.instances = dict(self.instances.iteritems())
        s.instance_changes = dict(self.pending_instances.iteritems())

        self._reset_pending()
        return s

    def _add_instance(self, instance):
        instance_id = instance.instance_id
        self.instances[instance_id] = instance
        self.pending_instances[instance_id].append(instance)
        return self.store.add_instance(instance)

    def _add_sensor(self, sensor):
        sensor_id = sensor.sensor_id
        previous = self.sensors.get(sensor_id)

        # we only update the current sensor value if the timestamp is newer.
        # But we can still add out-of-order items to the store and the
        # pending list.
        if previous and sensor.time < previous.time:
            log.warn("Received out of order %s sensor item!", sensor_id)
        else:
            self.sensors[sensor_id] = sensor

        self.pending_sensors[sensor_id].append(sensor)
        return self.store.add_sensor(sensor)

    def _reset_pending(self):
        self.pending_instances.clear()
        self.pending_sensors.clear()


REQUIRED_INSTANCE_FIELDS = ('instance_id', 'launch_id', 'site', 'allocation', 'state')
class CoreInstance(Instance):
    def __init__(self, **kwargs):
        for f in REQUIRED_INSTANCE_FIELDS:
            if not f in kwargs:
                raise KeyError("Missing required instance field: " + f)
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


class EngineState(object):
    """State object given to decision engine
    """

    def __init__(self):
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

        @retval Deferred
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
        """Returns instances in a healthy state (OK, UNKNOWN)

        Most likely the DE will want to terminate these and replace them
        """
        return [instance for instance in self.instances.itervalues()
                if instance.health in _HEALTHY_STATES]

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
        """
        return [instance for instance in self.instances.itervalues()
                if instance.health not in _HEALTHY_STATES]


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
        except KeyError, e:
            log.warn("Instance state message missing required field '%s': %s",
                     e, content)
            return None

        # this has gotten messy because of need to preserve health
        # info from previous record

        d = dict(instance_id=instance_id, state_time=now)
        d.update(content)
        d['health'] = previous.health
        d['errors'] = list(previous.errors) if previous.errors else None
        new = CoreInstance(**d)

        if new.state <= previous.state:
            log.warn("Got out of order or duplicate instance state message!"+
            " It will be dropped: %s", content)
            return None
        return new


class ControllerCoreControl(Control):
    def __init__(self, provisioner_client, state, prov_vars, controller_name):
        super(ControllerCoreControl, self).__init__()
        self.sleep_seconds = 5.0
        self.provisioner = provisioner_client
        self.state = state
        self.controller_name = controller_name
        self.prov_vars = prov_vars # can be None

    def configure(self, parameters):
        """
        Give the engine the opportunity to offer input about how often it
        should be called or what specific events it would always like to be
        triggered after.

        See the decision engine implementer's guide for specific configuration
        options.

        @retval None
        @exception Exception illegal/unrecognized input
        """
        if not parameters:
            log.info("ControllerCoreControl is configured, no parameters")
            return
            
        if parameters.has_key("timed-pulse-irregular"):
            sleep_ms = int(parameters["timed-pulse-irregular"])
            self.sleep_seconds = sleep_ms / 1000.0
            log.info("Configured to pulse every %.2f seconds" % self.sleep_seconds)
            
        if parameters.has_key(PROVISIONER_VARS_KEY):
            self.prov_vars = parameters[PROVISIONER_VARS_KEY]
            log.info("Configured with new provisioner vars:\n%s" % self.prov_vars)

    def launch(self, deployable_type_id, launch_description, extravars=None):
        """Choose instance IDs for each instance desired, a launch ID and send
        appropriate message to Provisioner.

        Control API method, see the decision engine implementer's guide.

        @param deployable_type_id string identifier of the DP to launch
        @param launch_description See engine implementer's guide
        @param extravars Optional, see engine implementer's guide
        @retval tuple (launch_id, launch_description), see guide
        @exception Exception illegal input
        @exception Exception message not sent
        """

        # right now we are sending some node-specific data in provisioner vars
        # (node_id at least)
        if len(launch_description) != 1:
            raise NotImplementedError("Only single-node launches are supported")

        launch_id = str(uuid.uuid4())
        log.info("Request for DP '%s' is a new launch with id '%s'" % (deployable_type_id, launch_id))
        new_instance_id_list = []
        for group,item in launch_description.iteritems():
            log.info(" - %s is %d %s from %s" % (group, item.num_instances, item.allocation_id, item.site))

            if item.num_instances != 1:
                raise NotImplementedError("Only single-node launches are supported")

            for i in range(item.num_instances):
                new_instance_id = str(uuid.uuid4())
                self.state.new_launch(new_instance_id, launch_id,
                                      item.allocation_id, item.site)
                item.instance_ids.append(new_instance_id)
                new_instance_id_list.append(new_instance_id)
        
        vars_send = self.prov_vars.copy()
        if extravars:
            vars_send.update(extravars)

        # The node_id var is the reason only single-node launches are supported.
        # It could be instead added by the provisioner or something? It also
        # is complicated by the contextualization system.
        vars_send['node_id'] = new_instance_id_list[0]
        vars_send['heartbeat_dest'] = self.controller_name

        log.debug("Launching with parameters:\n%s" % str(vars_send))

        subscribers = (self.controller_name,)
            
        self.provisioner.provision(launch_id, deployable_type_id,
                launch_description, subscribers, vars=vars_send)
        extradict = {"launch_id":launch_id,
                     "new_instance_ids":new_instance_id_list,
                     "subscribers":subscribers}
        cei_events.event("controller", "new_launch",
                         log, extra=extradict)
        return launch_id, launch_description

    def destroy_instances(self, instance_list):
        """Terminate particular instances.

        Control API method, see the decision engine implementer's guide.

        @param instance_list list size >0 of instance IDs to terminate
        @retval None
        @exception Exception illegal input/unknown ID(s)
        @exception Exception message not sent
        """
        self.provisioner.terminate_nodes(instance_list)

    def destroy_launch(self, launch_id):
        """Terminate an entire launch.

        Control API method, see the decision engine implementer's guide.

        @param launch_id launch to terminate
        @retval None
        @exception Exception illegal input/unknown ID
        @exception Exception message not sent
        """
        self.provisioner.terminate_launches([launch_id])
