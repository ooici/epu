import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

import time
import uuid
from collections import defaultdict
from epu.decisionengine import EngineLoader
import epu.states as InstanceStates
from epu import cei_events
from twisted.internet.task import LoopingCall
from twisted.internet import defer
from epu.epucontroller.health import HealthMonitor
from epu.epucontroller import de_states

from forengine import Control
from forengine import State
from forengine import StateItem

PROVISIONER_VARS_KEY = 'provisioner_vars'
MONITOR_HEALTH_KEY = 'monitor_health'
HEALTH_BOOT_KEY = 'health_boot_timeout'
HEALTH_MISSING_KEY = 'health_missing_timeout'
HEALTH_ZOMBIE_KEY = 'health_zombie_timeout'

class ControllerCore(object):
    """Controller functionality that is not specific to the messaging layer.
    """

    def __init__(self, provisioner_client, engineclass, controller_name, conf=None):
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

        if health_kwargs is not None:
            health_monitor = HealthMonitor(**health_kwargs)
        else:
            health_monitor = None

        self.state = ControllerCoreState(health_monitor)

        # There can only ever be one 'reconfigure' or 'decide' engine call run
        # at ANY time.  The 'decide' call is triggered via timed looping call
        # and 'reconfigure' is triggered asynchronously at any moment.  
        self.busy = defer.DeferredSemaphore(1)
        
        self.control = ControllerCoreControl(provisioner_client, self.state, prov_vars, controller_name)
        self.engine = EngineLoader().load(engineclass)

    def new_sensor_info(self, content):
        """Ingests new sensor information, decides on validity and type of msg.
        """

        # Keeping message differentiation first, before state_item is parsed.
        # There needs to always be a methodical way to differentiate.
        if content.has_key("node_id"):
            self.state.new_instancestate(content)
        elif content.has_key("queue_name"):
            self.state.new_queuelen(content)
        elif content.has_key("worker_status"):
            self.state.new_workerstatus(content)
        else:
            log.error("received unknown sensor info: '%s'" % content)

    def new_heartbeat(self, content):
        """Ingests new heartbeat information
        """
        self.state.new_heartbeat(content)

    def begin_controlling(self):
        """Call the decision engine at the appropriate times.
        """
        log.debug('Starting engine decision loop - %s second interval',
                self.control.sleep_seconds)
        self.control_loop = LoopingCall(self.run_decide)
        self.control_loop.start(self.control.sleep_seconds, now=False)
        
    def run_initialize(self, conf):
        """Performs initialization routines that may require async processing
        """
        # DE routines can optionally return a Deferred
        return defer.maybeDeferred(self.engine.initialize,
                                   self.control, self.state, conf)

    @defer.inlineCallbacks
    def run_decide(self):
        # update heartbeat states
        self.state.update()

        yield self.busy.run(self.engine.decide, self.control, self.state)
        
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
        all_queuelen = self.state.get_all("queue-length")
        if len(all_queuelen) > 1:
            raise Exception("unexpected: multiple queuelen channels to analyze")

        last_queuelen_size = -1
        last_queuelen_time = -1
        if len(all_queuelen) == 1:
            last_queuelen = all_queuelen[0]
            if len(last_queuelen) > 0:
                last_queuelen_size = last_queuelen[-1].value
                last_queuelen_time = last_queuelen[-1].time

        return last_queuelen_size, last_queuelen_time

    def _node_error(self, node_id):
        """Return a string (potentially long) for an error reported off the node via heartbeat.
        Return empty or None if there is nothing or if the node is not known."""
        return self.state.health.heartbeat_error(node_id)

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

        all_instance_lists = self.state.get_all("instance-state")
        for instance_list in all_instance_lists:
            one_state_item = instance_list[-1] # most recent state
            node_id = one_state_item.key
            hearbeat_time = self.state.health.last_heartbeat_time(node_id)
            hearbeat_state = self.state.health.last_heartbeat_state(node_id)
            instances[node_id] = {"iaas_state": one_state_item.value,
                                  "iaas_state_time": one_state_item.time,
                                  "heartbeat_time": hearbeat_time,
                                  "hearbeat_state": hearbeat_state}

        return { "de_state": de_state,
                 "de_conf_report": self.de_conf_report(),
                 "last_queuelen_size": last_queuelen_size,
                 "last_queuelen_time": last_queuelen_time,
                 "instances": instances }


class ControllerCoreState(State):
    """Keeps data, also what is passed to decision engine.

    In the future the decision engine will be passed more of a "view"
    """

    def __init__(self, health_monitor=None):
        super(ControllerCoreState, self).__init__()
        self.instance_state_parser = InstanceStateParser()
        self.queuelen_parser = QueueLengthParser()
        self.workerstatus_parser = WorkerStatusParser()
        self.instance_states = defaultdict(list)
        self.queue_lengths = defaultdict(list)
        self.worker_status = defaultdict(list)

        # these should be folded into an all-encompassing Instance structure
        self.instance_public_ip = {}
        self.instance_private_ip = {}
        self.ip_instance = {}

        self.health = health_monitor

    def new_instancestate(self, content):
        state_item = self.instance_state_parser.state_item(content)
        if state_item:
            instance_id = state_item.key
            self.instance_states[instance_id].append(state_item)

            if self.health:
                # need to send node state information to health monitor too.
                # it uses it to determine when nodes are missing or zombies
                self.health.node_state(state_item.key, state_item.value,
                                       state_item.time)

            # this should be folded into something less ad hoc
            if instance_id not in self.instance_public_ip:
                pub = content.get("public_ip")
                priv = content.get("private_ip")

                # assuming both IPs come in at same time
                if pub or priv:
                    if pub:
                        self.ip_instance[pub] = instance_id
                    if priv:
                        self.ip_instance[priv] = instance_id

                    self.instance_public_ip[instance_id] = pub
                    self.instance_private_ip[instance_id] = priv

    def new_launch(self, new_instance_id):
        state = InstanceStates.REQUESTING
        item = StateItem("instance-state", new_instance_id, time.time(), state)
        self.instance_states[item.key].append(item)

    def new_queuelen(self, content):
        state_item = self.queuelen_parser.state_item(content)
        if state_item:
            self.queue_lengths[state_item.key].append(state_item)

    def new_workerstatus(self, content):
        state_item = self.workerstatus_parser.state_item(content)
        if state_item:
            self.worker_status[state_item.key].append(state_item)

    def new_heartbeat(self, content):
        if self.health:
            self.health.new_heartbeat(content)
        else:
            log.info("Got heartbeat but node health isn't monitored: %s",
                     content)

    def update(self):
        if self.health:
            self.health.update()

    def get_all(self, typename):
        """
        Get all data about a particular type.

        State API method, see the decision engine implementer's guide.

        @retval list(StateItem) StateItem instances that match the type
        or an empty list if nothing matches.
        @exception KeyError if typename is unknown
        """
        if typename == "instance-state":
            data = self.instance_states
        elif typename == "queue-length":
            data = self.queue_lengths
        elif typename == "worker-status":
            data = self.worker_status
        elif typename == "instance-health":
            data = self.health.nodes if self.health else None
        else:
            raise KeyError("Unknown typename: '%s'" % typename)

        if data is not None:
            return data.values()
        else:
            return None

    def get(self, typename, key):
        """Get all data about a particular key of a particular type.

        State API method, see the decision engine implementer's guide.

        @retval list(StateItem) StateItem instances that match the key query
        or an empty list if nothing matches.
        @exception KeyError if typename is unknown
        """
        if typename == "instance-state":
            data = self.instance_states
        elif typename == "queue-length":
            data = self.queue_lengths
        elif typename == "worker-status":
            data = self.worker_status
        elif typename == "instance-health":
            data = self.health.nodes if self.health else None
        else:
            raise KeyError("Unknown typename: '%s'" % typename)

        if data and data.has_key(key):
            return data[key]
        else:
            return []

    def get_instance_public_ip(self, instance_id):
        return self.instance_public_ip.get(instance_id)

    def get_instance_private_ip(self, instance_id):
        return self.instance_private_ip.get(instance_id)

    def get_instance_from_ip(self, ip):
        return self.ip_instance.get(ip)


class InstanceStateParser(object):
    """Converts instance state message into a StateItem
    """

    def __init__(self):
        pass

    def state_item(self, content):
        log.debug("received new instance state message: '%s'" % content)
        try:
            instance_id = self._expected(content, "node_id")
            state = self._expected(content, "state")
        except KeyError:
            log.error("could not capture sensor info (full message: '%s')" % content)
            return None
        return StateItem("instance-state", instance_id, time.time(), state)

    def _expected(self, content, key):
        if content.has_key(key):
            return str(content[key])
        else:
            log.error("message does not contain part with key '%s'" % key)
            raise KeyError()

class QueueLengthParser(object):
    """Converts queuelen message into a StateItem
    """

    def __init__(self):
        pass

    def state_item(self, content):
        log.debug("received new queulen state message: '%s'" % content)
        try:
            queuelen = self._expected(content, "queue_length")
            queuelen = int(queuelen)
            queueid = self._expected(content, "queue_name")
        except KeyError:
            log.error("could not capture sensor info (full message: '%s')" % content)
            return None
        except ValueError:
            log.error("could not convert queulen into integer (full message: '%s')" % content)
            return None
        return StateItem("queue-length", queueid, time.time(), queuelen)

    def _expected(self, content, key):
        if content.has_key(key):
            return str(content[key])
        else:
            log.error("message does not contain part with key '%s'" % key)
            raise KeyError()

class WorkerStatusParser(object):
    """Converts worker status message into a StateItem
    """

    def __init__(self):
        pass

    def state_item(self, content):
        log.debug("received new worker status state message: '%s'" % content)
        try:
            worker_status = self._expected(content, "worker_status")
            queueid = self._expected(content, "queue_name")
        except KeyError:
            log.error("could not capture sensor info (full message: '%s')" % content)
            return None
        return StateItem("worker-status", queueid, time.time(), worker_status)

    def _expected(self, content, key):
        if content.has_key(key):
            return str(content[key])
        else:
            log.error("message does not contain part with key '%s'" % key)
            raise KeyError()

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
                self.state.new_launch(new_instance_id)
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
        return (launch_id, launch_description)

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
