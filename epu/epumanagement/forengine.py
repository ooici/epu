# Copyright 2013 University of Chicago

class Control(object):
    """
    This is the superclass for any implementation of the control object that
    is passed to the decision engine.  The control object is a way for the
    engine to make requests to the EPU Controller, like to send launch
    requests to the Provisioner.

    The abc (abstract base class) module is not present in Python 2.5 but
    Control should be treated as such.  It is not meant to be instantiated
    directly.

    """

    def __init__(self):
        pass

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
        raise NotImplementedError

    def launch(self, deployable_type_id, site, allocation, count=1, extravars=None, caller=None):
        """
        Choose instance IDs for each instance desired, a launch ID and send
        appropriate message to Provisioner.

        @param deployable_type_id string identifier of the DT to launch
        @param site IaaS site to launch on
        @param allocation IaaS allocation (size) to request
        @param count number of instances to launch
        @param extravars Optional, see engine implementer's guide
        @retval tuple (launch_id, instance_ids), see guide
        @exception Exception illegal input
        @exception Exception message not sent

        """
        raise NotImplementedError

    def destroy_instances(self, instance_list):
        """
        Terminate particular instances.

        @param instance_list list size >0 of instance IDs to terminate
        @retval None
        @exception Exception illegal input/unknown ID(s)
        @exception Exception message not sent

        """
        raise NotImplementedError


class Instance(object):
    """
    One instance state object. This object is considered immutable and is
    replaced, not changed. As new information is received about an instance,
    the existing Instance object is copied and new information is added. This
    updated object will be available to the decision engine at the next
    decide() call.

    This object exposes many values as attributes, not a fixed set.
    The following will be always be available and will never change:
        instance_id - unique identifier for instance
        launch_id - unique identifier for the instance's launch group
        site - deployment-specific IaaS site id; corresponds to DT sites
        allocation - deployment-specific IaaS allocation (small, large, etc)

    The following attributes will always be available but may change in future
    objects for an instance:
        state - the instance IaaS state; one of epu.states.InstanceHealthStates
        state_time - the time the current IaaS state was received by the
                     controller
        health - the instance's health state; one of
                 epu.states.InstanceHealthState

    The following attributes will be available no later than the instance's
    REQUESTED state, but may be available earlier. They are not guaranteed to
    ever exist on instances that do not reach this state. For example,
    instances that fail to resolve in the DTRS may never have these attributes.
        ctx_name - the name of the instance group in the DT contextualization
                   document
        iaas_image - the IaaS-specific instance image name (ami-XXXXXXXX, etc)
        iaas_allocation - the IaaS-specific instance allocation size (m1.small)
        iaas_sshkeyname - the IaaS-specific SSH key name, if applicable

    The following attributes will be available no later than the instance's
    STARTED state, but may be available earlier. They are not guaranteed to
    ever exist on instances that do not reach this state. For example,
    instances that fail immediately may never have been assigned an IP.
        public_ip - the public IP or hostname of the instance
        private_ip - the private IP or hostname of the instance

    The set of available properties can be found with items(), iteritems(),
    keys(), etc. These methods behave like that of a dict. Attempting to get
    an nonexistent property will return None.
    """
    def get(self, key, default=None):
        """Get a single instance property
        """

    def iteritems(self):
        """Iterator for (key,value) pairs of instance properties
        """

    def iterkeys(self):
        """Iterator for instance property keys
        """

    def items(self):
        """List of (key,value) pairs of instance properties
        """

    def keys(self):
        """List of available instance property keys
        """


class State(object):
    """State object given to decision engine.
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
        self.instance_last_heard = None

    def get_sensor(self, sensor_id):
        """Returns latest SensorItem for the specified sensor

        @param sensor_id Sensor ID to filter on
        """

    def get_sensor_changes(self, sensor_id=None):
        """Returns list of SensorItem objects received since last decide() call

        @param sensor_id Optional sensor ID to filter on
        """

    def get_sensor_history(self, sensor_id, count=None, reverse=True):
        """Queries datastore for historical values of the specified sensor
        """

    def get_instance(self, instance_id):
        """
        Returns latest state object for the specified instance
        """

    def get_instance_changes(self, instance_id=None):
        """
        Returns list of instance records received since the last decide() call

        Records are ordered by node and state and duplicates are omitted
        """

    def get_instance_history(self, instance_id, count):
        """Queries datastore for historical values of the specified instance

        @retval Deferred
        """

    def get_instances_by_state(self, state, maxstate=None):
        """Returns a list of instances in the specified state or state range

        @param state instance state to search for, or inclusive lower bound in range
        @param maxstate Optional inclusive upper bound of range search
        """

    def get_healthy_instances(self):
        """Returns instances in an unhealthy state (MISSING, ERROR, ZOMBIE, etc)

        Most likely the DE will want to terminate these and replace them
        """

    def get_pending_instances(self):
        """Returns instances that are in the process of starting.

        REQUESTED <= state < RUNNING
        """

    def get_unhealthy_instances(self):
        """Returns instances in an unhealthy state (MISSING, ERROR, ZOMBIE, etc)

        Most likely the DE will want to terminate these and replace them
        """
