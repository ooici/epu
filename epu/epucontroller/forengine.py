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
    
    def launch(self, deployable_type_id, launch_description, extravars=None):
        """
        Choose instance IDs for each instance desired, a launch ID and send
        appropriate message to Provisioner.
        
        @param deployable_type_id string identifier of the DP to launch
        @param launch_description See engine implementer's guide
        @param extravars Optional, see engine implementer's guide
        @retval tuple (launch_id, launch_description), see guide
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
    
    def destroy_launch(self, launch_id):
        """
        Terminate an entire launch.
        
        @param launch_id launch to terminate
        @retval None
        @exception Exception illegal input/unknown ID
        @exception Exception message not sent
        
        """
        raise NotImplementedError

class LaunchItem(object):
    """
    Values of the launch_description dict are of this type
    It has simple Python attributes (no property decorators).
    The instance_ids list is not populated when the object is created.
    
    """
    
    def __init__(self, num_instances, allocation_id, site, data):
        """
        @param num_instances integer, count of nodes necessary
        @param allocation_id string (Provisioner must recognize)
        @param site string, where to launch (Provisioner must recognize)
        @param data dict of arbitrary k/v passed to node via contextualization 
        
        """
        
        # TODO: validation
        self.num_instances = int(num_instances)
        self.allocation_id = str(allocation_id)
        self.site = str(site)
        self.data = data
        self.instance_ids = []


class SensorItem(object):
    """
    One data reading that the EPU Controller knows about.
    It has simple Python attributes (no property decorators).
    
    """
    def __init__(self, sensor_id, time, value):
        """
        @param key unique identifier, depends on the type
        @param time integer,  unixtime data was obtained by EPU Controller
        @param value arbitrary object
        """
        self.sensor_id = str(sensor_id)
        self.time = long(time)
        self.value = value


class Instance(object):
    """
    One instance state object. This object is considered immutable and is
    replaced, not changed.

    This object exposes many values as attributes, not a fixed set.
    The following will be always be available: instance_id, launch_id, site,
        allocation, state, state_time, health.

    The set of available properties can be found with items(), iteritems(),
    keys(), etc.
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

        @retval Deferred
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