from twisted.internet import defer

class ControllerStore(object):
    """Passthrough "persistence" for EPU Controller state

    The same interface is used for real persistence.
    """

    def add_instance(self, instance):
        """Adds a new instance object to persistence
        @param instance Instance to add
        @retval Deferred
        """
        return defer.succeed(None)

    def add_sensor(self, sensor):
        """Adds a new sensor object to persistence
        @param sensor Sensor to add
        @retval Deferred
        """
        return defer.succeed(None)

