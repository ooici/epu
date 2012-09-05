
# errors used in ZooKeeper storage abstraction

class WriteConflictError(Exception):
    """A write to the store conflicted with another write
    """

class NotFoundError(Exception):
    """Object not found in store
    """

class UserNotPermittedError(Exception):
    """Action was disallowed because action is not permitted
    by this user
    """

class ProgrammingError(Exception):
    """Something that you wouldn't expect to be able to happen happened.
    Must be the programmer's fault
    """

class GeneralIaaSException(Exception):
    """
    Unknown Exceptions that have come from the provisioner when communicating with IaaS
    """


# Exceptions used by DTRS
class DeployableTypeLookupError(Exception):
    pass

class DeployableTypeValidationError(Exception):
    """Problem validating a deployable type
    """
    def __init__(self, dt_name, *args, **kwargs):
        self.dt_name = dt_name
        Exception.__init__(self, *args, **kwargs)

    def __str__(self):
        return "Deployable Type '%s': %s" % (self.dt_name,
                                             Exception.__str__(self))
