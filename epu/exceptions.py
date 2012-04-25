
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


class GeneralIaaSException(Exception):
    """
    Unknown Exceptions that have come from the provisioner when communicating with IaaS
    """