
# errors used in ZooKeeper storage abstraction

class WriteConflictError(Exception):
    """A write to the store conflicted with another write
    """

class NotFoundError(Exception):
    """Object not found in store
    """
  