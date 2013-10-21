# Copyright 2013 University of Chicago

from kazoo.security import make_digest_acl
from kazoo.retry import KazooRetry

MAX_NODE_SIZE = 1048576


def check_data(data):
    """Check if data is bigger than the maximum node size supported by zookeeper.

    If it is, throw a ValueError
    """
    if len(data) > MAX_NODE_SIZE:
        raise ValueError("Data is too long (%s Bytes) for zookeeper" % len(data))


def is_zookeeper_enabled(config):

    zk_config = get_zookeeper_config(config)
    if not zk_config:
        return False

    enabled = zk_config.get("enabled", True)

    if isinstance(enabled, basestring):
        enabled = enabled.lower() != "false"

    return enabled


def get_zookeeper_config(config):
    server_config = config.get("server")
    if not server_config:
        return None

    zk_config = server_config.get("zookeeper")

    if not zk_config:
        return None
    return zk_config


def get_auth_data_and_acl(username, password):
    if username and password:
        auth_scheme = "digest"
        auth_credential = "%s:%s" % (username, password)
        auth_data = [(auth_scheme, auth_credential)]
        default_acl = [make_digest_acl(username, password, all=True)]
    elif username or password:
        raise ValueError("both username and password must be specified, if any")
    else:
        auth_data = None
        default_acl = None

    return auth_data, default_acl


def get_kazoo_kwargs(username=None, password=None, timeout=None, use_gevent=False,
        retry_backoff=1.1):
    """Get KazooClient optional keyword arguments as a dictionary
    """
    kwargs = {"retry_backoff": retry_backoff}

    if use_gevent:
        from kazoo.handlers.gevent import SequentialGeventHandler

        kwargs['handler'] = SequentialGeventHandler()

    auth_data, default_acl = get_auth_data_and_acl(username, password)
    if auth_data:
        kwargs['auth_data'] = auth_data
        kwargs['default_acl'] = default_acl

    if timeout:
        kwargs['timeout'] = timeout

    return kwargs


def get_kazoo_retry(**kwargs):
    # start with some defaults
    retry_kwargs = dict(max_tries=-1, backoff=1.2)
    retry_kwargs.update(kwargs)
    return KazooRetry(**retry_kwargs)
