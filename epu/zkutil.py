from kazoo.security import make_digest_acl
from kazoo.handlers.gevent import SequentialGeventHandler

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

def get_kazoo_kwargs(username=None, password=None, timeout=None, use_gevent=False):
    """Get KazooClient optional keyword arguments as a dictionary
    """
    kwargs = {}

    if use_gevent:
        kwargs['handler'] = SequentialGeventHandler()

    auth_data, default_acl = get_auth_data_and_acl(username, password)
    if auth_data:
        kwargs['auth_data'] = auth_data
        kwargs['default_acl'] = default_acl

    if timeout:
        kwargs['timeout'] = timeout

    return kwargs
