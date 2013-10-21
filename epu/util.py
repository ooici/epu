# Copyright 2013 University of Chicago

import os
import sys
import string
import numbers
from datetime import timedelta

from epu import rfc3339

from epu.exceptions import UserNotPermittedError


# .-_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789
_VALID = ".-_%s%s" % (string.ascii_letters, string.digits)
_VALID_SET = frozenset(_VALID)


def is_valid_identifier(ident):
    if not (isinstance(ident, basestring) and ident):
        return False
    return set(ident) <= _VALID_SET


def get_class(kls):
    """Get a class by name
    """
    parts = kls.split('.')
    if parts < 2:
        raise ValueError("expecting module and class separated by .")
    module = ".".join(parts[:-1])
    m = __import__(module)
    for comp in parts[1:]:
        m = getattr(m, comp)
    return m


def determine_path():
    """find path of current file,

    Borrowed from wxglade.py"""
    try:
        root = __file__
        if os.path.islink(root):
            root = os.path.realpath(root)
        return os.path.dirname(os.path.abspath(root))
    except:
        print "I'm sorry, but something is wrong."
        print "There is no __file__ variable. Please contact the author."
        sys.exit()


def get_config_paths(configs):
    """converts a list of config file names to a list of absolute paths
    to those config files, like so:

    get_config_files(["service", "provisioner"]

    returns:

    ["/path/to/epu/config/service.yml", "/path/to/epu/config/provisioner.yml"]
    """

    if not isinstance(configs, list):
        raise ValueError("get_config_files expects a list of configs")

    module_path = determine_path()
    config_dir = os.path.join(module_path, "config")

    paths = []
    for config in configs:
        if not config.endswith(".yml"):
            config = "%s.yml" % config
        path = os.path.join(config_dir, config)
        paths.append(path)

    return paths


unspecified = object()


def check_user(caller=unspecified, creator=unspecified, operation=None):

    if caller is unspecified and creator is unspecified:
        raise TypeError("You must provide a caller and an owner")

    if caller is None:
        # None is considered superuser, permit always
        return

    if not operation:
        operation = "Operation"

    if caller != creator:
        msg = "%s not permitted, creator is %s and caller is %s" % (
            operation, creator, caller)
        raise UserNotPermittedError(msg)


UTC = rfc3339.UTC_TZ


def now_datetime():
    return rfc3339.now()


def parse_datetime(s):
    """Parse a string into a datetime object
    """
    return rfc3339.parse_datetime(s)


def ceiling_datetime(d, now=None):
    if now is None:
        now = rfc3339.now()

    if d > now:
        return now
    return d


def ensure_timedelta(t):
    if isinstance(t, timedelta):
        return t

    if isinstance(t, numbers.Real):
        return timedelta(seconds=t)

    raise TypeError("cannot convert %s to timedelta" % (t,))
