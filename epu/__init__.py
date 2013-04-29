from os import environ
import logging


def patch_extras():
    # workaround for http://code.google.com/p/gevent/issues/detail?id=112
    # gevent isn't patching threading._sleep which causes problems
    # for Condition objects
    from gevent import sleep
    import threading
    threading._sleep = sleep

if environ.get('EPU_USE_GEVENT'):
    from gevent import monkey
    monkey.patch_all()
    patch_extras()

if not environ.get('AMQP_DEBUG_LOGGING'):
    amqp_logger = logging.getLogger("amqp")
    amqp_logger.setLevel(logging.INFO)
