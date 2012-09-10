from os import environ

def patch_extras():
    # workaround for http://code.google.com/p/gevent/issues/detail?id=112
    # gevent isn't patching threading._sleep which causes problems
    # for Condition objects
    from gevent import sleep
    import threading
    threading._sleep = sleep

if environ.get('EPU_USE_GEVENT'):
    from gevent import monkey; monkey.patch_all()
    patch_extras()
