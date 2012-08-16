import threading
"""
Helper functions for working with stdlib threading library

Inspired by the gevent api
"""

def spawn(func, *args, **kwargs):
    _thread = threading.Thread(target=func, args=tuple(args), kwargs=kwargs)
    _thread.daemon = True
    _thread.start()
    return _thread

def joinall(threads):
    for thread in threads:
        thread.join()
