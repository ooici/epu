import weakref
import threading
import signal
from multiprocessing.pool import ThreadPool
"""
Helper functions for working with stdlib threading library

Inspired by the gevent api
"""

def spawn(func, *args, **kwargs):
    if hasattr(func, 'im_class'):
        name = "%s.%s" % (func.im_class.__name__, func.__name__)
    else:
        name = func.__name__

    _thread = threading.Thread(target=func, name=name, args=tuple(args), kwargs=kwargs)
    _thread.daemon = True
    _thread.start()
    return _thread


def joinall(threads):
    for thread in threads:
        thread.join()


class Pool(ThreadPool):
    """Subclass multiprocessing's ThreadPool to have a similar API to gevent
    """

    def __init__(self, *args, **kwargs):
        """We need to patch threading to support ThreadPool being run in 
        child threads. 

        Shouldn't be necessary when http://bugs.python.org/issue10015 is fixed
        """

        if not hasattr(threading.current_thread(), "_children"):
            threading.current_thread()._children = weakref.WeakKeyDictionary()

        ThreadPool.__init__(self, *args, **kwargs)

    def spawn(self, func, *args, **kwargs):

        self.apply_async(func, tuple(args), kwargs)

    def join(self):
        self.close()
        ThreadPool.join(self)
