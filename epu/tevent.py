# Copyright 2013 University of Chicago

import os
import sys
import socket
import weakref
import traceback
import threading
from multiprocessing.pool import ThreadPool
"""
Helper functions for working with stdlib threading library

Inspired by the gevent api
"""


def spawn(func, *args, **kwargs):
    """spawn - spawn and start a thread

    @param func - function to run in the thread
    @param fail_fast - if set to True, if this thread raises an
        unhandled exception, it will terminate the parent process
    @param exit - alternate exit function to call to bring down
        the process
    """
    if hasattr(func, 'im_class'):
        name = "%s.%s" % (func.im_class.__name__, func.__name__)
    else:
        name = func.__name__

    if '_fail_fast' in kwargs:
        fail_fast = kwargs['_fail_fast']
        del(kwargs['_fail_fast'])
    else:
        fail_fast = None

    if '_exit' in kwargs:
        exit = kwargs['_exit']
        del(kwargs['_exit'])
    else:
        exit = None

    if fail_fast is True:
        def critical_wrapper():
            try:
                func()
            except (Exception, socket.timeout) as e:
                msg = "%s Thread has died (%s), but it is critical. Exiting." % (name, e)
                print >> sys.stderr, msg
                if exit is None:
                    traceback.print_exc()
                    os._exit(os.EX_SOFTWARE)
                else:
                    exit()

        _func = critical_wrapper
    else:
        _func = func

    _thread = threading.Thread(target=_func, name=name, args=tuple(args), kwargs=kwargs)
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
        self._patch_current_thread()
        ThreadPool.__init__(self, *args, **kwargs)

    def _patch_current_thread(self):
        """We need to patch threading to support ThreadPool being run in
        child threads.

        Shouldn't be necessary when/if http://bugs.python.org/issue10015 is fixed
        """

        if not hasattr(threading.current_thread(), "_children"):
            threading.current_thread()._children = weakref.WeakKeyDictionary()

    def spawn(self, func, *args, **kwargs):
        self._patch_current_thread()
        self.apply_async(func, tuple(args), kwargs)

    def join(self):
        self._patch_current_thread()
        self.close()
        ThreadPool.join(self)
