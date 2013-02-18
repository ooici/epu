import time
import socket
import threading

from mock import Mock

import epu.tevent as tevent

global_var = None

def test_spawn():
    global global_var

    def thread_function():
        global global_var
        global_var = True
        time.sleep(1)

    global_var = False

    mythread = tevent.spawn(thread_function)

    assert mythread.__class__ == threading.Thread().__class__
    
    time.sleep(0.5)
    assert global_var == True

def test_fail_fast():
    mock_exit = Mock()

    def raises_typerror():
        raise TypeError("This thread failed!")

    tevent.spawn(raises_typerror, fail_fast=True, exit=mock_exit)

