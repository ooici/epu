# Copyright 2013 University of Chicago

import time
import threading

from mock import Mock

import epu.tevent as tevent

global_var = None
global_args = None
global_kwargs = None


def test_spawn():
    global global_var
    global global_args
    global global_kwargs

    def thread_function(*args, **kwargs):
        global global_var
        global global_kwargs
        global global_args

        global_var = True
        global_kwargs = kwargs
        global_args = args

        time.sleep(1)

    global_var = False
    test_args = ('such', 'good', 'args',)
    test_kwargs = {'my': 'really', 'great': 'parameters'}

    mythread = tevent.spawn(thread_function, *test_args, **test_kwargs)

    assert mythread.__class__ == threading.Thread().__class__

    time.sleep(0.5)
    assert global_var is True
    assert global_args == test_args
    assert global_kwargs == test_kwargs


def test_fail_fast():
    mock_exit = Mock()

    def raises_typerror():
        raise TypeError("This thread failed!")

    tevent.spawn(raises_typerror, _fail_fast=True, _exit=mock_exit)
