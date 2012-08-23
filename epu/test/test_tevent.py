
import time
import threading

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
