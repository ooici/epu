
import logging

log = logging.getLogger(__name__)  


def dummy_dispatch_process_callback(*args, **kwargs):
    log.debug("dummy_dispatch_process_callback(%s, %s) called" % args, kwargs)

def dummy_terminate_process_callback(*args, **kwargs):
    log.debug("dummy_terminate_process_callback(%s, %s) called" % args, kwargs)

class NPreservingPolicy(object):

    def __init__(self, parameters=None, dispatch_process_callback=None, terminate_process_callback=None):

        self.dispatch_process_callback = dispatch_process_callback or dummy_dispatch_process_callback
        self.terminate_process_callback = terminate_process_callback or dummy_terminate_process_callback

        self.parameters = parameters


    def update_paramers(new_parameters):
        self.parameters = new_parameters

    
    def apply_policy(all_procs):
        if not self.parameters:
            log.warning("No parameters set. Returning early")
            return

        

