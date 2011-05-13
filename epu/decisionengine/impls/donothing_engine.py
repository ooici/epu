import ion.util.ionlog
log = ion.util.ionlog.getLogger(__name__)

from epu.decisionengine import Engine

class DoNothingEngine(Engine):
    """
    A decision engine that does nothing.  This is useful for rapid
    engine development: launch the infrastructure with this engine
    and ssh in to the controller node and kill/relaunch the controller
    unix process as you develop.
    """
    
    def __init__(self):
        super(DoNothingEngine, self).__init__()

    def initialize(self, control, state, conf=None):
        """Engine API method"""
        parameters = {"timed-pulse-irregular":2500}
        control.configure(parameters)

    def decide(self, control, state):
        """Engine API method"""
        log.debug("Decide called, # of instances: %d" % len(state.instances))
