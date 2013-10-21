# Copyright 2013 University of Chicago

import logging

from epu.states import InstanceState, DecisionEngineState, InstanceHealthState

log = logging.getLogger(__name__)


class Engine(object):
    """
    This is the superclass for any implementation of the state object that
    is passed to the decision engine.  The state object is a way for the
    engine to find out relevant information that has been collected by the
    EPU Controller.

    The abc (abstract base class) module is not present in Python 2.5 but
    Engine should be treated as such.  It is not meant to be instantiated
    directly.

    @note See the decision engine implementer's guide for more information.

    """

    def __init__(self):
        self.de_state = DecisionEngineState.PENDING

    def initialize(self, control, state, conf=None):
        """
        Give the engine a chance to initialize.  The current state of the
        system is given as well as a mechanism for the engine to offer the
        controller input about how often it should be called.

        @note Must be invoked and return before the 'decide' method can
        legally be invoked.

        @param control instance of Control, used to request changes to system
        @param state instance of State, used to obtain any known information
        @param conf None or dict of key/value pairs
        @exception Exception if engine cannot reach a sane state

        """
        raise NotImplementedError

    def reconfigure(self, control, newconf):
        """
        Give the engine a new configuration.

        @note There must not be a decide call in progress when this is called,
        and there must not be a new decide call while this is in progress.

        @param control instance of Control, used to request changes to system
        @param newconf None or dict of key/value pairs
        @exception Exception if engine cannot reach a sane state
        @exception NotImplementedError if engine does not support this

        """
        raise NotImplementedError

    def decide(self, control, state):
        """
        Give the engine a chance to act on the current state of the system.

        @note May only be invoked once at a time.
        @note When it is invoked is up to EPU Controller policy and engine
        preferences, see the decision engine implementer's guide.

        @param control instance of Control, used to request changes to system
        @param state instance of State, used to obtain any known information
        @retval None
        @exception Exception if the engine has been irrevocably corrupted

        """
        raise NotImplementedError

    def dying(self):
        """
        Give the engine a chance to act on its death
        """
        pass

    def _set_state(self, all_instances, needed_num, health_not_checked=True):
        """
        Sets the state to STABLE if the length of the instances list is equal
        to the needed_num *and* each state in the list is RUNNING (contextualized).

        needed_num can be -1 to signal to disregard it

        Override this if you need separate logic.
        """

        if needed_num >= 0:
            if len(all_instances) != needed_num:
                self.de_state = DecisionEngineState.PENDING
                return

        for instance in all_instances:
            if instance.state < InstanceState.RUNNING:
                if not health_not_checked and instance.health == InstanceHealthState.OK:
                    log.warn("State is not yet %s but we have an OK health reading for instance '%s'" % (
                        InstanceState.RUNNING, instance.instance_id))
                self.de_state = DecisionEngineState.PENDING
                return
            if not health_not_checked and instance.state == InstanceState.RUNNING:
                if instance.health != InstanceHealthState.OK:
                    log.debug("Instance '%s' is contextualized, but health is '%s'" % (
                        instance.instance_id, instance.health))
                    self.de_state = DecisionEngineState.PENDING
                    return

        self.de_state = DecisionEngineState.STABLE

    def _set_state_pending(self):
        """Force the state to be pending"""
        self.de_state = DecisionEngineState.PENDING

    def _set_state_stable(self):
        """Force the state to be stable"""
        self.de_state = DecisionEngineState.STABLE

    def _set_state_devmode_failed(self):
        """Force the state to be devmode failed"""
        self.de_state = DecisionEngineState.DEVMODE_FAILED

    def _set_devmode(self, conf):
        """Configure devmode_no_failure_compensation which, if set, instructs the
        EPU controller to never respond to failures.  This is used for development
        and automated integration tests where restarting on failure will mask
        problems instead of solving them.

        If the configuration is set in any way but fails to match "true" (including if
        it's not a string object), it will be set to false.
        """

        # no change (this method is used from reconfigure as well, so we cannot assume absense of the
        # config always implies disabling it: just "no change").
        if not conf:
            return
        if "devmode_no_failure_compensation" not in conf:
            return

        # config is present
        val = conf["devmode_no_failure_compensation"]

        # any configuration present (including None) but not equal to "true" means disable it.
        if not val:
            self.devmode_no_failure_compensation = False
            return
        if not isinstance(val, str):
            log.warn("devmode_no_failure_compensation configuration is not a string?")
            self.devmode_no_failure_compensation = False
            return

        if val.lower().strip() == "true":
            self.devmode_no_failure_compensation = True
            log.warn("devmode_no_failure_compensation configuration is ENABLED")
        else:
            self.devmode_no_failure_compensation = False
