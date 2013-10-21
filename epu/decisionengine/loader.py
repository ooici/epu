# Copyright 2013 University of Chicago

import logging

from epu.util import get_class
log = logging.getLogger(__name__)


class EngineLoader(object):
    """
    This class instantiates decision engine objects based on classname.

    """

    def __init__(self):
        pass

    def load(self, classname):
        """Return instance of specified decision engine"""
        log.debug("Loading Decision Engine '%s'" % classname)
        kls = get_class(classname)
        if not kls:
            raise Exception("Cannot find decision engine implementation: '%s'" % classname)
        # Could use zope.interface in the future and check implementedBy here,
        # but it is not actually that helpful.

        engine = kls()
        log.info("Loaded Decision Engine '%s'" % str(engine))
        return engine
