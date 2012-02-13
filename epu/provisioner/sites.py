from functools import partial
import logging

from epu.util import get_class

log = logging.getLogger(__name__)

class ProvisionerSites(object):

    def __init__(self, site_config, driver_creators=None):

        # for tests
        if driver_creators:
            self.driver_creators = driver_creators
        else:
            self.driver_creators = _get_driver_creators(site_config)

    #TODO implement pooling of drivers, to reuse connections
    def acquire_driver(self, site):

        #TODO right now just acquiring a new driver everytime
        libcloud_driver = self.driver_creators[site]()
        return SiteDriver(site, libcloud_driver, self)

    def release_driver(self, driver):
        pass


class SiteDriver(object):
    def __init__(self, site, driver, manager):
        self.site = site
        self.driver = driver

        # used to release the connection on __exit__
        self._manager =  manager

    def release(self):
        self._manager.release_driver(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()


def _get_driver_creators(site_config):
    """Loads IaaS drivers from config block

    returns a dict mapping site name to a function which creates the driver
    """
    if not site_config:
        log.warning("No sites configured")
        return {}

    drivers = {}
    for site, spec in site_config.iteritems():
        try:
            cls_name = spec["driver_class"]
            cls_kwargs = spec["driver_kwargs"]
        except KeyError,e:
            raise KeyError("IaaS site description '%s' missing key '%s'" % (site, str(e)))

        cls = get_class(cls_name)

        drivers[site] = partial(cls, **cls_kwargs)

    return drivers