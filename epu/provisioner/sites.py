import logging

from functools import partial

from epu.util import get_class

log = logging.getLogger(__name__)

class SiteDriver(object):
    def __init__(self, site_description=None, credentials_description=None, timeout=None):
  
        log.info("site desc %s" % site_description)

        try:
            cls_name = site_description["driver_class"]
            cls_kwargs = site_description.get("driver_kwargs", {})
        except KeyError, e:
            raise KeyError("IaaS site description '%s' missing key '%s'" % (site_description["name"], str(e)))

        try:
            key = credentials_description["access_key"]
            secret = credentials_description["secret_key"]
        except KeyError, e:
            raise KeyError("IaaS credentials description '%s' missing key '%s'" % (site_description["name"], str(e)))

        cls_kwargs["key"] = key
        cls_kwargs["secret"] = secret
        cls_kwargs["timeout"] = timeout

        cls = get_class(cls_name)
        self.driver = partial(cls, **cls_kwargs)()
        try:
            self.driver.connection.timeout = timeout
        except AttributeError:
            # Some mock drivers won't have this attribute
            pass
