from functools import partial

from epu.util import get_class


class SiteDriver(object):
    def __init__(self, site_description=None, credentials_description=None):
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

        cls = get_class(cls_name)

        self.driver = partial(cls, **cls_kwargs)()
