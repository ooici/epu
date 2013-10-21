# Copyright 2013 University of Chicago

from functools import partial
import logging

from epu.exceptions import SiteDefinitionValidationError
from epu.util import get_class

log = logging.getLogger(__name__)

libcloud_ec2_driver_map = {
    "us-east-1": "libcloud.compute.drivers.ec2.EC2NodeDriver",
    "us-west-1": "libcloud.compute.drivers.ec2.EC2USWestNodeDriver",
    "us-west-2": "libcloud.compute.drivers.ec2.EC2USWestOregonNodeDriver",
    "eu-west-1": "libcloud.compute.drivers.ec2.EC2EUNodeDriver",
    "ap-southeast-1": "libcloud.compute.drivers.ec2.EC2APSENodeDriver",
    "ap-northeast-1": "libcloud.compute.drivers.ec2.EC2APNENodeDriver",
    "sa-east-1": "libcloud.compute.drivers.ec2.EC2SAEastNodeDriver"
}


def validate_site(site_description):
    cloud_type = site_description.get("type")
    if cloud_type is None:
        raise SiteDefinitionValidationError("IaaS site description '%s' is missing key 'type'" % site_description)

    secure = site_description.get("secure")
    if secure is not None:
        if secure is not True and secure is not False:
            raise SiteDefinitionValidationError("secure value '%s' is not a valid boolean" % secure)

    if cloud_type == "ec2":
        region = site_description.get("region", "us-east-1")
        if region not in libcloud_ec2_driver_map.keys():
            raise SiteDefinitionValidationError("EC2 region %s is unknown" % region)
    elif cloud_type == "nimbus" or cloud_type == "openstack":
        try:
            site_description["host"]
            port = site_description["port"]
            try:
                port = int(port)
            except ValueError:
                raise SiteDefinitionValidationError("port value '%s' is not a valid port number" % port)
            if port < 1 or port > 65535:
                raise SiteDefinitionValidationError("port value '%s' is not a valid port number" % port)
        except KeyError, e:
            raise SiteDefinitionValidationError(
                "IaaS site description '%s' missing key '%s'" % (site_description, str(e)))


class SiteDriver(object):
    """This class is an abstraction on top of libcloud.

       It translates a site_description of the following form:

           type: ec2
           region: us-west-1 (optional, only used by EC2)
           host: svc.uc.futuregrid.org (required by Nimbus and OpenStack)
           port: 8444 (required by Nimbus and OpenStack)
           secure: True (optional, would default to True)
           path: /api/cloud (optional)

       and a credentials_description of the following form:

           access_key: abcd
           secret_key: efgh
           key_name: phantomkey

       into a libcloud driver.
    """
    def __init__(self, site_description, credentials_description, timeout=None):
        log.debug("Creating SiteDriver from site description '%s'" % site_description)

        cls_kwargs = {}

        # Secure defaults to True
        cls_kwargs["secure"] = site_description.get("secure", True)

        cloud_type = site_description.get("type")
        if cloud_type is None:
            raise KeyError("IaaS site description '%s' is missing key 'type'" % site_description)

        if cloud_type == "ec2":
            region = site_description.get("region", "us-east-1")

            cls_name = libcloud_ec2_driver_map[region]
            if cls_name is None:
                raise ValueError("Unknown libcloud driver for region %s" % region)
        elif cloud_type == "nimbus" or cloud_type == "openstack":
            try:
                cls_kwargs["host"] = site_description["host"]
                cls_kwargs["port"] = site_description["port"]
            except KeyError, e:
                raise KeyError("IaaS site description '%s' missing key '%s'" % (site_description, str(e)))

            if cloud_type == "nimbus":
                cls_name = "libcloud.compute.drivers.ec2.NimbusNodeDriver"
            elif cloud_type == "openstack":
                cls_name = "libcloud.compute.drivers.ec2.EucNodeDriver"
                path = site_description.get("path")
                if path is not None:
                    cls_kwargs["path"] = path
        elif cloud_type == "fake":
            cls_name = "epu.provisioner.test.util.FakeNodeDriver"
        elif cloud_type == "mock-ec2":
            cls_name = "epu.mocklibcloud.MockEC2NodeDriver"
            try:
                cls_kwargs["sqlite_db"] = site_description["sqlite_db"]
            except KeyError, e:
                raise KeyError("IaaS site description '%s' missing key '%s'" % (site_description, str(e)))

        try:
            key = credentials_description["access_key"]
            secret = credentials_description["secret_key"]
        except KeyError, e:
            raise KeyError("IaaS credentials description '%s' missing key '%s'" % (site_description, str(e)))

        cls_kwargs["key"] = key
        cls_kwargs["secret"] = secret

        cls = get_class(cls_name)
        self.driver = partial(cls, **cls_kwargs)()
        try:
            self.driver.connection.timeout = timeout
        except AttributeError:
            # Some mock drivers won't have this attribute
            pass
