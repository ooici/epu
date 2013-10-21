# Copyright 2013 University of Chicago

import logging
import time

from dashi import bootstrap, DashiError
from dashi.exceptions import BadRequestError as DashiBadRequestError

from epu.exceptions import SiteDefinitionValidationError

try:
    from statsd import StatsClient
except ImportError:
    StatsClient = None

from epu.dtrs.core import DTRSCore, CredentialType
from epu.dtrs.store import get_dtrs_store
from epu.exceptions import DeployableTypeLookupError, DeployableTypeValidationError
from epu.util import get_config_paths
import epu.dashiproc

log = logging.getLogger(__name__)


class DTRS(object):
    """Deployable Type Registry Service interface"""

    def __init__(self, *args, **kwargs):
        configs = ["service", "dtrs"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        amqp_uri = kwargs.get('amqp_uri')
        self.amqp_uri = amqp_uri

        self.sysname = kwargs.get('sysname')

        self.dashi = bootstrap.dashi_connect(self.CFG.dtrs.service_name,
                                             self.CFG, self.amqp_uri, self.sysname)

        store = kwargs.get('store')
        self.store = store or get_dtrs_store(self.CFG)
        self.store.initialize()

        self.core = DTRSCore(self.store)

    def start(self):

        log.info("starting DTRS instance %s" % self)

        epu.dashiproc.link_dashi_exceptions(self.dashi)
        self.dashi.link_exceptions(custom_exception=SiteDefinitionValidationError,
                                   dashi_exception=DashiBadRequestError)

        self.dashi.handle(self.add_dt)
        self.dashi.handle(self.describe_dt)
        self.dashi.handle(self.list_dts)
        self.dashi.handle(self.remove_dt)
        self.dashi.handle(self.update_dt)

        self.dashi.handle(self.add_site)
        self.dashi.handle(self.describe_site)
        self.dashi.handle(self.list_sites)
        self.dashi.handle(self.remove_site)
        self.dashi.handle(self.update_site)

        self.dashi.handle(self.add_credentials)
        self.dashi.handle(self.describe_credentials)
        self.dashi.handle(self.list_credentials)
        self.dashi.handle(self.remove_credentials)
        self.dashi.handle(self.update_credentials)

        self.dashi.handle(self.lookup)

        self.dashi.consume()

    def stop(self):
        self.dashi.cancel()
        self.dashi.disconnect()
        self.store.shutdown()

    # Deployable Types
    def add_dt(self, caller, dt_name, dt_definition):
        return self.core.store.add_dt(caller, dt_name, dt_definition)

    def describe_dt(self, caller, dt_name):
        return self.core.describe_dt(caller, dt_name)

    def list_dts(self, caller):
        return self.core.store.list_dts(caller)

    def remove_dt(self, caller, dt_name):
        return self.core.store.remove_dt(caller, dt_name)

    def update_dt(self, caller, dt_name, dt_definition):
        return self.core.store.update_dt(caller, dt_name, dt_definition)

    # Sites

    def add_site(self, site_name, site_definition, caller=None):
        return self.core.add_site(caller, site_name, site_definition)

    def describe_site(self, site_name, caller=None):
        return self.core.describe_site(caller, site_name)

    def list_sites(self, caller=None):
        return self.core.store.list_sites(caller)

    def remove_site(self, site_name, caller=None):
        return self.core.store.remove_site(caller, site_name)

    def update_site(self, site_name, site_definition, caller=None):
        return self.core.update_site(caller, site_name, site_definition)

    # Credentials

    # note: credential_type was added afterwards. kept at the end, and with a default value to
    # avoid breaking existing code. also left site_name argument unchanged, even though it is
    # just "name" elsewhere in the DTRS.

    def add_credentials(self, caller, site_name, site_credentials, credential_type=CredentialType.SITE):
        # Above function signature kept for backward compatibility
        name = site_name
        credentials = site_credentials
        return self.core.add_credentials(caller, credential_type, name, credentials)

    def describe_credentials(self, caller, site_name, credential_type=CredentialType.SITE):
        # Above function signature kept for backward compatibility
        name = site_name
        return self.core.describe_credentials(caller, credential_type, name)

    def list_credentials(self, caller, credential_type=CredentialType.SITE):
        return self.core.store.list_credentials(caller, credential_type)

    def remove_credentials(self, caller, site_name, credential_type=CredentialType.SITE):
        # Above function signature kept for backward compatibility
        name = site_name
        return self.core.store.remove_credentials(caller, credential_type, name)

    def update_credentials(self, caller, site_name, site_credentials, credential_type=CredentialType.SITE):
        # Above function signature kept for backward compatibility
        name = site_name
        credentials = site_credentials
        return self.core.store.update_credentials(caller, credential_type, name, credentials)

    # Old DTRS methods - keeping the API unmodified for now

    def lookup(self, caller, dt_name, dtrs_request_node, vars):
        return self.core.lookup(caller, dt_name, dtrs_request_node, vars)


def statsd(func):
    def call(dtrs_client, *args, **kwargs):
        before = time.time()
        ret = func(dtrs_client, *args, **kwargs)
        after = time.time()
        if dtrs_client.statsd_client is not None:
            try:
                client_name = dtrs_client.client_name or "dtrs_client"
                dtrs_client.statsd_client.timing('%s.%s.timing' % (client_name, func.__name__), (after - before) * 1000)
                dtrs_client.statsd_client.incr('%s.%s.count' % (client_name, func.__name__))
            except:
                log.exception("Failed to submit metrics")
        return ret
    return call


class DTRSClient(object):

    def __init__(self, dashi, topic=None, statsd_cfg=None, client_name=None):
        self.dashi = dashi
        self.topic = topic or 'dtrs'
        self.client_name = client_name
        self.statsd_client = None
        if statsd_cfg is not None:
            try:
                host = statsd_cfg["host"]
                port = statsd_cfg["port"]
                log.info("Setting up statsd client with host %s and port %d" % (host, port))
                self.statsd_client = StatsClient(host, port)
            except:
                log.exception("Failed to set up statsd client")

    @statsd
    def add_dt(self, caller, dt_name, dt_definition):
        return self.dashi.call(self.topic, 'add_dt', caller=caller,
                               dt_name=dt_name, dt_definition=dt_definition)

    @statsd
    def describe_dt(self, caller, dt_name):
        return self.dashi.call(self.topic, 'describe_dt', caller=caller,
                               dt_name=dt_name)

    @statsd
    def list_dts(self, caller):
        return self.dashi.call(self.topic, 'list_dts', caller=caller)

    @statsd
    def remove_dt(self, caller, dt_name):
        return self.dashi.call(self.topic, 'remove_dt', caller=caller,
                               dt_name=dt_name)

    @statsd
    def update_dt(self, caller, dt_name, dt_definition):
        return self.dashi.call(self.topic, 'update_dt', caller=caller,
                               dt_name=dt_name, dt_definition=dt_definition)

    @statsd
    def add_site(self, site_name, site_definition, caller=None):
        return self.dashi.call(self.topic, 'add_site', caller=caller, site_name=site_name,
                               site_definition=site_definition)

    @statsd
    def describe_site(self, site_name, caller=None):
        return self.dashi.call(self.topic, 'describe_site', caller=caller,
                               site_name=site_name)

    @statsd
    def list_sites(self, caller=None):
        return self.dashi.call(self.topic, 'list_sites', caller=caller)

    @statsd
    def remove_site(self, site_name, caller=None):
        return self.dashi.call(self.topic, 'remove_site', caller=caller, site_name=site_name)

    @statsd
    def update_site(self, site_name, site_definition, caller=None):
        return self.dashi.call(self.topic, 'update_site', site_name=site_name, site_definition=site_definition,
                               caller=caller)

    @statsd
    def add_credentials(self, caller, site_name, site_credentials, credential_type=CredentialType.SITE):
        return self.dashi.call(self.topic, 'add_credentials', caller=caller,
                               site_name=site_name,
                               site_credentials=site_credentials,
                               credential_type=credential_type)

    @statsd
    def describe_credentials(self, caller, site_name, credential_type=CredentialType.SITE):
        return self.dashi.call(self.topic, 'describe_credentials',
                               caller=caller, site_name=site_name,
                               credential_type=credential_type)

    @statsd
    def list_credentials(self, caller, credential_type=CredentialType.SITE):
        return self.dashi.call(self.topic, 'list_credentials', caller=caller,
                               credential_type=credential_type)

    @statsd
    def remove_credentials(self, caller, site_name, credential_type=CredentialType.SITE):
        return self.dashi.call(self.topic, 'remove_credentials', caller=caller,
                               site_name=site_name,
                               credential_type=credential_type)

    @statsd
    def update_credentials(self, caller, site_name, site_credentials, credential_type=CredentialType.SITE):
        return self.dashi.call(self.topic, 'update_credentials', caller=caller,
                               site_name=site_name,
                               site_credentials=site_credentials,
                               credential_type=credential_type)

    # Old DTRS methods - keeping the API unmodified for now

    @statsd
    def lookup(self, caller, dt_name, dtrs_request_node, vars=None):
        try:
            ret = self.dashi.call(self.topic, 'lookup', caller=caller,
                                  dt_name=dt_name,
                                  dtrs_request_node=dtrs_request_node,
                                  vars=vars)
        except DashiError, e:
            exception_class, _, exception_message = str(e).partition(':')
            if exception_class == 'DeployableTypeLookupError':
                raise DeployableTypeLookupError(
                    "Unknown deployable type name: %s" % dt_name)
            elif exception_class == 'DeployableTypeValidationError':
                raise DeployableTypeValidationError(dt_name, exception_message)
            else:
                raise

        return ret


def main():
    epu.dashiproc.epu_register_signal_stack_debug()
    dtrs = DTRS()
    dtrs.start()
