import logging

from dashi import bootstrap, DashiError
from dashi.exceptions import NotFoundError as DashiNotFoundError
from dashi.exceptions import WriteConflictError as DashiWriteConflictError

from epu.dtrs.core import DTRSCore
from epu.dtrs.store import get_dtrs_store
from epu.exceptions import DeployableTypeLookupError, DeployableTypeValidationError, NotFoundError, WriteConflictError
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

        self.dashi = bootstrap.dashi_connect(self.CFG.dtrs.service_name,
                                             self.CFG, self.amqp_uri)

        store = kwargs.get('store')
        self.store = store or get_dtrs_store(self.CFG)
        self.store.initialize()

        self.core = DTRSCore(self.store)

    def start(self):

        log.info("starting DTRS instance %s" % self)

        self.dashi.link_exceptions(custom_exception=NotFoundError,
                                   dashi_exception=DashiNotFoundError)
        self.dashi.link_exceptions(custom_exception=WriteConflictError,
                                   dashi_exception=DashiWriteConflictError)

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

    def add_site(self, site_name, site_definition):
        return self.core.store.add_site(site_name, site_definition)

    def describe_site(self, site_name):
        return self.core.describe_site(site_name)

    def list_sites(self):
        return self.core.store.list_sites()

    def remove_site(self, site_name):
        return self.core.store.remove_site(site_name)

    def update_site(self, site_name, site_definition):
        return self.core.store.update_site(site_name, site_definition)

    # Credentials

    def add_credentials(self, caller, site_name, site_credentials):
        return self.core.add_credentials(caller, site_name, site_credentials)

    def describe_credentials(self, caller, site_name):
        return self.core.describe_credentials(caller, site_name)

    def list_credentials(self, caller):
        return self.core.store.list_credentials(caller)

    def remove_credentials(self, caller, site_name):
        return self.core.store.remove_credentials(caller, site_name)

    def update_credentials(self, caller, site_name, site_credentials):
        return self.core.store.update_credentials(caller, site_name,
                                                  site_credentials)

    # Old DTRS methods - keeping the API unmodified for now

    def lookup(self, caller, dt_name, dtrs_request_node, vars):
        return self.core.lookup(caller, dt_name, dtrs_request_node, vars)


class DTRSClient(object):

    def __init__(self, dashi, topic=None):
        self.dashi = dashi
        self.topic = topic or 'dtrs'

    def add_dt(self, caller, dt_name, dt_definition):
        return self.dashi.call(self.topic, 'add_dt', caller=caller,
                               dt_name=dt_name, dt_definition=dt_definition)

    def describe_dt(self, caller, dt_name):
        return self.dashi.call(self.topic, 'describe_dt', caller=caller,
                               dt_name=dt_name)

    def list_dts(self, caller):
        return self.dashi.call(self.topic, 'list_dts', caller=caller)

    def remove_dt(self, caller, dt_name):
        return self.dashi.call(self.topic, 'remove_dt', caller=caller,
                               dt_name=dt_name)

    def update_dt(self, caller, dt_name, dt_definition):
        return self.dashi.call(self.topic, 'update_dt', caller=caller,
                               dt_name=dt_name, dt_definition=dt_definition)

    def add_site(self, site_name, site_definition):
        return self.dashi.call(self.topic, 'add_site', site_name=site_name,
                               site_definition=site_definition)

    def describe_site(self, site_name):
        return self.dashi.call(self.topic, 'describe_site',
                               site_name=site_name)

    def list_sites(self):
        return self.dashi.call(self.topic, 'list_sites')

    def remove_site(self, site_name):
        return self.dashi.call(self.topic, 'remove_site', site_name=site_name)

    def update_site(self, site_name, site_definition):
        return self.dashi.call(self.topic, 'update_site', site_name=site_name,
                               site_definition=site_definition)

    def add_credentials(self, caller, site_name, site_credentials):
        return self.dashi.call(self.topic, 'add_credentials', caller=caller,
                               site_name=site_name,
                               site_credentials=site_credentials)

    def describe_credentials(self, caller, site_name):
        return self.dashi.call(self.topic, 'describe_credentials',
                               caller=caller, site_name=site_name)

    def list_credentials(self, caller):
        return self.dashi.call(self.topic, 'list_credentials', caller=caller)

    def remove_credentials(self, caller, site_name):
        return self.dashi.call(self.topic, 'remove_credentials', caller=caller,
                               site_name=site_name)

    def update_credentials(self, caller, site_name, site_credentials):
        return self.dashi.call(self.topic, 'update_credentials', caller=caller,
                               site_name=site_name,
                               site_credentials=site_credentials)

    # Old DTRS methods - keeping the API unmodified for now

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
