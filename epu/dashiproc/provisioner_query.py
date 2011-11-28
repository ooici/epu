from dashi import DashiConnection
from dashi.bootstrap import Service
from dashi.util import LoopingCall

from epu.dashiproc.provisioner import ProvisionerClient
from epu.dashiproc.util import get_config_files

DEFAULT_QUERY_INTERVAL = 10.0

class ProvisionerQueryService(Service):

    topic = "provisioner_query"

    def __init__(self, *args, **kwargs):

        config_files = get_config_files("service") + get_config_files("provisioner")
        logging_config_files = get_config_files("logging")
        self.configure(config_files, logging_config_files)

        self.log = self.get_logger()

        interval = kwargs.get('interval_seconds')
        self.interval = interval or DEFAULT_QUERY_INTERVAL

        self.client = ProvisionerClient()

        try:
            self.enable_gevent()
        except:
            self.log.warning("gevent not available. Falling back to threading")

        self.dashi_connect()

    def start(self):

        self.log.debug("Starting provisioner query loop - %s second interval" %
                       self.interval)

        self.loop = LoopingCall(self.query)
        self.loop.start(self.interval)


    def query(self):
        try:
            self._do_query()
        except Exception, e:
            self.log.error("Error sending provisioner query request: %s", e,
                                  exc_info=True)

    def _do_query(self):
        self.log.debug("Sending query request to provisioner")
        self.client.query()

