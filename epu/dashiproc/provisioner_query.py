import dashi.bootstrap as bootstrap
from dashi.util import LoopingCall

import os
import uuid
import logging

from epu.dashiproc.provisioner import ProvisionerClient
from epu.util import determine_path

DEFAULT_QUERY_INTERVAL = 10.0

class ProvisionerQueryService(object):

    topic = "provisioner_query"

    def __init__(self, *args, **kwargs):

        service_config = os.path.join(determine_path(), "config", "service.yml")
        provisioner_config = os.path.join(determine_path(), "config", "provisioner.yml")
        config_files = [service_config, provisioner_config]
        self.CFG = bootstrap.configure(config_files)

        self.log = logging.getLogger()

        try:
            bootstrap.enable_gevent()
        except:
            self.log.warning("gevent not available. Falling back to threading")

        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG)

        interval = kwargs.get('interval_seconds')
        self.interval = interval or DEFAULT_QUERY_INTERVAL

        self.client = ProvisionerClient(self.dashi)



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

