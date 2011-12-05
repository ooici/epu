import os
import time
import logging

from dashi import DashiConnection
import dashi.bootstrap as bootstrap
from dashi.util import LoopingCall

from epu.dashiproc.provisioner import ProvisionerClient
from epu.util import determine_path
from epu import cei_events

class EPUWorkerService(object):

    topic = "epu_worker"

    def __init__(self, *args, **kwargs):

        service_config = os.path.join(determine_path(), "config", "service.yml")
        provisioner_config = os.path.join(determine_path(), "config", "epu_worker.yml")
        config_files = [service_config, provisioner_config]

        self.CFG = bootstrap.configure(config_files)

        self.log = logging.getLogger()

        try:
            bootstrap.enable_gevent()
        except:
            self.log.warning("gevent not available. Falling back to threading")


        self.queue_name_work = self.CFG.queue_name_work
        extradict = {"queue_name_work":self.queue_name_work}

        cei_events.event("worker", "init_begin", extra=extradict)

        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG)


    def start(self):

        self.dashi.handle(self.work)
        extradict = {"queue_name_work":self.queue_name_work}
        cei_events.event("worker", "init_end", extra=extradict)

        try:
            self.dashi.consume()
        except KeyboardInterrupt:
            self.log.info("Caught terminate signal. Bye!")

    def work(self, work_amount=0, batchid="", jobid=""):

        extradict = {"batchid": batchid,
                     "jobid": jobid,
                     "work_amount": work_amount}
        cei_events.event("worker", "job_begin", extra=extradict)
        self.log.info("WORK: sleeping for %d seconds ---" % work_amount)
        time.sleep(work_amount)
        cei_events.event("worker", "job_end", extra=extradict)
        return {"result":"work_complete"}


def main():
    epu_worker = EPUWorkerService()
    epu_worker.start()
