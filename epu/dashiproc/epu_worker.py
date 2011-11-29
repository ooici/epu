import time

from dashi import DashiConnection
import dashi.bootstrap as bootstrap
from dashi.util import LoopingCall

from epu.dashiproc.provisioner import ProvisionerClient
from epu.dashiproc.util import get_config_files

from epu import cei_events

class EPUWorkerService(object):

    topic = "epu_worker"

    def __init__(self, *args, **kwargs):

        config_files = get_config_files("service") + get_config_files("epu_worker")
        logging_config_files = get_config_files("logging")
        self.CFG = bootstrap.configure(config_files, logging_config_files)

        self.log = bootstrap.get_logger(self.__class__.__name__)

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



