# Copyright 2013 University of Chicago

import os
import time
import logging

import dashi.bootstrap as bootstrap

from epu.util import get_config_paths
from epu import cei_events


class EPUWorkerService(object):

    topic = "epu_worker"

    def __init__(self, *args, **kwargs):

        configs = ["service", "epu_worker"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        self.log = logging.getLogger()

        try:
            if os.environ.get('EPU_USE_GEVENT'):
                bootstrap.enable_gevent()
            else:
                self.log.info("Using standard python Threading")
        except:
            self.log.warning("gevent not available. Falling back to threading")

        self.queue_name_work = self.CFG.queue_name_work
        extradict = {"queue_name_work": self.queue_name_work}

        cei_events.event("worker", "init_begin", extra=extradict)

        self.dashi = bootstrap.dashi_connect(self.topic, self.CFG)

    def start(self):

        self.dashi.handle(self.work)
        extradict = {"queue_name_work": self.queue_name_work}
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
        return {"result": "work_complete"}


def main():
    epu_worker = EPUWorkerService()
    epu_worker.start()
