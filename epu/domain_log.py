# Copyright 2013 University of Chicago

import logging
import threading

g_threadlocal = threading.local()
g_context_log_fields = ["domain", "user", "request_id"]


class DomainLogAdapter(logging.LoggerAdapter):

    def process(self, msg, kwargs):
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        extra = kwargs['extra']

        context = kwargs.pop('context', None)
        if not context:
            context = g_threadlocal

        for f in g_context_log_fields:
            if hasattr(context, f):
                v = getattr(context, f)
            else:
                v = "(undefined)"
            extra[f] = v
        return msg, kwargs


class DomainLogFilter(logging.Filter):
    """
    This log filter adds the defined fields for the epu logs.  It does not actually filter any log records out,
    it simply changes the contents of the log record based on thread specific data.
    """
    def filter(self, record):
        record.domain_info = ""
        if not hasattr(g_threadlocal, 'epu_info'):
            return True
        if not g_threadlocal.epu_info:
            return True

        # set any value needed
        domain_info = ""
        for f in g_context_log_fields:
            kw = g_threadlocal.epu_info[-1]
            if f in kw:
                domain_info = "%s %s=%s" % (domain_info, f, kw[f])
        record.domain_info = domain_info.strip()
        return True


class EpuLoggerThreadSpecific():
    def __init__(self, **kw):
        self.kw = kw.copy()
        if not hasattr(g_threadlocal, 'epu_info'):
            g_threadlocal.epu_info = []

    def __enter__(self):
        g_threadlocal.epu_info.append(self.kw)
        return None

    def __exit__(self, type, value, traceback):
        g_threadlocal.epu_info.pop()
        return None
