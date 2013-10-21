# Copyright 2013 University of Chicago

import traceback
import signal
import threading
import sys
import logging

import dashi.exceptions
import epu.exceptions

log = logging.getLogger(__name__)


def link_dashi_exceptions(dashi_conn):
    """Link common epu.exceptions exceptions to their dashi counterparts
    """
    dashi_conn.link_exceptions(
        custom_exception=epu.exceptions.NotFoundError,
        dashi_exception=dashi.exceptions.NotFoundError)
    dashi_conn.link_exceptions(
        custom_exception=epu.exceptions.WriteConflictError,
        dashi_exception=dashi.exceptions.WriteConflictError)
    dashi_conn.link_exceptions(
        custom_exception=epu.exceptions.BadRequestError,
        dashi_exception=dashi.exceptions.BadRequestError)


def dumpstacks():
    id2name = dict([(th.ident, th.name) for th in threading.enumerate()])
    code = []
    for threadId, stack in sys._current_frames().items():
        code.append("\n# Thread: %s(%d)" % (id2name.get(threadId, ""), threadId))
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code.append('File: "%s", line %d, in %s' % (filename, lineno, name))
            if line:
                code.append("  %s" % (line.strip()))
    return "\n".join(code)


def epu_signal_stack_debug(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    d = {'_frame': frame}         # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    message = "Signal recieved : entering python shell.\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    log.info(message)

    message = dumpstacks()
    log.info(message)


def epu_register_signal_stack_debug():
    signal.signal(signal.SIGUSR1, epu_signal_stack_debug)
