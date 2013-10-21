# Copyright 2013 University of Chicago

from os import environ
import logging

if environ.get('EPU_USE_GEVENT'):
    from gevent import monkey
    monkey.patch_all()

if not environ.get('AMQP_DEBUG_LOGGING'):
    amqp_logger = logging.getLogger("amqp")
    amqp_logger.setLevel(logging.INFO)
