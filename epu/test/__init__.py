# Copyright 2013 University of Chicago


import os
import socket
import unittest
import uuid
import subprocess
import threading
import errno
import signal
import logging

from kazoo.client import KazooClient

log = logging.getLogger(__name__)


class MockLeader(object):

    def __init__(self):
        self.cancelled = threading.Event()
        self.running = threading.Event()

    def inaugurate(self):
        log.info("leader inaugurated. running=%s cancelled=%s",
            self.running.is_set(), self.cancelled.is_set())

        assert not self.running.is_set()
        self.cancelled.clear()
        self.running.set()
        try:
            self.cancelled.wait()
            log.info("leader got cancelled event")
        finally:
            self.running.clear()

    def cancel(self):
        self.cancelled.set()

    def wait_running(self, timeout=5):
        self.running.wait(timeout)
        assert self.running.is_set(), "expected leader to be running after %ss" % (timeout,)

    def wait_cancelled(self, timeout=5):
        self.cancelled.wait(timeout)
        assert self.cancelled.is_set(), "expected leader to be cancelled after %ss" % (timeout,)


class SocatProxy(object):
    """Manages a TCP forking proxy using socat
    """

    def __init__(self, destination, source_port=None):
        self.port = source_port or free_port()
        self.address = "localhost:%d" % self.port
        self.destination = destination
        self.process = None

    def start(self):
        assert not self.process
        src_arg = "TCP4-LISTEN:%d,fork,reuseaddr" % self.port
        dest_arg = "TCP4:%s" % self.destination
        try:
            self.process = subprocess.Popen(args=["socat", src_arg, dest_arg],
                preexec_fn=os.setpgrp)
        except OSError, e:
            if e.errno == errno.ENOENT:
                raise unittest.SkipTest("socat executable not found")

    def stop(self):
        if self.process and self.process.returncode is None:
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except OSError, e:
                if e.errno != errno.ESRCH:
                    raise
            self.process.wait()
            self.process = None
            return True
        return False

    def restart(self):
        self.stop()
        self.start()

    @property
    def running(self):
        return self.process and self.process.returncode is None


def free_port(host="localhost"):
    """Pick a free port on a local interface and return it.

    Races are possible but unlikely
    """
    sock = socket.socket()
    try:
        sock.bind((host, 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


class MultiProxy(object):
    """manages multiple proxies as one
    """

    def __init__(self, proxies):
        self.proxies = list(proxies)

    def start(self):
        for proxy in self.proxies:
            proxy.start()

    def stop(self):
        for proxy in self.proxies:
            proxy.stop()

    def restart(self):
        self.stop()
        self.start()

    @property
    def running(self):
        return any(proxy.running for proxy in self.proxies)


class SocatProxyRestartWrapper(object):
    """Wraps an object and calls proxy.restart() before any call
    """
    def __init__(self, proxy, obj):
        self.proxy = proxy
        self.obj = obj

    def __getattr__(self, attr):
        attr = self.obj.__getattribute__(attr)
        if callable(attr):
            def wrapped(*args, **kwargs):
                log.warn("restarting proxy before calling %s.%s",
                    type(self.obj).__name__, attr.__name__)
                self.proxy.restart()
                return attr(*args, **kwargs)
            return wrapped
        else:
            return attr


class ZooKeeperTestMixin(object):

    zk_hosts = None
    _zk_hosts_internal = None
    zk_base_path = None
    proxy = None

    def setup_zookeeper(self, base_path_prefix="/int_tests", use_proxy=False):

        zk_hosts = os.environ.get("ZK_HOSTS")
        if not zk_hosts:
            raise unittest.SkipTest("export ZK_HOSTS env to run ZooKeeper integration tests")

        if use_proxy:
            hosts_list = zk_hosts.split(",")
            if len(hosts_list) == 1:
                self.proxy = SocatProxy(zk_hosts)
                self.proxy.start()
                self.zk_hosts = self.proxy.address

            else:
                proxies = [SocatProxy(host) for host in hosts_list]
                self.proxy = MultiProxy(proxies)
                self.proxy.start()
                self.zk_hosts = ",".join(proxy.address for proxy in proxies)

            self._zk_hosts_internal = zk_hosts

        else:
            self.zk_hosts = self._zk_hosts_internal = zk_hosts

        self.zk_base_path = base_path_prefix + uuid.uuid4().hex

        if os.environ.get('EPU_USE_GEVENT'):
            from kazoo.handlers.gevent import SequentialGeventHandler
            handler = SequentialGeventHandler()
            self.use_gevent = True
        else:
            handler = None
            self.use_gevent = False

        self.kazoo = KazooClient(self._zk_hosts_internal + self.zk_base_path, handler=handler)
        self.kazoo.start()

    def teardown_zookeeper(self):
        if self.kazoo:
            try:
                self.kazoo.delete("/", recursive=True)
                self.kazoo.stop()
                self.kazoo.close()
            except Exception:
                log.exception("Problem tearing down ZooKeeper")
        if self.proxy:
            self.proxy.stop()

    cleanup_zookeeper = teardown_zookeeper
