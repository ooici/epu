import threading
import logging

from kazoo.security import make_digest_acl
from kazoo.retry import KazooRetry
from kazoo.client import KazooClient, KazooState

import epu.tevent as tevent

log = logging.getLogger(__name__)


def is_zookeeper_enabled(config):

    zk_config = get_zookeeper_config(config)
    if not zk_config:
        return False

    enabled = zk_config.get("enabled", True)

    if isinstance(enabled, basestring):
        enabled = enabled.lower() != "false"

    return enabled


def get_zookeeper_config(config):
    server_config = config.get("server")
    if not server_config:
        return None

    zk_config = server_config.get("zookeeper")

    if not zk_config:
        return None
    return zk_config


def get_auth_data_and_acl(username, password):
    if username and password:
        auth_scheme = "digest"
        auth_credential = "%s:%s" % (username, password)
        auth_data = [(auth_scheme, auth_credential)]
        default_acl = [make_digest_acl(username, password, all=True)]
    elif username or password:
        raise ValueError("both username and password must be specified, if any")
    else:
        auth_data = None
        default_acl = None

    return auth_data, default_acl


def get_kazoo_kwargs(username=None, password=None, timeout=None, use_gevent=False,
        retry_backoff=1.1):
    """Get KazooClient optional keyword arguments as a dictionary
    """
    kwargs = {"retry_backoff": retry_backoff}

    if use_gevent:
        from kazoo.handlers.gevent import SequentialGeventHandler

        kwargs['handler'] = SequentialGeventHandler()

    auth_data, default_acl = get_auth_data_and_acl(username, password)
    if auth_data:
        kwargs['auth_data'] = auth_data
        kwargs['default_acl'] = default_acl

    if timeout:
        kwargs['timeout'] = timeout

    return kwargs


def get_kazoo_retry(**kwargs):
    # start with some defaults
    retry_kwargs = dict(max_tries=-1, backoff=1.2)
    retry_kwargs.update(kwargs)
    return KazooRetry(**retry_kwargs)


class KazooBaseStore(object):
    """Provides base functionality for ZooKeeper-backed stores

    Leader election, connection state handling
    """

    def __init__(self, hosts, base_path, username=None, password=None,
                 timeout=None, use_gevent=False):
        kwargs = get_kazoo_kwargs(username=username, password=password,
                                         timeout=timeout, use_gevent=use_gevent)
        self.kazoo = KazooClient(hosts + base_path, **kwargs)
        self.kazoo.add_listener(self._connection_state_listener)
        self.retry = get_kazoo_retry()

        self._started = False
        self._election_enabled = False
        self._election_condition = threading.Condition()

        self._paths = []
        self._elections = {}
        self._election_threads = {}
        self._election_contenders = {}

    def start(self):
        self._started = True
        self.kazoo.start()

        # spawn threads for each election, if any
        for name, election in self._elections.iteritems():
            self._election_threads[name] = tevent.spawn(
                self._run_election,
                election,
                name)

    def stop(self):
        self._started = False
        if self._elections:
            self._stop_elections()

        # kill election threads
        for election_thread in self._election_threads.values():
            election_thread.join()
        self._election_threads.clear()

        self.kazoo.stop()

    def add_election(self, name, path):
        if self._started:
            raise Exception("Cannot add election while running")
        self._elections[name] = self.kazoo.Election(path)

    def contend_election(self, name, contender):
        if not self._started:
            raise Exception("not running")

        with self._election_condition:
            if name not in self._elections:
                raise Exception("election %s unknown" % name)
            self._election_contenders[name] = contender
            self._election_contenders.notify_all()

    def add_paths(self, paths):
        if self._started:
            raise Exception("Cannot add path while running")
        self._paths.extend(paths)

    def _connection_state_listener(self, state):
        # called by kazoo when the connection state changes.
        # handle in background
        tevent.spawn(self._handle_connection_state, state)

    def _stop_elections(self):
        with self._election_condition:
            self._election_enabled = False
            self._election_condition.notify_all()

        for name, contender in self._election_contenders.iteritems():
            try:
                contender.cancel()
            except Exception, e:
                log.exception("Error deposing %s: %s", name, e)

        for name, election in self._elections.iteritems():
            election.cancel()

    def _handle_connection_state(self, state):

        if state in (KazooState.LOST, KazooState.SUSPENDED):
            if self._elections:
                log.warn("ZooKeeper connection lost! disabling elections and leaders")
                self._stop_elections()
            else:
                log.warn("ZooKeeper connection lost!")

        elif state == KazooState.CONNECTED:
            if self._elections:
                log.info("Connected to ZooKeeper. Enabling elections.")
                with self._election_condition:
                    self._election_enabled = True
                    self._election_condition.notify_all()

            for path in self._paths:
                self.retry(self.kazoo.ensure_path, path)

    def _run_election(self, election, name):
        """Election thread function
        """
        leader = None
        while True:
            with self._election_condition:
                # first wait for the contender object to be added
                while not leader and self._started:
                    leader = self._election_contenders.get(name)
                    if not leader:
                        self._election_condition.wait()

                # then wait for election to be enabled
                while not self._election_enabled:
                    if not self._started:
                        return
                    log.debug("%s election waiting for to be enabled", name)
                    self._election_condition.wait()
                if not self._started:
                    return

            try:
                election.run(leader.inaugurate)
            except Exception, e:
                log.exception("Error in %s election: %s", name, e)
            except:
                log.exception("Unhandled error in election")
                raise
