#!/usr/bin/env python

# Copyright 2013 University of Chicago

import sys
import argparse
import yaml
import os

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsException, NoNodeException

from epu import zkutil
from epu.processdispatcher.store import ProcessDispatcherZooKeeperStore
from epu.provisioner.store import ProvisionerZooKeeperStore


class CliError(Exception):
    pass


def get_kazoo(zk_config):
    hosts = zk_config.get('hosts')
    if not hosts:
        raise CliError("ZK config missing hosts")
    username = zk_config.get('username')
    password = zk_config.get('password')
    if (username or password) and not (username and password):
        raise CliError("Both username and password must be specified, or neither")
    kwargs = zkutil.get_kazoo_kwargs(username, password)
    return KazooClient(hosts, **kwargs)


def get_path(zk_config, args):
    path = args.path or zk_config.get('path')
    if not path:
        raise CliError("path must be specified on command line or in config")
    if not path.startswith("/"):
        raise CliError("ZK path does not begin with '/'?")
    return path


def cmd_setup(zk_config, args):
    path = get_path(zk_config, args)
    kazoo = get_kazoo(zk_config)

    kazoo.start()

    if args.clean:
        kazoo.delete(path, recursive=True)

    kazoo.ensure_path(path)

    # create this special path in the store to tell the Process Dispatcher
    # to start up in SYSTEM BOOT mode. This will prevent it from automatically
    # rescheduling any processes until the boot has ended.

    # WARNING: pd path is hardcoded here!
    system_boot_path = "%s/pd/%s" % (path, ProcessDispatcherZooKeeperStore.SYSTEM_BOOT_PATH)
    system_boot_path = os.path.normpath(system_boot_path)

    try:
        kazoo.create(system_boot_path, "", makepath=True)
    except NodeExistsException:
        pass

    if not args.clean:
        if args.clean_epu:
            # clean out the EPU-layer storage as it is not needed between restarts

            kazoo.delete("%s/provisioner" % path, recursive=True)
            kazoo.delete("%s/epum" % path, recursive=True)

        else:
            # at the least, clear out the provisioner DISABLED flag if set.
            # this allows new VMs to be launched.

            # WARNING: provisioner path is hardcoded here!
            prov_disabled_path = "%s/provisioner/%s" % (path, ProvisionerZooKeeperStore.DISABLED_PATH)
            prov_disabled_path = os.path.normpath(prov_disabled_path)
            try:
                kazoo.delete(prov_disabled_path)
            except NoNodeException:
                pass

    kazoo.stop()


def cmd_destroy(zk_config, args):
    path = get_path(zk_config, args)
    kazoo = get_kazoo(zk_config)

    kazoo.start()
    kazoo.delete(path, recursive=True)
    kazoo.stop()


def main(args=None):
    parser = argparse.ArgumentParser(description='EPU ZooKeeper management utility')
    parser.add_argument('--config', '-c', metavar="config.yml",
        type=argparse.FileType('r'), required=True)

    subparsers = parser.add_subparsers()
    setup_parser = subparsers.add_parser("setup",
        description=cmd_setup.__doc__, help="prepare path for system launch")
    setup_parser.add_argument("path", help="ZooKeeper base path", nargs='?')
    setup_parser.add_argument("--clean", help="Remove existing contents of path",
        action="store_true")
    setup_parser.add_argument("--clean-epu", help="Remove EPU-layer data",
        action="store_true")
    setup_parser.set_defaults(func=cmd_setup)

    destroy_parser = subparsers.add_parser("destroy",
        description=cmd_destroy.__doc__, help="delete path")
    destroy_parser.add_argument("path", help="ZooKeeper base path", nargs='?')
    destroy_parser.set_defaults(func=cmd_destroy)

    args = parser.parse_args(args=args)
    config = yaml.load(args.config)

    if not zkutil.is_zookeeper_enabled(config):
        print >>sys.stderr, "ZooKeeper is not enabled in config. Doing nothing."
        return

    zk_config = zkutil.get_zookeeper_config(config)
    try:
        args.func(zk_config, args)
    except CliError, e:
        parser.error(str(e))


if __name__ == '__main__':
    main()
