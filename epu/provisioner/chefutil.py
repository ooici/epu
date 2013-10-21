# Copyright 2013 University of Chicago

import json
import string

try:
    import chef
except ImportError:
    chef = None

from epu.exceptions import NotFoundError, WriteConflictError

DEFAULT_CLIENT_NAME = "admin"
DEFAULT_VALIDATOR_NAME = "chef-validator"


def get_chef_node(node_id, server_url=None, client_key=None, client_name=None):
    """Return a Chef node from the server

    if server_url and client_key are not specified, a default server will be used
    (if available.)
    """
    if chef is None:
        raise Exception("pychef library is not available")

    if client_name is None:
        client_name = DEFAULT_CLIENT_NAME

    api = None
    if server_url and client_key:
        validate_key(client_key)
        api = chef.ChefAPI(server_url, client_key, client_name)

    try:
        return chef.Node.list(api=api)[node_id]
    except KeyError:
        return None


def create_chef_node(node_id, attributes, runlist,
                     server_url=None, client_key=None, client_name=None):
    """Create a new Chef node in the server

    if server_url and client_key are not specified, a default server will be used
    (if available.)
    """
    if chef is None:
        raise Exception("pychef library is not available")

    if client_name is None:
        client_name = DEFAULT_CLIENT_NAME

    if not attributes:
        attributes = {}

    if not isinstance(attributes, dict):
        raise ValueError("invalid attributes: must be a dictionary")

    api = None
    if server_url and client_key:
        validate_key(client_key)
        api = chef.ChefAPI(server_url, client_key, client_name)

    try:
        return chef.Node.create(node_id, api=api, run_list=runlist, **attributes)
    except chef.exceptions.ChefServerError, e:
        if e.code == 409:
            raise WriteConflictError('Chef node "%s" already exists in server %s' %
                (node_id, server_url))
        else:
            raise


def delete_chef_node(node_id, server_url=None, client_key=None, client_name=None):
    """Drop a Chef node from the server

    if server_url and client_key are not specified, a default server will be used
    (if available.)
    """
    if client_name is None:
        client_name = DEFAULT_CLIENT_NAME

    api = None
    if server_url and client_key:
        validate_key(client_key)
        api = chef.ChefAPI(server_url, client_key, client_name)

    try:
        chef.Node(node_id).delete(api=api)
    except chef.exceptions.ChefServerNotFoundError:
        raise NotFoundError('Chef node "%s" not found in server %s; could not delete' %
            (node_id, server_url))


def validate_key(key):
    if not isinstance(key, basestring) and key:
        raise ValueError("invalid key")

    if not key.startswith('-----'):
        raise ValueError("invalid key")


_CHEF_INSTALL_SH_TMPL = """#!/bin/bash

mkdir -p /etc/chef /var/log/chef

cat >/etc/chef/validation.pem <<END
${validation_key}
END

cat >/etc/chef/first-boot.json <<END
${first_boot}
END

cat > /etc/chef/client.rb <<END
log_level              :info
log_location           "/var/log/chef/client.log"
ssl_verify_mode        :verify_none
validation_client_name "${validator_name}"
validation_key         "/etc/chef/validation.pem"
client_key             "/etc/chef/client.pem"
chef_server_url        "${server_url}"
environment            "_default"
node_name              "${node_name}"
file_cache_path        "/var/cache/chef"
file_backup_path       "/var/backups/chef"
pid_file               "/var/run/chef/client.pid"
Chef::Log::Formatter.show_time = true
END

true && curl -L https://www.opscode.com/chef/install.sh | bash
chef-client -j /etc/chef/first-boot.json
chef-client -d
"""


def get_chef_cloudinit_userdata(node_id, server_url, validation_key,
                                run_list, attributes, validator_name=None):

    if validator_name is None:
        validator_name = DEFAULT_VALIDATOR_NAME

    if not attributes:
        attributes = {}

    if not isinstance(attributes, dict):
        raise ValueError("invalid attributes: must be a dictionary")

    validate_key(validation_key)
    attributes_with_run_list = dict(attributes)
    attributes_with_run_list.update({"run_list": run_list})
    first_boot = json.dumps(attributes_with_run_list)
    vals = dict(validation_key=validation_key, server_url=server_url,
                node_name=node_id, validator_name=validator_name,
                first_boot=first_boot)
    tmpl = string.Template(_CHEF_INSTALL_SH_TMPL)
    return tmpl.safe_substitute(vals)
