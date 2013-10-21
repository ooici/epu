# Copyright 2013 University of Chicago

import logging
import simplejson as json

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsException, BadVersionException, \
    NoNodeException

from epu import zkutil
from epu.exceptions import WriteConflictError, NotFoundError, \
    DeployableTypeValidationError, BadRequestError
from epu.util import is_valid_identifier

log = logging.getLogger(__name__)

VERSION_KEY = "__version"


def get_dtrs_store(config, use_gevent=False):
    """Instantiate DTRS store object for the given configuration
    """
    if zkutil.is_zookeeper_enabled(config):
        zookeeper = zkutil.get_zookeeper_config(config)

        log.info("Using ZooKeeper DTRS store")
        store = DTRSZooKeeperStore(zookeeper['hosts'], zookeeper['path'],
            username=zookeeper.get('username'),
            password=zookeeper.get('password'),
            timeout=zookeeper.get('timeout'),
            use_gevent=use_gevent)

    else:
        log.info("Using in-memory DTRS store")
        store = DTRSStore()

    return store


class DTRSStore(object):
    """In-memory version of DTRS storage"""

    def __init__(self):
        self.users = {}
        self.sites = {}

    def initialize(self):
        pass

    def shutdown(self):
        pass

    # Deployable Types methods

    def add_dt(self, caller, dt_name, dt_definition):
        """
        Store a new DT
        @param dt_name: name of the DT
        @param dt_definition: DT definition
        @raise WriteConflictError if DT exists
        """
        if not dt_definition:
            raise DeployableTypeValidationError(dt_name, 'The definition of the dt cannot be None nor can it be empty')

        if caller not in self.users:
            self.users[caller] = {"credentials": {}, "dts": {}, "sites": {}}

        if dt_name in self.users[caller]["dts"]:
            raise WriteConflictError("DT %s already exists" % dt_name)

        log.debug("add_dt %s for user %s | %s" % (dt_name, caller, str(dt_definition)))
        self.users[caller]["dts"][dt_name] = json.dumps(dt_definition)

    def describe_dt(self, caller, dt_name):
        """
        @brief Retrieves a DT by name
        @param dt_name Name of the DT to retrieve
        @retval DT definition or None if not found
        """
        log.debug("describe_dt %s for user %s" % (dt_name, caller))

        try:
            dts = self.users[caller]["dts"]
        except KeyError:
            return None

        record = dts.get(dt_name)
        log.debug("describe_dt %s for user %s dt found %s" % (dt_name, caller, str(record)))
        if record:
            ret = json.loads(record)
        else:
            ret = None

        return ret

    def list_dts(self, caller):
        """
        @brief Retrieves all DTs for a specific caller
        @param caller caller id
        @retval List of DTs
        """
        try:
            caller_dts = self.users[caller]["dts"].keys()
        except KeyError:
            caller_dts = []
        return caller_dts

    def remove_dt(self, caller, dt_name):
        if caller not in self.users:
            raise NotFoundError('Caller %s has no DT' % caller)

        dts = self.users[caller]["dts"]
        try:
            del dts[dt_name]
        except KeyError:
            raise NotFoundError('Caller %s has no DT named %s' % (caller,
                dt_name))

    def update_dt(self, caller, dt_name, dt_definition):
        if caller not in self.users:
            raise NotFoundError('Caller %s has no DT' % caller)

        try:
            self.users[caller]["dts"][dt_name]
        except KeyError:
            raise NotFoundError('Caller %s has no DT named %s' % (caller,
                dt_name))

        self.users[caller]["dts"][dt_name] = json.dumps(dt_definition)

    # Sites methods

    def add_site(self, caller, site_name, site_definition):
        """
        Store a new site
        @param caller: user_id or None
        @param site_name: name of the site
        @param site_definition: site definition
        @raise WriteConflictError if site exists
        """
        log.debug("add_site %s for user %s" % (site_name, caller))

        if caller is not None and site_name.startswith("common::"):
            raise BadRequestError("Can't add a user site starting with common::")

        if caller is None:
            sites = self.sites
        else:
            try:
                sites = self.users[caller]["sites"]
            except KeyError:
                self.users[caller] = {}
                sites = self.users[caller]["sites"] = {}

        if site_name in sites:
            raise WriteConflictError("Site %s already exists" % (site_name))

        sites[site_name] = json.dumps(site_definition)

    def describe_site(self, caller, site_name):
        """
        @brief Retrieves a site by name
        @param caller: user_id or None
        @param site_name Name of the site to retrieve
        @retval site definition or None if not found
        """
        log.debug("describe_site %s for user %s" % (site_name, caller))
        sites = self.sites.copy()

        if caller is not None:
            try:
                caller_sites = self.users[caller]["sites"].copy()
            except KeyError:
                caller_sites = {}
            finally:
                sites.update(caller_sites)

        record = sites.get(site_name)
        if record:
            ret = json.loads(record)
        else:
            ret = None

        return ret

    def list_sites(self, caller):
        """
        @brief Retrieves all sites
        @param caller: user_id or None
        @retval List of sites
        """
        log.debug("list_sites for user %s" % caller)
        sites = self.sites.keys()

        if caller is not None:
            try:
                caller_sites = self.users[caller]["sites"].keys()
            except KeyError:
                caller_sites = []
            finally:
                sites = sites + caller_sites

        return sites

    def remove_site(self, caller, site_name):
        log.debug("remove_site %s for user %s" % (site_name, caller))

        if caller is None:
            sites = self.sites
        else:
            try:
                sites = self.users[caller]["sites"]
            except KeyError:
                sites = {}

        try:
            del sites[site_name]
        except KeyError:
            raise NotFoundError('No site named %s' % site_name)

    def update_site(self, caller, site_name, site_definition):
        log.debug("update_site %s for user %s" % (site_name, caller))

        if caller is None:
            sites = self.sites
        else:
            try:
                sites = self.users[caller]["sites"]
            except KeyError:
                sites = {}

        # Check that the site already exists
        if site_name not in sites:
            raise NotFoundError('No site named %s' % site_name)

        sites[site_name] = json.dumps(site_definition)

    # Credentials methods

    def add_credentials(self, caller, credential_type, name, credentials):
        """
        Store new credentials
        @param caller: User owning the site credentials
        @param credential_type: type of credentials
        @param name: name of credentials
        @param credentials: site credentials
        @raise WriteConflictError if credentials exists
        """
        if caller not in self.users:
            self.users[caller] = {"credentials": {}, "dts": {}, "sites": {}}

        if credential_type not in self.users[caller]["credentials"]:
            self.users[caller]["credentials"][credential_type] = {}

        if name in self.users[caller]["credentials"][credential_type]:
            raise WriteConflictError("Credentials '%s' of type '%s' already exist"
                % (name, credential_type))

        self.users[caller]["credentials"][credential_type][name] = \
                json.dumps(credentials)

    def describe_credentials(self, caller, credential_type, name):
        """
        @brief Retrieves credentials by site
        @param credential_type: type of credentials
        @param name: name of credentials
        @param caller caller owning the credentials
        @retval Credentials definition or None if not found
        """
        try:
            caller_credentials = self.users[caller]["credentials"][credential_type]
        except KeyError:
            return None

        record = caller_credentials.get(name)
        if record:
            ret = json.loads(record)
        else:
            ret = None

        return ret

    def list_credentials(self, caller, credential_type):
        """
        @brief Retrieves all credentials of a type for a specific caller
        @param caller caller id
        @param credential_type: type of credentials
        @retval List of credentials
        """
        try:
            caller_credentials = self.users[caller]["credentials"][credential_type].keys()
        except KeyError:
            caller_credentials = []
        return caller_credentials

    def remove_credentials(self, caller, credential_type, name):
        if caller not in self.users:
            raise NotFoundError('Caller %s has no credentials' % caller)

        caller_credentials = self.users[caller]["credentials"]
        try:
            del caller_credentials[credential_type][name]
        except KeyError:
            raise NotFoundError("Credentials '%s' not found for user %s and type %s"
                    % (name, caller, credential_type))

    def update_credentials(self, caller, credential_type, name, credentials):
        if caller not in self.users:
            raise NotFoundError('Caller %s has no credentials' % caller)

        try:
            self.users[caller]["credentials"][credential_type][name]
        except KeyError:
            raise NotFoundError("Credentials '%s' not found for user %s and type %s"
                    % (name, caller, credential_type))

        self.users[caller]["credentials"][credential_type][name] = json.dumps(credentials)


class DTRSZooKeeperStore(object):
    """ZooKeeper-backed DTRS storage
    """

    # this path is used to store site information. Each child of this path
    # is a site, named with its site_name
    SITE_PATH = "/sites"

    CREDENTIALS_PATH = "/credentials"

    DT_PATH = "/dts"

    # this path is used to store user information. Each child of this path
    # is a user, named with its username
    USER_PATH = "/users"

    def __init__(self, hosts, base_path, username=None, password=None, timeout=None, use_gevent=False):

        kwargs = zkutil.get_kazoo_kwargs(username=username, password=password,
            timeout=timeout, use_gevent=use_gevent)
        self.kazoo = KazooClient(hosts + base_path, **kwargs)
        self.retry = zkutil.get_kazoo_retry()

    def initialize(self):

        self.kazoo.start()

        for path in (self.SITE_PATH, self.USER_PATH):
            self.kazoo.ensure_path(path)

    def shutdown(self):
        self.kazoo.stop()
        try:
            self.kazoo.close()
        except Exception:
            log.exception("Problem cleaning up kazoo")

    #########################################################################
    # SITES
    #########################################################################

    def _make_site_path(self, site_name, user=None):
        if user is None:
            path = self.SITE_PATH
            self.retry(self.kazoo.ensure_path, path)
            if site_name:
                path = path + "/" + site_name
        else:
            path = self.USER_PATH + "/" + user + self.SITE_PATH
            self.retry(self.kazoo.ensure_path, path)
            if site_name:
                path = path + "/" + site_name

        return path

    def add_site(self, caller, site_name, site):
        """
        Store a new site record
        @param caller: user_id or None
        @param site_name Id of site record to add
        @param site: site dictionary
        @raise WriteConflictError if site exists
        """
        log.debug("add_site %s for user %s" % (site_name, caller))

        if caller is not None and site_name.startswith("common::"):
            raise BadRequestError("Can't add a user site starting with common::")

        value = json.dumps(site)
        try:
            self.retry(self.kazoo.create, self._make_site_path(site_name, user=caller), value)
        except NodeExistsException:
            raise WriteConflictError("Site %s already exists" % (site_name))

    def describe_site(self, caller, site_name):
        """
        @brief Retrieves a site record by id
        @param caller: user_id or None
        @param site_name Id of site record to retrieve
        @retval site dictionary or None if not found
        """
        log.debug("describe_site %s for user %s" % (site_name, caller))
        try:
            data, stat = self.retry(self.kazoo.get, self._make_site_path(site_name, user=caller))
        except NoNodeException:
            # Fall back on common sites
            try:
                data, stat = self.retry(self.kazoo.get, self._make_site_path(site_name))
            except NoNodeException:
                return None

        site = json.loads(data)
        return site

    def list_sites(self, caller):
        """
        @brief Retrieves all sites
        @param caller: user_id or None
        @retval List of sites
        """
        log.debug("list_sites for user %s" % caller)
        try:
            children = self.retry(self.kazoo.get_children, self._make_site_path(None))
        except NoNodeException:
            raise NotFoundError()

        if caller is not None:
            try:
                caller_children = self.retry(self.kazoo.get_children, self._make_site_path(None, user=caller))
            except NoNodeException:
                caller_children = []
            finally:
                children = children + caller_children

        records = []
        for site_name in children:
            records.append(site_name)
        return records

    def remove_site(self, caller, site_name):
        """
        Remove a site record from the store
        @param caller: user_id or None
        @param site_name:
        @return:
        """
        log.debug("remove_site %s for user %s" % (site_name, caller))

        # If there is no caller, we can remove directly from /sites
        try:
            self.retry(self.kazoo.delete, self._make_site_path(site_name, user=caller))
        except NoNodeException:
            raise NotFoundError()

    def update_site(self, caller, site_name, site):
        """
        @brief updates a site record in the store
        @param caller: user_id or None
        @param site site record to store
        """
        log.debug("update_site %s for user %s" % (site_name, caller))
        value = json.dumps(site)

        try:
            self.retry(self.kazoo.set, self._make_site_path(site_name, user=caller), value, -1)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

    #########################################################################
    # CREDENTIALS
    #########################################################################

    def _make_credentials_path(self, user, credential_type, name):
        if not is_valid_identifier(user):
            raise ValueError('invalid user "%s"' % (user,))
        if not is_valid_identifier(credential_type):
            raise ValueError('invalid credential type "%s"' % (credential_type,))

        path = self.USER_PATH + "/" + user + self.CREDENTIALS_PATH + "/" + credential_type
        self.retry(self.kazoo.ensure_path, path)
        if name:
            path = path + "/" + name
        return path

    def add_credentials(self, caller, credential_type, name, credentials):
        """
        Store new credentials
        @param caller: User owning the site credentials
        @param credential_type: type of credentials
        @param name: name of credentials
        @param credentials: site credentials
        @raise WriteConflictError if credentials exists
        """
        value = json.dumps(credentials)
        try:
            self.retry(self.kazoo.create, self._make_credentials_path(caller, credential_type, name), value)
        except NodeExistsException:
            raise WriteConflictError("Credentials '%s' of type '%s' already exist"
                % (name, credential_type))

    def describe_credentials(self, caller, credential_type, name):
        """
        @brief Retrieves credentials by site
        @param credential_type: type of credentials
        @param name: name of credentials
        @param caller caller owning the credentials
        @retval Credentials definition or None if not found
        """
        try:
            data, stat = self.retry(self.kazoo.get,
                self._make_credentials_path(caller, credential_type, name))
        except NoNodeException:
            return None

        site = json.loads(data)
        return site

    def list_credentials(self, caller, credential_type):
        """
        @brief Retrieves all credentials of a type for a specific caller
        @param caller caller id
        @param credential_type: type of credentials
        @retval List of credentials
        """
        try:
            children = self.retry(self.kazoo.get_children,
                self._make_credentials_path(caller, credential_type, None))
        except NoNodeException:
            raise NotFoundError()
        return children

    def remove_credentials(self, caller, credential_type, name):
        """
        Remove a credential from the store
        @param caller
        @param site_name:
        @return:
        """
        try:
            self.retry(self.kazoo.delete,
                self._make_credentials_path(caller, credential_type, name))
        except NoNodeException:
            raise NotFoundError()

    def update_credentials(self, caller, credential_type, name, credentials):
        """
        @brief updates a site credentials record in the store
        @param caller
        @param name
        @param credentials credentials record to store
        """
        value = json.dumps(credentials)

        try:
            self.retry(self.kazoo.set,
                self._make_credentials_path(caller, credential_type, name),
                value, -1)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

    #########################################################################
    # DEPLOYABLE TYPES
    #########################################################################

    def _make_dt_path(self, user, dt_name):
        if not user:
            raise ValueError('invalid user')

        path = self.USER_PATH + "/" + user + self.DT_PATH
        self.retry(self.kazoo.ensure_path, path)
        if dt_name:
            path = path + "/" + dt_name
        return path

    def add_dt(self, caller, dt_name, dt_definition):
        """
        Store a new DT
        @param dt_name: name of the DT
        @param dt_definition: DT definition
        @raise WriteConflictError if DT exists
        """
        value = json.dumps(dt_definition)
        try:
            self.retry(self.kazoo.create, self._make_dt_path(caller, dt_name), value)
        except NodeExistsException:
            raise WriteConflictError("DT %s already exists" % dt_name)

    def describe_dt(self, caller, dt_name):
        """
        @brief Retrieves a DT by name
        @param dt_name Name of the DT to retrieve
        @retval DT definition or None if not found
        """
        try:
            data, stat = self.retry(self.kazoo.get, self._make_dt_path(caller, dt_name))
        except NoNodeException:
            return None

        dt = json.loads(data)
        return dt

    def list_dts(self, caller):
        """
        @brief Retrieves all DTs for a specific caller
        @param caller caller id
        @retval List of DTs
        """
        try:
            children = self.retry(self.kazoo.get_children,
                self._make_dt_path(caller, None))
        except NoNodeException:
            raise NotFoundError()

        records = []
        for site_name in children:
            records.append(site_name)
        return records

    def remove_dt(self, caller, dt_name):
        try:
            self.retry(self.kazoo.delete, self._make_dt_path(caller, dt_name))
        except NoNodeException:
            raise NotFoundError()

    def update_dt(self, caller, dt_name, dt_definition):
        value = json.dumps(dt_definition)

        try:
            self.retry(self.kazoo.set, self._make_dt_path(caller, dt_name), value, -1)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()


def sanitize_record(record):
    """Strips record of DTRS Store metadata

    @param record: record dictionary
    @return:
    """
    if record is not None:
        if VERSION_KEY in record:
            del record[VERSION_KEY]
    return record
