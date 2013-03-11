import logging
import simplejson as json

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsException, BadVersionException, \
    NoNodeException

from epu import zkutil
from epu.exceptions import WriteConflictError, NotFoundError, DeployableTypeValidationError

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
            self.users[caller] = {"credentials": {}, "dts": {}}

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
        if caller not in self.users:
            raise NotFoundError('Caller %s has no DT' % caller)

        dts = self.users[caller]["dts"]

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
            existing = self.users[caller]["dts"][dt_name]
        except KeyError:
            raise NotFoundError('Caller %s has no DT named %s' % (caller,
                dt_name))

        self.users[caller]["dts"][dt_name] = json.dumps(dt_definition)

    # Sites methods

    def add_site(self, site_name, site_definition):
        """
        Store a new site
        @param site_name: name of the site
        @param site_definition: site definition
        @raise WriteConflictError if site exists
        """
        if site_name in self.sites:
            raise WriteConflictError("Site %s already exists" % (site_name))

        log.debug("add_site %s" % (site_name))
        self.sites[site_name] = json.dumps(site_definition)

    def describe_site(self, site_name):
        """
        @brief Retrieves a site by name
        @param site_name Name of the site to retrieve
        @retval site definition or None if not found
        """
        sites = self.sites

        record = sites.get(site_name)
        if record:
            ret = json.loads(record)
        else:
            ret = None

        return ret

    def list_sites(self):
        """
        @brief Retrieves all sites
        @retval List of sites
        """
        sites = self.sites.keys()
        return sites

    def remove_site(self, site_name):
        try:
            del self.sites[site_name]
        except KeyError:
            raise NotFoundError('No site named %s' % site_name)

    def update_site(self, site_name, site_definition):
        try:
            existing = self.sites[site_name]
        except KeyError:
            raise NotFoundError('No site named %s' % site_name)

        self.sites[site_name] = json.dumps(site_definition)

    # Credentials methods

    def add_credentials(self, caller, site_name, site_credentials):
        """
        Store new credentials
        @param caller: User owning the site credentials
        @param site_name: name of the site
        @param site_credentials: site credentials
        @raise WriteConflictError if credentials exists
        """
        if caller not in self.users:
            self.users[caller] = {"credentials": {}, "dts": {}}

        if site_name in self.users[caller]["credentials"]:
            raise WriteConflictError("Credentials for site %s already exist" % (site_name))

        self.users[caller]["credentials"][site_name] = \
                json.dumps(site_credentials)

    def describe_credentials(self, caller, site_name):
        """
        @brief Retrieves credentials by site
        @param site_name Name of the site
        @param caller caller owning the credentials
        @retval Credentials definition or None if not found
        """
        if caller not in self.users:
            raise NotFoundError("Credentials not found for user %s and site %s"
                    % (caller, site_name))

        try:
            caller_credentials = self.users[caller]["credentials"]
        except KeyError:
            return None

        record = caller_credentials.get(site_name)
        if record:
            ret = json.loads(record)
        else:
            ret = None

        return ret

    def list_credentials(self, caller):
        """
        @brief Retrieves all credentials for a specific caller
        @param caller caller id
        @retval List of credentials
        """
        try:
            caller_credentials = self.users[caller]["credentials"].keys()
        except KeyError:
            caller_credentials = []
        return caller_credentials

    def remove_credentials(self, caller, site_name):
        if caller not in self.users:
            raise NotFoundError('Caller %s has no credentials' % caller)

        caller_credentials = self.users[caller]["credentials"]
        try:
            del caller_credentials[site_name]
        except KeyError:
            raise NotFoundError("Credentials not found for user %s and site %s"
                    % (caller, site_name))

    def update_credentials(self, caller, site_name, site_credentials):
        if caller not in self.users:
            raise NotFoundError('Caller %s has no credentials' % caller)

        try:
            existing = self.users[caller]["credentials"][site_name]
        except KeyError:
            raise NotFoundError("Credentials not found for user %s and site %s"
                    % (caller, site_name))

        self.users[caller]["credentials"][site_name] = json.dumps(site_credentials)


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

    #########################################################################
    # SITES
    #########################################################################

    def _make_site_path(self, site_name):
        if not site_name:
            raise ValueError('invalid site_name')
        return self.SITE_PATH + "/" + site_name

    def add_site(self, site_name, site):
        """
        Store a new site record
        @param site: site dictionary
        @raise WriteConflictError if site exists
        """
        value = json.dumps(site)
        try:
            self.retry(self.kazoo.create, self._make_site_path(site_name), value)
        except NodeExistsException:
            raise WriteConflictError("Site %s already exists" % (site_name))

    def describe_site(self, site_name):
        """
        @brief Retrieves a site record by id
        @param site_name Id of site record to retrieve
        @retval site dictionary or None if not found
        """
        try:
            data, stat = self.retry(self.kazoo.get, self._make_site_path(site_name))
        except NoNodeException:
            return None

        site = json.loads(data)
        return site

    def list_sites(self):
        """
        @brief Retrieves all sites
        @retval List of sites
        """
        try:
            children = self.retry(self.kazoo.get_children, self.SITE_PATH)
        except NoNodeException:
            raise NotFoundError()

        records = []
        for site_name in children:
            records.append(site_name)
        return records

    def remove_site(self, site_name):
        """
        Remove a site record from the store
        @param site_name:
        @return:
        """
        try:
            self.retry(self.kazoo.delete, self._make_site_path(site_name))
        except NoNodeException:
            raise NotFoundError()

    def update_site(self, site_name, site):
        """
        @brief updates a site record in the store
        @param site site record to store
        """
        value = json.dumps(site)

        try:
            self.retry(self.kazoo.set, self._make_site_path(site_name), value, -1)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

    #########################################################################
    # CREDENTIALS
    #########################################################################

    def _make_credentials_path(self, user, site_name):
        if not user:
            raise ValueError('invalid user')

        path = self.USER_PATH + "/" + user + self.CREDENTIALS_PATH
        self.retry(self.kazoo.ensure_path, path)
        if site_name:
            path = path + "/" + site_name
        return path

    def add_credentials(self, caller, site_name, site_credentials):
        """
        Store new credentials
        @param caller: User owning the site credentials
        @param site_name: name of the site
        @param site_credentials: site credentials
        @raise WriteConflictError if credentials exists
        """
        value = json.dumps(site_credentials)
        try:
            self.retry(self.kazoo.create, self._make_credentials_path(caller, site_name), value)
        except NodeExistsException:
            raise WriteConflictError("Credentials for site %s already exist" % (site_name))

    def describe_credentials(self, caller, site_name):
        """
        @brief Retrieves credentials by site
        @param site_name Name of the site
        @param caller caller owning the credentials
        @retval Credentials definition or None if not found
        """
        try:
            data, stat = self.retry(self.kazoo.get, self._make_credentials_path(caller, site_name))
        except NoNodeException:
            return None

        site = json.loads(data)
        return site

    def list_credentials(self, caller):
        """
        @brief Retrieves all credentials for a specific caller
        @param caller caller id
        @retval List of credentials
        """
        try:
            children = self.retry(self.kazoo.get_children, self._make_credentials_path(caller, None))
        except NoNodeException:
            raise NotFoundError()

        records = []
        for site_name in children:
            records.append(site_name)
        return records

    def remove_credentials(self, caller, site_name):
        """
        Remove a site record from the store
        @param caller
        @param site_name:
        @return:
        """
        try:
            self.retry(self.kazoo.delete, self._make_credentials_path(caller, site_name))
        except NoNodeException:
            raise NotFoundError()

    def update_credentials(self, caller, site_name, site_credentials):
        """
        @brief updates a site credentials record in the store
        @param caller
        @param site_name
        @param site_credentials credentials record to store
        """
        value = json.dumps(site_credentials)

        try:
            self.retry(self.kazoo.set, self._make_credentials_path(caller,
                site_name), value, -1)
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
    if VERSION_KEY in record:
        del record[VERSION_KEY]
    return record
