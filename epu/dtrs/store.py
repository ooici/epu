import logging

import simplejson as json

# conditionally import these so we can use the in-memory store without ZK
try:
    from kazoo.client import KazooClient, KazooState, EventType
    from kazoo.exceptions import NodeExistsException, BadVersionException, \
        NoNodeException

except ImportError:
    KazooClient = None
    KazooState = None
    EventType = None
    NodeExistsException = None
    BadVersionException = None
    NoNodeException = None

from epu.exceptions import WriteConflictError, NotFoundError

log = logging.getLogger(__name__)

VERSION_KEY = "__version"


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
        if caller not in self.users:
            self.users[caller] = {"credentials": {}, "dts": {}}

        if dt_name in self.users[caller]["dts"]:
            raise WriteConflictError()

        self.users[caller]["dts"][dt_name] = json.dumps(dt_definition), 0

        # also add a version to the input dict.
        dt_definition[VERSION_KEY] = 0

    def describe_dt(self, caller, dt_name):
        """
        @brief Retrieves a DT by name
        @param dt_name Name of the DT to retrieve
        @retval DT definition or None if not found
        """
        if caller not in self.users:
            raise NotFoundError('Caller %s has no DT' % caller)

        dts = self.users[caller]["dts"]

        record = dts.get(dt_name)
        if record:
            dt_json, version = record
            ret = json.loads(dt_json)
            ret[VERSION_KEY] = version
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

        _, version = existing

        if dt_definition[VERSION_KEY] != version:
            raise WriteConflictError()

        version += 1
        self.users[caller]["dts"][dt_name] = json.dumps(dt_definition), version
        dt_definition[VERSION_KEY] = version

    # Sites methods

    def add_site(self, site_name, site_definition):
        """
        Store a new site
        @param site_name: name of the site
        @param site_definition: site definition
        @raise WriteConflictError if site exists
        """
        if site_name in self.sites:
            raise WriteConflictError()

        self.sites[site_name] = json.dumps(site_definition), 0

        # also add a version to the input dict.
        site_definition[VERSION_KEY] = 0

    def describe_site(self, site_name):
        """
        @brief Retrieves a site by name
        @param site_name Name of the site to retrieve
        @retval site definition or None if not found
        """
        sites = self.sites

        record = sites.get(site_name)
        if record:
            site_json, version = record
            ret = json.loads(site_json)
            ret[VERSION_KEY] = version
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

        _, version = existing

        if site_definition[VERSION_KEY] != version:
            raise WriteConflictError()

        version += 1
        self.sites[site_name] = json.dumps(site_definition), version
        site_definition[VERSION_KEY] = version

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
            raise WriteConflictError()

        self.users[caller]["credentials"][site_name] = \
                json.dumps(site_credentials), 0

        # also add a version to the input dict.
        site_credentials[VERSION_KEY] = 0

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
            credentials_json, version = record
            ret = json.loads(credentials_json)
            ret[VERSION_KEY] = version
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

        _, version = existing

        if site_credentials[VERSION_KEY] != version:
            raise WriteConflictError()

        version += 1
        self.users[caller]["credentials"][site_name] = json.dumps(site_credentials), version
        site_credentials[VERSION_KEY] = version


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

    def __init__(self, hosts, base_path, timeout=None):
        self.kazoo = KazooClient(hosts, timeout=timeout, namespace=base_path)

    def initialize(self):

        self.kazoo.connect()

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
            self.kazoo.create(self._make_site_path(site_name), value)
        except NodeExistsException:
            raise WriteConflictError()

        # also add a version to the input dict.
        site[VERSION_KEY] = 0

    def describe_site(self, site_name):
        """
        @brief Retrieves a site record by id
        @param site_name Id of site record to retrieve
        @retval site dictionary or None if not found
        """
        try:
            data, stat = self.kazoo.get(self._make_site_path(site_name))
        except NoNodeException:
            return None

        site = json.loads(data)
        site[VERSION_KEY] = stat['version']
        return site

    def list_sites(self):
        """
        @brief Retrieves all sites
        @retval List of sites
        """
        try:
            children = self.kazoo.get_children(self.SITE_PATH)
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
            self.kazoo.delete(self._make_site_path(site_name))
        except NoNodeException:
            raise NotFoundError()

    def update_site(self, site_name, site):
        """
        @brief updates a site record in the store
        @param site site record to store
        """
        version = site[VERSION_KEY]

        # make a shallow copy so we can prune the version
        site = site.copy()
        del site[VERSION_KEY]

        value = json.dumps(site)

        try:
            stat = self.kazoo.set(self._make_site_path(site_name), value,
                version)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

        site[VERSION_KEY] = stat['version']

    #########################################################################
    # CREDENTIALS
    #########################################################################

    def _make_credentials_path(self, user, site_name):
        if not user:
            raise ValueError('invalid user')

        path = self.USER_PATH + "/" + user + self.CREDENTIALS_PATH
        self.kazoo.ensure_path(path)
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
            self.kazoo.create(self._make_credentials_path(caller, site_name), value)
        except NodeExistsException:
            raise WriteConflictError()

        # also add a version to the input dict.
        site_credentials[VERSION_KEY] = 0

    def describe_credentials(self, caller, site_name):
        """
        @brief Retrieves credentials by site
        @param site_name Name of the site
        @param caller caller owning the credentials
        @retval Credentials definition or None if not found
        """
        try:
            data, stat = self.kazoo.get(self._make_credentials_path(caller, site_name))
        except NoNodeException:
            return None

        site = json.loads(data)
        site[VERSION_KEY] = stat['version']
        return site

    def list_credentials(self, caller):
        """
        @brief Retrieves all credentials for a specific caller
        @param caller caller id
        @retval List of credentials
        """
        try:
            children = self.kazoo.get_children(self._make_credentials_path(caller, None))
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
            self.kazoo.delete(self._make_credentials_path(caller, site_name))
        except NoNodeException:
            raise NotFoundError()

    def update_credentials(self, caller, site_name, site_credentials):
        """
        @brief updates a site credentials record in the store
        @param caller
        @param site_name
        @param site_credentials credentials record to store
        """
        version = site_credentials[VERSION_KEY]

        # make a shallow copy so we can prune the version
        credentials = site_credentials.copy()
        del credentials[VERSION_KEY]

        value = json.dumps(credentials)

        try:
            stat = self.kazoo.set(self._make_credentials_path(caller, site_name), value,
                version)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

        site_credentials[VERSION_KEY] = stat['version']

    #########################################################################
    # DEPLOYABLE TYPES
    #########################################################################

    def _make_dt_path(self, user, dt_name):
        if not user:
            raise ValueError('invalid user')

        path = self.USER_PATH + "/" + user + self.DT_PATH
        self.kazoo.ensure_path(path)
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
            self.kazoo.create(self._make_dt_path(caller, dt_name), value)
        except NodeExistsException:
            raise WriteConflictError()

        # also add a version to the input dict.
        dt_definition[VERSION_KEY] = 0

    def describe_dt(self, caller, dt_name):
        """
        @brief Retrieves a DT by name
        @param dt_name Name of the DT to retrieve
        @retval DT definition or None if not found
        """
        try:
            data, stat = self.kazoo.get(self._make_dt_path(caller, dt_name))
        except NoNodeException:
            return None

        dt = json.loads(data)
        dt[VERSION_KEY] = stat['version']
        return dt

    def list_dts(self, caller):
        """
        @brief Retrieves all DTs for a specific caller
        @param caller caller id
        @retval List of DTs
        """
        try:
            children = self.kazoo.get_children(self._make_dt_path(caller, None))
        except NoNodeException:
            raise NotFoundError()

        records = []
        for site_name in children:
            records.append(site_name)
        return records

    def remove_dt(self, caller, dt_name):
        try:
            self.kazoo.delete(self._make_dt_path(caller, dt_name))
        except NoNodeException:
            raise NotFoundError()

    def update_dt(self, caller, dt_name, dt_definition):
        version = dt_definition[VERSION_KEY]

        # make a shallow copy so we can prune the version
        dt = dt_definition.copy()
        del dt[VERSION_KEY]

        value = json.dumps(dt)

        try:
            stat = self.kazoo.set(self._make_dt_path(caller, dt_name), value, version)
        except BadVersionException:
            raise WriteConflictError()
        except NoNodeException:
            raise NotFoundError()

        dt_definition[VERSION_KEY] = stat['version']


def sanitize_record(record):
    """Strips record of DTRS Store metadata

    @param record: record dictionary
    @return:
    """
    if VERSION_KEY in record:
        del record[VERSION_KEY]
    return record
