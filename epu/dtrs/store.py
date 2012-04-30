import logging

import simplejson as json

from epu.exceptions import WriteConflictError, NotFoundError

log = logging.getLogger(__name__)


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
        else:
            ret = None

        return ret

    def list_dts(self, caller):
        """
        @brief Retrieves all DTs for a specific caller
        @param caller caller id
        @retval List of DTs
        """
        caller_dts = self.users[caller]["dts"].keys()
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
        else:
            ret = None

        return ret

    def list_credentials(self, caller):
        """
        @brief Retrieves all credentials for a specific caller
        @param caller caller id
        @retval List of credentials
        """
        caller_credentials = self.users[caller]["credentials"].keys()
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
