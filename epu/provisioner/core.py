#!/usr/bin/env python

"""
@file epu/provisioner/core.py
@author David LaBissoniere
@brief Starts, stops, and tracks instance and context state.
"""

import time
import logging
from itertools import izip
from socket import timeout

from libcloud.compute.types import NodeState as LibcloudNodeState
from libcloud.compute.base import Node as LibcloudNode
from libcloud.compute.base import NodeImage as LibcloudNodeImage
from libcloud.compute.base import NodeSize as LibcloudNodeSize
try:
    from statsd import StatsClient
except ImportError:
    StatsClient = None

from epu.provisioner.ctx import ContextClient, BrokerError, BrokerAuthError,\
    ContextNotFoundError, NimbusClusterDocument, ValidationError
from epu.provisioner.sites import SiteDriver
from epu.provisioner.store import group_records, sanitize_record, VERSION_KEY
from epu.exceptions import DeployableTypeLookupError, DeployableTypeValidationError
from epu.states import InstanceState
from epu.exceptions import WriteConflictError, UserNotPermittedError, GeneralIaaSException, IaaSIsFullException
from epu import cei_events
from epu.util import check_user
from epu.domain_log import EpuLoggerThreadSpecific
from epu.tevent import Pool

log = logging.getLogger(__name__)

# alias for shorter code
states = InstanceState

__all__ = ['ProvisionerCore', 'ProvisioningError']

_LIBCLOUD_STATE_MAP = {
    LibcloudNodeState.RUNNING: states.STARTED,
    LibcloudNodeState.REBOOTING: states.STARTED,  # TODO hmm
    LibcloudNodeState.PENDING: states.PENDING,
    LibcloudNodeState.TERMINATED: states.TERMINATED,
    LibcloudNodeState.UNKNOWN: states.ERROR_RETRYING}

# Window of time in which nodes are allowed to be launched
# but not returned in queries to the IaaS. After this, nodes
# are assumed to be terminated out of band and marked FAILED
_IAAS_NODE_QUERY_WINDOW_SECONDS = 60

# If a node has been started for more than INSTANCE_READY_TIMEOUT and has not
# checked in with the context broker, its state is changed to RUNNING_FAILED.
INSTANCE_READY_TIMEOUT = 90


class ProvisionerCore(object):
    """Provisioner functionality that is not specific to the service.
    """

    # Maximum time that any IaaS query can take before throwing a timeout exception
    _IAAS_DEFAULT_TIMEOUT = 60

    UNKNOWN_LAUNCH_ID = "provisioner-unknown-launch"

    def __init__(self, store, notifier, dtrs, context, logger=None, iaas_timeout=None, statsd_cfg=None):
        """

        @type store: ProvisionerStore
        """

        if logger:
            global log
            log = logger

        self.store = store
        self.notifier = notifier
        self.dtrs = dtrs

        self.context = context

        if iaas_timeout is not None:
            self.iaas_timeout = iaas_timeout
        else:
            self.iaas_timeout = self._IAAS_DEFAULT_TIMEOUT

        self.statsd_client = None
        if statsd_cfg is not None:
            try:
                host = statsd_cfg["host"]
                port = statsd_cfg["port"]
                log.info("Setting up statsd client with host %s and port %d" % (host, port))
                self.statsd_client = StatsClient(host, port)
            except:
                log.exception("Failed to set up statsd client")

        if not context:
            log.warn("No context client provided. Contextualization disabled.")

    def _validation_error(self, msg, *args):
        log.debug("raising provisioning validation error: " + msg, *args)
        raise ProvisioningError("Invalid provision request: " + msg % args)

    def prepare_provision(self, launch_id, deployable_type, instance_ids,
                          site, allocation=None, vars=None, caller=None):
        # wrapper for logging

        """Validates request and commits to datastore.

        If the request is invalid and doesn't contain enough information
        to notify subscribers via normal channels, a ProvisioningError
        is raised. This is almost certainly a client programming error.

        If the request is well-formed but invalid, for example if the
        deployable type does not exist in the DTRS, FAILED records are
        recorded in data store and subscribers are notified.

        Returns a tuple (launch record, node records). It is the caller's
        responsibility to check the launch record for a FAILED state
        before proceeding with launch.
        """
        with EpuLoggerThreadSpecific(user=caller):
            return self._prepare_provision(
                launch_id, deployable_type,
                instance_ids, site, allocation=allocation,
                vars=vars, caller=caller)

    def _prepare_provision(self, launch_id, deployable_type, instance_ids,
                           site, allocation=None, vars=None, caller=None):
        # initial validation
        if not launch_id:
            self._validation_error("bad launch_id '%s'", launch_id)

        if not deployable_type:
            self._validation_error("bad deployable_type '%s'", deployable_type)

        if not (isinstance(instance_ids, (list, tuple)) and
                len(instance_ids) == 1 and instance_ids[0]):
            self._validation_error(
                "bad instance_ids '%s': need a list or tuple of length 1 -- " +
                "multi-node launches are not supported yet.", instance_ids)

        if not site:
            self._validation_error("invalid site: '%s'", site)

        if not caller:
            log.debug("No caller specified. Not restricting access to this launch")

        # validate nodes and build DTRS request
        dtrs_request_node = dict(count=len(instance_ids), site=site,
                                 allocation=allocation)

        # from this point on, errors result in failure records, not exceptions.
        # except for, you know, bugs.
        state = states.REQUESTED
        state_description = None
        try:
            dt = self.dtrs.lookup(caller, deployable_type, dtrs_request_node, vars)
            document = dt['document']
            dtrs_node = dt['node']
            log.debug('got dtrs node: ' + str(dtrs_node))
        except DeployableTypeLookupError, e:
            log.error('Failed to lookup deployable type "%s" in DTRS: %s',
                      deployable_type, str(e))
            state = states.FAILED
            state_description = "DTRS_LOOKUP_FAILED " + str(e)
            document = "N/A"
            dtrs_node = None
        except DeployableTypeValidationError, e:
            log.error('Failed to validate deployable type "%s" in DTRS: %s',
                      deployable_type, str(e))
            state = states.FAILED
            state_description = "DTRS_VALIDATION_FAILED " + str(e)
            document = "N/A"
            dtrs_node = None

        if self.store.is_disabled():
            state = states.REJECTED
            state_description = "PROVISIONER_DISABLED"

        context = None
        try:
            # don't try to create a context when contextualization is disabled
            # or when userdata is passed through directly
            if (self.context and dtrs_node is not None and
               not dtrs_node.get('iaas_userdata') and state == states.REQUESTED):

                context = self.context.create()
                log.debug('Created new context: ' + context.uri)
        except BrokerError, e:
            log.warn('Error while creating new context for launch: %s', e)
            state = states.FAILED
            state_description = "CONTEXT_CREATE_FAILED " + str(e)

        launch_record = {
            'launch_id': launch_id,
            'document': document,
            'deployable_type': deployable_type,
            'context': context,
            'state': state,
            'node_ids': list(instance_ids),
            'creator': caller,
        }

        node_records = []
        for node_id in instance_ids:
            record = {
                'launch_id': launch_id,
                'node_id': node_id,
                'state': state,
                'state_desc': state_description,
                'site': site,
                'allocation': allocation,
                'client_token': launch_id,
                'creator': caller,
            }
            # DTRS returns a bunch of IaaS specific info:
            # ssh key name, "real" allocation name, etc.
            # we fold it in blindly
            if dtrs_node:
                record.update(dtrs_node)

            add_state_change(record, state)
            node_records.append(record)

        try:
            self.store.add_launch(launch_record)
        except WriteConflictError:
            log.debug("record for launch %s already exists, proceeding.",
                      launch_id)
            launch_record = self.store.get_launch(launch_id)

        for index, node in enumerate(node_records):
            try:
                self.store.add_node(node)
            except WriteConflictError:
                log.debug("record for node %s already exists, proceeding.",
                          node['node_id'])
                node_records[index] = self.store.get_node(node['node_id'])

        self.notifier.send_records(node_records)
        return launch_record, node_records

    # XX log here
    def execute_provision(self, launch, nodes, caller):
        """Brings a launch to the PENDING state.

        Any errors or problems will result in FAILURE states
        which will be recorded in datastore and sent to subscribers.
        """
        with EpuLoggerThreadSpecific(user=caller):
            return self._execute_provision(launch, nodes, caller)

    def _execute_provision(self, launch, nodes, caller):
        error_state = None
        error_description = None

        try:
            if self.store.is_disabled():
                error_state = states.REJECTED
                error_description = "PROVISIONER_DISABLED"
                log.error('Provisioner is DISABLED. Rejecting provision request!')

            else:

                self._really_execute_provision_request(launch, nodes, caller)

        except ProvisioningError, e:
            log.error('Failed to execute launch. Problem: ' + str(e))
            error_state = states.FAILED
            error_description = e.message

        except Exception, e:  # catch all exceptions, need to ensure nodes are marked FAILED
            log.error('Launch failed due to an unexpected error. ' +
                      'This is likely a bug and should be reported. Problem: ' +
                      str(e), exc_info=True)
            error_state = states.FAILED
            error_description = 'PROGRAMMER_ERROR ' + str(e)

        if error_state:
            for node in nodes:
                # some nodes may have been successfully launched.
                # only mark others as failed
                if node['state'] < states.PENDING:
                    node['state'] = error_state
                    add_state_change(node, error_state)
                    node['state_desc'] = error_description

            # store and notify launch and nodes with FAILED states
            self.maybe_update_launch_state(
                launch, error_state,
                state_desc=error_description)
            self.store_and_notify(nodes)

    def _really_execute_provision_request(self, launch, nodes, caller):
        """Brings a launch to the PENDING state.
        """
        docstr = launch['document']
        context = launch['context']

        try:
            doc = NimbusClusterDocument(docstr)
        except ValidationError, e:
            raise ProvisioningError('CONTEXT_DOC_INVALID ' + str(e))

        # HACK: sneak in and disable contextualization for the node if we
        # are not using it. Nimboss should really be restructured to better
        # support this.
        if not self.context or nodes[0].get('iaas_userdata'):
            for member in doc.members:
                member.needs_contextualization = False

        specs = doc.build_specs(context)
        if not (specs and len(specs) == 1):
            raise ProvisioningError(
                'INVALID_REQUEST expected exactly one workspace in cluster doc')
        spec = specs[0]

        # we want to fail early, before we launch anything if possible
        self._validate_launch(nodes, spec)

        has_failed = False
        failure_message = None

        # for recovery case
        if not any(node['state'] < states.PENDING for node in nodes):
            log.info('Skipping IaaS launch %s -- all nodes started',
                     spec.name)

        else:
            newstate = None
            try:
                log.info("Launching group:\nlaunch_spec: '%s'\nlaunch_nodes: '%s'",
                         spec, nodes)
                self._launch_one_group(spec, nodes, caller=caller)

            except IaaSIsFullException, iif:
                log.warning('Problem launching group %s: %s',
                            spec.name, str(iif))
                newstate = states.FAILED
                has_failed = True
                failure_message = "IAAS_FULL: " + str(iif)
            except GeneralIaaSException, gie:
                log.exception('Problem launching group %s: %s',
                              spec.name, str(gie))
                newstate = states.FAILED
                has_failed = True
                failure_message = str(gie)
            except Exception, e:
                log.exception('Problem launching group %s: %s',
                              spec.name, str(e))
                newstate = states.FAILED
                has_failed = True
                failure_message = str(e)

            if newstate:
                for node in nodes:
                    node['state'] = newstate
                    if failure_message:
                        node['state_desc'] = failure_message
                    add_state_change(node, newstate)
            self.store_and_notify(nodes)

        if has_failed:
            newstate = states.FAILED
        else:
            newstate = states.PENDING

        self.maybe_update_launch_state(launch, newstate)

    def _validate_launch(self, nodes, spec):
        if spec.count != len(nodes):
            raise ProvisioningError(
                'INVALID_REQUEST node group ' +
                '%s specifies %s nodes but cluster document has %s' %
                (spec.name, len(nodes), spec.count))

    def _launch_one_group(self, spec, nodes, caller=None):
        """Launches a single group: a single IaaS request.
        """

        # assumption here is that a launch group does not span sites or
        # allocations. That may be a feature for later.

        one_node = nodes[0]
        site_name = one_node['site']

        if not caller:
            raise ProvisioningError("Called with bad caller %s" % caller)

        # Get the site description from DTRS
        site_description = self.dtrs.describe_site(site_name)
        if not site_description:
            raise ProvisioningError("Site description not found for %s" % site_name)

        # Get the credentials from DTRS
        credentials_description = self.dtrs.describe_credentials(caller, site_name)
        if not credentials_description:
            raise ProvisioningError("Credentials description not found for %s" % site_name)

        # set some extras in the spec
        allocstring = "default"
        allocation = one_node.get('iaas_allocation')
        if allocation:
            spec.size = allocation
            allocstring = str(allocation)
        keystring = "default"
        sshkeyname = one_node.get('iaas_sshkeyname')
        if sshkeyname:
            spec.keyname = sshkeyname
            keystring = str(sshkeyname)
        userdata = one_node.get('iaas_userdata')
        if userdata:
            spec.userdata = userdata

        client_token = one_node.get('client_token')

        log.debug('Launching group %s - %s nodes (keypair=%s) (allocation=%s)',
                  spec.name, spec.count, keystring, allocstring)

        try:
            driver = SiteDriver(site_description, credentials_description, timeout=self.iaas_timeout)
            try:
                before = time.time()
                iaas_nodes = self._launch_node_spec(
                    spec, driver.driver,
                    ex_clienttoken=client_token)
                after = time.time()
                if self.statsd_client is not None:
                    try:
                        self.statsd_client.timing('provisioner.run_instances.timing', (after - before) * 1000)
                        self.statsd_client.incr('provisioner.run_instances.count')
                        self.statsd_client.timing(
                            'provisioner.run_instances.%s.timing' % site_name, (after - before) * 1000)
                        self.statsd_client.incr('provisioner.run_instances.%s.count' % site_name)
                    except:
                        log.exception("Failed to submit metrics")
            except timeout, t:
                log.exception('Timeout when contacting IaaS to launch nodes: ' + str(t))

                one_node['state'] = states.FAILED
                add_state_change(one_node, states.FAILED)
                one_node['state_desc'] = 'IAAS_TIMEOUT'

                launch = self.store.get_launch(one_node['launch_id'])
                if launch:
                    self.maybe_update_launch_state(launch, states.FAILED)
                self.store_and_notify([one_node])

                raise timeout('IAAS_TIMEOUT')
        except Exception, e:
            # XXX TODO introspect the exception to get more specific error information
            exp_as_str = str(e)
            if "InstanceLimitExceeded" in exp_as_str:
                raise IaaSIsFullException(exp_as_str)
            else:
                raise GeneralIaaSException(exp_as_str)

        # underlying node driver may return a list or an object
        if not hasattr(iaas_nodes, '__iter__'):
            iaas_nodes = [iaas_nodes]

        if len(iaas_nodes) != len(nodes):
            message = '%s nodes from IaaS launch but %s were expected' % (
                len(iaas_nodes), len(nodes))
            log.error(message)
            raise ProvisioningError('IAAS_PROBLEM ' + message)

        for node_rec, iaas_node in izip(nodes, iaas_nodes):
            node_rec['iaas_id'] = iaas_node.id

            update_node_ip_info(node_rec, iaas_node)

            node_rec['state'] = states.PENDING
            add_state_change(node_rec, states.PENDING)
            node_rec['pending_timestamp'] = time.time()

            extradict = {'public_ip': node_rec.get('public_ip'),
                         'iaas_id': iaas_node.id, 'node_id': node_rec['node_id']}
            cei_events.event("provisioner", "new_node", extra=extradict)

    def _launch_node_spec(self, spec, driver, **kwargs):
        """Launches a single node group.

        Returns a single Node or a list of Nodes.
        """
        node_data = self._create_node_data(spec, driver, **kwargs)
        node = driver.create_node(**node_data)

        if isinstance(node, (list, tuple)):
            for n in node:
                n.ctx_name = spec.name
        else:
            node.ctx_name = spec.name

        return node

    def _create_node_data(self, spec, driver, **kwargs):
        """Utility to get correct form of data to create a Node.
        """

        # if we expand beyond EC2/Nimbus we may need to do something better here.
        # libcloud would prefer we do driver.list_sizes() and list_images() but
        # those are ugly for lots of reasons.
        image = LibcloudNodeImage(spec.image, spec.name, driver)
        size = LibcloudNodeSize(spec.size, spec.size, None, None, None, None, driver)

        node_data = {
            'name': spec.name,
            'size': size,
            'image': image,
            'ex_mincount': str(spec.count),
            'ex_maxcount': str(spec.count),
            'ex_userdata': spec.userdata,
            'ex_keyname': spec.keyname,
        }

        node_data.update(kwargs)
        # libcloud doesn't like args with None values
        return dict(pair for pair in node_data.iteritems() if pair[1] is not None)

    def store_and_notify(self, records):
        """Convenience method to store records and notify subscribers.
        """
        for node in records:
            node_update_counter = node.get('update_counter', 0)
            node['update_counter'] = node_update_counter + 1
            self.maybe_update_node(node)

        self.notifier.send_records(records)

    def maybe_update_node(self, node):
        updated = False
        current = node
        while not updated and current['state'] <= node['state']:
            # HACK copying store metadata. really each operation should
            # carefully decide whether to retry an update.
            node[VERSION_KEY] = current[VERSION_KEY]
            try:
                self.store.update_node(node)
                updated = True
            except WriteConflictError:
                current = self.store.get_node(node['node_id'])
        return node, updated

    def maybe_update_launch_state(self, launch, newstate, **updates):
        updated = False
        while launch['state'] < newstate:
            try:
                if updates:
                    launch.update(updates)
                launch['state'] = newstate

                self.store.update_launch(launch)
                updated = True
            except WriteConflictError:
                launch = self.store.get_launch(launch['launch_id'])
        return launch, updated

    def dump_state(self, nodes):
        """Resends node state information to subscribers

        @param nodes list of node IDs
        """
        for node_id in nodes:
            node = self.store.get_node(node_id)
            if node:
                self.notifier.send_record(node)
            else:
                log.warn("Got dump_state request for unknown node '%s'", node_id)

    def query_nodes(self, concurrency=1):
        """Performs queries of IaaS sites and sends updates to subscribers.
        """
        # Right now we just query everything. Could be made more granular later

        nodes = self.store.get_nodes(max_state=states.TERMINATING)
        site_nodes = group_records(nodes, 'site')

        if nodes:
            log.debug("Querying state of %d nodes", len(nodes))

        if concurrency > 1:
            pool = Pool(concurrency)

        for site, nodes in site_nodes.iteritems():
            user_nodes = group_records(nodes, 'creator')
            for user, nodes in user_nodes.iteritems():
                if concurrency > 1:
                    pool.spawn(self.query_one_site, site, nodes, caller=user)
                else:
                    self.query_one_site(site, nodes, caller=user)

        if concurrency > 1:
            pool.join()

        if self.statsd_client is not None:
            try:
                nodes = self.store.get_nodes(max_state=states.TERMINATING)
                self.statsd_client.gauge("instances", len(nodes))
                pending_nodes = self.store.get_nodes(state=states.PENDING)
                self.statsd_client.gauge("pending_instances", len(pending_nodes))
                running_nodes = self.store.get_nodes(min_state=states.STARTED, max_state=states.RUNNING)
                self.statsd_client.gauge("running_instances", len(running_nodes))
                terminating_nodes = self.store.get_nodes(state=states.TERMINATING)
                self.statsd_client.gauge("terminating_instances", len(terminating_nodes))
            except:
                log.exception("Failed to submit metrics")

    def query_one_site(self, site, nodes, caller=None):
        with EpuLoggerThreadSpecific(user=caller):
            return self._query_one_site(site, nodes, caller=caller)

    def _query_one_site(self, site, nodes, caller=None):
        log.info("Querying site %s about %d nodes", site, len(nodes))

        if not caller:
            raise ProvisioningError("Called with bad caller %s" % caller)

        # Get the site description from DTRS
        site_description = self.dtrs.describe_site(site)
        if not site_description:
            raise ProvisioningError("Site description not found for %s" % site)

        # Get the credentials from DTRS
        credentials_description = self.dtrs.describe_credentials(caller, site)
        if not credentials_description:
            raise ProvisioningError("Credentials description not found for %s" % site)

        site_driver = SiteDriver(site_description, credentials_description, timeout=self.iaas_timeout)

        try:
            before = time.time()
            libcloud_nodes = site_driver.driver.list_nodes()
            after = time.time()
            if self.statsd_client is not None:
                try:
                    self.statsd_client.timing('provisioner.list_instances.timing', (after - before) * 1000)
                    self.statsd_client.incr('provisioner.list_instances.count')
                    self.statsd_client.timing('provisioner.list_instances.%s.timing' % site, (after - before) * 1000)
                    self.statsd_client.incr('provisioner.list_instances.%s.count' % site)
                except:
                    log.exception("Failed to submit metrics")
        except timeout:
            log.exception('Timeout when querying site "%s"', site)
            raise

        libcloud_nodes = dict((node.id, node) for node in libcloud_nodes)

        # note we are walking the nodes from datastore, NOT from libcloud
        for node in nodes:
            state = node['state']
            if state < states.PENDING or state >= states.TERMINATED:
                continue

            libcloud_id = node.get('iaas_id')
            libcloud_node = libcloud_nodes.pop(libcloud_id, None)
            if not libcloud_node:
                # this state is unknown to underlying IaaS. What could have
                # happened? IaaS error? Recovery from loss of net to IaaS?

                # Or lazily-updated records. On EC2, there can be a short
                # window where pending instances are not included in query
                # response

                start_time = node.get('pending_timestamp')
                now = time.time()
                if node['state'] == states.TERMINATING:
                    log.debug('node %s: %s in datastore but unknown to IaaS.' +
                              ' Termination must have already happened.',
                              node['node_id'], states.TERMINATING)

                    node['state'] = states.TERMINATED
                    add_state_change(node, states.TERMINATED)
                    self.store_and_notify([node])

                elif (node['state'] < states.STARTED and start_time and
                      (now - start_time) <= _IAAS_NODE_QUERY_WINDOW_SECONDS):
                    log.debug('node %s: not in query of IaaS, but within ' +
                              'allowed startup window (%d seconds)',
                              node['node_id'], _IAAS_NODE_QUERY_WINDOW_SECONDS)
                else:
                    log.warn('node %s: in data store but unknown to IaaS. ' +
                             'Marking as terminated.', node['node_id'])

                    node['state'] = states.FAILED
                    add_state_change(node, states.FAILED)
                    node['state_desc'] = 'NODE_DISAPPEARED'
                    self.store_and_notify([node])
            else:
                libcloud_state = _LIBCLOUD_STATE_MAP[libcloud_node.state]

                # when contextualization is disabled or userdata is passed
                # through directly, instances go straight to the RUNNING
                # state
                if libcloud_state == states.STARTED and (not self.context or node.get('iaas_userdata')):
                    libcloud_state = states.RUNNING

                if libcloud_state > node['state']:
                    # TODO libcloud could go backwards in state.

                    node['state'] = libcloud_state
                    add_state_change(node, libcloud_state)

                    update_node_ip_info(node, libcloud_node)

                    if libcloud_state == states.STARTED:
                        node['running_timestamp'] = time.time()
                        extradict = {'iaas_id': libcloud_id,
                                     'node_id': node.get('node_id'),
                                     'public_ip': node.get('public_ip'),
                                     'private_ip': node.get('private_ip')}
                        cei_events.event("provisioner", "node_started",
                                         extra=extradict)

                    self.store_and_notify([node])
                elif libcloud_state == node['state']:
                    updated_ip = update_node_ip_info(node, libcloud_node)
                    if updated_ip:
                        self.store_and_notify([node])
        # TODO libcloud_nodes now contains any other running instances that
        # are unknown to the datastore (or were started after the query)
        # Could do some analysis of these nodes

    def _get_nodes_by_id(self, node_ids, skip_missing=True):
        """Helper method to retrieve node records from a list of IDs
        """
        nodes = []
        for node_id in node_ids:
            node = self.store.get_node(node_id)
            # when skip_missing is false, include a None entry for missing nodes
            if node or not skip_missing:
                nodes.append(node)
        return nodes

    def query_contexts(self, concurrency=1):
        """Queries all open launch contexts and sends node updates.
        """

        if not self.context:
            return

        # grab all the launches in the pending state
        launches = self.store.get_launches(state=states.PENDING)
        if launches:
            log.debug("Querying state of %d contexts", len(launches))

        if concurrency > 1:
            pool = Pool(concurrency)
            for launch in launches:
                pool.spawn(self._query_one_context, launch)
            pool.join()
        else:
            for launch in launches:
                self._query_one_context(launch)

    def _query_one_context(self, launch):

        launch_id = launch['launch_id']
        node_ids = launch['node_ids']
        nodes = self._get_nodes_by_id(node_ids)

        if nodes[0].get('iaas_userdata'):
            return

        context = launch.get('context')
        if not context:
            log.warn('Launch %s is in %s state but it has no context!',
                     launch['launch_id'], launch['state'])
            return

        all_pending = all(node['state'] >= states.PENDING for node in nodes)
        if not all_pending:
            log.debug("Not all nodes for launch %s are pending or running" +
                      " in IaaS yet. Skipping this context query for now.",
                      launch_id)

            # note that this check is important for preventing races (I think).
            # if we start querying before all nodes are running in IaaS the
            # following scenario is problematic:
            #
            # - launch has a node in REQUESTED state and it is being started
            #    by one provisioner worker.
            # - On another worker, the ctx query runs and receives a permanent
            #    error (maybe the ctx broker has been reset and the context is
            #    no longer known). It marks the launch as FAILED.
            # - Now we have a problem: we can't mark the REQUESTING node as
            #   RUNNING_FAILED because it is not (necessarily) running yet
            #   (and we don't even have an IaaS handle for it). But if we just
            #   mark the node as FAILED, it is possible the other worker will
            #   simultaneously be starting it and the node will be "leaked".

            return  # *** EARLY RETURN ***

        valid = any(node['state'] < states.TERMINATING for node in nodes)
        if not valid:
            log.info("The context for launch %s has no valid nodes. They " +
                     "have likely been terminated. Marking launch as FAILED. " +
                     "nodes: %s", launch_id, node_ids)
            self.maybe_update_launch_state(launch, states.FAILED)
            return  # *** EARLY RETURN ***

        ctx_uri = context['uri']
        log.debug('Querying context %s for launch %s ', ctx_uri, launch_id)

        try:
            context_status = self.context.query(ctx_uri)

        except (BrokerAuthError, ContextNotFoundError), e:
            log.error("permanent error from context broker for launch %s. " +
                      "Marking launch as FAILED. Error: %s", launch_id, e)

            # first mark instances as failed, then the launch. Otherwise a
            # crash at this moment could leave some nodes stranded at
            # STARTING

            # we are assured above that all nodes are >= STARTED

            updated_nodes = []
            for node in nodes:
                if node['state'] < states.RUNNING_FAILED:
                    node['state'] = states.RUNNING_FAILED
                    add_state_change(node, states.RUNNING_FAILED)
                    updated_nodes.append(node)
            if updated_nodes:
                log.debug("Marking %d nodes as %s", len(updated_nodes), states.RUNNING_FAILED)
                self.store_and_notify(updated_nodes)

            self.maybe_update_launch_state(launch, states.FAILED)

            return  # *** EARLY RETURN ***

        except BrokerError, e:
            log.error("Error querying context broker: %s", e, exc_info=True)
            # hopefully this is some temporal failure, query will be retried
            return  # *** EARLY RETURN ***

        ctx_nodes = context_status.nodes

        matched_nodes = match_nodes_from_context(nodes, ctx_nodes)
        updated_nodes = update_nodes_from_context(matched_nodes)

        # Catch contextualization timeouts
        now = time.time()
        for node in nodes:
            if node not in map(lambda mn: mn['node'], matched_nodes):
                if node['state'] == states.STARTED and node['running_timestamp'] + INSTANCE_READY_TIMEOUT < now:
                    log.info(("Node %s failed to check in with the context " +
                             "broker within the %d seconds timeout, marking " +
                             "it as RUNNING_FAILED") %
                             (node['node_id'], INSTANCE_READY_TIMEOUT))

                    node['state'] = states.RUNNING_FAILED
                    add_state_change(node, states.RUNNING_FAILED)
                    updated_nodes.append(node)

                    extradict = {'iaas_id': node.get('iaas_id'),
                                 'node_id': node.get('node_id'),
                                 'public_ip': node.get('public_ip'),
                                 'private_ip': node.get('private_ip')}
                    cei_events.event("provisioner", "node_ctx_timeout",
                                     extra=extradict)

        if updated_nodes:
            log.debug("%d nodes need to be updated as a result of the context query" %
                      len(updated_nodes))
            self.store_and_notify(updated_nodes)

        all_done = all(ctx_node.ok_occurred or
                       ctx_node.error_occurred for ctx_node in ctx_nodes)

        if context_status.complete and all_done:
            log.info('Launch %s context is "all-ok": done!', launch_id)
            # update the launch record so this context won't be re-queried
            extradict = {'launch_id': launch_id, 'node_ids': launch['node_ids']}
            if all(ctx_node.ok_occurred for ctx_node in ctx_nodes):
                cei_events.event("provisioner", "launch_ctx_done", extra=extradict)
            else:
                cei_events.event("provisioner", "launch_ctx_error", extra=extradict)
            self.maybe_update_launch_state(launch, states.RUNNING)

        elif context_status.complete:
            log.info('Launch %s context is "complete" (all checked in, but not all-ok)', launch_id)
        else:
            if not ctx_nodes:
                log.debug('Launch %s context has no nodes (yet)', launch_id)
            else:
                log.debug('Launch %s context is incomplete: %s of %s nodes',
                          launch_id, len(context_status.nodes),
                          context_status.expected_count)

    # Record reaper main method
    def reap_records(self, record_reaping_max_age):
        """Removes node records in terminal states TERMINATED, FAILED, or
        REJECTED that are older than record_reaping_max_age.

        Also cleans up launch records that don't have any VM anymore.
        """
        log.info("Reaping records older than %f seconds", record_reaping_max_age)
        now = time.time()

        nodes = self.store.get_nodes()
        for node in nodes:
            if node['state'] in [states.TERMINATED, states.FAILED, states.REJECTED]:
                try:
                    launch = self.store.get_launch(node['launch_id'])
                    last_state_change = node['state_changes'][-1]
                    _, timestamp = last_state_change
                    if now > timestamp + record_reaping_max_age:
                        log.info("Removing node %s with no state change for %f seconds",
                                 node['node_id'], now - timestamp)
                        self.store.remove_node(node['node_id'])

                        updated = False
                        if launch is not None:
                            while not updated and node['node_id'] in launch['node_ids']:
                                launch['node_ids'].remove(node['node_id'])
                                try:
                                    self.store.update_launch(launch)
                                    updated = True
                                except WriteConflictError:
                                    launch = self.store.get_launch(launch['launch_id'])

                            if not launch['node_ids']:
                                launch_id = launch['launch_id']
                                log.info("Removing launch %s with no node record", launch_id)
                                self.store.remove_launch(launch_id)
                        else:
                            log.warn("Node %s was part of missing launch %s" % (node['node_id'], node['launch_id']))
                except Exception:
                    log.exception('Error when deleting old node record %s' % node['node_id'])
                    continue

    def terminate_all(self):
        """Mark all nodes as terminating
        """
        nodes = self.store.get_nodes(max_state=states.TERMINATING)
        for node in nodes:
            if node['state'] < states.TERMINATING:
                self._mark_one_node_terminating(node)

    def check_terminate_all(self):
        """Check if there are no launches left to terminate
        """
        nodes = self.store.get_nodes(max_state=states.TERMINATING)
        return len(nodes) == 0

    def mark_nodes_terminating(self, node_ids, caller=None):
        with EpuLoggerThreadSpecific(user=caller):
            return self._mark_nodes_terminating(node_ids, caller=caller)

    def _create_unknown_node_terminated_record(self, node_id, caller=None):
        node = dict(launch_id=self.UNKNOWN_LAUNCH_ID, node_id=node_id,
            state_desc=None, creator=caller, state=states.TERMINATED)
        add_state_change(node, states.TERMINATED)
        return node

    def _mark_nodes_terminating(self, node_ids, caller=None):
        """Mark a set of nodes as terminating in the data store
        """
        log.debug("Marking nodes for termination: %s", node_ids)
        nodes = []
        for node_id in node_ids:
            node = self.store.get_node(node_id)
            if node is None:
                # construct a fake TERMINATED node record
                node = self._create_unknown_node_terminated_record(node_id)
                try:
                    self.store.add_node(node)
                    self.notifier.send_record(node)
                    log.warn("Created stub TERMINATED record for unknown node %s", node_id)
                except WriteConflictError:
                    # record must have been just added
                    node = self.store.get_node(node_id)
            if node:
                check_user(caller=caller, creator=node.get('creator'))
                nodes.append(node)

            for node in nodes:
                self._mark_one_node_terminating(node)
        return nodes

    def _mark_one_node_terminating(self, node):
        if node['state'] < states.TERMINATING:
            self.store.add_terminating(node['node_id'])
            node['state'] = states.TERMINATING
            add_state_change(node, states.TERMINATING)
            extradict = {'iaas_id': node.get('iaas_id'),
                         'node_id': node.get('node_id'),
                         'public_ip': node.get('public_ip'),
                         'private_ip': node.get('private_ip')}
            cei_events.event("provisioner", "node_terminating",
                             extra=extradict)
        self.store_and_notify([node])

    def terminate_nodes(self, node_ids, caller=None, remove_terminating=True):
        """Destroy all specified nodes.

        This is currently only used from tests
        """
        with EpuLoggerThreadSpecific(user=caller):
            nodes = self._get_nodes_by_id(node_ids, skip_missing=False)
            for node_id, node in izip(node_ids, nodes):
                if not node:
                    # maybe an error should make it's way to controller from here?
                    log.warn('Node %s unknown but requested for termination',
                             node_id)
                    continue

                self._terminate_node(
                    node, remove_terminating=remove_terminating)

    def terminate_node(self, node, remove_terminating=True):
        try:
            self._terminate_node(node, remove_terminating)
        except Exception:
            log.exception("Error terminating node %s", node)
            raise

    def _terminate_node(self, node, remove_terminating):

        if node['state'] < states.PENDING:
            log.info("Node %s requested for termination before it reached PENDING, "
                     "no need to terminate in IaaS", node['node_id'])
        elif node['state'] >= states.TERMINATED:
            log.info("Node %s requested for termination but it is already %s",
                node['node_id'], node['state'])

        else:
            log.info("Terminating node %s", node['node_id'])

            site_name = node['site']
            caller = node['creator']
            if not caller:
                msg = "Node %s has bad creator %s" % (node['node_id'], caller)
                log.error(msg)
                raise ProvisioningError(msg)

            # Get the site description from DTRS
            site_description = self.dtrs.describe_site(site_name)
            if not site_description:
                msg = "Site description not found for %s" % site_name
                log.error(msg)
                raise ProvisioningError(msg)

            # Get the credentials from DTRS
            credentials_description = self.dtrs.describe_credentials(caller, site_name)
            if not credentials_description:
                msg = "Credentials description not found for %s" % site_name
                log.error(msg)
                raise ProvisioningError(msg)

            site_driver = SiteDriver(site_description, credentials_description, timeout=self.iaas_timeout)
            libcloud_node = self._to_libcloud_node(node, site_driver.driver)
            try:
                log.info("Destroying node %s on IaaS", node.get('node_id'))
                site_driver.driver.destroy_node(libcloud_node)
            except timeout:
                log.exception('Timeout when terminating node %s with iaas_id %s',
                              node.get('node_id'), node.get('iaas_id'))
                raise
            except Exception:
                log.exception('Problem when terminating %s with iaas_id %s',
                              node.get('node_id'), node.get('iaas_id'))
                raise

        node['state'] = states.TERMINATED
        add_state_change(node, states.TERMINATED)
        extradict = {'iaas_id': node.get('iaas_id'),
                     'node_id': node.get('node_id'),
                     'public_ip': node.get('public_ip'),
                     'private_ip': node.get('private_ip')}
        cei_events.event("provisioner", "node_terminated",
                         extra=extradict)

        self.store_and_notify([node])
        if remove_terminating:
            self.store.remove_terminating(node.get('node_id'))
            log.info("Removed terminating entry for node %s from store",
                     node.get('node_id'))

    def _to_libcloud_node(self, node, driver):
        """libcloud drivers need a Node object for termination.
        """
        # this is unfortunately tightly coupled with EC2 libcloud driver
        # right now. We are building a fake Node object that only has the
        # attribute needed for termination (id). Would need to be fleshed out
        # to work with other drivers.
        return LibcloudNode(id=node['iaas_id'], name=None, state=None,
                            public_ips=None, private_ips=None,
                            driver=driver)

    def describe_nodes(self, nodes=None, caller=None):
        """Produce a list of node records
        """
        with EpuLoggerThreadSpecific(user=caller):
            return self._describe_nodes(nodes=nodes, caller=caller)

    def _describe_nodes(self, nodes=None, caller=None):
        if not nodes:
            results = []
            for node in self.store.get_nodes():
                try:
                    check_user(creator=node.get('creator'), caller=caller)
                except UserNotPermittedError:
                    log.debug("%s isn't owned by %s, skipping" % (node.get('id'), caller))
                    continue
                results.append(sanitize_record(node))

            return results

        results = []
        for node_id in nodes:
            node = self.store.get_node(node_id)
            if node is None:
                raise KeyError("specified node '%s' is unknown" % node_id)

            try:
                check_user(creator=node.get('creator'), caller=caller)
            except UserNotPermittedError:
                log.debug("%s isn't owned by %s, skipping" % (node.get('id'), caller))
                continue

            # sanitize node record of store metadata
            sanitize_record(node)

            results.append(node)
        return results


def update_node_ip_info(node_rec, iaas_node):
    """Grab node IP information from libcloud Node object, if not already set.
    Update information if it has changed.
    """
    # ec2 libcloud driver places IP in a list
    #
    # if we support drivers that actually have multiple
    # public and private IPs, we will need to revisit this
    updated = False

    public_ip = node_rec.get('public_ip')
    iaas_public_ip = iaas_node.public_ip
    if isinstance(iaas_public_ip, (list, tuple)):
        iaas_public_ip = iaas_public_ip[0] if iaas_public_ip else None
    if not public_ip or (iaas_public_ip and public_ip != iaas_public_ip):
        node_rec['public_ip'] = iaas_public_ip
        updated = True

    private_ip = node_rec.get('private_ip')
    iaas_private_ip = iaas_node.private_ip
    if isinstance(iaas_private_ip, (list, tuple)):
        iaas_private_ip = iaas_private_ip[0] if iaas_private_ip else None
    if not private_ip or (iaas_private_ip and private_ip != iaas_private_ip):
        node_rec['private_ip'] = iaas_private_ip
        updated = True

    hostname = node_rec.get('hostname')
    iaas_hostname = iaas_node.extra.get('dns_name')
    if isinstance(iaas_hostname, (list, tuple)):
        iaas_hostname = iaas_hostname[0] if iaas_hostname else None
    if iaas_hostname and (not hostname or (iaas_hostname and hostname != iaas_hostname)):
        node_rec['hostname'] = iaas_hostname
        updated = True

    return updated


def match_nodes_from_context(nodes, ctx_nodes):
    matched_nodes = []
    for ctx_node in ctx_nodes:
        for ident in ctx_node.identities:

            match_reason = None
            match_node = None
            for node in nodes:
                if ident.ip and ident.ip == node['public_ip']:
                    match_node = node
                    match_reason = 'public IP match'
                    break
                elif ident.hostname and ident.hostname == node['public_ip']:
                    match_node = node
                    match_reason = 'libcloud IP matches ctx hostname'
                # can add more matches if needed

            if match_node:
                log.debug('Matched ctx identity to node by: ' + match_reason)
                matched_nodes.append(dict(node=match_node, ctx_node=ctx_node, ident=ident))
                break

            else:
                # this isn't necessarily an exceptional condition. could be a private
                # IP for example. Right now we are only matching against public
                log.debug('Context identity has unknown IP (%s) and hostname (%s)',
                          ident.ip, ident.hostname)
    return matched_nodes


def update_nodes_from_context(matched_nodes):
    updated_nodes = []
    for match_node in matched_nodes:
        if _update_one_node_from_ctx(match_node['node'], match_node['ctx_node'], match_node['ident']):
            updated_nodes.append(match_node['node'])

    return updated_nodes


def _update_one_node_from_ctx(node, ctx_node, identity):
    node_done = ctx_node.ok_occurred or ctx_node.error_occurred
    if not node_done or node['state'] >= states.RUNNING:
        return False
    if ctx_node.ok_occurred:
        node['state'] = states.RUNNING
        add_state_change(node, states.RUNNING)
        node['pubkey'] = identity.pubkey
    else:
        node['state'] = states.RUNNING_FAILED
        add_state_change(node, states.RUNNING_FAILED)
        node['state_desc'] = "CTX_ERROR"
        node['ctx_error_code'] = ctx_node.error_code
        node['ctx_error_message'] = ctx_node.error_message
    return True


def add_state_change(node, state):
    now = time.time()
    if 'state_changes' not in node:
        node['state_changes'] = []
    state_change = state, now
    node['state_changes'].append(state_change)


class ProvisionerContextClient(object):
    """Provisioner calls to context broker.
    """
    def __init__(self, broker_uri, key, secret):
        self._broker_uri = broker_uri
        self._key = key
        self._secret = secret

    def _get_client(self):
        # we ran into races with sharing a ContextClient between threads so
        # now we create a new one for each call. Technically we could probably
        # just have one for create() and one for query() but this is safer
        # in case we start handling multiple provisions simultaneously or
        # something.
        return ContextClient(self._broker_uri, self._key, self._secret)

    def create(self):
        """Creates a new context with the broker
        """
        client = self._get_client()
        context = client.create_context()
        return context

    def query(self, resource):
        """Queries an existing context.

        resource is the uri returned by create operation
        """
        client = self._get_client()
        return client.get_status(resource)


class ProvisioningError(Exception):
    pass


class ProvisionerDisabledError(Exception):
    pass
