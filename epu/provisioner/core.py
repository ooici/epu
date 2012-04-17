#!/usr/bin/env python

"""
@file epu/provisioner/core.py
@author David LaBissoniere
@brief Starts, stops, and tracks instance and context state.
"""

import time
import logging
from itertools import izip
from gevent import Timeout

from nimboss.ctx import ContextClient, BrokerError, BrokerAuthError, \
    ContextNotFoundError
from nimboss.cluster import ClusterDriver
from nimboss.nimbus import NimbusClusterDocument, ValidationError
from libcloud.compute.types import NodeState as NimbossNodeState
from libcloud.compute.base import Node as NimbossNode
from epu.provisioner.store import group_records, sanitize_record, VERSION_KEY
from epu.localdtrs import DeployableTypeLookupError, DeployableTypeValidationError
from epu.states import InstanceState
from epu.exceptions import WriteConflictError, UserNotPermittedError
from epu import cei_events
from epu.util import check_user

log = logging.getLogger(__name__)

# alias for shorter code
states = InstanceState

__all__ = ['ProvisionerCore', 'ProvisioningError']

_NIMBOSS_STATE_MAP = {
        NimbossNodeState.RUNNING : states.STARTED,
        NimbossNodeState.REBOOTING : states.STARTED, #TODO hmm
        NimbossNodeState.PENDING : states.PENDING,
        NimbossNodeState.TERMINATED : states.TERMINATED,
        NimbossNodeState.UNKNOWN : states.ERROR_RETRYING}

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

    def __init__(self, store, notifier, dtrs, sites, context, logger=None):
        """

        @type store: ProvisionerStore
        """

        if logger:
            global log
            log = logger

        self.store = store
        self.notifier = notifier
        self.dtrs = dtrs

        self.sites = sites
        self.context = context

        if not context:
            log.warn("No context client provided. Contextualization disabled.")

        self.cluster_driver = ClusterDriver()

    def recover(self):
        """Finishes any incomplete launches or terminations
        """
        incomplete_launches = self.store.get_launches(
                state=states.REQUESTED)
        for launch in incomplete_launches:
            nodes = self._get_nodes_by_id(launch['node_ids'])

            log.info('Attempting recovery of incomplete launch: %s',
                     launch['launch_id'])
            self.execute_provision(launch, nodes)

        terminating_nodes = self.store.get_nodes(
                state=states.TERMINATING)
        if terminating_nodes:
            node_ids = [node['node_id'] for node in terminating_nodes]
            log.info('Attempting recovery of incomplete node terminations: %s',
                         ','.join(node_ids))
            self.terminate_nodes(node_ids)

    def _validation_error(self, msg, *args):
        log.debug("raising provisioning validation error: "+ msg, *args)
        raise ProvisioningError("Invalid provision request: " + msg % args)

    def prepare_provision(self, launch_id, deployable_type, instance_ids,
                          subscribers, site, allocation=None, vars=None, caller=None):
        """Validates request and commits to datastore.

        If the request has subscribers, they are notified with the
        node state records.

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

        # initial validation
        if not launch_id:
            self._validation_error("bad launch_id '%s'", launch_id)

        if not deployable_type:
            self._validation_error("bad deployable_type '%s'", deployable_type)

        if not (isinstance(instance_ids, (list,tuple)) and
                len(instance_ids) == 1 and instance_ids[0]):
            self._validation_error(
                "bad instance_ids '%s': need a list or tuple of length 1 -- "+
                "multi-node launches are not supported yet.", instance_ids)

        if not isinstance(subscribers, (list,tuple)):
            self._validation_error("bad subscribers '%s'", subscribers)

        if not site:
            self._validation_error("invalid site: '%s'", site)

        if not caller:
            log.debug("No caller specified. Not restricting access to this launch")

        #validate nodes and build DTRS request
        dtrs_request_node = dict(count=len(instance_ids), site=site,
            allocation=allocation)

        # from this point on, errors result in failure records, not exceptions.
        # except for, you know, bugs.
        state = states.REQUESTED
        state_description = None
        try:
            # TODO: Change to work with new DTRS
            dt = self.dtrs.lookup(deployable_type, dtrs_request_node, vars)
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
            log.error(str(e))
            state = states.FAILED
            state_description = "DTRS_LOOKUP_FAILED " + str(e)
            document = "N/A"
            dtrs_node = None

        if self.store.is_disabled():
            state = states.REJECTED
            state_description = "PROVISIONER_DISABLED"

        context = None
        try:
            # don't try to create a context when contextualization is disabled
            if self.context and state == states.REQUESTED:

                context = self.context.create()
                log.debug('Created new context: ' + context.uri)
        except BrokerError, e:
            log.warn('Error while creating new context for launch: %s', e)
            state = states.FAILED
            state_description = "CONTEXT_CREATE_FAILED " + str(e)

        launch_record = {
                'launch_id' : launch_id,
                'document' : document,
                'deployable_type' : deployable_type,
                'context' : context,
                'subscribers' : subscribers,
                'state' : state,
                'node_ids' : list(instance_ids),
                'creator' : caller,
                }

        node_records = []
        for node_id in instance_ids:
            record = {'launch_id' : launch_id,
                    'node_id' : node_id,
                    'state' : state,
                    'state_desc' : state_description,
                    'site' : site,
                    'allocation' : allocation,
                    'client_token' : launch_id,
                    'creator' : caller,
                    }
            #DTRS returns a bunch of IaaS specific info:
            # ssh key name, "real" allocation name, etc.
            # we fold it in blindly
            if dtrs_node:
                record.update(dtrs_node)

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

        self.notifier.send_records(node_records, subscribers)
        return launch_record, node_records

    def execute_provision(self, launch, nodes):
        """Brings a launch to the PENDING state.

        Any errors or problems will result in FAILURE states
        which will be recorded in datastore and sent to subscribers.
        """

        error_state = None
        error_description = None

        try:
            if self.store.is_disabled():
                error_state = states.REJECTED
                error_description = "PROVISIONER_DISABLED"
                log.error('Provisioner is DISABLED. Rejecting provision request!')

            else:

                self._really_execute_provision_request(launch, nodes)

        except ProvisioningError, e:
            log.error('Failed to execute launch. Problem: ' + str(e))
            error_state = states.FAILED
            error_description = e.message

        except Exception, e: # catch all exceptions, need to ensure nodes are marked FAILED
            log.error('Launch failed due to an unexpected error. '+
                    'This is likely a bug and should be reported. Problem: ' +
                    str(e), exc_info=True)
            error_state = states.FAILED
            error_description = 'PROGRAMMER_ERROR '+str(e)

        if error_state:
            for node in nodes:
                # some nodes may have been successfully launched.
                # only mark others as failed  
                if node['state'] < states.PENDING:
                    node['state'] = error_state
                    add_state_change(node, error_state)
                    node['state_desc'] = error_description

            #store and notify launch and nodes with FAILED states
            self.maybe_update_launch_state(launch, error_state,
                state_desc=error_description)
            self.store_and_notify(nodes, launch['subscribers'])

    def _really_execute_provision_request(self, launch, nodes):
        """Brings a launch to the PENDING state.
        """
        subscribers = launch['subscribers']
        docstr = launch['document']
        context = launch['context']

        try:
            doc = NimbusClusterDocument(docstr)
        except ValidationError, e:
            raise ProvisioningError('CONTEXT_DOC_INVALID '+str(e))

        # HACK: sneak in and disable contextualization for the node if we
        # are not using it. Nimboss should really be restructured to better
        # support this.
        if not self.context:
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

        # for recovery case
        if not any(node['state'] < states.PENDING for node in nodes):
            log.info('Skipping IaaS launch %s -- all nodes started',
                     spec.name)

        else:
            newstate = None
            try:
                log.info("Launching group:\nlaunch_spec: '%s'\nlaunch_nodes: '%s'",
                         spec, nodes)
                self._launch_one_group(spec, nodes)

            except Exception,e:
                log.exception('Problem launching group %s: %s',
                        spec.name, str(e))
                newstate = states.FAILED
                has_failed = True

            if newstate:
                for node in nodes:
                    node['state'] = newstate
                    add_state_change(node, newstate)
            self.store_and_notify(nodes, subscribers)

        if has_failed:
            newstate = states.FAILED
        else:
            newstate = states.PENDING

        self.maybe_update_launch_state(launch, newstate)

    def _validate_launch(self, nodes, spec):
        if spec.count != len(nodes):
            raise ProvisioningError(
                'INVALID_REQUEST node group '+
                '%s specifies %s nodes but cluster document has %s' %
                (spec.name, len(nodes), spec.count))

    def _launch_one_group(self, spec, nodes):
        """Launches a single group: a single IaaS request.
        """

        #assumption here is that a launch group does not span sites or
        #allocations. That may be a feature for later.

        one_node = nodes[0]
        site = one_node['site']

        #set some extras in the spec
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

        client_token = one_node.get('client_token')

        log.debug('Launching group %s - %s nodes (keypair=%s) (allocation=%s)',
                spec.name, spec.count, keystring, allocstring)

        try:
            with self.sites.acquire_driver(site) as driver:
                try:
                    with Timeout(self._IAAS_DEFAULT_TIMEOUT):
                        iaas_nodes = self.cluster_driver.launch_node_spec(spec,
                                driver.driver, ex_clienttoken=client_token)
                except Timeout, t:
                    log.exception('Timeout when contacting IaaS to launch nodes: ' + str(e))
                    raise
        except Exception, e:
            log.exception('Error launching nodes: ' + str(e))
            # wrap this up?
            raise

        # underlying node driver may return a list or an object
        if not hasattr(iaas_nodes, '__iter__'):
            iaas_nodes = [iaas_nodes]

        if len(iaas_nodes) != len(nodes):
            message = '%s nodes from IaaS launch but %s were expected' % (
                    len(iaas_nodes), len(nodes))
            log.error(message)
            raise ProvisioningError('IAAS_PROBLEM '+ message)

        for node_rec, iaas_node in izip(nodes, iaas_nodes):
            node_rec['iaas_id'] = iaas_node.id

            update_node_ip_info(node_rec, iaas_node)

            node_rec['state'] = states.PENDING
            add_state_change(node_rec, states.PENDING)
            node_rec['pending_timestamp'] = time.time()

            extradict = {'public_ip': node_rec.get('public_ip'),
                         'iaas_id': iaas_node.id, 'node_id': node_rec['node_id']}
            cei_events.event("provisioner", "new_node", extra=extradict)

    def store_and_notify(self, records, subscribers):
        """Convenience method to store records and notify subscribers.
        """
        for node in records:
            self.maybe_update_node(node)

        self.notifier.send_records(records, subscribers)

    def maybe_update_node(self, node):
        updated = False
        current = node
        while not updated and current['state'] <= node['state']:
            #HACK copying store metadata. really each operation should
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

    def dump_state(self, nodes, force_subscribe=None):
        """Resends node state information to subscribers

        @param nodes list of node IDs
        @param force_subscribe optional, an extra subscriber that may not be listed in local node records
        """
        for node_id in nodes:
            node = self.store.get_node(node_id)
            if node:
                launch = self.store.get_launch(node['launch_id'])
                subscribers = launch['subscribers']
                if force_subscribe and not force_subscribe in subscribers:
                    subscribers.append(force_subscribe)
                self.notifier.send_record(node, subscribers)
            else:
                log.warn("Got dump_state request for unknown node '%s', notifying '%s' it is failed", node_id, force_subscribe)
                record = {"node_id":node_id, "state":states.FAILED}
                subscribers = [force_subscribe]
                self.notifier.send_record(record, subscribers)

    def query_nodes(self):
        """Performs queries of IaaS and broker, sends updates to subscribers.
        """
        # Right now we just query everything. Could be made more granular later

        nodes = self.store.get_nodes(max_state=states.TERMINATING)
        site_nodes = group_records(nodes, 'site')

        if nodes:
            log.debug("Querying state of %d nodes", len(nodes))

        for site, nodes in site_nodes.iteritems():
            log.debug("Querying site %s about %d nodes", site, len(nodes))
            self.query_one_site(site, nodes)

    def query_one_site(self, site, nodes, driver=None):

        log.info('Querying site "%s"', site)

        if driver:
            # for tests
            nimboss_nodes = driver.list_nodes()
        else:
            with self.sites.acquire_driver(site) as site_driver:
                try:
                    with Timeout(self._IAAS_DEFAULT_TIMEOUT):
                        nimboss_nodes = site_driver.driver.list_nodes()
                except Timeout, t:
                    log.exception('Timeout when querying site "%s"', site)
                    raise

        nimboss_nodes = dict((node.id, node) for node in nimboss_nodes)

        # note we are walking the nodes from datastore, NOT from nimboss
        for node in nodes:
            state = node['state']
            if state < states.PENDING or state >= states.TERMINATED:
                continue

            nimboss_id = node.get('iaas_id')
            nimboss_node = nimboss_nodes.pop(nimboss_id, None)
            if not nimboss_node:
                # this state is unknown to underlying IaaS. What could have
                # happened? IaaS error? Recovery from loss of net to IaaS?

                # Or lazily-updated records. On EC2, there can be a short
                # window where pending instances are not included in query
                # response

                start_time = node.get('pending_timestamp')
                now = time.time()
                if node['state'] == states.TERMINATING:
                    log.debug('node %s: %s in datastore but unknown to IaaS.'+
                              ' Termination must have already happened.',
                        node['node_id'], states.TERMINATING)

                    node['state'] = states.TERMINATED
                    add_state_change(node, states.TERMINATED)
                    launch = self.store.get_launch(node['launch_id'])
                    if launch:
                        self.store_and_notify([node], launch['subscribers'])

                elif start_time and (now - start_time) <= _IAAS_NODE_QUERY_WINDOW_SECONDS:
                    log.debug('node %s: not in query of IaaS, but within '+
                            'allowed startup window (%d seconds)',
                            node['node_id'], _IAAS_NODE_QUERY_WINDOW_SECONDS)
                else:
                    log.warn('node %s: in data store but unknown to IaaS. '+
                            'Marking as terminated.', node['node_id'])

                    node['state'] = states.FAILED
                    add_state_change(node, states.FAILED)
                    node['state_desc'] = 'NODE_DISAPPEARED'

                    launch = self.store.get_launch(node['launch_id'])
                    if launch:
                        self.store_and_notify([node], launch['subscribers'])
            else:
                nimboss_state = _NIMBOSS_STATE_MAP[nimboss_node.state]
                if nimboss_state > node['state']:
                    #TODO nimboss could go backwards in state.

                    # when contextualization is disabled instances go straight
                    # to the RUNNING state
                    if nimboss_state == states.STARTED and not self.context:
                        nimboss_state = states.RUNNING

                    node['state'] = nimboss_state
                    add_state_change(node, nimboss_state)

                    update_node_ip_info(node, nimboss_node)

                    if nimboss_state == states.STARTED:
                        node['running_timestamp'] = time.time()
                        extradict = {'iaas_id': nimboss_id,
                                     'node_id': node.get('node_id'),
                                     'public_ip': node.get('public_ip'),
                                     'private_ip': node.get('private_ip') }
                        cei_events.event("provisioner", "node_started",
                                         extra=extradict)

                    launch = self.store.get_launch(node['launch_id'])
                    self.store_and_notify([node], launch['subscribers'])
        #TODO nimboss_nodes now contains any other running instances that
        # are unknown to the datastore (or were started after the query)
        # Could do some analysis of these nodes

    def _get_nodes_by_id(self, node_ids, skip_missing=True):
        """Helper method tp retrieve node records from a list of IDs
        """
        nodes = []
        for node_id in node_ids:
            node = self.store.get_node(node_id)
            # when skip_missing is false, include a None entry for missing nodes
            if node or not skip_missing:
                nodes.append(node)
        return nodes

    def query_contexts(self):
        """Queries all open launch contexts and sends node updates.
        """

        if not self.context:
            return

        #grab all the launches in the pending state
        launches = self.store.get_launches(state=states.PENDING)
        if launches:
            log.debug("Querying state of %d contexts", len(launches))

        for launch in launches:
            self._query_one_context(launch)

    def _query_one_context(self, launch):

        context = launch.get('context')
        launch_id = launch['launch_id']
        if not context:
            log.warn('Launch %s is in %s state but it has no context!',
                    launch['launch_id'], launch['state'])
            return

        node_ids = launch['node_ids']
        nodes = self._get_nodes_by_id(node_ids)

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

            return # *** EARLY RETURN ***

        valid = any(node['state'] < states.TERMINATING for node in nodes)
        if not valid:
            log.info("The context for launch %s has no valid nodes. They "+
                     "have likely been terminated. Marking launch as FAILED. "+
                     "nodes: %s", launch_id, node_ids)
            self.maybe_update_launch_state(launch, states.FAILED)
            return # *** EARLY RETURN ***

        ctx_uri = context['uri']
        log.debug('Querying context %s for launch %s ', ctx_uri, launch_id)

        try:
            context_status = self.context.query(ctx_uri)

        except (BrokerAuthError, ContextNotFoundError), e:
            log.error("permanent error from context broker for launch %s. "+
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
                self.store_and_notify(updated_nodes, launch['subscribers'])

            self.maybe_update_launch_state(launch, states.FAILED)

            return # *** EARLY RETURN ***

        except BrokerError,e:
            log.error("Error querying context broker: %s", e, exc_info=True)
            # hopefully this is some temporal failure, query will be retried
            return # *** EARLY RETURN ***

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
                                 'private_ip': node.get('private_ip') }
                    cei_events.event("provisioner", "node_ctx_timeout",
                                     extra=extradict)

        if updated_nodes:
            log.debug("%d nodes need to be updated as a result of the context query" %
                    len(updated_nodes))
            self.store_and_notify(updated_nodes, launch['subscribers'])

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

    def terminate_all(self, concurrency=1):
        """Terminate all running nodes
        """
        nodes = self.store.get_nodes(max_state=states.TERMINATING)
        launch_groups = group_records(nodes, 'launch_id')
        launch_nodes_pairs = []
        for launch_id, launch_nodes in launch_groups.iteritems():
            launch = self.store.get_launch(launch_id)
            if not launch:
                log.warn('Failed to find launch record %s', launch_id)
                continue

            launch_nodes_pairs.append((launch, launch_nodes))

            for node in launch_nodes:
                if node['state'] < states.TERMINATING:
                    self._mark_one_node_terminating(node, launch)

        if concurrency > 1:
            from gevent.pool import Pool

            pool = Pool(concurrency)
            for launch, nodes in launch_nodes_pairs:
                for node in nodes:
                    pool.spawn(self._terminate_node, node, launch)
            pool.join()

        else:
            for launch, nodes in launch_nodes_pairs:
                for node in nodes:
                    self._terminate_node(node, launch)

    def check_terminate_all(self):
        """Check if there are no launches left to terminate
        """
        nodes = self.store.get_nodes(max_state=states.TERMINATING)
        return len(nodes) == 0

    def mark_nodes_terminating(self, node_ids, caller=None):
        """Mark a set of nodes as terminating in the data store
        """
        nodes = self._get_nodes_by_id(node_ids)
        log.debug("Marking nodes for termination: %s", node_ids)
        
        launches = group_records(nodes, 'launch_id')
        for launch_id, launch_nodes in launches.iteritems():
            launch = self.store.get_launch(launch_id)


            if not launch:
                log.warn('Failed to find launch record %s', launch_id)
                continue

            check_user(caller=caller, creator=launch.get('creator'))

            for node in launch_nodes:
                self._mark_one_node_terminating(node, launch)

    def _mark_one_node_terminating(self, node, launch):
        if node['state'] < states.TERMINATING:
            node['state'] = states.TERMINATING
            add_state_change(node, states.TERMINATING)
            extradict = {'iaas_id': node.get('iaas_id'),
                         'node_id': node.get('node_id'),
                         'public_ip': node.get('public_ip'),
                         'private_ip': node.get('private_ip') }
            cei_events.event("provisioner", "node_terminating",
                             extra=extradict)
            self.store_and_notify([node], launch['subscribers'])

    def terminate_nodes(self, node_ids, caller=None):
        """Destroy all specified nodes.
        """
        nodes = self._get_nodes_by_id(node_ids, skip_missing=False)
        for node_id, node in izip(node_ids, nodes):
            if not node:
                #maybe an error should make it's way to controller from here?
                log.warn('Node %s unknown but requested for termination',
                        node_id)
                continue

            print "terminating node: %s" % node

            log.info("Terminating node %s", node_id)
            launch = self.store.get_launch(node['launch_id'])
            self._terminate_node(node, launch)

    def _terminate_node(self, node, launch):
        with self.sites.acquire_driver(node['site']) as site_driver:
            nimboss_node = self._to_nimboss_node(node, site_driver.driver)
            try:
                with Timeout(self._IAAS_DEFAULT_TIMEOUT):
                    site_driver.driver.destroy_node(nimboss_node)
            except Timeout, t:
                log.exception('Timeout when terminating node %s',
                        node.get('iaas_id'))
                raise

        node['state'] = states.TERMINATED
        add_state_change(node, states.TERMINATED)
        extradict = {'iaas_id': node.get('iaas_id'),
                     'node_id': node.get('node_id'),
                     'public_ip': node.get('public_ip'),
                     'private_ip': node.get('private_ip') }
        cei_events.event("provisioner", "node_terminated",
                         extra=extradict)

        self.store_and_notify([node], launch['subscribers'])

    def _to_nimboss_node(self, node, driver):
        """Nimboss drivers need a Node object for termination.
        """
        #this is unfortunately tightly coupled with EC2 libcloud driver
        # right now. We are building a fake Node object that only has the
        # attribute needed for termination (id). Would need to be fleshed out
        # to work with other drivers.
        return NimbossNode(id=node['iaas_id'], name=None, state=None,
                public_ips=None, private_ips=None,
                driver=driver)

    def describe_nodes(self, nodes=None, caller=None):
        """Produce a list of node records
        """
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
    """
    # ec2 libcloud driver places IP in a list
    #
    # if we support drivers that actually have multiple
    # public and private IPs, we will need to revisit this
    if not node_rec.get('public_ip'):
        public_ip = iaas_node.public_ip
        if isinstance(public_ip, (list, tuple)):
            public_ip = public_ip[0] if public_ip else None
        node_rec['public_ip'] = public_ip

    if not node_rec.get('private_ip'):
        private_ip = iaas_node.private_ip
        if isinstance(private_ip, (list, tuple)):
            private_ip = private_ip[0] if private_ip else None
        node_rec['private_ip'] = private_ip

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
                    match_reason = 'nimboss IP matches ctx hostname'
                # can add more matches if needed

            if match_node:
                log.debug('Matched ctx identity to node by: ' + match_reason)
                matched_nodes.append(dict(node=match_node,ctx_node=ctx_node,ident=ident))
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
        #TODO: was:
        #return threads.deferToThread(client.create_context)
        context = client.create_context()
        return context

    def query(self, resource):
        """Queries an existing context.

        resource is the uri returned by create operation
        """
        client = self._get_client()
        #TODO: was:
        #return threads.deferToThread(client.get_status, resource)
        return client.get_status(resource)


class ProvisioningError(Exception):
    pass

class ProvisionerDisabledError(Exception):
    pass
