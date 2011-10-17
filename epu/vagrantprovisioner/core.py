#!/usr/bin/env python

"""
@file epu/provisioner/core.py
@author David LaBissoniere
@brief Starts, stops, and tracks instance and context state.
"""

import time
import ion.util.ionlog

from itertools import izip
from twisted.internet import defer, threads

from nimboss.ctx import ContextClient, BrokerError, BrokerAuthError, \
    ContextNotFoundError
from nimboss.cluster import ClusterDriver
from nimboss.nimbus import NimbusClusterDocument, ValidationError
from libcloud.compute.types import NodeState as NimbossNodeState
from libcloud.compute.base import Node as NimbossNode
from epu.provisioner.store import group_records
from epu.vagrantprovisioner.vagrant import Vagrant, VagrantState, FakeVagrant, VagrantManager
from epu.ionproc.dtrs import DeployableTypeLookupError
from epu import states
from epu import cei_events
from epu.provisioner.core import ProvisionerCore

log = ion.util.ionlog.getLogger(__name__)


__all__ = ['ProvisionerCore', 'ProvisioningError']

_VAGRANT_STATE_MAP = {
        VagrantState.ABORTED : states.ERROR_RETRYING,
        VagrantState.INACCESSIBLE : states.ERROR_RETRYING,
        VagrantState.NOT_CREATED : states.TERMINATED,
        VagrantState.POWERED_OFF : states.TERMINATED,
        VagrantState.RUNNING : states.STARTED,
        VagrantState.SAVED : states.TERMINATED,
        VagrantState.STUCK : states.ERROR_RETRYING, #TODO hmm
        VagrantState.LISTING : states.ERROR_RETRYING #TODO hmm
        }

# Window of time in which nodes are allowed to be launched
# but not returned in queries to the IaaS. After this, nodes
# are assumed to be terminated out of band and marked FAILED
_IAAS_NODE_QUERY_WINDOW_SECONDS = 60

class VagrantProvisionerCore(ProvisionerCore):
    """Provisioner functionality that is not specific to the service.
    """

    def __init__(self, store, notifier, dtrs, site_drivers, context, fake=False):
        self.store = store
        self.notifier = notifier
        self.dtrs = dtrs

        self.site_drivers = site_drivers
        self.context = context

        if not fake:
            self.vagrant_manager = VagrantManager(vagrant=Vagrant)
        else:
            self.vagrant_manager = VagrantManager(vagrant=FakeVagrant)

    @defer.inlineCallbacks
    def recover(self):
        """Finishes any incomplete launches or terminations
        """
        incomplete_launches = yield self.store.get_launches(
                state=states.REQUESTED)
        for launch in incomplete_launches:
            nodes = yield self._get_nodes_by_id(launch['node_ids'])

            log.info('Attempting recovery of incomplete launch: %s', 
                     launch['launch_id'])
            yield self.execute_provision(launch, nodes)

        terminating_launches = yield self.store.get_launches(
                state=states.TERMINATING)
        for launch in terminating_launches:
            log.info('Attempting recovery of incomplete launch termination: %s',
                     launch['launch_id'])
            yield self.terminate_launch(launch['launch_id'])

        terminating_nodes = yield self.store.get_nodes(
                state=states.TERMINATING)
        if terminating_nodes:
            node_ids = [node['node_id'] for node in terminating_nodes]
            log.info('Attempting recovery of incomplete node terminations: %s',
                         ','.join(node_ids))
            yield self.terminate_nodes(node_ids)

    @defer.inlineCallbacks
    def prepare_provision(self, request):
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

        try:
            deployable_type = request['deployable_type']
            launch_id = request['launch_id']
            subscribers = request['subscribers']
            nodes = request['nodes']
        except KeyError,e:
            raise ProvisioningError('Invalid request. Missing key: ' + str(e))


        if not (isinstance(nodes, dict) and len(nodes) > 0):
            raise ProvisioningError('Invalid request. nodes must be a non-empty dict')

        # optional variables to sub into ctx document template
        vars = request.get('vars')

        #validate nodes and build DTRS request
        dtrs_request_nodes = {}
        for node_name, node in nodes.iteritems():
            try:
                dtrs_request_nodes[node_name] = {
                        'count' : len(node['ids']),
                        'site' : node['site'],
                        'allocation' : node['allocation']}
            except (KeyError, ValueError):
                raise ProvisioningError('Invalid request. Node %s spec is invalid' %
                        node_name)

        # from this point on, errors result in failure records, not exceptions.
        # except for, you know, bugs.
        state = states.REQUESTED
        state_description = None
        #TODO: Look up dt in some kind of fake dtrs


        all_node_ids = []
        launch_record = {
                'launch_id' : launch_id,
                'deployable_type' : deployable_type,
                'subscribers' : subscribers,
                'state' : state,
                'node_ids' : all_node_ids}

        node_records = []
        index = 0
        for (group_name, group) in nodes.iteritems():

            # idempotency client token. We don't have a unique identifier 
            # per launch group, so we concatenate the launch_id with an index
            token = '_'.join((str(launch_id), str(index), str(group_name)))
            index += 1

            node_ids = group['ids']
            all_node_ids.extend(node_ids)
            for node_id in node_ids:
                record = {'launch_id' : launch_id,
                        'node_id' : node_id,
                        'state' : state,
                        'state_desc' : state_description,
                        'site' : group['site'],
                        'allocation' : group['allocation'],
                        'ctx_name' : group_name,
                        'client_token' : token,
                        }

                node_records.append(record)

        yield self.store.put_launch(launch_record)
        yield self.store_and_notify(node_records, subscribers)

        defer.returnValue((launch_record, node_records))

    @defer.inlineCallbacks
    def execute_provision(self, launch, nodes):
        """Brings a launch to the PENDING state.

        Any errors or problems will result in FAILURE states
        which will be recorded in datastore and sent to subscribers.
        """

        error_state = None
        error_description = None
        try:
            yield self._really_execute_provision_request(launch, nodes)

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
            launch['state'] = error_state
            launch['state_desc'] = error_description

            for node in nodes:
                # some groups may have been successfully launched.
                # only mark others as failed  
                if node['state'] < states.PENDING:
                    node['state'] = error_state
                    node['state_desc'] = error_description

            #store and notify launch and nodes with FAILED states
            yield self.store.put_launch(launch)
            yield self.store_and_notify(nodes, launch['subscribers'])

    @defer.inlineCallbacks
    def _really_execute_provision_request(self, launch, nodes):
        """Brings a launch to the PENDING state.
        """
        subscribers = launch['subscribers']

        has_failed = False
        #launch_pairs is a list of (spec, node list) tuples
        for node in nodes:

            # for recovery case
            if not node['state'] < states.PENDING:
                log.info('Skipping launch')
                continue

            newstate = None
            try:
                log.info("Launching node:\nnode: '%s'\n",
                         node)
                yield self._launch_one_node(node)

            except Exception,e:
                log.exception('Problem launching node %s : %s',
                        node, str(e))
                newstate = states.FAILED
                has_failed = True
                # should we have a backout of earlier groups here? or just leave it up
                # to EPU controller to decide what to do?

            if newstate:
                node['state'] = newstate
            yield self.store_and_notify([node], subscribers)

            if has_failed:
                break

        if has_failed:
            launch['state'] = states.FAILED
        else:
            launch['state'] = states.PENDING

        yield self.store.put_launch(launch)

    def _validate_launch_groups(self, groups, specs):
        if len(specs) != len(groups):
            raise ProvisioningError('INVALID_REQUEST group count mismatch '+
                    'between cluster document and request')
        pairs = []
        for spec in specs:
            group = groups.get(spec.name)
            if not group:
                raise ProvisioningError('INVALID_REQUEST missing \''+ spec.name +
                        '\' node group, present in cluster document')
            if spec.count != len(group):
                raise ProvisioningError(
                    'INVALID_REQUEST node group '+
                    '%s specifies %s nodes but cluster document has %s' %
                    (spec.name, len(group), spec.count))
            pairs.append((spec, group))
        return pairs

    @defer.inlineCallbacks
    def _launch_one_node(self, node):
        """Launches a single node: a single vagrant
        request.
        """

        #assumption here is that a launch group does not span sites or
        #allocations. That may be a feature for later.

        vagrant_vm = yield threads.deferToThread(self.vagrant_manager.new_vm)

        try:
            yield threads.deferToThread(vagrant_vm.up)
        except Exception, e:
            log.exception('Error launching nodes: ' + str(e))
            # wrap this up?
            raise

        node['state'] = states.PENDING
        node['pending_timestamp'] = time.time()
        node['vagrant_directory'] = vagrant_vm.directory

        extradict = {'public_ip': node.get('public_ip'),
                     'vagrant_directory': node['vagrant_directory'], 'node_id': node['node_id']}
        cei_events.event("provisioner", "new_node", extra=extradict)

    @defer.inlineCallbacks
    def store_and_notify(self, records, subscribers):
        """Convenience method to store records and notify subscribers.
        """
        yield self.store.put_nodes(records)
        yield self.notifier.send_records(records, subscribers)

    @defer.inlineCallbacks
    def dump_state(self, nodes, force_subscribe=None):
        """Resends node state information to subscribers

        @param nodes list of node IDs
        @param force_subscribe optional, an extra subscriber that may not be listed in local node records
        """
        for node_id in nodes:
            node = yield self.store.get_node(node_id)
            if node:
                launch = yield self.store.get_launch(node['launch_id'])
                subscribers = launch['subscribers']
                if force_subscribe and not force_subscribe in subscribers:
                    subscribers.append(force_subscribe)
                yield self.notifier.send_record(node, subscribers)
            else:
                log.warn("Got dump_state request for unknown node '%s', notifying '%s' it is failed", node_id, force_subscribe)
                record = {"node_id":node_id, "state":states.FAILED}
                subscribers = [force_subscribe]
                yield self.notifier.send_record(record, subscribers)

    @defer.inlineCallbacks
    def query(self, request=None):
        try:
            yield self.query_nodes(request)
        except Exception,e:
            log.error('Query failed due to an unexpected error. '+
                    'This is likely a bug and should be reported. Problem: ' +
                    str(e), exc_info=True)
            # don't let query errors bubble up any further. 

    @defer.inlineCallbacks
    def query_nodes(self, request=None):
        """Performs Vagrant queries, sends updates to subscribers.
        """
        # Right now we just query everything. Could be made more granular later

        nodes = yield self.store.get_nodes(max_state=states.TERMINATING)

        if len(nodes):
            log.debug("Querying state of %d nodes", len(nodes))

        for node in nodes:
            state = node['state']
            if state < states.PENDING or state >= states.TERMINATED:
                continue

            vagrant_vm = yield threads.deferToThread(self.vagrant_manager.get_vm, vagrant_directory=node.get('vagrant_directory'))
            status = yield threads.deferToThread(vagrant_vm.status)
            vagrant_state = _VAGRANT_STATE_MAP[status]

            if vagrant_state == states.STARTED:
                extradict = {'vagrant_directory': node.get('vagrant_directory'),
                             'node_id': node.get('node_id'),
                             'public_ip': node.get('public_ip'), #FIXME
                             'private_ip': node.get('private_ip') } #FIXME
                cei_events.event("provisioner", "node_started",
                                 extra=extradict)

            node['state'] = vagrant_state

            launch = yield self.store.get_launch(node['launch_id'])
            yield self.store_and_notify([node], launch['subscribers'])


    @defer.inlineCallbacks
    def _get_nodes_by_id(self, node_ids, skip_missing=True):
        """Helper method tp retrieve node records from a list of IDs
        """
        nodes = []
        for node_id in node_ids:

            node = yield self.store.get_node(node_id)
            # when skip_missing is false, include a None entry for missing nodes
            if node or not skip_missing:
                nodes.append(node)
        defer.returnValue(nodes)

    @defer.inlineCallbacks
    def query_contexts(self):
        """Queries all open launch contexts and sends node updates.
        """
        #grab all the launches in the pending state
        launches = yield self.store.get_launches(state=states.PENDING)
        if launches:
            log.debug("Querying state of %d contexts", len(launches))

        for launch in launches:
            yield self._query_one_context(launch)

    @defer.inlineCallbacks
    def _query_one_context(self, launch):

        context = launch.get('context')
        launch_id = launch['launch_id']
        if not context:
            log.warn('Launch %s is in %s state but it has no context!',
                    launch['launch_id'], launch['state'])
            defer.returnValue(None) # *** EARLY RETURN ***

        node_ids = launch['node_ids']
        nodes = yield self._get_nodes_by_id(node_ids)

        all_started = all(node['state'] >= states.STARTED for node in nodes)
        if not all_started:
            log.debug("Not all nodes for launch %s are running in IaaS yet. "+
                     "Skipping this context query for now.", launch_id)

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

            defer.returnValue(None) # *** EARLY RETURN ***

        valid = any(node['state'] < states.TERMINATING for node in nodes)
        if not valid:
            log.info("The context for launch %s has no valid nodes. They "+
                     "have likely been terminated. Marking launch as FAILED. "+
                     "nodes: %s", launch_id, node_ids)
            launch['state'] = states.FAILED
            yield self.store.put_launch(launch)
            defer.returnValue(None) # *** EARLY RETURN ***

        ctx_uri = context['uri']
        log.debug('Querying context %s for launch %s ', ctx_uri, launch_id)

        context_status = None
        try:
            context_status = yield self.context.query(ctx_uri)

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
                    updated_nodes.append(node)
            if updated_nodes:
                log.debug("Marking %d nodes as %s", len(updated_nodes), states.RUNNING_FAILED)
                yield self.store_and_notify(updated_nodes, launch['subscribers'])

            launch['state'] = states.FAILED
            yield self.store.put_launch(launch)

            defer.returnValue(None) # *** EARLY RETURN ***

        except BrokerError,e:
            log.error("Error querying context broker: %s", e, exc_info=True)
            # hopefully this is some temporal failure, query will be retried
            defer.returnValue(None) # *** EARLY RETURN ***

        ctx_nodes = context_status.nodes
        if not ctx_nodes:
            log.debug('Launch %s context has no nodes (yet)', launch_id)
            defer.returnValue(None) # *** EARLY RETURN ***

        updated_nodes = update_nodes_from_context(nodes, ctx_nodes)

        if updated_nodes:
            log.debug("%d nodes need to be updated as a result of the context query" %
                    len(updated_nodes))
            yield self.store_and_notify(updated_nodes, launch['subscribers'])

        all_done = all(ctx_node.ok_occurred or
                       ctx_node.error_occurred for ctx_node in ctx_nodes)

        if context_status.complete and all_done:
            log.info('Launch %s context is "all-ok": done!', launch_id)
            # update the launch record so this context won't be re-queried
            launch['state'] = states.RUNNING
            extradict = {'launch_id': launch_id, 'node_ids': launch['node_ids']}
            cei_events.event("provisioner", "launch_ctx_done", extra=extradict)
            yield self.store.put_launch(launch)

        elif context_status.complete:
            log.info('Launch %s context is "complete" (all checked in, but not all-ok)', launch_id)
        else:
            log.debug('Launch %s context is incomplete: %s of %s nodes',
                    launch_id, len(context_status.nodes),
                    context_status.expected_count)

    @defer.inlineCallbacks
    def mark_launch_terminating(self, launch_id):
        """Mark a launch as Terminating in data store.
        """
        launch = yield self.store.get_launch(launch_id)
        nodes = yield self._get_nodes_by_id(launch['node_ids'])
        updated = []
        for node in nodes:
            if node['state'] < states.TERMINATING:
                node['state'] = states.TERMINATING
                updated.append(node)
        if updated:
            yield self.store_and_notify(nodes, launch['subscribers'])
        launch['state'] = states.TERMINATING
        yield self.store.put_launch(launch)

    @defer.inlineCallbacks
    def terminate_launch(self, launch_id):
        """Destroy all nodes in a launch and mark as terminated in store.
        """
        launch = yield self.store.get_launch(launch_id)
        nodes = yield self._get_nodes_by_id(launch['node_ids'])

        for node in nodes:
            state = node['state']
            if state < states.PENDING or state >= states.TERMINATED:
                continue
            #would be nice to do this as a batch operation
            yield self._terminate_node(node, launch)

        launch['state'] = states.TERMINATED
        yield self.store.put_launch(launch)

    @defer.inlineCallbacks
    def terminate_launches(self, launch_ids):
        """Destroy all node in a set of launches.
        """
        for launch in launch_ids:
            yield self.terminate_launch(launch)

    @defer.inlineCallbacks
    def terminate_all(self):
        """Terminate all running nodes
        """
        launches = yield self.store.get_launches(max_state=states.TERMINATING)
        for launch in launches:
            yield self.mark_launch_terminating(launch['launch_id'])
            yield self.terminate_launch(launch['launch_id'])
            log.critical("terminate-all for launch '%s'" % launch['launch_id'])

    @defer.inlineCallbacks
    def check_terminate_all(self):
        """Check if there are no launches left to terminate
        """
        launches = yield self.store.get_launches(max_state=states.TERMINATING)
        defer.returnValue(len(launches) < 1)

    @defer.inlineCallbacks
    def mark_nodes_terminating(self, node_ids):
        """Mark a set of nodes as terminating in the data store
        """
        nodes = yield self._get_nodes_by_id(node_ids)
        log.debug("Marking nodes for termination: %s", node_ids)
        
        launches = group_records(nodes, 'launch_id')
        for launch_id, launch_nodes in launches.iteritems():
            launch = yield self.store.get_launch(launch_id)
            if not launch:
                log.warn('Failed to find launch record %s', launch_id)
                continue
            for node in launch_nodes:
                if node['state'] < states.TERMINATING:
                    node['state'] = states.TERMINATING
            yield self.store_and_notify(launch_nodes, launch['subscribers'])

    @defer.inlineCallbacks
    def terminate_nodes(self, node_ids):
        """Destroy all specified nodes.
        """
        nodes = yield self._get_nodes_by_id(node_ids, skip_missing=False)
        for node_id, node in izip(node_ids, nodes):
            if not node:
                #maybe an error should make it's way to controller from here?
                log.warn('Node %s unknown but requested for termination',
                        node_id)
                continue

            log.info("Terminating node %s", node_id)
            launch = yield self.store.get_launch(node['launch_id'])
            yield self._terminate_node(node, launch)

    @defer.inlineCallbacks
    def _terminate_node(self, node, launch):
        vagrant_directory = node['vagrant_directory']
        vagrant_vm = yield threads.deferToThread(self.vagrant_manager.get_vm, vagrant_directory=vagrant_directory)
        yield threads.deferToThread(self.vagrant_manager.remove_vm, vagrant_directory)
        node['state'] = states.TERMINATED

        yield self.store_and_notify([node], launch['subscribers'])


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

def update_nodes_from_context(nodes, ctx_nodes):
    updated_nodes = []
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

                if _update_one_node_from_ctx(match_node, ctx_node, ident):
                    updated_nodes.append(match_node)
                    break

            else:
                # this isn't necessarily an exceptional condition. could be a private
                # IP for example. Right now we are only matching against public
                log.debug('Context identity has unknown IP (%s) and hostname (%s)',
                        ident.ip, ident.hostname)
    return updated_nodes

def _update_one_node_from_ctx(node, ctx_node, identity):
    node_done = ctx_node.ok_occurred or ctx_node.error_occurred
    if not node_done or node['state'] >= states.RUNNING:
        return False
    if ctx_node.ok_occurred:
        node['state'] = states.RUNNING
        node['pubkey'] = identity.pubkey
    else:
        node['state'] = states.RUNNING_FAILED
        node['state_desc'] = "CTX_ERROR"
        node['ctx_error_code'] = ctx_node.error_code
        node['ctx_error_message'] = ctx_node.error_message
    return True


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
        return threads.deferToThread(client.create_context)

    def query(self, resource):
        """Queries an existing context.

        resource is the uri returned by create operation
        """
        client = self._get_client()
        return threads.deferToThread(client.get_status, resource)


class ProvisioningError(Exception):
    pass
