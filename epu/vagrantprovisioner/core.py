#!/usr/bin/env python

"""
@file epu/vagrantprovisioner/core.py
@brief Starts, stops, and tracks vagrant instance and context state
"""

import time
import ion.util.ionlog

from itertools import izip
from twisted.internet import defer, threads

from epu.provisioner.store import group_records
from epu.vagrantprovisioner.vagrant import Vagrant, VagrantState, FakeVagrant, VagrantManager
from epu.vagrantprovisioner.directorydtrs import DeployableTypeLookupError
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

class VagrantProvisionerCore(ProvisionerCore):
    """Provisioner functionality that is not specific to the service.
    """

    def __init__(self, store, notifier, dtrs, site_drivers, context, fake=False):
        self.store = store
        self.notifier = notifier
        self.dtrs = dtrs

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

        # from this point on, errors result in failure records, not exceptions.
        # except for, you know, bugs.
        state = states.REQUESTED
        state_description = None
        
        dt = {}
        try:
            dt = yield self.dtrs.lookup(deployable_type)
        except DeployableTypeLookupError, e:
            log.error('Failed to lookup deployable type "%s" in DTRS: %s',
                    deployable_type, str(e))
            state = states.FAILED
            state_description = "DTRS_LOOKUP_FAILED " + str(e)
            document = "N/A"
            dtrs_nodes = None
            log.debug("Couldn't find dt %s" % deployable_type)


        all_node_ids = []
        launch_record = {
                'launch_id' : launch_id,
                'deployable_type' : deployable_type,
                'chef_json' : dt.get('chef_json'),
                'cookbook_dir' : dt.get('cookbook_dir'),
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
                        'client_token' : token,
                        }

                node_records.append(record)

        yield self.store.put_launch(launch_record)
        yield self.store_and_notify(node_records, subscribers)

        defer.returnValue((launch_record, node_records))

    @defer.inlineCallbacks
    def execute_provision(self, launch, nodes):
        """Brings a launch to the STARTED state.

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
        """Brings a launch to the STARTED state.
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
                yield self._launch_one_node(node, launch['chef_json'], launch['cookbook_dir'])

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
            launch['state'] = states.STARTED

        yield self.store.put_launch(launch)


    @defer.inlineCallbacks
    def _launch_one_node(self, node, chef_json=None, cookbook_dir=None):
        """Launches a single node: a single vagrant request.
        """

        #assumption here is that a launch group does not span sites or
        #allocations. That may be a feature for later.

        vagrant_vm = yield threads.deferToThread(self.vagrant_manager.new_vm,
                                                 cookbooks_path=cookbook_dir, chef_json=chef_json)

        try:
            yield threads.deferToThread(vagrant_vm.up)
        except Exception, e:
            log.exception('Error launching nodes: ' + str(e))
            # wrap this up?
            raise

        status = yield threads.deferToThread(vagrant_vm.status)
        
        vagrant_state = _VAGRANT_STATE_MAP[status]
        log.debug("status: %s state %s" % (status, vagrant_state))
        node['state'] = vagrant_state
        node['pending_timestamp'] = time.time()
        node['vagrant_directory'] = vagrant_vm.directory

        extradict = {'public_ip': node.get('public_ip'),
                     'vagrant_directory': node['vagrant_directory'],
                     'node_id': node['node_id']}
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
            ip = vagrant_vm.ip

            if vagrant_state == states.STARTED:
                extradict = {'vagrant_directory': node.get('vagrant_directory'),
                             'node_id': node.get('node_id'),
                             'public_ip': ip,
                             'private_ip': ip}
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

class ProvisioningError(Exception):
    pass
