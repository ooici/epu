"""
@file epu/vagrantprovisioner/core.py
@brief Starts, stops, and tracks vagrant instance and context state
"""

import time
import logging

from itertools import izip

import epu.tevent as tevent

from epu.provisioner.store import group_records
from epu.vagrantprovisioner.vagrant import Vagrant, VagrantState, FakeVagrant, VagrantManager, VagrantException
from epu.exceptions import DeployableTypeLookupError
from epu.states import InstanceState
from epu import cei_events
from epu.exceptions import WriteConflictError
from epu.provisioner.core import ProvisionerCore

# alias for shorter code
states = InstanceState

log = logging.getLogger(__name__)

__all__ = ['VagrantProvisionerCore', 'ProvisioningError']

_VAGRANT_STATE_MAP = {
        VagrantState.ABORTED: states.ERROR_RETRYING,
        VagrantState.INACCESSIBLE: states.ERROR_RETRYING,
        VagrantState.NOT_CREATED: states.PENDING,
        VagrantState.POWERED_OFF: states.PENDING,
        VagrantState.STARTING: states.PENDING,
        VagrantState.RUNNING: states.STARTED,
        VagrantState.SAVED: states.TERMINATED,
        VagrantState.STUCK: states.ERROR_RETRYING,  # TODO hmm
        VagrantState.LISTING: states.ERROR_RETRYING  # TODO hmm
        }
DEFAULT_VAGRANT_BOX = "base"
DEFAULT_VAGRANT_MEMORY = 512


class VagrantProvisionerCore(ProvisionerCore):
    """Provisioner functionality that is not specific to the service.
    """

    def __init__(self, store, notifier, dtrs, site_drivers, context, fake=False, **kwargs):
        self.store = store
        self.notifier = notifier
        self.dtrs = dtrs

        if not fake:
            self.vagrant_manager = VagrantManager(vagrant=Vagrant)
        else:
            self.vagrant_manager = VagrantManager(vagrant=FakeVagrant)

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

        terminating_launches = self.store.get_launches(
                state=states.TERMINATING)
        for launch in terminating_launches:
            log.info('Attempting recovery of incomplete launch termination: %s',
                     launch['launch_id'])
            self.terminate_launch(launch['launch_id'])

        terminating_nodes = self.store.get_nodes(
                state=states.TERMINATING)
        if terminating_nodes:
            node_ids = [node['node_id'] for node in terminating_nodes]
            log.info('Attempting recovery of incomplete node terminations: %s',
                         ','.join(node_ids))
            self.terminate_nodes(node_ids)

    def prepare_provision(self, launch_id, deployable_type, instance_ids,
            subscribers, site, allocation=None, vars=None):
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

        if not (isinstance(instance_ids, (list, tuple)) and
                len(instance_ids) == 1 and instance_ids[0]):
            self._validation_error(
                "bad instance_ids '%s': need a list or tuple of length 1 -- " +
                "multi-node launches are not supported yet.", instance_ids)

        if not isinstance(subscribers, (list, tuple)):
            self._validation_error("bad subscribers '%s'", subscribers)

        if not site:
            self._validation_error("invalid site: '%s'", site)

        if not (isinstance(instance_ids, (list, tuple)) and
                len(instance_ids) == 1 and instance_ids[0]):
            self._validation_error(
                "bad instance_ids '%s': need a list or tuple of length 1 -- " +
                "multi-node launches are not supported yet.", instance_ids)

        if not vars:
            vars = {}

        # validate nodes and build DTRS request
        dtrs_request_node = dict(count=len(instance_ids), site=site,
            allocation=allocation)

        # from this point on, errors result in failure records, not exceptions.
        # except for, you know, bugs.
        state = states.REQUESTED
        state_description = None

        dt = {}
        try:
            dt = self.dtrs.lookup(deployable_type, dtrs_request_node, None)
        except DeployableTypeLookupError, e:
            log.error('Failed to lookup deployable type "%s" in DTRS: %s',
                    deployable_type, str(e))
            state = states.FAILED
            state_description = "DTRS_LOOKUP_FAILED " + str(e)
            document = "N/A"
            dtrs_nodes = None

        if self.store.is_disabled():
            state = states.REJECTED
            state_description = "PROVISIONER_DISABLED"

        launch_record = {
                'launch_id': launch_id,
                'deployable_type': deployable_type,
                'chef_json': dt.get('chef_json'),
                'cookbook_dir': dt.get('cookbook_dir'),
                'subscribers': subscribers,
                'state': state,
                'node_ids': list(instance_ids)}

        node_records = []
        for node_id in instance_ids:
            record = {'launch_id': launch_id,
                    'node_id': node_id,
                    'state': state,
                    'vagrant_box': vars.get('vagrant_box'),
                    'vagrant_memory': vars.get('vagrant_memory'),
                    'state_desc': state_description,
                    }

            node_records.append(record)

        try:
            self.store.add_launch(launch_record)
        except WriteConflictError:
            log.debug("record for launch %s already exists, proceeding.",
                launch_id)

        for node in node_records:
            try:
                self.store.add_node(node)
            except WriteConflictError:
                log.debug("record for node %s already exists, proceeding.",
                    node['node_id'])

        self.notifier.send_records(node_records, subscribers)
        return launch_record, node_records

    def execute_provision(self, launch, nodes):
        """Brings a launch to the STARTED state.

        Any errors or problems will result in FAILURE states
        which will be recorded in datastore and sent to subscribers.
        """

        error_state = None
        error_description = None
        try:
            self._really_execute_provision_request(launch, nodes)

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
            launch['state'] = error_state
            launch['state_desc'] = error_description

            for node in nodes:
                # some groups may have been successfully launched.
                # only mark others as failed
                if node['state'] < states.PENDING:
                    node['state'] = error_state
                    node['state_desc'] = error_description

            # store and notify launch and nodes with FAILED states
            self.store.update_launch(launch)
            self.store_and_notify(nodes, launch['subscribers'])

    def _really_execute_provision_request(self, launch, nodes):
        """Brings a launch to the STARTED state.
        """
        subscribers = launch['subscribers']

        has_failed = False
        # launch_pairs is a list of (spec, node list) tuples
        for node in nodes:

            def _launch_failure_handler(*args):
                if args:
                    glet = args[0]

            # for recovery case
            if not node['state'] < states.PENDING:
                log.info('Skipping launch')
                continue

            newstate = None
            try:
                log.debug("Launching node:\n'%s'\n",
                         node)
                self._launch_one_node(node, launch, subscribers, launch['chef_json'], launch['cookbook_dir'])

            except Exception, e:
                log.exception('Problem launching node %s : %s',
                        node, str(e))
                newstate = states.FAILED
                has_failed = True
                # should we have a backout of earlier groups here? or just leave it up
                # to EPU controller to decide what to do?

            if newstate:
                node['state'] = newstate
            self.store_and_notify([node], subscribers)

            if has_failed:
                break

        if has_failed:
            launch['state'] = states.FAILED
        else:
            launch['state'] = states.STARTED

        self.store.update_launch(launch)

    def _launch_one_node(self, node, launch, subscribers, chef_json=None, cookbook_dir=None):
        """Launches a single node: a single vagrant request.
        """
        def _launch_failure_callback(err_msg):
            log.error('Vagrant launch failed: %s' % err_msg)
            launch['state'] = states.FAILED
            node['state'] = states.FAILED
            self.store_and_notify([node], subscribers)
            self.store.update_launch(launch)

        # assumption here is that a launch group does not span sites or
        # allocations. That may be a feature for later.

        vagrant_box = node.get('vagrant_box') or DEFAULT_VAGRANT_BOX
        vagrant_memory = node.get('vagrant_memory') or DEFAULT_VAGRANT_MEMORY

        vagrant_config = """
        Vagrant::Config.run do |config|
          config.vm.box = "%s"
          config.vm.customize ["modifyvm", :id, "--memory", "%s"]
        end
        """ % (vagrant_box, vagrant_memory)

        vagrant_vm = self.vagrant_manager.new_vm(config=vagrant_config,
                                                 cookbooks_path=cookbook_dir,
                                                 chef_json=chef_json)
        node['vagrant_directory'] = vagrant_vm.directory
        node['pending_timestamp'] = time.time()

        try:
            log.debug("Starting vagrant at %s" % vagrant_vm.directory)
            tevent.spawn(vagrant_vm.up, failure_callback=_launch_failure_callback)
        except Exception, e:
            log.exception('Error launching nodes: ' + str(e))
            # wrap this up?
            raise

        node['state'] = states.PENDING
        node['public_ip'] = vagrant_vm.ip
        node['private_ip'] = vagrant_vm.ip

        extradict = {'public_ip': node.get('public_ip'),
                     'vagrant_directory': node['vagrant_directory'],
                     'node_id': node['node_id']}
        cei_events.event("provisioner", "new_node", extra=extradict)

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
                record = {"node_id": node_id, "state": states.FAILED}
                subscribers = [force_subscribe]
                self.notifier.send_record(record, subscribers)

    def query(self, request=None):
        try:
            self.query_nodes(request)
        except Exception, e:
            log.error('Query failed due to an unexpected error. ' +
                    'This is likely a bug and should be reported. Problem: ' +
                    str(e), exc_info=True)
            # don't let query errors bubble up any further.

    def query_nodes(self, request=None):
        """Performs Vagrant queries, sends updates to subscribers.
        """
        # Right now we just query everything. Could be made more granular later

        nodes = self.store.get_nodes(max_state=states.TERMINATING)

        if len(nodes):
            log.debug("Querying state of %d nodes", len(nodes))

        for node in nodes:
            state = node['state']
            if state < states.PENDING or state >= states.TERMINATED:
                continue

            # TODO: was defertothread
            vagrant_vm = self.vagrant_manager.get_vm(vagrant_directory=node.get('vagrant_directory'))
            # TODO: was defertothread
            try:
                status = vagrant_vm.status()
                vagrant_state = _VAGRANT_STATE_MAP.get(status, states.TERMINATED)
            except VagrantException:
                log.debug("Got exception, assuming terminated")
                status = VagrantException
                vagrant_state = state.FAILED
            ip = vagrant_vm.ip

            if vagrant_state == states.STARTED:
                extradict = {'vagrant_directory': node.get('vagrant_directory'),
                             'node_id': node.get('node_id'),
                             'public_ip': node.get('public_ip'),
                             'private_ip': node.get('private_ip')}
                cei_events.event("provisioner", "node_started",
                                 extra=extradict)

            node['state'] = vagrant_state

            launch = self.store.get_launch(node['launch_id'])
            self.store_and_notify([node], launch['subscribers'])

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

    def mark_launch_terminating(self, launch_id):
        """Mark a launch as Terminating in data store.
        """
        launch = self.store.get_launch(launch_id)
        nodes = self._get_nodes_by_id(launch['node_ids'])
        updated = []
        for node in nodes:
            if node['state'] < states.TERMINATING:
                node['state'] = states.TERMINATING
                updated.append(node)
        if updated:
            self.store_and_notify(nodes, launch['subscribers'])
        launch['state'] = states.TERMINATING
        self.store.update_launch(launch)

    def terminate_launch(self, launch_id):
        """Destroy all nodes in a launch and mark as terminated in store.
        """
        launch = self.store.get_launch(launch_id)
        nodes = self._get_nodes_by_id(launch['node_ids'])

        for node in nodes:
            state = node['state']
            if state < states.PENDING or state >= states.TERMINATED:
                continue
            # would be nice to do this as a batch operation
            self._terminate_node(node, launch)

        launch['state'] = states.TERMINATED
        self.store.update_launch(launch)

    def terminate_launches(self, launch_ids):
        """Destroy all node in a set of launches.
        """
        for launch in launch_ids:
            self.terminate_launch(launch)

    def terminate_all(self):
        """Terminate all running nodes
        """
        launches = self.store.get_launches(max_state=states.TERMINATING)
        for launch in launches:
            self.mark_launch_terminating(launch['launch_id'])
            self.terminate_launch(launch['launch_id'])
            log.critical("terminate-all for launch '%s'" % launch['launch_id'])

    def check_terminate_all(self):
        """Check if there are no launches left to terminate
        """
        launches = self.store.get_launches(max_state=states.TERMINATING)
        return len(launches) < 1

    def mark_nodes_terminating(self, node_ids):
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
            for node in launch_nodes:
                if node['state'] < states.TERMINATING:
                    node['state'] = states.TERMINATING
            self.store_and_notify(launch_nodes, launch['subscribers'])

    def terminate_nodes(self, node_ids):
        """Destroy all specified nodes.
        """
        nodes = self._get_nodes_by_id(node_ids, skip_missing=False)
        for node_id, node in izip(node_ids, nodes):
            if not node:
                # maybe an error should make it's way to controller from here?
                log.warn('Node %s unknown but requested for termination',
                        node_id)
                continue

            log.info("Terminating node %s", node_id)
            launch = self.store.get_launch(node['launch_id'])
            self._terminate_node(node, launch)

    def _terminate_node(self, node, launch):
        vagrant_directory = node.get('vagrant_directory')
        # TODO: was defertothread
        self.vagrant_manager.remove_vm(vagrant_directory=vagrant_directory)
        node['state'] = states.TERMINATED

        self.store_and_notify([node], launch['subscribers'])


class ProvisioningError(Exception):
    pass
