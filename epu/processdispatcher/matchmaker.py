import logging
import threading
from math import ceil
from copy import deepcopy
from collections import defaultdict
from operator import attrgetter

from epu.exceptions import WriteConflictError, NotFoundError
from epu.states import ProcessState, ProcessDispatcherState
from epu.processdispatcher.modes import QueueingMode
from epu.processdispatcher.engines import domain_id_from_engine
from epu.processdispatcher.util import get_process_state_message

log = logging.getLogger(__name__)


class PDMatchmaker(object):
    """The matchmaker is a singleton process (enforced by ZK leader election)
    that matches process requests to available execution engine slots.

    Responsibilities:
    - Tracks available resources in memory, backed by persistence. Maintains
      watches on the persistence for updates from other workers.
    - Pulls process requests from a queue and matches them to available slots.

    The matchmaker attempts to fill up nodes, but balance processes across
    resources on a node. So for example say you have two blank nodes each with
    two execution resources, and each of those with four slots. If you start
    four processes, you'll end up with one node with two processes on each
    resource. The other node will be blank. If you start four more processes,
    they will slot onto the already-occupied node and the second node will
    still be empty. A ninth process will finally spill onto the second node.
    """

    def __init__(self, store, sync, resource_client, ee_registry, epum_client,
                 notifier, service_name, domain_definition_id,
                 base_domain_config, run_type):
        """
        @type store: ProcessDispatcherStore
        @type resource_client: EEAgentClient
        @type ee_registry: EngineRegistry
        @type notifier: SubscriberNotifier
        """
        self.store = store
        self.sync = sync
        self.resource_client = resource_client
        self.ee_registry = ee_registry
        self.epum_client = epum_client
        self.notifier = notifier
        self.service_name = service_name
        self.domain_definition_id = domain_definition_id
        self.base_domain_config = base_domain_config
        self.run_type = run_type
        self._cached_pd_state = None

        self.resources = None
        self.queued_processes = None
        self.stale_processes = None
        self.unscheduled_pending_processes = []

        self.condition = threading.Condition()

        self.is_leader = False

        self.needs_matchmaking = False

        self.registered_needs = None

    def start_election(self):
        """Initiates participation in the leader election"""
        self.sync.contend_matchmaker(self)

    def inaugurate(self):
        """Callback from the election fired when this leader is elected

        This routine is expected to never return as long as we want to
        remain the leader.
        """
        if self.is_leader:
            # already the leader???
            raise Exception("already the leader???")
        self.is_leader = True

        self.initialize()
        self.run()

    def initialize(self):

        self.resources = {}
        self.queued_processes = []
        self.stale_processes = []

        self.resource_set_changed = True
        self.changed_resources = set()
        self.process_set_changed = True

        self.needs_matchmaking = True

        self.registered_needs = {}
        self._get_pending_processes()

        # create the domains if they don't already exist
        if self.epum_client:
            for engine in list(self.ee_registry):

                if not self.domain_definition_id:
                    raise Exception("domain definition must be provided")

                if not self.base_domain_config:
                    raise Exception("domain config must be provided")

                domain_id = domain_id_from_engine(engine.engine_id)
                try:
                    self.epum_client.describe_domain(domain_id)
                except NotFoundError:
                    config = self._get_domain_config(engine)
                    self.epum_client.add_domain(domain_id,
                        self.domain_definition_id, config,
                        subscriber_name=self.service_name,
                        subscriber_op='node_state')

    def queued_processes_by_engine(self, engine_id):
        procs = []
        for p in self.queued_processes:
            proc = self.store.get_process(p[0], p[1])
            if proc and proc.constraints.get('engine') == engine_id:
                procs.append(proc)
            elif engine_id == self.ee_registry.default and not proc.constraints.get('engine'):
                procs.append(proc)
        return procs

    def pending_processes_by_engine(self, engine_id):
        procs = []
        for proc in self.unscheduled_pending_processes:
            if proc and proc.constraints.get('engine') == engine_id:
                procs.append(proc)
            elif engine_id == self.ee_registry.default and not proc.constraints.get('engine'):
                procs.append(proc)
        return procs

    def resources_by_engine(self, engine_id):
        filtered = {rid: r
                for rid, r in self.resources.iteritems()
                  if r and r.properties.get('engine') == engine_id}
        return filtered

    def engine(self, engine_id):
        return self.ee_registry.get_engine_by_id(engine_id)

    def _get_domain_config(self, engine, initial_n=0):
        config = deepcopy(self.base_domain_config)
        engine_conf = config['engine_conf']
        if engine_conf is None:
            config['engine_conf'] = engine_conf = {}

        if engine.iaas_allocation:
            engine_conf['iaas_allocation'] = engine.iaas_allocation

        if engine.maximum_vms:
            engine_conf['maximum_vms'] = engine.maximum_vms

        if engine.config:
            engine_conf.update(engine.config)

        if engine_conf.get('provisioner_vars') is None:
            engine_conf['provisioner_vars'] = {}

        if engine_conf['provisioner_vars'].get('slots') is None:
            engine_conf['provisioner_vars']['slots'] = engine.slots

        if engine_conf['provisioner_vars'].get('replicas') is None:
            engine_conf['provisioner_vars']['replicas'] = engine.replicas

        engine_conf['preserve_n'] = initial_n
        return config

    def _find_assigned_resource(self, owner, upid, round):
        # could speedup with cache
        for resource in self.resources.itervalues():
            if resource.is_assigned(owner, upid, round):
                return resource
        return None

    def _notify_resource_set_changed(self, *args):
        with self.condition:
            self.resource_set_changed = True
            self.condition.notifyAll()

    def _notify_process_set_changed(self, *args):
        with self.condition:
            self.process_set_changed = True
            self.condition.notifyAll()

    def _notify_resource_changed(self, resource_id, *args):
        with self.condition:
            self.changed_resources.add(resource_id)
            self.condition.notifyAll()

    def _get_pd_state(self):
        if self._cached_pd_state != ProcessDispatcherState.OK:
            self._cached_pd_state = self.sync.get_pd_state()
        return self._cached_pd_state

    def _get_pending_processes(self):

        if self._get_pd_state() == ProcessDispatcherState.SYSTEM_BOOTING:

            if self.unscheduled_pending_processes != []:
                # This list shouldn't change while the system is booting
                # so if it is set, we can safely skip querying the store
                # for processes
                return

            process_ids = self.store.get_process_ids()
            for process_id in process_ids:
                process = self.store.get_process(process_id[0], process_id[1])
                if process.state == ProcessState.UNSCHEDULED_PENDING:
                    self.unscheduled_pending_processes.append(process)
        elif self.unscheduled_pending_processes:
            self.unscheduled_pending_processes = []

    def _get_queued_processes(self):
        self.process_set_changed = False
        processes = self.sync.get_queued_processes(
            watcher=self._notify_process_set_changed)

        #TODO not really caring about priority or queue order
        # at this point

        if processes != self.queued_processes:
            log.debug("Queued process list has changed")
            self.queued_processes = processes
            self.needs_matchmaking = True

    def _get_resource_set(self):
        self.resource_set_changed = False
        resource_ids = set(self.sync.get_shadow_resource_ids(
            watcher=self._notify_resource_set_changed))

        previous = set(self.resources.keys())

        added = resource_ids - previous
        removed = previous - resource_ids

        # resource removal doesn't need to trigger matchmaking
        if added:
            self._dump_stale_processes()
            self.needs_matchmaking = True

        for resource_id in removed:
            del self.resources[resource_id]

        for resource_id in added:
            resource = self.sync.get_shadow_resource(resource_id,
                                               watcher=self._notify_resource_changed)
            self.resources[resource_id] = resource

    def _get_resources(self):
        with self.condition:
            changed = self.changed_resources.copy()
            self.changed_resources.clear()

        if changed:
            self._dump_stale_processes()
            self.needs_matchmaking = True

        for resource_id in changed:
            resource = self.sync.get_shadow_resource(resource_id,
                                               watcher=self._notify_resource_changed)
            #TODO fold in assignment vector in some fancy way?
            if resource:
                self.resources[resource_id] = resource

    def cancel(self):
        log.info("Stopping matchmaker")

        with self.condition:
            self.is_leader = False
            self.condition.notifyAll()

    def run(self):
        log.info("Elected as matchmaker!")

        while self.is_leader:
            # first fold in any changes to queued processes and available resources

            if self.process_set_changed:
                self._get_queued_processes()

            if self.resource_set_changed:
                self._get_resource_set()

            if self.changed_resources:
                self._get_resources()

            # check again if we lost leadership
            if not self.is_leader:
                return

            # now do a matchmaking cycle if anything changed enough to warrant
            if self.needs_matchmaking:
                self.matchmake()

            # only update needs if matchmaking round was successful
            # (and that is enabled)
            if not self.needs_matchmaking and self.epum_client:
                self.register_needs()

            with self.condition:
                if self.is_leader and not (self.resource_set_changed or
                        self.changed_resources or self.process_set_changed):
                    self.condition.wait()

    def matchmake(self):
        # this is inefficient but that is ok for now

        node_containers = self.get_available_resources()
        log.debug("Matchmaking. Processes: %d  Available nodes: %d",
                  len(self.queued_processes), len(node_containers))

        fresh_processes = self._get_fresh_processes()

        for owner, upid, round in list(fresh_processes):
            log.debug("Matching process %s", upid)

            process = self.store.get_process(owner, upid)
            if not (process and process.round == round and
                    process.state < ProcessState.PENDING):
                try:
                    self.sync.remove_queued_process(owner, upid, round)
                except NotFoundError:
                    # no problem if some other process removed the queue entry
                    pass

                self.queued_processes.remove((owner, upid, round))
                continue

            # ensure process is not already assigned a slot
            matched_resource = self._find_assigned_resource(owner, upid, round)
            if matched_resource:
                log.debug("process already assigned to resource %s",
                    matched_resource.resource_id)
            if not matched_resource and node_containers:
                matched_resource = self.matchmake_process(process, node_containers)

            if matched_resource:

                # get the real resource object
                store_resource = self.store.get_resource(matched_resource.resource_id)
                if store_resource:
                    # create the real process assignment in the store
                    self.store.create_process_assignment(process, store_resource)
                else:
                    log.error("Shadow resource has no entry in store: %s", matched_resource.resource_id)

                # update the shadow resource record

                if not matched_resource.is_assigned(owner, upid, round):
                    matched_resource.assigned.append((owner, upid, round))
                try:
                    self.sync.update_shadow_resource(matched_resource)
                except WriteConflictError:
                    log.error("WriteConflictError updating resource. will retry.")

                    # in case of write conflict, bail out of the matchmaker
                    # run and the outer loop will take care of updating data
                    # and trying again
                    return

                matched_node = None
                if process.node_exclusive:
                    matched_node = self.store.get_node(matched_resource.node_id)
                    if matched_node is None:
                        log.error("Couldn't find node %s to update node_exclusive",
                                matched_resource.node_id)
                        return

                    log.debug("updating %s with node_exclusive %s for %s" % (
                        matched_node.node_id, process.node_exclusive, process.upid))
                    matched_node.node_exclusive.append(process.node_exclusive)

                    try:
                        self.store.update_node(matched_node)
                    except WriteConflictError:
                        log.error("WriteConflictError updating node. will retry.")

                        # in case of write conflict, bail out of the matchmaker
                        # run and the outer loop will take care of updating data
                        # and trying again
                        return

                # attempt to also update the process record and mark it as pending.
                # If the process has since been terminated, this update will fail.
                # In that case we must back out the resource record update we just
                # made.
                process, assigned = self._maybe_update_assigned_process(
                    process, matched_resource)

                if assigned:
                    #TODO: move this to a separate operation that MM submits to queue?
                    try:
                        self._dispatch_process(process, matched_resource)
                    except Exception:
                        #TODO: this is not a good failure behavior
                        log.exception("Problem dispatching process from matchmaker")

                else:
                    # backout resource record update if the process update failed due to
                    # 3rd party changes to the process.
                    log.debug("failed to assign process. it moved to %s out of band",
                        process.state)
                    self._clear_process_assignments(process)
                    matched_resource, removed = self._backout_shadow_resource_assignment(
                        matched_resource, process)

                # update modified resource's node container and prune it out
                # if the node has no more available slots
                for i, node_container in enumerate(node_containers):
                    if matched_resource.node_id == node_container.node_id:
                        node_container.update()
                        if matched_node:
                            node_container.update_node(matched_node)
                        if not node_container.available_slots:
                            node_containers.pop(i)
                        break  # there can only be one match

                self.sync.remove_queued_process(owner, upid, round)
                self.queued_processes.remove((owner, upid, round))

            elif process.state < ProcessState.WAITING:
                self._mark_process_waiting(process)

                # remove rejected processes from the queue
                if process.state == ProcessState.REJECTED:
                    self.sync.remove_queued_process(owner, upid, round)
                    self.queued_processes.remove((owner, upid, round))

            self._mark_process_stale((owner, upid, round))

        # if we made it through all processes, we don't need to matchmake
        # again until new information arrives
        self.needs_matchmaking = False

    def _dispatch_process(self, process, resource):
        """Launch the process on a resource
        """
        definition = process.definition
        executable = definition['executable']
        # build up the spec form EE Agent expects
        if self.run_type in ('pyon', 'pyon_single'):
            parameters = dict(name=process.name,
                module=executable['module'], cls=executable['class'],
                module_uri=executable.get('url'))
            config = _get_process_config(process)
            if config is not None:
                parameters['config'] = config
        elif self.run_type == 'supd':
            parameters = executable
        else:
            msg = "Don't know how to format parameters for '%s' run type" % self.run_type
            log.warning(msg)
            parameters = {}

        self.resource_client.launch_process(
            resource.resource_id, process.upid, process.round,
            self.run_type, parameters)

    def _maybe_update_assigned_process(self, process, resource):
        updated = False
        while process.state < ProcessState.PENDING:
            process.assigned = resource.resource_id
            process.state = ProcessState.PENDING

            # pull hostname directly onto process record, if available.
            # it is commonly desired information and this saves the need to
            # make multiple queries to get it.
            process.hostname = resource.properties.get('hostname')

            try:
                self.store.update_process(process)
                updated = True

                log.info(get_process_state_message(process))

            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)

        if updated:
            self.notifier.notify_process(process)

        return process, updated

    def _clear_process_assignments(self, process):
        assignments = self.store.get_process_assignments(process)
        if assignments:
            for resource_id in assignments:
                resource = self.store.get_resource(resource_id)
                if resource:
                    try:
                        self.store.remove_process_assignment(process, resource)
                    except NotFoundError:
                        pass

    def _backout_shadow_resource_assignment(self, resource, process):
        removed = False
        pkey = process.key
        while resource and pkey in resource.assigned:
            resource.assigned.remove(pkey)
            try:
                self.sync.update_shadow_resource(resource)
                removed = True
            except WriteConflictError:
                resource = self.sync.get_shadow_resource(resource.resource_id)
            except NotFoundError:
                resource = None

        return resource, removed

    def _set_resource_enabled_state(self, resource, enabled):
        updated = False
        while resource and resource.enabled != enabled:
            resource.enabled = enabled
            try:
                self.sync.update_shadow_resource(resource)
                updated = True
            except WriteConflictError:
                resource = self.sync.get_shadow_resource(resource.resource_id)
            except NotFoundError:
                resource = None

        return resource, updated

    def _mark_process_waiting(self, process):

        # update process record to indicate queuing state. if writes conflict
        # retry until success or until process has moved to a state where it
        # doesn't matter anymore

        updated = False
        while process.state < ProcessState.WAITING:
            if process.queueing_mode == QueueingMode.NEVER:
                process.state = ProcessState.REJECTED
                log.info("Process %s: no available slots. REJECTED due to NEVER queueing mode",
                         process.upid)
            elif process.queueing_mode == QueueingMode.START_ONLY:
                if process.starts == 0:
                    log.info("Process %s: no available slots. WAITING in queue",
                         process.upid)
                    process.state = ProcessState.WAITING
                else:
                    process.state = ProcessState.REJECTED
                    log.info("Process %s: no available slots. REJECTED due to "
                             "START_ONLY queueing mode, and process has "
                             "started before.", process.upid)
            elif process.queueing_mode == QueueingMode.RESTART_ONLY:
                if process.starts == 0:
                    process.state = ProcessState.REJECTED
                    log.info("Process %s: no available slots. REJECTED due to "
                             "RESTART_ONLY queueing mode, and process hasn't "
                             "started before.", process.upid)
                else:
                    log.info("Process %s: no available slots. WAITING in queue",
                         process.upid)
                    process.state = ProcessState.WAITING
            else:
                log.info("Process %s: no available slots. WAITING in queue",
                         process.upid)
                process.state = ProcessState.WAITING

            try:
                self.store.update_process(process)
                updated = True
            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)
                continue

        if updated:
            self.notifier.notify_process(process)

        return process, updated

    def _mark_process_stale(self, process):
        self.stale_processes.append(process)

    def _dump_stale_processes(self):
        self.stale_processes = []

    def _get_fresh_processes(self):
        stale = set(self.stale_processes)
        fresh_processes = [p for p in self.queued_processes if p not in stale]
        return fresh_processes

    def get_available_resources(self):
        """Select the available execution resources

        Returns a sorted list of NodeContainer objects
        """

        # first break down available resources by node
        available_by_node = defaultdict(list)
        for resource in self.resources.itervalues():
            if resource.enabled and resource.available_slots:
                node_id = resource.node_id
                available_by_node[node_id].append(resource)

        # now create NodeResources container objects for each node,
        # splitting unoccupied nodes off into a separate list
        unoccupied_node_containers = []
        node_containers = []
        for node_id, resources in available_by_node.iteritems():
            container = NodeContainer(node_id, resources)

            if any(resource.assigned for resource in resources):
                node_containers.append(container)
            else:
                unoccupied_node_containers.append(container)

        # sort the occupied nodes by the aggregate number of available slots
        node_containers.sort(key=attrgetter('available_slots'))

        # append the unoccupied node lists onto the end
        node_containers.extend(unoccupied_node_containers)
        return node_containers

    def calculate_need(self, engine_id):

        # track a set of all known processes, both assigned and queued. it
        # is possible for there to be some brief overlap between assigned
        # and queued processes, where round N of a process is assigned and
        # round N+1 is requeued.
        process_set = set()
        occupied_node_set = set()
        node_set = set()

        for process in self.pending_processes_by_engine(engine_id):
            process_set.add((process.owner, process.upid))

        for process in self.queued_processes_by_engine(engine_id):
            process_set.add((process.owner, process.upid))

        resources = self.resources_by_engine(engine_id)
        for resource in resources.itervalues():
            if resource.assigned:
                for owner, upid, _ in resource.assigned:
                    process_set.add((owner, upid))
                occupied_node_set.add(resource.node_id)

            node_set.add(resource.node_id)

        # need is the greater of the base need, the number of occupied
        # resources, and the number of instances that could be occupied
        # by the current process set
        engine = self.engine(engine_id)

        # total number of unique runnable processes in the system plus
        # minimum free slots
        process_count = len(process_set) + engine.spare_slots

        process_need = int(ceil(process_count / float(engine.slots * engine.replicas)))
        need = max(engine.base_need, len(occupied_node_set), process_need)
        if engine.maximum_vms is not None:
            need = min(need, engine.maximum_vms)
            log.debug("Engine '%s' need=%d = min(maximum_vms=%d, max(base_need=%d, occupied=%d, process_need=%d))",
            engine_id, need, engine.maximum_vms, engine.base_need, len(occupied_node_set), process_need)
        else:
            log.debug("Engine '%s' need=%d = max(base_need=%d, occupied=%d, process_need=%d)",
                engine_id, need, engine.base_need, len(occupied_node_set), process_need)
        return need, list(node_set - occupied_node_set)

    def register_needs(self):

        self._get_pending_processes()

        for engine in list(self.ee_registry):

            engine_id = engine.engine_id

            need, unoccupied_nodes = self.calculate_need(engine_id)
            registered_need = self.registered_needs.get(engine_id)
            if need != registered_need:

                retiree_ids = None
                # on scale down, request for specific nodes to be terminated
                if need < registered_need:

                    unoccupied_nodes.sort(key=self._node_state_time, reverse=True)
                    retiree_ids = unoccupied_nodes[:registered_need - need]
                    for resource in self.resources.itervalues():
                        if resource.node_id in retiree_ids:
                            self._set_resource_enabled_state(resource, False)

                log.info("Scaling engine '%s' to %s nodes (was %s)",
                        engine_id, need, self.registered_needs.get(engine_id, 0))
                if retiree_ids:
                    log.info("Retiring engine '%s' nodes: %s", engine_id, ", ".join(retiree_ids))
                config = get_domain_reconfigure_config(need, retiree_ids)
                domain_id = domain_id_from_engine(engine_id)
                self.epum_client.reconfigure_domain(domain_id, config)
                self.registered_needs[engine_id] = need

    def _node_state_time(self, node_id):
        node = self.store.get_node(node_id)
        if node:
            return node.state_time
        else:
            return 0

    def matchmake_process(self, process, node_containers):

        # node_resources is a list of NodeResources objects. each contains a
        # sublist of resources.
        for node_container in node_containers:

            # skip ahead by whole nodes if we care about node exclusivity
            if process.node_exclusive:
                node_id = node_container.node_id

                # grab node object out of data store and cache it on the
                # container
                if node_container.node is None:
                    node = node_container.node = self.store.get_node(node_id)
                else:
                    node = node_container.node

                if not node:
                    log.warning("Can't find node %s?", node_id)
                    continue
                if not node.node_exclusive_available(process.node_exclusive):
                    log.debug("Process %s with node_exclusive %s is not being "
                              "matched to %s, which has this attribute" % (
                                  process.upid, process.node_exclusive, node_id))
                    continue

            # now inspect each resource in the node looking for a match
            for resource in node_container.resources:
                logstr = "%s: process %s constraints: %s against resource %s properties: %s"
                if match_constraints(process.constraints, resource.properties):
                    log.debug(logstr, "MATCH", process.upid, process.constraints,
                        resource.resource_id, resource.properties)

                    return resource

                else:
                    log.debug(logstr, "NOTMATCH", process.upid, process.constraints,
                        resource.resource_id, resource.properties)

        # no match was found!
        return None


def get_domain_reconfigure_config(preserve_n, retirables=None):
    engine_conf = {"preserve_n": preserve_n}
    if retirables:
        engine_conf['retirable_nodes'] = list(retirables)
    return dict(engine_conf=engine_conf)


def match_constraints(constraints, properties):
    """Match process constraints against resource properties

    Simple equality matches for now.
    """
    if constraints is None:
        return True

    for key, value in constraints.iteritems():
        if value is None:
            continue

        if properties is None:
            return False

        advertised = properties.get(key)
        if advertised is None:
            return False

        if isinstance(value, (list, tuple)):
            if not advertised in value:
                return False
        else:
            if advertised != value:
                return False

    return True


def _get_process_config(process):
    """gets process config dictionary, adding in start_mode if necessary

    Returns None if no configuration is needed
    """

    if process.round == 0:
        if process.configuration:
            return process.configuration

    else:
        if process.configuration:
            config = deepcopy(process.configuration)
            process_block = config.get('process')
            if process_block is None or not isinstance(process_block, dict):
                process_block = config['process'] = {}
        else:
            config = {}
            process_block = config['process'] = {}

        process_block['start_mode'] = "RESTART"
        return config

    return None


class NodeContainer(object):
    """Represents the execution resources on a single node (VM)

    Used only internally in matchmaking algorithm. Keeps the node's resources
    sorted by their available slot count, descending. This way the matchmaker
    will favor less-utilized resources on each node. Note that among the full
    set of nodes, the matchmaker favors utilized nodes first. So the net effect
    is that we fill up nodes but balance processes across all containers on
    each node as we go.
    """
    def __init__(self, node_id, resources):
        self.node_id = node_id
        self.resources = list(resources)
        self.node = None

        # sort the resource list to begin with
        self.update()

    @property
    def available_slots(self):
        return sum(r.available_slots for r in self.resources)

    def update(self):
        """Ensure the resource set is sorted and available

        Prunes any resources that have no free slots.
        """
        resources = self.resources
        resources.sort(key=attrgetter('available_slots'), reverse=True)

        # walk from the end of list and prune off resources with no free slots
        while resources and resources[-1].available_slots == 0:
            resources.pop()

    def update_node(self, new_node):
        self.node = new_node
