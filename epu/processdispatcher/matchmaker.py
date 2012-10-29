import logging
import threading
from math import ceil
from itertools import islice
from copy import deepcopy

from epu.exceptions import WriteConflictError, NotFoundError
from epu.states import ProcessState
from epu.processdispatcher.modes import QueueingMode
from epu.processdispatcher.engines import domain_id_from_engine

log = logging.getLogger(__name__)


class PDMatchmaker(object):
    """The matchmaker is a singleton process (enforced by ZK leader election)
    that matches process requests to available execution engine slots.

    Responsibilities:
    - Tracks available resources in memory, backed by persistence. Maintains
      watches on the persistence for updates from other workers.
    - Pulls process requests from a queue and matches them to available slots.

    While there are any engine updates, process them. Otherwise, if there are
    any slots available
    """

    def __init__(self, store, resource_client, ee_registry, epum_client,
                 notifier, service_name, domain_definition_id,
                 base_domain_config, run_type):
        """
        @type store: ProcessDispatcherStore
        @type resource_client: EEAgentClient
        @type ee_registry: EngineRegistry
        @type notifier: SubscriberNotifier
        """
        self.store = store
        self.resource_client = resource_client
        self.ee_registry = ee_registry
        self.epum_client = epum_client
        self.notifier = notifier
        self.service_name = service_name
        self.domain_definition_id = domain_definition_id
        self.base_domain_config = base_domain_config
        self.run_type = run_type

        self.resources = None
        self.queued_processes = None
        self.stale_processes = None

        self.condition = threading.Condition()

        self.is_leader = False

        self.needs_matchmaking = False

        self.registered_needs = None

    def start_election(self):
        """Initiates participation in the leader election"""
        self.store.contend_matchmaker(self)

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

    def stale_processes_by_engine(self, engine_id):
        procs = []
        for p in self.stale_processes:
            proc = self.store.get_process(p[0], p[1])
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

    def _get_queued_processes(self):
        self.process_set_changed = False
        processes = self.store.get_queued_processes(
            watcher=self._notify_process_set_changed)

        #TODO not really caring about priority or queue order
        # at this point

        for process_handle in processes:
            if process_handle not in self.queued_processes:
                log.debug("Found new queued process: %s", process_handle)
                self.queued_processes.append(process_handle)

                self.needs_matchmaking = True

    def _get_resource_set(self):
        self.resource_set_changed = False
        resource_ids = set(self.store.get_resource_ids(
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
            resource = self.store.get_resource(resource_id,
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
            resource = self.store.get_resource(resource_id,
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

        resources = self.get_available_resources()
        log.debug("Matchmaking. Processes: %d  Available resources: %d",
                  len(self.queued_processes), len(resources))

        fresh_processes = self._get_fresh_processes()

        for owner, upid, round in list(fresh_processes):
            log.debug("Matching process %s", upid)

            process = self.store.get_process(owner, upid)
            if not (process and process.round == round and
                    process.state < ProcessState.PENDING):
                try:
                    self.store.remove_queued_process(owner, upid, round)
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
            if not matched_resource and resources:
                matched_resource = self.matchmake_process(process, resources)

            if matched_resource:
                # update the resource record

                if not matched_resource.is_assigned(owner, upid, round):
                    matched_resource.assigned.append((owner, upid, round))
                try:
                    self.store.update_resource(matched_resource)
                except WriteConflictError:
                    log.error("WriteConflictError!")

                    # in case of write conflict, bail out of the matchmaker
                    # run and the outer loop will take care of updating data
                    # and trying again
                    return

                if process.node_exclusive:
                    matched_node = self.store.get_node(matched_resource.node_id)
                    if matched_node is None:
                        log.error("Couldn't find node %s to update node_exclusive",
                                matched_resource.node_id)
                        return

                    matched_node.node_exclusive.append(process.node_exclusive)

                    try:
                        self.store.update_node(matched_node)
                    except WriteConflictError:
                        log.error("WriteConflictError!")

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
                    matched_resource, removed = self._backout_resource_assignment(
                        matched_resource, process)

                # either way, remove resource from consideration if no slots remain
                if not matched_resource.available_slots and matched_resource in resources:
                    resources.remove(matched_resource)

                self.store.remove_queued_process(owner, upid, round)
                self.queued_processes.remove((owner, upid, round))

            elif process.state < ProcessState.WAITING:
                self._mark_process_waiting(process)

                # remove rejected processes from the queue
                if process.state == ProcessState.REJECTED:
                    self.store.remove_queued_process(owner, upid, round)
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
            parameters = dict(name=definition['name'],
                module=executable['module'], cls=executable['class'])
            if process.configuration:
                parameters['config'] = process.configuration
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

            except WriteConflictError:
                process = self.store.get_process(process.owner, process.upid)

        if updated:
            self.notifier.notify_process(process)

        return process, updated

    def _backout_resource_assignment(self, resource, process):
        removed = False
        pkey = process.key
        while resource and pkey in resource.assigned:
            resource.assigned.remove(pkey)
            try:
                self.store.update_resource(resource)
                removed = True
            except WriteConflictError:
                resource = self.store.get_resource(resource.resource_id)
            except NotFoundError:
                resource = None

        return resource, removed

    def _set_resource_enabled_state(self, resource, enabled):
        updated = False
        while resource and resource.enabled != enabled:
            resource.enabled = enabled
            try:
                self.store.update_resource(resource)
                updated = True
            except WriteConflictError:
                resource = self.store.get_resource(resource.resource_id)
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
                    log.info("Process %s: no available slots. REJECTED due to START_ONLY queueing mode, and process has started before.",
                         process.upid)
            elif process.queueing_mode == QueueingMode.RESTART_ONLY:
                if process.starts == 0:
                    process.state = ProcessState.REJECTED
                    log.info("Process %s: no available slots. REJECTED due to RESTART_ONLY queueing mode, and process hasn't started before.",
                         process.upid)
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
        available = []
        for resource in self.resources.itervalues():
            log.debug("Examining %s", resource)
            if resource.enabled and resource.available_slots:
                available.append(resource)

        # sort by 1) whether any processes are already assigned to resource
        # and 2) number of available slots
        available.sort(key=lambda r: (0 if r.assigned else 1,
                                      r.slot_count - len(r.assigned)))
        return available

    def calculate_need(self, engine_id):
        queued_process_count = len(self.queued_processes_by_engine(engine_id))
        assigned_process_count = 0
        occupied_resource_count = 0

        resources = self.resources_by_engine(engine_id)
        for resource in resources.itervalues():
            assigned_count = len(resource.assigned)
            if assigned_count:
                assigned_process_count += assigned_count
                occupied_resource_count += 1

        process_count = queued_process_count + assigned_process_count

        # need is the greater of the base need, the number of occupied
        # resources, and the number of instances that could be occupied
        # by the current process set
        engine = self.engine(engine_id)
        return max(engine.base_need, occupied_resource_count,
                int(ceil(process_count / float(engine.slots))))

    def register_needs(self):
        # TODO real dumb.

        for engine in list(self.ee_registry):

            engine_id = engine.engine_id

            need = self.calculate_need(engine_id)
            registered_need = self.registered_needs.get(engine_id)
            if need != registered_need:
                log.debug("need %s != registered_needs %s" % (need, registered_need))

                retiree_ids = None
                # on scale down, request for specific nodes to be terminated
                if need < registered_need:
                    retirables = (r for r in self.resources.itervalues()
                        if r.enabled and not r.assigned)
                    retirees = list(islice(retirables, registered_need - need))
                    retiree_ids = []
                    for retiree in retirees:
                        self._set_resource_enabled_state(retiree, False)
                        retiree_ids.append(retiree.node_id)

                    log.debug("Retiring empty nodes: %s", retirees)

                log.debug("Reconfiguring need for %d %s instances (was %s)",
                        need, engine_id, self.registered_needs.get(engine_id, 0))
                config = get_domain_reconfigure_config(need, retiree_ids)
                domain_id = domain_id_from_engine(engine_id)
                self.epum_client.reconfigure_domain(domain_id, config)
                self.registered_needs[engine_id] = need

    def matchmake_process(self, process, resources):
        matched = None
        for resource in resources:
            node = self.store.get_node(resource.node_id)
            if process.node_exclusive is not None and node is None:
                log.warning("Looking at resource %s with no node?", resource.resource_id)
                continue
            elif node and not node.node_exclusive_available(process.node_exclusive):
                continue
            logstr = "%s: process %s constraints: %s against resource %s properties: %s"
            if match_constraints(process.constraints, resource.properties):
                log.debug(logstr, "MATCH", process.upid, process.constraints,
                    resource.resource_id, resource.properties)
                matched = resource
                break
            else:
                log.debug(logstr, "NOTMATCH", process.upid, process.constraints,
                    resource.resource_id, resource.properties)
        return matched


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
