# Copyright 2013 University of Chicago

import logging

from epu.util import ensure_timedelta

log = logging.getLogger(__name__)

DOMAIN_PREFIX = "pd_domain_"


def engine_id_from_domain(domain_id):
    if not (domain_id and domain_id.startswith(DOMAIN_PREFIX)):
        raise ValueError("domain_id %s doesn't have expected prefix %s" % (
            domain_id, DOMAIN_PREFIX))
    engine_id = domain_id[len(DOMAIN_PREFIX):]
    if not engine_id:
        raise ValueError("domain_id has empty engine id")
    return engine_id


def domain_id_from_engine(engine_id):
    if not engine_id and not isinstance(engine_id, basestring):
        raise ValueError("invalid engine_id %s" % engine_id)
    return "%s%s" % (DOMAIN_PREFIX, engine_id)


class EngineRegistry(object):
    """
    Real dumb for now

    Need to use this when:

    1. A new EEAgent shows up. Its heartbeat contains a node_id.
       The node is found and domain/engine_id determined from it.

    2. More slots are needed for a particular engine type. The engine type
       is used to lookup the corresponding DT and slot information
    """

    @classmethod
    def from_config(cls, config, default=None, process_engines=None):
        if default and default not in config:
            raise KeyError("default engine '%s' not present in config" % (default,))

        registry = cls(default=default)
        for engine_id, engine_conf in config.iteritems():
            spec = EngineSpec(engine_id, engine_conf['slots'],
                base_need=engine_conf.get('base_need', 0),
                config=engine_conf.get('config'),
                replicas=engine_conf.get('replicas', 1),
                spare_slots=engine_conf.get('spare_slots', 0),
                iaas_allocation=engine_conf.get('iaas_allocation', None),
                maximum_vms=engine_conf.get('maximum_vms', None),
                heartbeat_period=engine_conf.get('heartbeat_period', 30),
                heartbeat_warning=engine_conf.get('heartbeat_warning'),
                heartbeat_missing=engine_conf.get('heartbeat_missing'))
            registry.add(spec)

        if process_engines:
            for path, engine_id in process_engines.iteritems():
                registry.set_process_engine_mapping(path, engine_id)
        return registry

    def __init__(self, default=None):
        self.default = default
        self.by_engine = {}
        self.process_module_engines = {}

    def __len__(self):
        return len(self.by_engine)

    def __iter__(self):
        return self.by_engine.itervalues()

    def add(self, engine):
        if engine.engine_id in self.by_engine:
            raise KeyError("engine %s already in registry" % engine.engine_id)

        self.by_engine[engine.engine_id] = engine

    def get_engine_by_id(self, engine):
        return self.by_engine[engine]

    def set_process_engine_mapping(self, path, engine_id):
        if not path:
            raise ValueError("invalid path")
        if engine_id not in self.by_engine:
            raise KeyError("engine mapped to %s is unknown" % (path,))
        self.process_module_engines[path] = engine_id

    def get_process_definition_engine_id(self, definition):
        """returns an engine id associated with a process definition, or None

        a.b.SomeClass matches, in order of preference:
            a.b.SomeClass
            a.b
            a
        """
        # don't search if there are no mappings
        if not self.process_module_engines:
            return None

        executable = definition.get('executable')
        if not (executable and executable.get('module') and executable.get('class')):
            return None

        path = str(executable['module']) + "." + str(executable['class'])
        while path:
            if path in self.process_module_engines:
                return self.process_module_engines[path]
            dot = path.rfind('.')
            if dot == -1:
                path = ""
            else:
                path = path[:dot]

        return None


_DEFAULT_HEARTBEAT_PERIOD = 30


class EngineSpec(object):
    def __init__(self, engine_id, slots, base_need=0, config=None, replicas=1,
                 spare_slots=0, iaas_allocation=None, maximum_vms=None,
                 heartbeat_period=30, heartbeat_warning=45, heartbeat_missing=60):
        self.engine_id = engine_id
        self.config = config
        self.base_need = int(base_need)
        self.iaas_allocation = iaas_allocation

        slots = int(slots)
        if slots < 1:
            raise ValueError("slots must be a positive integer")
        self.slots = slots

        replicas = int(replicas)
        if replicas < 1:
            raise ValueError("replicas must be a positive integer")
        self.replicas = replicas

        spare_slots = int(spare_slots)
        if spare_slots < 0:
            raise ValueError("spare slots must be at least 0")
        self.spare_slots = spare_slots

        self.maximum_vms = None
        if maximum_vms is not None:
            maximum_vms = int(maximum_vms)
            if maximum_vms < 0:
                raise ValueError("maximum vms must be at least 0")
            self.maximum_vms = maximum_vms

        self.heartbeat_period = heartbeat_period
        self.heartbeat_warning = heartbeat_warning
        self.heartbeat_missing = heartbeat_missing

        if (heartbeat_missing is None or heartbeat_warning is None) and not (
                heartbeat_missing is None and heartbeat_warning is None):
            raise ValueError("All heartbeat parameters must be specified, or none")

        if self.heartbeat_period is None:
            self.heartbeat_period = ensure_timedelta(_DEFAULT_HEARTBEAT_PERIOD)
        else:
            self.heartbeat_period = ensure_timedelta(self.heartbeat_period)

        if self.heartbeat_missing is not None:
            self.heartbeat_warning = ensure_timedelta(self.heartbeat_warning)
            self.heartbeat_missing = ensure_timedelta(self.heartbeat_missing)

            if self.heartbeat_period <= ensure_timedelta(0):
                raise ValueError("heartbeat_period must be a positive value")
            if self.heartbeat_warning <= self.heartbeat_period:
                raise ValueError("heartbeat_warning must be greater than heartbeat_period")
            if self.heartbeat_missing <= self.heartbeat_warning:
                raise ValueError("heartbeat_missing must be greater than heartbeat_warning")
