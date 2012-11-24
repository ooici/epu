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
    def from_config(cls, config, default=None):
        registry = cls(default=default)
        for engine_id, engine_conf in config.iteritems():
            spec = EngineSpec(engine_id, engine_conf['slots'],
                engine_conf.get('base_need', 0),
                engine_conf.get('config'), engine_conf.get('replicas', 1))
            registry.add(spec)
        return registry

    def __init__(self, default=None):
        self.default = default
        self.by_engine = {}

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


class EngineSpec(object):
    def __init__(self, engine_id, slots, base_need=0, config=None, replicas=1):
        self.engine_id = engine_id
        slots = int(slots)
        if slots < 1:
            raise ValueError("slots must be a positive integer")
        self.slots = slots
        self.config = config
        self.base_need = int(base_need)
        replicas = int(replicas)
        if replicas < 1:
            raise ValueError("replicas must be a positive integer")
        self.replicas = replicas
