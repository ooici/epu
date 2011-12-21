class EngineRegistry(object):
    """
    Real dumb for now

    Need to use this when:

    1. A new EEAgent shows up. Its name is dereferenced into a node_id.
       The node is found and DT determined from it. DT is fed into registry
       to figure out engine type.

    2. More slots are needed for a particular engine type. The engine type
       is used to lookup the corresponding DT and slot information
    """

    @classmethod
    def from_config(cls, config):
        registry = cls()
        for engine_id, engine_conf in config.iteritems():
            spec = EngineSpec(engine_id, engine_conf['deployable_type'],
                              engine_conf['slots'], engine_conf.get('base_need', 0),
                              engine_conf.get('config'))
            registry.add(spec)
        return registry

    def __init__(self):
        self.by_engine = {}
        self.by_dt = {}

    def __iter__(self):
        return self.by_engine.itervalues()

    def add(self, engine):
        if engine.engine_id in self.by_engine:
            raise KeyError("engine %s already in registry" % engine.engine_id)
        if engine.deployable_type in self.by_dt:
            raise KeyError("deployable type %s already in registry" %
                           engine.deployable_type)

        self.by_engine[engine.engine_id] = engine
        self.by_dt[engine.deployable_type] = engine

    def get_engine_by_id(self, engine):
        return self.by_engine[engine]

    def get_engine_by_dt(self, dt_id):
        return self.by_dt[dt_id]

class EngineSpec(object):
    def __init__(self, engine_id, deployable_type, slots, base_need=0, config=None):
        self.engine_id = engine_id
        self.deployable_type = deployable_type
        self.slots = int(slots)
        self.config = config
        self.base_need = int(base_need)