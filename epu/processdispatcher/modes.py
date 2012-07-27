class QueueingMode(object):

    NEVER = "NEVER"
    ALWAYS = "ALWAYS"
    START_ONLY = "START_ONLY"
    RESTART_ONLY = "RESTART_ONLY"

    _str_map = {1: 'NEVER', 2: 'ALWAYS', 3: 'START_ONLY', 4: 'RESTART_ONLY'}

    @classmethod
    def from_pyon_enum(value):
        return QueueingMode._str_map[value]


class RestartMode(object):

    NEVER = "NEVER"
    ALWAYS = "ALWAYS"
    ABNORMAL = "ABNORMAL"
    _str_map = {1: 'NEVER', 2: 'ALWAYS', 3: 'ABNORMAL'}

    @classmethod
    def from_pyon_enum(value):
        return RestartMode._str_map[value]

