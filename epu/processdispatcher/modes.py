class QueueingMode(object):

    NEVER = "NEVER"
    ALWAYS = "ALWAYS"
    START_ONLY = "START_ONLY"
    RESTART_ONLY = "RESTART_ONLY"


class RestartMode(object):

    NEVER = "NEVER"
    ALWAYS = "ALWAYS"
    ABNORMAL = "ABNORMAL"
    EXCEPT_SYSTEM_RESTART = "EXCEPT_SYSTEM_RESTART"
