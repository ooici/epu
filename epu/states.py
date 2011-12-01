#!/usr/bin/env python

class InstanceState(object):
    """Instance states
    """

    REQUESTING = '100-REQUESTING'
    """Request has been made but not acknowledged through SA"""

    REQUESTED = '200-REQUESTED'
    """Request has been acknowledged by provisioner"""

    ERROR_RETRYING = '300-ERROR_RETRYING'
    """Request encountered an error but is still being attempted"""

    PENDING = '400-PENDING'
    """Request is pending in IaaS layer"""

    STARTED = '500-STARTED'
    """Instance has been started in IaaS layer"""

    RUNNING = '600-RUNNING'
    """Instance has been contextualized and is operational"""

    RUNNING_FAILED = '650-RUNNING_FAILED'
    """Instance is started in IaaS but contextualization failed"""

    TERMINATING = '700-TERMINATING'
    """Termination of the instance has been requested"""

    TERMINATED = '800-TERMINATED'
    """Instance has been terminated in IaaS layer"""

    FAILED = '900-FAILED'
    """Instance has failed and will not be retried"""


class InstanceHealthState(object):

    # Health for an instance is unknown. It may be terminated, booting,
    # or health monitoring may even be disabled.
    UNKNOWN = "UNKNOWN"

    # Instance has sent an OK heartbeat within the past missing_timeout
    # seconds, and has sent no errors.
    OK = "OK"

    # Most recent heartbeat from the instance includes an error from the
    # process monitor itself (supervisord)
    MONITOR_ERROR = "MONITOR_ERROR"

    # Most recent heartbeat from the instance includes at least one error
    # from a monitored process
    PROCESS_ERROR = "PROCESS_ERROR"

    # Instance is running but we haven't received a heartbeat for more than
    # missing_timeout seconds
    OUT_OF_CONTACT = "OUT_OF_CONTACT"

    # Instance is running but we haven't received a heartbeat for more than
    # missing_timeout seconds, a dump_state() message was sent, and we
    # subsequently haven't recieved a heartbeat in really_missing_timeout
    # seconds
    MISSING = "MISSING"

    # Instance is terminated but we have received a heartbeat in the past
    # zombie_timeout seconds
    ZOMBIE = "ZOMBIE"


class DecisionEngineState(object):
    """Decision engine states
    """
    PENDING = 'PENDING_DE' # EPU is waiting on something

    STABLE = 'STABLE_DE' # EPU is in a stable state (with respect to its policy)

    UNKNOWN = 'UNKNOWN' # DE does not implement the contract

    DEVMODE_FAILED = 'DEVMODE_FAILED_DE' # EPU is in development mode and received a node failure notification
