# Copyright 2013 University of Chicago

#!/usr/bin/env python


class InstanceState(object):
    """Instance states
    """

    REQUESTING = '100-REQUESTING'
    """Request has been made but not acknowledged by Provisioner"""

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
    """Instance has been terminated in IaaS layer or before it reached PENDING"""

    FAILED = '900-FAILED'
    """Instance has failed and will not be retried"""

    REJECTED = '950-REJECTED'
    """Instance has been rejected by the Provisioner or IaaS and will not be retried
    """


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
    # subsequently haven't received a heartbeat in really_missing_timeout
    # seconds
    MISSING = "MISSING"

    # Instance is terminated but we have received a heartbeat in the past
    # zombie_timeout seconds
    ZOMBIE = "ZOMBIE"


class DecisionEngineState(object):
    """Decision engine states
    """
    PENDING = 'PENDING_DE'  # EPU is waiting on something

    STABLE = 'STABLE_DE'  # EPU is in a stable state (with respect to its policy)

    UNKNOWN = 'UNKNOWN'  # DE does not implement the contract

    DEVMODE_FAILED = 'DEVMODE_FAILED_DE'  # EPU is in development mode and received a node failure notification


class ProcessState(object):
    """Valid states for processes in the system

    In addition to this state value, each process also has a "round" number.
    This is the number of times the process has been assigned a slot and later
    been ejected (due to failure perhaps).

    These two values together move only in a single direction, allowing
    the system to detect and handle out-of-order messages. The state values are
    ordered and any backwards movement will be accompanied by an increment of
    the round.

    So for example a new process starts in Round 0 and state UNSCHEDULED and
    proceeds through states as it launches:

    Round   State

    0       100-UNSCHEDULED
    0       200-REQUESTED
    0       300-WAITING             process is waiting in a queue
    0       400-PENDING             process is assigned a slot and deploying

    Unfortunately the assigned resource spontaneously catches on fire. When
    this is detected, the process round is incremented and state rolled back
    until a new slot can be assigned. Perhaps it is at least given a higher
    priority.

    1       250-DIED_REQUESTED      process is waiting in the queue
    1       400-PENDING             process is assigned a new slot
    1       500-RUNNING             at long last

    The fire spreads to a neighboring node which happens to be running the
    process. Again the process is killed and put back in the queue.

    2       250-DIED_REQUESTED
    2       300-WAITING             this time there are no more slots


    At this point the client gets frustrated and terminates the process to
    move to another datacenter.

    2       600-TERMINATING
    2       700-TERMINATED

    """

    UNSCHEDULED = "100-UNSCHEDULED"
    """Process has been created but not scheduled to run. It will not be
    scheduled until requested by the user
    """

    UNSCHEDULED_PENDING = "150-UNSCHEDULED_PENDING"
    """Process is unscheduled but will be automatically scheduled in the
    future. This is used by the Doctor role to hold back some processes
    during system bootstrap.
    """

    REQUESTED = "200-REQUESTED"
    """Process request has been acknowledged by Process Dispatcher

    The process is pending a decision about whether it can be immediately
    assigned a slot or if it must wait for one to become available.
    """

    DIED_REQUESTED = "250-DIED_REQUESTED"
    """Process was >= PENDING but died, waiting for a new slot

    The process is pending a decision about whether it can be immediately
    assigned a slot or if it must wait for one to become available (or
    be rejected).
    """

    WAITING = "300-WAITING"
    """Process is waiting for a slot to become available

    There were no available slots when this process was reviewed by the
    matchmaker. Processes which request not to be queued, by their
    queueing mode flag will never reach this state and will go directly to
    REJECTED.
    """

    ASSIGNED = "350-ASSIGNED"
    """Process is deploying to a slot

    Process is assigned to a slot and deployment is underway. Once a
    process reaches this state, moving back to an earlier state requires an
    increment of the process' round.

    """

    PENDING = "400-PENDING"
    """Process is starting on a resource

    A process has been deployed to a slot and is starting.
    """

    RUNNING = "500-RUNNING"
    """Process is running
    """

    TERMINATING = "600-TERMINATING"
    """Process termination has been requested
    """

    TERMINATED = "700-TERMINATED"
    """Process is terminated
    """

    EXITED = "800-EXITED"
    """Process has finished execution successfully
    """

    FAILED = "850-FAILED"
    """Process request failed
    """

    REJECTED = "900-REJECTED"
    """Process could not be scheduled and it was rejected

    This is the terminal state of processes with queueing mode NEVER and
    RESTART_ONLY when no resources are immediately available, or START_ONLY
    when there are no resources immediately available on restart
    """

    TERMINAL_STATES = (UNSCHEDULED, UNSCHEDULED_PENDING, TERMINATED, EXITED,
                       FAILED, REJECTED)
    """Process states which will not change without a request from outside.
    """


class HAState(object):

    PENDING = "PENDING"
    """HA Process has been requested, but not enough instances of it have been
    started that it is useful
    """

    READY = "READY"
    """HA Process has been requested, and enough instances of it have been started
    that it is useful. It is still scaling, however
    """

    STEADY = "STEADY"
    """HA Process is ready and stable. No longer scaling.
    """

    FAILED = "FAILED"
    """HA Process has been started, but is not able to recover from a problem
    """


class ProcessDispatcherState(object):

    UNINITIALIZED = "UNINITIALIZED"
    """Initial state at Process Dispatcher boot. Maintained until the Doctor
    inspects and repairs system state. During this state, no matches are made
    and no processes are dispatched.
    """

    SYSTEM_BOOTING = "SYSTEM_BOOTING"
    """State set after the Process Dispatcher is initialized but while the
    system is still bootstrapping. During this time, the Matchmaker operates
    and matches/dispatches processes. However, the doctor is potentially holding
    back a batch of processes which will be released to the queue after the
    system boot finishes.
    """

    OK = "OK"
    """Process dispatcher is running and healthy
    """

    VALID_STATES = (UNINITIALIZED, SYSTEM_BOOTING, OK)


class ExecutionResourceState(object):

    OK = "OK"
    """Resource is active and healthy
    """

    WARNING = "WARNING"
    """The resource is under suspicion due to missing or late heartbeats

    Running processes are not rescheduled yet, but the resource is not
    assigned any new processes while in this state. Note: This could later
    be refined to allow processes, but only if there are no compatible slots
    available on healthy resources.
    """

    MISSING = "MISSING"
    """The resource has been declared dead by the PD Doctor due to a prolonged
    lack of heartbeats.

    Running processes on the resource have been rescheduled (if applicable)
    and the resource is ineligible for running new processes. If the resource
    resumes sending heartbeats, it will be returned to the OK state and made
    available for processes.
    """

    DISABLED = "DISABLED"
    """The resource has been disabled, likely in advance of being terminated
    """
