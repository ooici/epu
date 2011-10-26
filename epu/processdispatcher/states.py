class ProcessStates(object):
    """Valid states for processes in the system

    In addition to this state value, each process also has a "round" number.
    This is the number of times the process has been assigned a slot and later
    been ejected (due to failure perhaps).

    These two values together move only in a single direction, allowing
    the system to detect and handle out-of-order messages. The state values are
    ordered and any backwards movement will be accompanied by an increment of
    the round.

    So for example a new process starts in Round 0 and state REQUESTING and
    proceeds through states as it launches:

    Round   State

    0       100-REQUESTING
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
    REQUESTING = "100-REQUESTING"
    """Process request has not yet been acknowledged by Process Dispatcher

    This state will only exist inside of clients of the Process Dispatcher
    """

    REQUESTED = "200-REQUESTED"
    """Process request has been acknowledged by Process Dispatcher

    The process is pending a decision about whether it can be immediately
    assigned a slot or if it must wait for one to become available.
    """

    DIED_REQUESTED = "250-DIED_REQUESTED"
    """Process was >= PENDING but died, waiting for a new slot

    The process is pending a decision about whether it can be immediately
    assigned a slot or if it must wait for one to become available.
    """

    WAITING = "300-WAITING"
    """Process is waiting for a slot to become available

    There were no available slots when this process was reviewed by the
    matchmaker. Processes with the immediate flag set will never reach this
    state and will instead go straight to FAILED.
    """

    PENDING = "400-PENDING"
    """Process is deploying to a slot

    A slot has been assigned to the process and deployment is underway. It
    is quite possible for the resource or process to die before deployment
    succeeds however. Once a process reaches this state, moving back to
    an earlier state requires an increment of the process' round.
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

    FAILED = "800-FAILED"
    """Process request failed
    """

    REJECTED = "850-REJECTED"
    """Process could not be scheduled and it rejected

    This is the terminal state of processes with the immediate flag when
    no resources are immediately available.
    """

  