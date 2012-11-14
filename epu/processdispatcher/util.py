def get_process_state_message(process):
    """Get a string suitable for logging a process state
    """

    if process.name:
        name = " [%s]" % process.name
    else:
        name = ""

    if process.assigned:
        location = ": host=%s eeagent=%s" % (process.hostname,
            process.assigned)
    else:
        location = ""

    state = process.state
    state_parts = state.split('-', 1)
    if len(state_parts) == 2:
        state = state_parts[1]

    return "Process %s%s -> %s%s" % (process.upid, name, process.state, location)
