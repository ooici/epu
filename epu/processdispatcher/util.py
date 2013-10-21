# Copyright 2013 University of Chicago

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

    return "Process %s%s -> %s round=%s%s" % (process.upid, name,
        process.state, process.round, location)


def get_set_difference(set1, set2):
    """Returns a tuple of lists (added, removed)
    """
    added = list(set2.difference(set1))
    removed = list(set1.difference(set2))
    return added, removed


def get_set_difference_debug_message(set1, set2):
    """Utility function for building log messages about set content changes
    """
    try:
        difference1, difference2 = get_set_difference(set1, set2)
    except Exception, e:
        return "can't calculate set difference. are these really sets?: %s" % str(e)

    if difference1 and difference2:
        return "removed=%s added=%s" % (difference1, difference2)
    elif difference1:
        return "removed=%s" % (difference1,)
    elif difference2:
        return "added=%s" % (difference2,)
    else:
        return "sets are equal"
