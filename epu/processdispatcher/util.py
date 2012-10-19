EEAGENT_TOPIC_PREFIX = "eeagent_"
def node_id_from_eeagent_name(eeagent_name):
    if not eeagent_name.startswith(EEAGENT_TOPIC_PREFIX):
        raise ValueError("sender name doesn't begin with %s: %s" %
                         (EEAGENT_TOPIC_PREFIX, eeagent_name))
    node_id = eeagent_name[len(EEAGENT_TOPIC_PREFIX):]
    if not node_id:
        raise ValueError("bad sender: %s" % eeagent_name)
    return node_id

def node_id_to_eeagent_name(node_id):
    raise Exception("You shouldn't use this function anymore")
    if not node_id:
        raise ValueError("node_id")
    return EEAGENT_TOPIC_PREFIX + str(node_id)
