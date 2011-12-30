class MockResourceClient(object):
    def __init__(self):
        self.launches = []

    @property
    def launch_count(self):
        return len(self.launches)

    def launch_process(self, eeagent, upid, round, run_type, parameters):
        self.launches.append((eeagent, upid, round, run_type, parameters))

    def check_process_launched(self, process, resource_id=None):
        for eeagent, upid, round, run_type, parameters in self.launches:
            if upid == process.upid and round == process.round and \
                resource_id is None or resource_id == eeagent:
                return True

        raise Exception("Process %s not launched: launches: %s",
                        process, self.launches)

    def terminate_process(self, eeagent, upid, round):
        pass

    def cleanup_process(self, eeagent, upid, round):
        pass