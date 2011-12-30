class MockResourceClient(object):
    def __init__(self):
        self.launches = []

    def launch_process(self, eeagent, upid, round, run_type, parameters):
        self.launches.append((eeagent, upid, round, run_type, parameters))

    def terminate_process(self, eeagent, upid, round):
        pass

    def cleanup_process(self, eeagent, upid, round):
        pass