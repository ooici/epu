import yaml
import gevent
import unittest
import uuid
from socket import timeout

from nose.plugins.attrib import attr
from nose.plugins.skip import SkipTest

from epu.dashiproc.processdispatcher import ProcessDispatcherService, ProcessDispatcherClient
from epu.dashiproc.highavailability import HighAvailabilityService, HighAvailabilityServiceClient
deployment = """
process-dispatchers:
  pd_0:
    logfile: /tmp/pd_0.log
    engines:
      default:
        deployable_type: eeagent
        slots: 100
        base_need: 1
  pd_1:
    logfile: /tmp/pd_1.log
    engines:
      default:
        deployable_type: eeagent
        slots: 100
        base_need: 1
nodes:
  nodeone:
    dt: eeagent
    process-dispatcher: pd_0
    eeagents:
      eeagent_nodeone:
        slots: 100
        launch_type: supd
        logfile: /tmp/eeagent_nodeone.log
  nodetwo:
    dt: eeagent
    process-dispatcher: pd_1
    eeagents:
      eeagent_nodetwo:
        slots: 100
        launch_type: supd
        logfile: /tmp/eeagent_nodetwo.log
"""

deployment_one_pd_two_eea = """
process-dispatchers:
  pd_0:
    logfile: /tmp/pd_0.log
    engines:
      default:
        deployable_type: eeagent
        slots: 100
        base_need: 2
nodes:
  nodeone:
    dt: eeagent
    process-dispatcher: pd_0
    eeagents:
      eeagent_nodeone:
        slots: 100
        launch_type: supd
        logfile: /tmp/eeagent_nodeone.log
  nodetwo:
    dt: eeagent
    process-dispatcher: pd_0
    eeagents:
      eeagent_nodetwo:
        slots: 100
        launch_type: supd
        logfile: /tmp/eeagent_nodetwo.log
"""

class HighAvailabilityServiceTests(unittest.TestCase):

    def setUp(self):
        try:
            from epuharness.harness import EPUHarness
        except ImportError:
            raise SkipTest("EPUHarness not available")
        self.exchange = "hatestexchange-%s" % str(uuid.uuid4())

        parsed_deployment = yaml.load(deployment)
        self.pd_names = parsed_deployment['process-dispatchers'].keys()
        policy_params = {'preserve_n': 0}
        self.process_spec = {
                'run_type': 'supd',
                'parameters': {
                    'exec': 'sleep',
                    'argv': ['1000']
                    }
                }
        self.haservice = HighAvailabilityService(policy_parameters=policy_params,
                process_dispatchers=self.pd_names, exchange=self.exchange,
                process_spec=self.process_spec)
        self.haservice_greenlet = gevent.spawn(self.haservice.start)

        self.epuharness = EPUHarness(exchange=self.exchange)
        self.dashi = self.epuharness.dashi

        self.epuharness.start(deployment_str=deployment)

        # Ensure that all of the PDs are up
        for pd in self.pd_names:
            pd_client = ProcessDispatcherClient(self.dashi, pd)
            pd_client.dump()

        self.dashi = self.haservice.dashi
        self.haservice_client = HighAvailabilityServiceClient(self.dashi, topic=self.haservice.topic)


    def tearDown(self):
        self.haservice_greenlet.kill(exception=KeyboardInterrupt, block=True)
        self.epuharness.stop()

        while True:
            try:
                self.haservice_client.dump()
                print "Waiting for HA Service to quit"
                continue
            except timeout:
                break

        for pd in self.pd_names:
            while True:
                pd_client = ProcessDispatcherClient(self.dashi, pd)
                try:
                    print "Waiting for PD to exit..."
                    pd_client.dump()
                    continue
                except timeout:
                    break

    @attr('INT')
    def test_basic(self):

        n = 2
        self._update_policy_params_and_assert({'preserve_n': n})
        self._assert_n_processes(n)

        n = 1
        self._update_policy_params_and_assert({'preserve_n': n})
        self._assert_n_processes(n)

        n = 3
        self._update_policy_params_and_assert({'preserve_n': n})
        self._assert_n_processes(n)

        n = 0
        self._update_policy_params_and_assert({'preserve_n': n})
        self._assert_n_processes(n)


    @attr('INT')
    def test_balance(self):

        n = 1
        self._update_policy_params_and_assert({'preserve_n': n})
        self._assert_n_processes(1)

        n = 2
        self._update_policy_params_and_assert({'preserve_n': n})
        for pd in self.pd_names:
            self._assert_n_processes(1, only_pd=pd)

        n = 0
        self._update_policy_params_and_assert({'preserve_n': n})
        self._assert_n_processes(n)


    @attr('INT')
    def test_kill_a_pd(self):
        """Recover from killed PD

        Ensure that procs are balanced between two pds, kill one, then
        make sure that the HA Service compensates
        """

        n = 1
        self._update_policy_params_and_assert({'preserve_n': n})
        self._assert_n_processes(n)

        n = 2
        self._update_policy_params_and_assert({'preserve_n': n})
        for pd in self.pd_names:
            self._assert_n_processes(1, only_pd=pd)


        upids_before_kill = list(self.haservice.core.managed_upids)

        killed_pd = self.pd_names.pop()
        self.epuharness.stop(services=[killed_pd])
        

        timeout = 30
        while timeout >= 0 and upids_before_kill == self.haservice.core.managed_upids:
            # Waiting for HA Service to notice
            print "Managed UPIDs: %s" % self.haservice.core.managed_upids
            gevent.sleep(1)
            timeout -= 1
        if timeout <= 0:
            assert "Took too long for haservice to notice missing upid"

        assert upids_before_kill != self.haservice.core.managed_upids

        n = 2
        self._assert_n_processes(n)

    @attr('INT')
    def test_kill_an_eeagent(self):
        """Do nothing when an eeagent dies

        The Process Dispatcher should manage this scenario, so HA shouldn't
        do anything
        """
        #raise SkipTest("Processes aren't running on EEAs")

        # Shuffle deployment
        self.epuharness.stop()
        self.epuharness.start(deployment_str=deployment_one_pd_two_eea)
        parsed_deployment = yaml.load(deployment_one_pd_two_eea)
        self.pd_names = parsed_deployment['process-dispatchers'].keys()
        self.eea_names = []
        for node in parsed_deployment['nodes'].values():
            for eeagent in node['eeagents'].keys():
                self.eea_names.append(eeagent)

        n = 2
        self._update_policy_params_and_assert({'preserve_n': n})
        self._assert_n_processes(n)

        upids_before_kill = list(self.haservice.core.managed_upids)

        # Kill an eeagent that has some procs on it
        print "PD state %s" % self.dashi.call(self.pd_names[0], "dump")
        for eeagent in self.eea_names:
            print "Calling Dump State for %s" % eeagent
            state = self.dashi.call(eeagent, "dump_state", rpc=True)
            if len(state['processes']) > 0:
                self.epuharness.stop(services=[eeagent])
                break

        gevent.sleep(10)
        msg = "HA shouldn't have touched those procs! Getting too big for its britches!"
        assert upids_before_kill == self.haservice.core.managed_upids, msg


    @attr('INT')
    def test_missing_proc(self):
        """Kill a proc, and ensure HA starts a replacement
        """

        n = 2
        self._update_policy_params_and_assert({'preserve_n': n})
        self._assert_n_processes(n)

        upid_to_kill = self.haservice.core.managed_upids[0]
        pd = self._find_procs_pd(upid_to_kill)
        assert pd

        pd_client = ProcessDispatcherClient(self.dashi, pd)
        pd_client.terminate_process(upid_to_kill)
        print self._get_all_procs()
        print self._get_all_procs()
        print self._get_all_procs()

        gevent.sleep(5)
        self._assert_n_processes(n)
        gevent.sleep(5)
        self._assert_n_processes(n)
        print self._get_all_procs()


    def _update_policy_params_and_assert(self, new_params, maxattempts=None):
        if not maxattempts:
            maxattempts = 5

        for i in range(0, maxattempts):
            try:
                self.haservice_client.reconfigure_policy(new_params)
                break
            except timeout:
                print "reconfigure failed due to timeout"
                gevent.sleep(0.5)
                continue
            except:
                break

        assert self.haservice.core.policy_params == new_params

    def _find_procs_pd(self, upid):
        all_procs = self._get_all_procs()
        for pd, procs in all_procs.iteritems():
            for proc in procs:
                if proc['upid'] == upid:
                    return pd
        return None



    def _get_proc_from_all_pds(self, upid):
        for pd_name in self.pd_names:
            pd_client = ProcessDispatcherClient(self.dashi, pd_name)
            procs = pd_client.describe_processes()
            for proc in procs:
                if upid == proc.get('upid'):
                    return proc

        return None

    def _get_all_procs(self):
        all_procs = {}
        for pd_name in self.pd_names:
            pd_client = ProcessDispatcherClient(self.dashi, pd_name)
            print "Querying %s" % pd_name
            procs = pd_client.describe_processes()
            all_procs[pd_name] = procs

        return all_procs


    def _get_proc_from_pd(self, upid, pd_name):
        pd_client = ProcessDispatcherClient(self.dashi, pd_name)
        procs = pd_client.describe_processes()
        for proc in procs:
            if upid == proc.get('upid'):
                return proc

        return None

    def _assert_n_processes(self, n, timeout=None, only_pd=None):
        if not timeout:
            # HA service works every 5s, so should take no longer than 10s
            timeout=10
        processes = None
        for i in range(0,20):
            processes = self.haservice.core.managed_upids
            if n == 0 and len(processes) == n:
                # Check to make sure nothing running, or at least all marked terminated
                all_procs = self.haservice.core._query_process_dispatchers()
                print all_procs
                proc_list = []
                for pd_name, procs in all_procs.iteritems():
                    proc_list += procs

                if len(proc_list) == 0:
                    return
                else:
                    for proc in procs:
                        msg = "expected %s to be terminated but is %s" % (
                                proc['upid'], proc['state'])
                        assert proc['state'] in ('600-TERMINATING', '700-TERMINATED', '800-EXITED'), msg
                    return

            elif len(processes) == n or (only_pd and len(processes) >= n):
                confirmed_procs = []
                if only_pd:
                    for proc_upid in processes:
                        got_proc = self._get_proc_from_pd(proc_upid, only_pd)
                        if got_proc:
                            confirmed_procs.append(got_proc)
                else:
                    for proc_upid in processes:
                        got_proc = self._get_proc_from_all_pds(proc_upid)
                        if got_proc:
                            confirmed_procs.append(got_proc)

                print "confirmed procs: %s =?= %s" % (len(confirmed_procs), n)
                if len(confirmed_procs) == n or (only_pd and len(confirmed_procs) >= n):
                    print "OK"
                    return

            gevent.sleep(1)
        else:
            assert False, "HA took more than %ss to get to %s processes. Had %s" % (timeout, n, processes)

