import gevent
import unittest

from epuharness.harness import EPUHarness
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

        self.epuharness = EPUHarness()
        self.dashi = self.epuharness.dashi
        print "XXX self.dashi.exchange: %s" % self.dashi._exchange_name
        self.exchange = self.epuharness.exchange

        self.epuharness.start(deployment_str=deployment)
        self.pd_names = ["pd_0"]

        self.process_spec = {
                'run_type': 'supd',
                'parameters': {
                    'bogus': 'coho',
                    'exec': 'true',
                    'argv': []
                    }
                }

        for pd in self.pd_names:
            pd_client = ProcessDispatcherClient(self.dashi, pd)
            print pd_client.dump()

        policy_params = {'preserve_n': 0}
        self.haservice = HighAvailabilityService(policy_parameters=policy_params,
                process_dispatchers=self.pd_names, exchange=self.exchange)
        self.haservice_greenlet = gevent.spawn(self.haservice.start)

        self.dashi = self.haservice.dashi
        print "XXX self.dashi.exchange: %s" % self.dashi._exchange_name

        self.haservice_client = HighAvailabilityServiceClient(self.dashi)

        gevent.sleep(0.05)

    def tearDown(self):
        self.haservice_greenlet.kill(exception=KeyboardInterrupt, block=True)
        self.epuharness.stop()

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


    def _update_policy_params_and_assert(self, new_params):
        self.haservice_client.reconfigure_policy(new_params)
        assert self.haservice.core.policy_params == new_params


    def _get_proc_from_all_pds(self, upid):
        for pd_name in self.pd_names:
            pd_client = ProcessDispatcherClient(self.dashi, pd_name)
            procs = pd_client.describe_processes()
            for proc in procs:
                if upid == proc.get('upid'):
                    return proc

        return None

    def _assert_n_processes(self, n, timeout=None):
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
                    break
                else:
                    for proc in procs:
                        msg = "expected %s to be terminated but is %s" % (
                                proc['upid'], proc['state'])
                        assert proc['state'] == '700-TERMINATED', msg
                    break

            elif len(processes) == n:
                for proc_upid in processes:
                    assert self._get_proc_from_all_pds(proc_upid), "%s isn't in a PD" % proc_upid
                break

            gevent.sleep(1)
        else:
            assert False, "HA took more than %ss to get to %s processes. Had %s" % (timeout, n, processes)

