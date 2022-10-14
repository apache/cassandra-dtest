import os
import time
import pytest
import logging

from cassandra import ConsistencyLevel

from dtest import Tester, create_ks
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.assertions import assert_stderr_clean
from tools.jmxutils import (JolokiaAgent, make_mbean)

since = pytest.mark.since
ported_to_in_jvm = pytest.mark.ported_to_in_jvm
logger = logging.getLogger(__name__)


@since('3.0')
class TestHintedHandoffConfig(Tester):
    """
    Tests the hinted handoff configuration options introduced in
    CASSANDRA-9035.

    @jira_ticket CASSANDRA-9035
    """

    def _start_two_node_cluster(self, config_options=None):
        """
        Start a cluster with two nodes and return them
        """
        cluster = self.cluster

        if config_options:
            cluster.set_configuration_options(values=config_options)

        cluster.populate([2]).start()
        return cluster.nodelist()

    def _launch_nodetool_cmd(self, node, cmd):
        """
        Launch a nodetool command and check there is no error, return the result
        """
        out, err, _ = node.nodetool(cmd)
        assert_stderr_clean(err)
        return out

    def _do_hinted_handoff(self, node1, node2, enabled, keyspace='ks'):
        """
        Test that if we stop one node the other one
        will store hints only when hinted handoff is enabled
        """
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, keyspace, 2)
        create_c1c2_table(self, session)

        node2.stop(wait_other_notice=True)

        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)

        log_mark = node1.mark_log()
        node2.start()

        if enabled:
            node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=120)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been delivered via HH if enabled or not if not enabled
        session = self.patient_exclusive_cql_connection(node2, keyspace=keyspace)
        for n in range(0, 100):
            if enabled:
                query_c1c2(session, n, ConsistencyLevel.ONE)
            else:
                query_c1c2(session, n, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)

    @ported_to_in_jvm('4.0')
    def test_nodetool(self):
        """
        Test various nodetool commands
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

            self._launch_nodetool_cmd(node, 'disablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is not running' == res.rstrip()

            self._launch_nodetool_cmd(node, 'enablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

            self._launch_nodetool_cmd(node, 'disablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep) == res.rstrip()

            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

    def test_hintedhandoff_disabled(self):
        """
        Test gloabl hinted handoff disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': False})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is not running' == res.rstrip()

        self._do_hinted_handoff(node1, node2, False)

    def test_hintedhandoff_enabled(self):
        """
        Test global hinted handoff enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        self._do_hinted_handoff(node1, node2, True)

    @since('4.0')
    def test_hintedhandoff_setmaxwindow(self):
        """
        Test global hinted handoff against max_hint_window_in_ms update via nodetool
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True, "max_hint_window_in_ms": 300000})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        res = self._launch_nodetool_cmd(node, 'getmaxhintwindow')
        assert 'Current max hint window: 300000 ms' == res.rstrip()
        self._do_hinted_handoff(node1, node2, True)
        node1.start()
        for node in node1, node2:
            # Make sure HH is effective on both nodes despite node startup races CASSANDRA-15865
            self._launch_nodetool_cmd(node, 'setmaxhintwindow 1')
            res = self._launch_nodetool_cmd(node, 'getmaxhintwindow')
            assert 'Current max hint window: 1 ms' == res.rstrip()
        self._do_hinted_handoff(node1, node2, False, keyspace='ks2')

    def test_hintedhandoff_dc_disabled(self):
        """
        Test global hinted handoff enabled with the dc disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True,
                                                     'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep) == res.rstrip()

        self._do_hinted_handoff(node1, node2, False)

    def test_hintedhandoff_dc_reenabled(self):
        """
        Test global hinted handoff enabled with the dc disabled first and then re-enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True,
                                                     'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep) == res.rstrip()

        for node in node1, node2:
            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            assert 'Hinted handoff is running' == res.rstrip()

        self._do_hinted_handoff(node1, node2, True)


class TestHintedHandoff(Tester):

    @ported_to_in_jvm('4.0')
    @pytest.mark.no_vnodes
    def test_hintedhandoff_decom(self):
        self.fixture_dtest_setup.ignore_log_patterns = [
            'Could not update repaired ranges.*Giving up'
        ]
        self.cluster.populate(4).start()
        [node1, node2, node3, node4] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)
        node4.stop(wait_other_notice=True)
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)
        node1.decommission()
        node4.start(wait_for_binary_proto=True)

        force = True if self.cluster.version() >= '3.12' else False
        node2.decommission(force=force)
        node3.decommission(force=force)

        time.sleep(5)
        for x in range(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)

    @since('4.1')
    def test_hintedhandoff_window(self):
        """
        Test that we only store at a maximum the hint window worth of hints.
        Prior to CASSANDRA-14309 we would store another window worth of hints
        if the down node was brought up and then taken back down immediately.
        We would also store another window of hints on a live node if the live
        node was restarted.
        @jira_ticket CASSANDRA-14309
        """

        def wait_for_downtime(node_to_query, node, downtime):
            def endpoint_downtime(node_to_query, node):
                mbean = make_mbean('net', type='Gossiper')
                with JolokiaAgent(node_to_query) as jmx:
                    return jmx.execute_method(mbean, 'getEndpointDowntime(java.lang.String)', [node])

            run = True
            while run:
                try:
                    if downtime == 0:
                        while endpoint_downtime(node_to_query, node) != downtime:
                            time.sleep(1)
                    else:
                        while endpoint_downtime(node_to_query, node) <= downtime:
                            time.sleep(1)
                    run = False
                except Exception:
                    pass

        # hint_window_persistent_enabled is set to true by default
        self.cluster.set_configuration_options({'max_hint_window_in_ms': 10000,
                                                'hinted_handoff_enabled': True,
                                                'max_hints_delivery_threads': 1,
                                                'hints_flush_period_in_ms': 100, })
        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)

        # Stop handoff until very end and take node2 down for first round of hints
        node1.nodetool('pausehandoff')

        node2.nodetool('disablebinary')
        node2.watch_log_for(["Stop listening for CQL clients"], timeout=120)

        node2.nodetool('disablegossip')

        node2.watch_log_for(["Announcing shutdown", "state jump to shutdown"], timeout=120)
        node1.watch_log_for(["state jump to shutdown"], timeout=120)

        log_mark_node_1 = node1.mark_log()
        log_mark_node_2 = node2.mark_log()

        wait_for_downtime(node1, "127.0.0.2", 1000)

        # First round of hints. We expect these to be replayed and the only
        # hints within the window
        insert_c1c2(session, n=(0, 100), consistency=ConsistencyLevel.ONE)
        node1.nodetool('flush')

        # let the hint window pass
        wait_for_downtime(node1, "127.0.0.2", 10000)

        # Re-enable and disable the node. Prior to CASSANDRA-14215 this should make the hint window on node1 reset.
        node2.nodetool('enablegossip')
        node2.watch_log_for(["state jump to NORMAL"], timeout=120, from_mark=log_mark_node_2)
        node1.watch_log_for(["state jump to NORMAL"], timeout=120, from_mark=log_mark_node_1)

        # no downtime for node for which we enabled gossip
        wait_for_downtime(node1, "127.0.0.2", 0)

        log_mark_node_1 = node1.mark_log()
        log_mark_node_2 = node2.mark_log()

        # disable gossip again
        node2.nodetool('disablegossip')

        wait_for_downtime(node1, "127.0.0.2", 1000)

        node2.watch_log_for(["Announcing shutdown", "state jump to shutdown"], timeout=120, from_mark=log_mark_node_2)
        node1.watch_log_for(["state jump to shutdown"], timeout=120, from_mark=log_mark_node_1)

        log_mark_node_1 = node1.mark_log()
        log_mark_node_2 = node2.mark_log()

        wait_for_downtime(node1, "127.0.0.2", 5000)

        # Second round of inserts. We do not expect hints to be stored.
        insert_c1c2(session, n=(100, 200), consistency=ConsistencyLevel.ONE)

        # Restart node1. Prior to CASSANDRA-14215 this would reset node1's hint window.
        node1.stop()
        node1.start(wait_for_binary_proto=True, wait_other_notice=False)
        session = self.patient_exclusive_cql_connection(node1)
        session.execute('USE ks')
        # Third round of inserts. We do not expect hints to be stored.
        insert_c1c2(session, n=(200, 300), consistency=ConsistencyLevel.ONE)

        # Enable node2 and wait for hints to be replayed
        node2.nodetool('enablegossip')
        node2.watch_log_for(["state jump to NORMAL"], timeout=120, from_mark=log_mark_node_2)

        node2.nodetool('enablebinary')
        node2.watch_log_for(["Starting listening for CQL clients"], timeout=120, from_mark=log_mark_node_2)
        node1.nodetool('resumehandoff')
        node1.watch_log_for('Finished hinted handoff')
        # Stop node1 so that we only query node2
        node1.stop()

        session = self.patient_exclusive_cql_connection(node2)
        session.execute('USE ks')
        # Ensure first dataset is present (through replayed hints)
        for x in range(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)

        exception = None

        # Ensure second and third datasets are not present
        for x in range(100, 300):
            try:
                query_c1c2(session, x, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)
            except AssertionError as ex:
                logger.info("failed for %d" % x)
                exception = ex
                pass

        if exception is not None:
            raise exception
