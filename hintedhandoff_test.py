import os
import time
import glob
import pytest
import logging

from cassandra import ConsistencyLevel

from dtest import Tester, create_ks
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.assertions import assert_stderr_clean

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
    def test_hintedhandoff_list_pending_hints(self):
        """
        Test getting list of pending hints
        @jira_ticket: CASSANDRA-14795
        """
        # hints must be flushed on disk and the writers must be closed for the hints to be available in the
        # dispatch queue and therefore visible to listendpointspendinghints
        self.cluster.set_configuration_options(values={"max_hints_file_size_in_mb": 1})
        self.cluster.populate(2).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1, node2 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)

        session2 = self.patient_exclusive_cql_connection(node2)
        node2_local_row = session2.execute("SELECT host_id, rack, data_center FROM system.local;").current_rows[0]
        node2_host_id = node2_local_row.host_id
        node2_rack = node2_local_row.rack
        node2_data_center = node2_local_row.data_center
        session2.shutdown()

        node2.stop(wait_other_notice=True)
        insert_c1c2(session, n=15000, consistency=ConsistencyLevel.ONE)

        # wait for the hints to be flushed to disk
        hints_dir_path = os.path.join(node1.get_path(), 'hints')
        assert os.path.isdir(hints_dir_path)

        start = time.time()
        wait_minutes = 1
        while True:
            # check for *.crc32 to count only finished hint files
            hints = [name for name in glob.glob(os.path.join(hints_dir_path, "*.crc32"))]
            if len(hints) > 0:
                break

            elapsed = (time.time() - start) / 60
            if elapsed > wait_minutes:
                pytest.fail("Node 1 hasn't flushed hints in {} minutes. Hint directory contents: {}".format(
                    wait_minutes,
                    os.listdir(hints_dir_path)))
            time.sleep(1)

        # test the nodetool command
        endpoints = node1.nodetool('listpendinghints')[0].split("\n")
        assert len(endpoints) == 3  # header, 1 row, empty line

        hints_row = endpoints[1].split()
        assert str(node2_host_id) == hints_row[0]
        node2_address = node2.ip_addr + ":" + str(node2.network_interfaces['storage'][1])
        assert node2_address == hints_row[1]
        assert node2_rack == hints_row[2]
        assert node2_data_center == hints_row[3]
        assert node2.status == hints_row[4]
        assert len(hints) == int(hints_row[5])

        # test the virtual table
        rows = list(session.execute("""
            SELECT host_id, address, port, rack, dc, status, files
            FROM system_views.pending_hints
        """))
        assert len(rows) == 1
        row = rows[0]
        assert row.host_id == node2_local_row.host_id
        assert row.address == node2.ip_addr
        assert row.port == node2.network_interfaces['storage'][1]
        assert row.rack == node2_rack
        assert row.dc == node2_data_center
        assert row.status == node2.status
        assert row.files == len(hints)

        log_mark = node1.mark_log()
        node2.start(no_wait=True, wait_other_notice=False)
        node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=120)
        assert node1.nodetool('listpendinghints')[0] == "This node does not have any pending hints\n"
        rows = list(session.execute("SELECT * FROM system_views.pending_hints"))
        assert len(rows) == 0
