import os
import os.path
import re
from time import sleep

import pytest
import logging

from ccmlib.node import Node
from dtest import Tester, create_ks
from tools.assertions import assert_almost_equal
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.jmxutils import (JolokiaAgent, make_mbean)
from tools.misc import new_node
from compaction_test import grep_sstables_in_each_level

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('3.2')
class TestDiskBalance(Tester):
    """
    @jira_ticket CASSANDRA-6696
    """

    @pytest.fixture(scope='function', autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        cluster = fixture_dtest_setup.cluster
        cluster.schema_event_refresh_window = 0

        # CASSANDRA-14556 should be disabled if you need directories to be perfectly balanced.
        if cluster.version() >= '4.0':
            cluster.set_configuration_options({'stream_entire_sstables': 'false'})

    def test_disk_balance_stress(self):
        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(4).start()
        node1 = cluster.nodes['node1']

        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=3)',
                      'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()
        # make sure the data directories are balanced:
        for node in cluster.nodelist():
            self.assert_balanced(node)

    @pytest.mark.resource_intensive
    def test_disk_balance_bootstrap(self):
        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 256})
        # apparently we have legitimate errors in the log when bootstrapping (see bootstrap_test.py)
        self.fixture_dtest_setup.allow_log_errors = True
        cluster.populate(4).start()
        node1 = cluster.nodes['node1']

        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=3)',
                      'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()
        node5 = new_node(cluster, data_center="dc1")
        node5.start(wait_for_binary_proto=True)
        self.assert_balanced(node5)

    def test_disk_balance_replace_same_address(self):
        self._test_disk_balance_replace(same_address=True)

    def test_disk_balance_replace_different_address(self):
        self._test_disk_balance_replace(same_address=False)

    def _test_disk_balance_replace(self, same_address):
        logger.debug("Creating cluster")
        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 256})
        # apparently we have legitimate errors in the log when bootstrapping (see bootstrap_test.py)
        self.fixture_dtest_setup.allow_log_errors = True
        cluster.populate(4).start()
        node1 = cluster.nodes['node1']

        logger.debug("Populating")
        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=3)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()

        logger.debug("Stopping and removing node2")
        node2 = cluster.nodes['node2']
        node2.stop(gently=False)
        self.cluster.remove(node2)

        node5_address = node2.address() if same_address else '127.0.0.5'
        logger.debug("Starting replacement node")
        node5 = Node('node5', cluster=self.cluster, auto_bootstrap=True,
                     thrift_interface=None, storage_interface=(node5_address, 7000),
                     jmx_port='7500', remote_debug_port='0', initial_token=None,
                     binary_interface=(node5_address, 9042))
        self.cluster.add(node5, False, data_center="dc1")
        node5.start(jvm_args=["-Dcassandra.replace_address_first_boot={}".format(node2.address())],
                    wait_for_binary_proto=180,
                    wait_other_notice=True)

        logger.debug("Checking replacement node is balanced")
        self.assert_balanced(node5)

    def test_disk_balance_decommission(self):
        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(4).start()
        node1 = cluster.nodes['node1']
        node4 = cluster.nodes['node4']
        node1.stress(['write', 'n=50k', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=2)',
                      'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)'])
        cluster.flush()

        node4.decommission()

        for node in cluster.nodelist():
            node.nodetool('relocatesstables')

        for node in cluster.nodelist():
            self.assert_balanced(node)

    def test_blacklisted_directory(self):
        cluster = self.cluster
        cluster.set_datadir_count(3)
        cluster.populate(1)
        [node] = cluster.nodelist()
        cluster.start()

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)
        create_c1c2_table(self, session)
        insert_c1c2(session, n=10000)
        node.flush()
        for k in range(0, 10000):
            query_c1c2(session, k)

        node.compact()
        mbean = make_mbean('db', type='BlacklistedDirectories')
        with JolokiaAgent(node) as jmx:
            jmx.execute_method(mbean, 'markUnwritable', [os.path.join(node.get_path(), 'data1')])

        for k in range(0, 10000):
            query_c1c2(session, k)

        node.nodetool('relocatesstables')

        for k in range(0, 10000):
            query_c1c2(session, k)

    def test_alter_replication_factor(self):
        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 256})
        cluster.populate(3).start()
        node1 = cluster.nodes['node1']
        node1.stress(['write', 'n=1', 'no-warmup', '-rate', 'threads=100', '-schema', 'replication(factor=1)'])
        cluster.flush()
        session = self.patient_cql_connection(node1)
        session.execute("ALTER KEYSPACE keyspace1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        node1.stress(['write', 'n=100k', 'no-warmup', '-rate', 'threads=100'])
        cluster.flush()
        for node in cluster.nodelist():
            self.assert_balanced(node)

    def assert_balanced(self, node):
        old_sums = new_sums = None
        # This extra looping and logic is to account for a race with obsolete file deletions, which are scheduled
        # asynchronously in the server. We want to allow a chance to settle if files are still being removed
        for _ in range(20):
            old_sums = new_sums
            new_sums = []
            for sstabledir in node.get_sstables_per_data_directory('keyspace1', 'standard1'):
                sum = 0
                for sstable in sstabledir:
                    sum = sum + os.path.getsize(sstable)
                new_sums.append(sum)
            if new_sums == old_sums:
                break
            sleep(2)

        assert len(new_sums)
        assert new_sums == old_sums  # we settled
        assert_almost_equal(*new_sums, error=0.1, error_message=node.name)

    @since('3.10')
    def test_disk_balance_after_boundary_change_stcs(self):
        """
        @jira_ticket CASSANDRA-13948
        """
        self._disk_balance_after_boundary_change_test(lcs=False)

    @since('3.10')
    def test_disk_balance_after_boundary_change_lcs(self):
        """
        @jira_ticket CASSANDRA-13948
        """
        self._disk_balance_after_boundary_change_test(lcs=True)

    def _disk_balance_after_boundary_change_test(self, lcs):
        """
            @jira_ticket CASSANDRA-13948

            - Creates a 1 node cluster with 5 disks and insert data with compaction disabled
            - Bootstrap a node2 to make disk boundary changes on node1
            - Enable compaction on node1 and check disks are balanced
            - Decommission node1 to make disk boundary changes on node2
            - Enable compaction on node2 and check disks are balanced
        """

        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 1024})
        num_disks = 5
        cluster.set_datadir_count(num_disks)
        cluster.set_configuration_options(values={'concurrent_compactors': num_disks})

        logger.debug("Starting node1 with {} data dirs and concurrent_compactors".format(num_disks))
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        # reduce system_distributed RF to 1 so we don't require forceful decommission
        session.execute("ALTER KEYSPACE system_distributed WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")
        session.execute("ALTER KEYSPACE system_traces WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':'1'};")

        num_flushes = 10
        keys_per_flush = 10000
        keys_to_write = num_flushes * keys_per_flush

        compaction_opts = "LeveledCompactionStrategy,sstable_size_in_mb=1" if lcs else "SizeTieredCompactionStrategy"
        logger.debug("Writing {} keys in {} flushes (compaction_opts={})".format(keys_to_write, num_flushes, compaction_opts))
        total_keys = num_flushes * keys_per_flush
        current_keys = 0
        while current_keys < total_keys:
            start_key = current_keys + 1
            end_key = current_keys + keys_per_flush
            logger.debug("Writing keys {}..{} and flushing".format(start_key, end_key))
            node1.stress(['write', 'n={}'.format(keys_per_flush), "no-warmup", "cl=ALL", "-pop",
                          "seq={}..{}".format(start_key, end_key), "-rate", "threads=1", "-schema", "replication(factor=1)",
                          "compaction(strategy={},enabled=false)".format(compaction_opts)])
            node1.nodetool('flush keyspace1 standard1')
            current_keys = end_key

        # Add a new node, so disk boundaries will change
        logger.debug("Bootstrap node2 and flush")
        # Fixed initial token to bisect the ring and make sure the nodes are balanced (otherwise a random token is generated).
        balanced_tokens = cluster.balanced_tokens(2)
        assert self.dtest_config.use_vnodes or balanced_tokens[0] == node1.initial_token  # make sure cluster population still works as assumed
        node2 = new_node(cluster, token=balanced_tokens[1], bootstrap=True, data_center="dc1")
        node2.set_configuration_options(values={'num_tokens': 1})
        node2.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.migration_task_wait_in_seconds=10"], set_migration_task=False)
        node2.flush()

        self._assert_balanced_after_boundary_change(node1, total_keys, lcs)

        logger.debug("Decommissioning node1")
        node1.decommission()
        node1.stop()

        self._assert_balanced_after_boundary_change(node2, total_keys, lcs)

    @since('3.10')
    def test_disk_balance_after_joining_ring_stcs(self):
        """
        @jira_ticket CASSANDRA-13948
        """
        self._disk_balance_after_joining_ring_test(lcs=False)

    @since('3.10')
    def test_disk_balance_after_joining_ring_lcs(self):
        """
        @jira_ticket CASSANDRA-13948
        """
        self._disk_balance_after_joining_ring_test(lcs=True)

    def _disk_balance_after_joining_ring_test(self, lcs):
        """
            @jira_ticket CASSANDRA-13948

            - Creates a 3 node cluster with 5 disks and insert data with compaction disabled
            - Stop node1
            - Start node1 without joining gossip and loading ring state so disk boundaries will not reflect actual ring state
            - Join node1 to the ring to make disk boundaries change
            - Enable compaction on node1 and check disks are balanced
        """

        cluster = self.cluster
        if self.dtest_config.use_vnodes:
            cluster.set_configuration_options(values={'num_tokens': 1024})
        num_disks = 5
        cluster.set_datadir_count(num_disks)
        cluster.set_configuration_options(values={'concurrent_compactors': num_disks})

        logger.debug("Starting 3 nodes with {} data dirs and concurrent_compactors".format(num_disks))
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        num_flushes = 10
        keys_per_flush = 10000
        keys_to_write = num_flushes * keys_per_flush

        compaction_opts = "LeveledCompactionStrategy,sstable_size_in_mb=1" if lcs else "SizeTieredCompactionStrategy"
        logger.debug("Writing {} keys in {} flushes (compaction_opts={})".format(keys_to_write, num_flushes, compaction_opts))
        total_keys = num_flushes * keys_per_flush
        current_keys = 0
        while current_keys < total_keys:
            start_key = current_keys + 1
            end_key = current_keys + keys_per_flush
            logger.debug("Writing keys {}..{} and flushing".format(start_key, end_key))
            node1.stress(['write', 'n={}'.format(keys_per_flush), "no-warmup", "cl=ALL", "-pop",
                          "seq={}..{}".format(start_key, end_key), "-rate", "threads=1", "-schema", "replication(factor=1)",
                          "compaction(strategy={},enabled=false)".format(compaction_opts)])
            node1.nodetool('flush keyspace1 standard1')
            current_keys = end_key

        logger.debug("Stopping node1")
        node1.stop()

        logger.debug("Starting node1 without joining ring")
        node1.start(wait_for_binary_proto=True, wait_other_notice=False, join_ring=False,
                    jvm_args=["-Dcassandra.load_ring_state=false", "-Dcassandra.write_survey=true"])

        logger.debug("Joining node1 to the ring")
        node1.nodetool("join")
        node1.nodetool("join")  # Need to run join twice - one to join ring, another to leave write survey mode

        self._assert_balanced_after_boundary_change(node1, total_keys, lcs)

    def _assert_balanced_after_boundary_change(self, node, total_keys, lcs):
        logger.debug("Cleanup {}".format(node.name))
        node.cleanup()

        logger.debug("Enabling compactions on {} now that boundaries changed".format(node.name))
        node.nodetool('enableautocompaction')

        logger.debug("Waiting for compactions on {}".format(node.name))
        node.wait_for_compactions()

        logger.debug("Disabling compactions on {} should not block forever".format(node.name))
        node.nodetool('disableautocompaction')

        logger.debug("Major compact {} and check disks are balanced".format(node.name))
        node.compact()

        node.wait_for_compactions()
        self.assert_balanced(node)

        logger.debug("Reading data back ({} keys)".format(total_keys))
        node.stress(['read', 'n={}'.format(total_keys), "no-warmup", "cl=ALL", "-pop", "seq=1...{}".format(total_keys), "-rate", "threads=1"])

        if lcs:
            output = grep_sstables_in_each_level(node, "standard1")
            logger.debug("SSTables in each level: {}".format(output))

            # [0, ?/, 0, 0, 0, 0...]
            p = re.compile(r'(\d+)(/\d+)?,\s(\d+).*')
            m = p.search(output)
            cs_count = int(m.group(1)) + int(m.group(3))
            sstable_count = len(node.get_sstables('keyspace1', 'standard1'))
            logger.debug("Checking that compaction strategy sstable # ({}) is equal to actual # ({})".format(cs_count, sstable_count))
            assert sstable_count == cs_count
            assert not node.grep_log("is already present on leveled manifest")
