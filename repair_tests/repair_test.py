import threading
import time
from collections import namedtuple
from threading import Thread
from unittest import skip, skipIf

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.node import ToolError
from nose.plugins.attrib import attr

from dtest import CASSANDRA_VERSION_FROM_BUILD, FlakyRetryPolicy, Tester, debug, create_ks, create_cf
from tools.data import insert_c1c2, query_c1c2
from tools.decorators import known_failure, no_vnodes, since


def _repair_options(version, ks='', cf=None, sequential=True):
    """
    Function for assembling appropriate repair CLI options,
    based on C* version, as defaults have changed.
    @param ks The keyspace to repair
    @param cf The table to repair
    @param sequential If the repair should be a sequential repair [vs parallel]
    """
    opts = []

    # since version 2.2, default is parallel, otherwise it's sequential
    if sequential:
        if version >= '2.2':
            opts += ['-seq']
    else:
        if version < '2.2':
            opts += ['-par']

    # test with full repair
    if version >= '2.2':
        opts += ['-full']
    if ks:
        opts += [ks]
    if cf:
        opts += [cf]
    return opts


class BaseRepairTest(Tester):
    __test__ = False

    def check_rows_on_node(self, node_to_check, rows, found=None, missings=None, restart=True):
        """
        Function to verify the rows on a given node, without interference
        from the other nodes in the cluster
        @param node_to_check The given node to check. Should be the node, not the index
        @param rows The number of rows we expect
        @param found A list of partition keys that we expect to be on the node
        @param missings A list of partition keys we expect NOT to be on the node
        @param restart Whether or not we should restart the nodes we shut down to perform the assertions. Should only be False if the call to check_rows_on_node is the last line in the test.
        """
        if found is None:
            found = []
        if missings is None:
            missings = []
        stopped_nodes = []

        for node in self.cluster.nodes.values():
            if node.is_running() and node is not node_to_check:
                stopped_nodes.append(node)
                node.stop(wait_other_notice=True)

        session = self.patient_exclusive_cql_connection(node_to_check, 'ks')
        result = list(session.execute("SELECT * FROM cf LIMIT {}".format(rows * 2)))
        self.assertEqual(len(result), rows)

        for k in found:
            query_c1c2(session, k, ConsistencyLevel.ONE)

        for k in missings:
            query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k{}'".format(k), consistency_level=ConsistencyLevel.ONE)
            res = list(session.execute(query))
            self.assertEqual(len(filter(lambda x: len(x) != 0, res)), 0, res)

        if restart:
            for node in stopped_nodes:
                node.start(wait_other_notice=True)

    def _populate_cluster(self, start=True):
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        debug("Starting cluster..")
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        session.cluster.default_retry_policy = FlakyRetryPolicy(max_retries=15)
        create_ks(session, 'ks', 3)
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
        debug("Inserting data...")
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ALL)
        node3.flush()
        node3.stop(wait_other_notice=True)
        insert_c1c2(session, keys=(1000, ), consistency=ConsistencyLevel.TWO)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        insert_c1c2(session, keys=range(1001, 2001), consistency=ConsistencyLevel.ALL)

        cluster.flush()

    def _repair_and_verify(self, sequential=True):
        cluster = self.cluster
        node1, node2, node3 = cluster.nodelist()

        # Verify that node3 has only 2000 keys
        debug("Checking data on node3...")
        self.check_rows_on_node(node3, 2000, missings=[1000])

        # Verify that node1 has 2001 keys
        debug("Checking data on node1...")
        self.check_rows_on_node(node1, 2001, found=[1000])

        # Verify that node2 has 2001 keys
        debug("Checking data on node2...")
        self.check_rows_on_node(node2, 2001, found=[1000])

        time.sleep(10)  # see CASSANDRA-4373
        # Run repair
        start = time.time()
        debug("starting repair...")
        node1.repair(_repair_options(self.cluster.version(), ks='ks', sequential=sequential))
        debug("Repair time: {end}".format(end=time.time() - start))

        # Validate that only one range was transfered
        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")

        self.assertEqual(len(out_of_sync_logs), 2, "Lines matching: " + str([elt[0] for elt in out_of_sync_logs]))

        valid_out_of_sync_pairs = [{node1.address(), node3.address()},
                                   {node2.address(), node3.address()}]

        for line, m in out_of_sync_logs:
            num_out_of_sync_ranges, out_of_sync_nodes = m.group(3), {m.group(1), m.group(2)}
            self.assertEqual(int(num_out_of_sync_ranges), 1, "Expecting 1 range out of sync for {}, but saw {}".format(out_of_sync_nodes, line))
            self.assertIn(out_of_sync_nodes, valid_out_of_sync_pairs, str(out_of_sync_nodes))

        # Check node3 now has the key
        self.check_rows_on_node(node3, 2001, found=[1000], restart=False)


class TestRepair(BaseRepairTest):
    __test__ = True

    @since('2.2.1')
    def no_anticompaction_after_dclocal_repair_test(self):
        """
        * Launch a four node, two DC cluster
        * Start a -local repair on node1 in dc1
        * Assert that the dc1 nodes see repair messages
        * Assert that the dc2 nodes do not see repair messages
        * Assert no nodes anticompact
        # TODO: Verify the anticompaction with sstablemetadata, not just logs
        @jira_ticket CASSANDRA-10422
        """
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate([2, 2]).start(wait_for_binary_proto=True)
        node1_1, node2_1, node1_2, node2_2 = cluster.nodelist()
        node1_1.stress(stress_options=['write', 'n=50K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=4)', '-rate', 'threads=50'])
        node1_1.nodetool("repair -local keyspace1 standard1")
        self.assertTrue(node1_1.grep_log("Not a global repair"))
        self.assertTrue(node2_1.grep_log("Not a global repair"))

        # dc2 should not see these messages:
        self.assertFalse(node1_2.grep_log("Not a global repair"))
        self.assertFalse(node2_2.grep_log("Not a global repair"))

        # and no nodes should do anticompaction:
        for node in cluster.nodelist():
            self.assertFalse(node.grep_log("Starting anticompaction"))

    @skipIf(CASSANDRA_VERSION_FROM_BUILD == '3.9', "Test doesn't run on 3.9")
    def nonexistent_table_repair_test(self):
        """
        * Check that repairing a non-existent table fails
        @jira_ticket CASSANDRA-12279
        """
        self.ignore_log_patterns = [r'Unknown keyspace/cf pair']
        cluster = self.cluster
        debug('Starting nodes')
        cluster.populate(2).start(wait_for_binary_proto=True)
        node1, _ = cluster.nodelist()
        debug('Creating keyspace and tables')
        node1.stress(stress_options=['write', 'n=1', 'no-warmup',
                                     'cl=ONE', '-schema', 'replication(factor=2)',
                                     '-rate', 'threads=1'])
        debug('Repairing non-existent table')

        def repair_non_existent_table():
            global nodetool_error
            try:
                node1.nodetool('repair keyspace1 standard2')
            except Exception, e:
                nodetool_error = e

        # Launch in a external thread so it does not hang process
        t = Thread(target=repair_non_existent_table)
        t.start()

        t.join(timeout=60)
        self.assertFalse(t.isAlive(), 'Repair thread on inexistent table is still running')

        if self.cluster.version() >= '2.2':
            node1.watch_log_for("Unknown keyspace/cf pair", timeout=60)
        # Repair only finishes with error status after CASSANDRA-12508 on 3.0+
        if self.cluster.version() >= '3.0':
            self.assertTrue('nodetool_error' in globals() and isinstance(nodetool_error, ToolError),
                            'Repair thread on inexistent table did not throw exception')
            debug(nodetool_error.message)
            self.assertTrue('Unknown keyspace/cf pair' in nodetool_error.message,
                            'Repair thread on inexistent table did not detect inexistent table.')

    @since('2.2.1')
    def no_anticompaction_after_hostspecific_repair_test(self):
        """
        * Launch a four node, two DC cluster
        * Start a repair on all nodes, by enumerating with -hosts
        * Assert all nodes see a repair messages
        * Assert no nodes anticompact
        # TODO: Verify the anticompaction with sstablemetadata, not just logs
        @jira_ticket CASSANDRA-10422
        """
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate([2, 2]).start(wait_for_binary_proto=True)
        node1_1, node2_1, node1_2, node2_2 = cluster.nodelist()
        node1_1.stress(stress_options=['write', 'n=100K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=4)', '-rate', 'threads=50'])
        node1_1.nodetool("repair -hosts 127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4 keyspace1 standard1")
        for node in cluster.nodelist():
            self.assertTrue(node.grep_log("Not a global repair"))
        for node in cluster.nodelist():
            self.assertFalse(node.grep_log("Starting anticompaction"))

    @since('2.2.4')
    def no_anticompaction_after_subrange_repair_test(self):
        """
        * Launch a three node, two DC cluster
        * Start a repair on a token range
        * Assert all nodes see repair messages
        * Assert no nodes anticompact
        # TODO: Verify the anticompaction with sstablemetadata, not just logs
        @jira_ticket CASSANDRA-10422
        """
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()
        node1.stress(stress_options=['write', 'n=50K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=3)', '-rate', 'threads=50'])
        node1.nodetool("repair -st 0 -et 1000 keyspace1 standard1")
        for node in cluster.nodelist():
            self.assertTrue(node.grep_log("Not a global repair"))
        for node in cluster.nodelist():
            self.assertFalse(node.grep_log("Starting anticompaction"))

    @since('2.2.1')
    def anticompaction_after_normal_repair_test(self):
        """
        * Launch a four node, two DC cluster
        * Start a normal repair
        * Assert every node anticompacts
        @jira_ticket CASSANDRA-10422
        """
        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate([2, 2]).start(wait_for_binary_proto=True)
        node1_1, node2_1, node1_2, node2_2 = cluster.nodelist()
        node1_1.stress(stress_options=['write', 'n=50K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=4)'])
        node1_1.nodetool("repair keyspace1 standard1")
        for node in cluster.nodelist():
            self.assertTrue("Starting anticompaction")

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12162',
                   flaky=True,
                   notes='windows')
    def simple_sequential_repair_test(self):
        """
        Calls simple repair test with a sequential repair
        """
        self._simple_repair(sequential=True)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11247',
                   flaky=True,
                   notes='windows')
    def simple_parallel_repair_test(self):
        """
        Calls simple repair test with a parallel repair
        """
        self._simple_repair(sequential=False)

    def empty_vs_gcable_sequential_repair_test(self):
        """
        Calls empty_vs_gcable repair test with a sequential repair
        """
        self._empty_vs_gcable_no_repair(sequential=True)

    def empty_vs_gcable_parallel_repair_test(self):
        """
        Calls empty_vs_gcable repair test with a parallel repair
        """
        self._empty_vs_gcable_no_repair(sequential=False)

    def range_tombstone_digest_sequential_repair_test(self):
        """
        Calls range_tombstone_digest with a sequential repair
        """
        self._range_tombstone_digest(sequential=True)

    def range_tombstone_digest_parallel_repair_test(self):
        """
        Calls range_tombstone_digest with a parallel repair
        """
        self._range_tombstone_digest(sequential=False)

    @since('2.1')
    def shadowed_cell_digest_sequential_repair_test(self):
        """
        Calls _cell_shadowed_by_range_tombstone with sequential repair
        """
        self._cell_shadowed_by_range_tombstone(sequential=True)

    @since('2.1')
    def shadowed_cell_digest_parallel_repair_test(self):
        """
        Calls _cell_shadowed_by_range_tombstone with parallel repair
        """
        self._cell_shadowed_by_range_tombstone(sequential=False)

    @since('3.0')
    def shadowed_range_tombstone_digest_sequential_repair_test(self):
        """
        Calls _range_tombstone_shadowed_by_range_tombstone with sequential repair
        """
        self._range_tombstone_shadowed_by_range_tombstone(sequential=True)

    @since('3.0')
    def shadowed_range_tombstone_digest_parallel_repair_test(self):
        """
        Calls _range_tombstone_shadowed_by_range_tombstone with parallel repair
        """
        self._range_tombstone_shadowed_by_range_tombstone(sequential=False)

    @no_vnodes()
    def simple_repair_order_preserving_test(self):
        """
        Calls simple repair test with OPP and sequential repair
        @jira_ticket CASSANDRA-5220
        """
        self._simple_repair(order_preserving_partitioner=True)

    def _simple_repair(self, order_preserving_partitioner=False, sequential=True):
        """
        * Configure a three node cluster to not use hinted handoff, and to use batch commitlog
        * Launch the cluster
        * Create a keyspace at RF 3 and table
        * Insert one thousand rows at CL ALL
        * Flush on node3 and shut it down
        * Insert one row at CL TWO
        * Restart node3
        * Insert one thousand more rows at CL ALL
        * Flush all nodes
        * Check node3 only has 2000 keys
        * Check node1 and node2 have 2001 keys
        * Perform the repair type specified by the parent test
        * Assert the appropriate messages are logged
        * Assert node3 now has all data

        @jira_ticket CASSANDRA-4373
        """
        if order_preserving_partitioner:
            self.cluster.set_partitioner('org.apache.cassandra.dht.ByteOrderedPartitioner')

        self._populate_cluster()
        self._repair_and_verify(sequential)

    def _empty_vs_gcable_no_repair(self, sequential):
        """
        Repairing empty partition and tombstoned partition older than gc grace
        should be treated as the same and no repair is necessary.
        @jira_ticket CASSANDRA-8979.
        """
        cluster = self.cluster
        cluster.populate(2)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        cluster.start()
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        # create keyspace with RF=2 to be able to be repaired
        create_ks(session, 'ks', 2)
        # we create two tables, one has low gc grace seconds so that the data
        # can be dropped during test (but we don't actually drop them).
        # the other has default gc.
        # compaction is disabled not to purge data
        query = """
            CREATE TABLE cf1 (
                key text,
                c1 text,
                c2 text,
                PRIMARY KEY (key, c1)
            )
            WITH gc_grace_seconds=1
            AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """
        session.execute(query)
        time.sleep(.5)
        query = """
            CREATE TABLE cf2 (
                key text,
                c1 text,
                c2 text,
                PRIMARY KEY (key, c1)
            )
            WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """
        session.execute(query)
        time.sleep(.5)

        # take down node2, so that only node1 has gc-able data
        node2.stop(wait_other_notice=True)
        for cf in ['cf1', 'cf2']:
            # insert some data
            for i in xrange(0, 10):
                for j in xrange(0, 1000):
                    query = SimpleStatement("INSERT INTO {} (key, c1, c2) VALUES ('k{}', 'v{}', 'value')".format(cf, i, j), consistency_level=ConsistencyLevel.ONE)
                    session.execute(query)
            node1.flush()
            # delete those data, half with row tombstone, and the rest with cell range tombstones
            for i in xrange(0, 5):
                query = SimpleStatement("DELETE FROM {} WHERE key='k{}'".format(cf, i), consistency_level=ConsistencyLevel.ONE)
                session.execute(query)
            node1.flush()
            for i in xrange(5, 10):
                for j in xrange(0, 1000):
                    query = SimpleStatement("DELETE FROM {} WHERE key='k{}' AND c1='v{}'".format(cf, i, j), consistency_level=ConsistencyLevel.ONE)
                    session.execute(query)
            node1.flush()

        # sleep until gc grace seconds pass so that cf1 can be dropped
        time.sleep(2)

        # bring up node2 and repair
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node2.repair(_repair_options(self.cluster.version(), ks='ks', sequential=sequential))

        # check no rows will be returned
        for cf in ['cf1', 'cf2']:
            for i in xrange(0, 10):
                query = SimpleStatement("SELECT c1, c2 FROM {} WHERE key='k{}'".format(cf, i), consistency_level=ConsistencyLevel.ALL)
                res = list(session.execute(query))
                self.assertEqual(len(filter(lambda x: len(x) != 0, res)), 0, res)

        # check log for no repair happened for gcable data
        out_of_sync_logs = node2.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync for cf1")
        self.assertEqual(len(out_of_sync_logs), 0, "GC-able data does not need to be repaired with empty data: " + str([elt[0] for elt in out_of_sync_logs]))
        # check log for actual repair for non gcable data
        out_of_sync_logs = node2.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync for cf2")
        self.assertGreater(len(out_of_sync_logs), 0, "Non GC-able data should be repaired")

    def _range_tombstone_digest(self, sequential):
        """
        multiple range tombstones for same partition and interval must not create a digest mismatch as long
        as the most recent tombstone is present.
        @jira_ticket cassandra-11349.
        """

        def withsession(session, node1):
            session.execute("delete from table1 where c1 = 'a' and c2 = 'b'")
            node1.flush()
            # recreate same tombstone (will be flushed by repair, so we end up with 2x on node1 and 1x on node2)
            session.execute("delete from table1 where c1 = 'a' and c2 = 'b'")

        self._repair_digest(sequential, withsession)

    def _cell_shadowed_by_range_tombstone(self, sequential):
        """
        Cells shadowed by range tombstones must not effect repairs (given tombstones are present on all nodes)
        @jira_ticket CASSANDRA-11349.
        """

        def withSession(session, node1):
            session.execute("INSERT INTO table1 (c1, c2, c3, c4) VALUES ('a', 'b', 'c', 1)")
            node1.flush()
            session.execute("DELETE FROM table1 WHERE c1 = 'a' AND c2 = 'b'")

        self._repair_digest(sequential, withSession)

    def _range_tombstone_shadowed_by_range_tombstone(self, sequential):
        """
        Range tombstones shadowed by other range tombstones must not effect repairs
        @jira_ticket CASSANDRA-11349.
        """

        def withSession(session, node1):
            session.execute("DELETE FROM table1 WHERE c1 = 'a' AND c2 = 'b' AND c3 = 'c'")
            node1.flush()
            session.execute("DELETE FROM table1 WHERE c1 = 'a' AND c2 = 'b'")
            node1.flush()
            session.execute("DELETE FROM table1 WHERE c1 = 'a' AND c2 = 'b' AND c3 = 'd'")
            node1.flush()
            session.execute("DELETE FROM table1 WHERE c1 = 'a' AND c2 = 'b' AND c3 = 'a'")

        self._repair_digest(sequential, withSession)

    def _repair_digest(self, sequential, populate):
        cluster = self.cluster
        cluster.populate(2)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        cluster.start()
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        # create keyspace with RF=2 to be able to be repaired
        create_ks(session, 'ks', 2)
        query = """
            CREATE TABLE IF NOT EXISTS table1 (
                c1 text,
                c2 text,
                c3 text,
                c4 float,
                PRIMARY KEY (c1, c2, c3)
            )
            WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """
        session.execute(query)

        populate(session, node1)

        node2.repair(_repair_options(self.cluster.version(), ks='ks', sequential=sequential))

        # check log for no repair happened for gcable data
        out_of_sync_logs = node2.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync for table1")
        self.assertEqual(len(out_of_sync_logs), 0, "Digest mismatch for range tombstone: {}".format(str([elt[0] for elt in out_of_sync_logs])))

    def local_dc_repair_test(self):
        """
        * Set up a multi DC cluster
        * Perform a -local repair on one DC
        * Assert only nodes in that DC are repaired
        """
        cluster = self._setup_multi_dc()
        node1 = cluster.nodes["node1"]
        node2 = cluster.nodes["node2"]

        debug("starting repair...")
        opts = ["-local"]
        opts += _repair_options(self.cluster.version(), ks="ks")
        node1.repair(opts)

        # Verify that only nodes in dc1 are involved in repair
        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        self.assertEqual(len(out_of_sync_logs), 1, "Lines matching: {}".format(len(out_of_sync_logs)))

        line, m = out_of_sync_logs[0]
        num_out_of_sync_ranges, out_of_sync_nodes = m.group(3), {m.group(1), m.group(2)}

        self.assertEqual(int(num_out_of_sync_ranges), 1, "Expecting 1 range out of sync for {}, but saw {}".format(out_of_sync_nodes, line))
        valid_out_of_sync_pairs = {node1.address(), node2.address()}
        self.assertEqual(out_of_sync_nodes, valid_out_of_sync_pairs, "Unrelated node found in local repair: {}, expected {}".format(out_of_sync_nodes, valid_out_of_sync_pairs))
        # Check node2 now has the key
        self.check_rows_on_node(node2, 2001, found=[1000], restart=False)

    def dc_repair_test(self):
        """
        * Set up a multi DC cluster
        * Perform a -dc repair on two dc's
        * Assert only nodes on those dcs were repaired
        """
        cluster = self._setup_multi_dc()
        node1 = cluster.nodes["node1"]
        node2 = cluster.nodes["node2"]
        node3 = cluster.nodes["node3"]

        debug("starting repair...")
        opts = ["-dc", "dc1", "-dc", "dc2"]
        opts += _repair_options(self.cluster.version(), ks="ks")
        node1.repair(opts)

        # Verify that only nodes in dc1 and dc2 are involved in repair
        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        self.assertEqual(len(out_of_sync_logs), 2, "Lines matching: " + str([elt[0] for elt in out_of_sync_logs]))
        valid_out_of_sync_pairs = [{node1.address(), node2.address()},
                                   {node2.address(), node3.address()}]

        for line, m in out_of_sync_logs:
            num_out_of_sync_ranges, out_of_sync_nodes = m.group(3), {m.group(1), m.group(2)}
            self.assertEqual(int(num_out_of_sync_ranges), 1, "Expecting 1 range out of sync for {}, but saw {}".format(out_of_sync_nodes, line))
            self.assertIn(out_of_sync_nodes, valid_out_of_sync_pairs, str(out_of_sync_nodes))

        # Check node2 now has the key
        self.check_rows_on_node(node2, 2001, found=[1000], restart=False)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11605',
                   flaky=True,
                   notes='flaky on Windows')
    def dc_parallel_repair_test(self):
        """
        * Set up a multi DC cluster
        * Perform a -dc repair on two dc's, with -dcpar
        * Assert only nodes on those dcs were repaired
        """
        cluster = self._setup_multi_dc()
        node1 = cluster.nodes["node1"]
        node2 = cluster.nodes["node2"]
        node3 = cluster.nodes["node3"]

        debug("starting repair...")
        opts = ["-dc", "dc1", "-dc", "dc2", "-dcpar"]
        opts += _repair_options(self.cluster.version(), ks="ks", sequential=False)
        node1.repair(opts)

        # Verify that only nodes in dc1 and dc2 are involved in repair
        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        self.assertEqual(len(out_of_sync_logs), 2, "Lines matching: " + str([elt[0] for elt in out_of_sync_logs]))
        valid_out_of_sync_pairs = [{node1.address(), node2.address()},
                                   {node2.address(), node3.address()}]

        for line, m in out_of_sync_logs:
            num_out_of_sync_ranges, out_of_sync_nodes = m.group(3), {m.group(1), m.group(2)}
            self.assertEqual(int(num_out_of_sync_ranges), 1, "Expecting 1 range out of sync for {}, but saw {}".format(out_of_sync_nodes, line))
            self.assertIn(out_of_sync_nodes, valid_out_of_sync_pairs, str(out_of_sync_nodes))

        # Check node2 now has the key
        self.check_rows_on_node(node2, 2001, found=[1000], restart=False)

        # Check the repair was a dc parallel repair
        if self.cluster.version() >= '2.2':
            self.assertEqual(len(node1.grep_log('parallelism: dc_parallel')), 1, str(node1.grep_log('parallelism')))
        else:
            self.assertEqual(len(node1.grep_log('parallelism=PARALLEL')), 1, str(node1.grep_log('parallelism')))

    def _setup_multi_dc(self):
        """
        Sets up 3 DCs (2 nodes in 'dc1', and one each in 'dc2' and 'dc3').
        After set up, node2 in dc1 lacks some data and needs to be repaired.
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        debug("Starting cluster..")
        # populate 2 nodes in dc1, and one node each in dc2 and dc3
        cluster.populate([2, 1, 1]).start(wait_for_binary_proto=True)

        node1, node2, node3, node4 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1, 'dc3':1}")
        session.execute("USE ks")
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys, kill node 2, insert 1 key, restart node 2, insert 1000 more keys
        debug("Inserting data...")
        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ALL)
        node2.flush()
        node2.stop(wait_other_notice=True)
        insert_c1c2(session, keys=(1000, ), consistency=ConsistencyLevel.THREE)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node1.watch_log_for_alive(node2)
        insert_c1c2(session, keys=range(1001, 2001), consistency=ConsistencyLevel.ALL)

        cluster.flush()

        # Verify that only node2 has only 2000 keys and others have 2001 keys
        debug("Checking data...")
        self.check_rows_on_node(node2, 2000, missings=[1000])
        for node in [node1, node3, node4]:
            self.check_rows_on_node(node, 2001, found=[1000])
        return cluster

    @since('2.2')
    def parallel_table_repair_noleak(self):
        """
        @jira_ticket CASSANDRA-11215

        Tests that multiple parallel repairs on the same table isn't
        causing reference leaks.
        """
        self.ignore_log_patterns = [
            "Cannot start multiple repair sessions over the same sstables",  # The message we are expecting
            "Validation failed in",                                          # Expecting validation to fail
            "RMI Runtime",                                                   # JMX Repair failures
            "Session completed with the following error",                    # The nodetool repair error
            "ValidationExecutor",                                            # Errors by the validation executor
            "RepairJobTask"                                                  # Errors by the repair job task
        ]

        cluster = self.cluster
        debug("Starting cluster..")
        cluster.populate([3]).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()
        node1.stress(stress_options=['write', 'n=10k', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=3)', '-rate', 'threads=50'])

        # Start multiple repairs in parallel
        threads = []
        for i in range(3):
            t = threading.Thread(target=node1.nodetool, args=("repair keyspace1 standard1",))
            threads.append(t)
            t.start()

        # Wait for the repairs to finish
        for t in threads:
            t.join()

        found_message = False
        # All nodes should reject multiple repairs and have no reference leaks
        for node in cluster.nodelist():
            if len(node.grep_log("Cannot start multiple repair sessions over the same sstables")) > 0:
                found_message = True
                break
        self.assertTrue(found_message)

    @no_vnodes()
    def token_range_repair_test(self):
        """
        Test repair using the -st and -et options
        * Launch a three node cluster
        * Insert some data at RF 2
        * Shut down node2, insert more data, restore node2
        * Issue a repair on a range that only belongs to node1
        * Verify that nodes 1 and 2, and only nodes 1+2, are repaired
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        debug("Starting cluster..")
        cluster.populate(3).start(wait_for_binary_proto=True)

        node1, node2, node3 = cluster.nodelist()

        self._parameterized_range_repair(repair_opts=['-st', str(node3.initial_token), '-et', str(node1.initial_token)])

    @no_vnodes()
    def token_range_repair_test_with_cf(self):
        """
        @jira_ticket CASSANDRA-11866

        Test repair using the -st and -et, and -cf options
        * Launch a three node cluster
        * Insert some data at RF 2
        * Shut down node2, insert more data, restore node2
        * Issue a repair on a range that only belongs to node1 on the wrong cf
        * Verify that the data did not get repaired
        * Issue a repair on a range that belongs to the right cf
        * Verify that the data was repaired
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        debug("Starting cluster..")
        cluster.populate(3).start(wait_for_binary_proto=True)

        node1, node2, node3 = cluster.nodelist()

        # Insert data, kill node 2, insert more data, restart node 2, insert another set of data
        debug("Inserting data...")
        node1.stress(['write', 'n=1k', 'no-warmup', 'cl=ALL', '-schema', 'replication(factor=2)', '-rate', 'threads=30'])
        node2.flush()
        node2.stop(wait_other_notice=True)
        node1.stress(['write', 'n=1K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=2)', '-rate', 'threads=30', '-pop', 'seq=20..40K'])
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node1.stress(['write', 'n=1K', 'no-warmup', 'cl=ALL', '-schema', 'replication(factor=2)', '-rate', 'threads=30', '-pop', 'seq=40..60K'])
        cluster.flush()

        # Repair only the range node 1 owns on the wrong CF, assert everything is still broke
        opts = ['-st', str(node3.initial_token), '-et', str(node1.initial_token), ]
        opts += _repair_options(self.cluster.version(), ks='keyspace1', cf='counter1', sequential=False)
        node1.repair(opts)
        self.assertEqual(len(node1.grep_log('are consistent for standard1')), 0, "Nodes 1 and 2 should not be consistent.")
        self.assertEqual(len(node3.grep_log('Repair command')), 0, "Node 3 should not have been involved in the repair.")
        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        self.assertEqual(len(out_of_sync_logs), 0, "We repaired the wrong CF, so things should still be broke")

        # Repair only the range node 1 owns on the right  CF, assert everything is fixed
        opts = ['-st', str(node3.initial_token), '-et', str(node1.initial_token), ]
        opts += _repair_options(self.cluster.version(), ks='keyspace1', cf='standard1', sequential=False)
        node1.repair(opts)
        self.assertEqual(len(node1.grep_log('are consistent for standard1')), 0, "Nodes 1 and 2 should not be consistent.")
        self.assertEqual(len(node3.grep_log('Repair command')), 0, "Node 3 should not have been involved in the repair.")
        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        _, matches = out_of_sync_logs[0]
        out_of_sync_nodes = {matches.group(1), matches.group(2)}
        valid_out_of_sync_pairs = [{node1.address(), node2.address()}]
        self.assertIn(out_of_sync_nodes, valid_out_of_sync_pairs, str(out_of_sync_nodes))

    @no_vnodes()
    def partitioner_range_repair_test(self):
        """
        Test repair using the -pr option
        * Launch a three node cluster
        * Insert some data at RF 2
        * Shut down node2, insert more data, restore node2
        * Issue a repair on a range that only belongs to node1
        * Verify that nodes 1 and 2, and only nodes 1+2, are repaired
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        debug("Starting cluster..")
        cluster.populate(3).start(wait_for_binary_proto=True)

        node1, node2, node3 = cluster.nodelist()

        self._parameterized_range_repair(repair_opts=['-pr'])

    @since('3.10')
    @no_vnodes()
    def pull_repair_test(self):
        """
        Test repair using the --pull option
        @jira_ticket CASSANDRA-9876
        * Launch a three node cluster
        * Insert some data at RF 2
        * Shut down node2, insert more data, restore node2
        * Issue a pull repair on a range that only belongs to node1
        * Verify that nodes 1 and 2, and only nodes 1+2, are repaired
        * Verify that node1 only received data
        * Verify that node2 only sent data
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        debug("Starting cluster..")
        cluster.populate(3).start(wait_for_binary_proto=True)

        node1, node2, node3 = cluster.nodelist()

        node1_address = node1.network_interfaces['binary'][0]
        node2_address = node2.network_interfaces['binary'][0]

        self._parameterized_range_repair(repair_opts=['--pull', '--in-hosts', node1_address + ',' + node2_address, '-st', str(node3.initial_token), '-et', str(node1.initial_token)])

        # Node 1 should only receive files (as we ran a pull repair on node1)
        self.assertTrue(len(node1.grep_log("Receiving [1-9][0-9]* files")) > 0)
        self.assertEqual(len(node1.grep_log("sending [1-9][0-9]* files")), 0)
        self.assertTrue(len(node1.grep_log("sending 0 files")) > 0)

        # Node 2 should only send files (as we ran a pull repair on node1)
        self.assertEqual(len(node2.grep_log("Receiving [1-9][0-9]* files")), 0)
        self.assertTrue(len(node2.grep_log("Receiving 0 files")) > 0)
        self.assertTrue(len(node2.grep_log("sending [1-9][0-9]* files")) > 0)

    def _parameterized_range_repair(self, repair_opts):
        """
        @param repair_opts A list of strings which represent cli args to nodetool repair
        * Launch a three node cluster
        * Insert some data at RF 2
        * Shut down node2, insert more data, restore node2
        * Issue a repair on a range that only belongs to node1, using repair_opts
        * Verify that nodes 1 and 2, and only nodes 1+2, are repaired
        """
        cluster = self.cluster
        node1, node2, node3 = cluster.nodelist()

        # Insert data, kill node 2, insert more data, restart node 2, insert another set of data
        debug("Inserting data...")
        node1.stress(['write', 'n=20K', 'no-warmup', 'cl=ALL', '-schema', 'replication(factor=2)', '-rate', 'threads=30'])

        node2.flush()
        node2.stop(wait_other_notice=True)

        node1.stress(['write', 'n=20K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=2)', '-rate', 'threads=30', '-pop', 'seq=20..40K'])
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        node1.stress(['write', 'n=20K', 'no-warmup', 'cl=ALL', '-schema', 'replication(factor=2)', '-rate', 'threads=30', '-pop', 'seq=40..60K'])

        cluster.flush()

        # Repair only the range node 1 owns
        opts = repair_opts
        opts += _repair_options(self.cluster.version(), ks='keyspace1', cf='standard1', sequential=False)
        node1.repair(opts)

        self.assertEqual(len(node1.grep_log('are consistent for standard1')), 0, "Nodes 1 and 2 should not be consistent.")
        self.assertEqual(len(node3.grep_log('Repair command')), 0, "Node 3 should not have been involved in the repair.")

        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        _, matches = out_of_sync_logs[0]
        out_of_sync_nodes = {matches.group(1), matches.group(2)}

        valid_out_of_sync_pairs = [{node1.address(), node2.address()}]

        self.assertIn(out_of_sync_nodes, valid_out_of_sync_pairs, str(out_of_sync_nodes))

    @since('2.2')
    def trace_repair_test(self):
        """
        * Launch a three node cluster
        * Insert some data at RF 2
        * Shut down node2, insert more data, restore node2
        * Issue a repair on to node1, setting job threads to 2 and with tracing enabled
        * Check the trace data was written, and that the right job thread count was used
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        debug("Starting cluster..")
        cluster.populate(3).start(wait_for_binary_proto=True)

        node1, node2, node3 = cluster.nodelist()

        debug("Inserting data...")
        node1.stress(['write', 'n=20K', 'no-warmup', 'cl=ALL', '-schema', 'replication(factor=2)', '-rate', 'threads=30'])

        node2.flush()
        node2.stop(wait_other_notice=True)

        node1.stress(['write', 'n=20K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=2)', '-rate', 'threads=30', '-pop', 'seq=20..40K'])
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        cluster.flush()

        job_thread_count = '2'
        opts = ['-tr', '-j', job_thread_count]
        opts += _repair_options(self.cluster.version(), ks='keyspace1', cf='standard1', sequential=False)
        node1.repair(opts)

        time.sleep(5)  # Give the trace table some time to populate

        session = self.patient_cql_connection(node1)
        rows = list(session.execute("SELECT activity FROM system_traces.events"))
        # This check assumes that the only (or at least first) thing to write to `system_traces.events.activity` is
        # the repair task triggered in the test.
        self.assertIn('job threads: {}'.format(job_thread_count),
                      rows[0][0],
                      'Expected {} job threads in repair options. Instead we saw {}'.format(job_thread_count, rows[0][0]))

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11836',
                   flaky=True,
                   notes='Windows')
    @since('2.2')
    def thread_count_repair_test(self):
        """
        * Launch a three node cluster
        * Insert some data at RF 2
        * Shut down node2, insert more data, restore node2
        * Issue a repair on to node1, setting job threads
        * Check the right job thread count was used
        * Repeat steps 2 through 5 with all job count options
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.set_batch_commitlog(enabled=True)
        debug("Starting cluster..")
        cluster.populate(3).start(wait_for_binary_proto=True)

        node1, node2, node3 = cluster.nodelist()

        # Valid job thread counts: 1, 2, 3, and 4
        for job_thread_count in range(1, 5):
            debug("Inserting data...")
            node1.stress(['write', 'n=2K', 'no-warmup', 'cl=ALL', '-schema', 'replication(factor=2)', '-rate',
                          'threads=30', '-pop', 'seq={}..{}K'.format(2 * (job_thread_count - 1), 2 * job_thread_count)])

            node2.flush()
            node2.stop(wait_other_notice=True)

            node1.stress(['write', 'n=2K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=2)', '-rate',
                          'threads=30', '-pop', 'seq={}..{}K'.format(2 * (job_thread_count), 2 * (job_thread_count + 1))])
            node2.start(wait_for_binary_proto=True, wait_other_notice=True)

            cluster.flush()
            session = self.patient_cql_connection(node1)
            session.execute("TRUNCATE system_traces.events")

            opts = ['-tr', '-j', str(job_thread_count)]
            opts += _repair_options(self.cluster.version(), ks='keyspace1', cf='standard1', sequential=False)
            node1.repair(opts)

            time.sleep(5)  # Give the trace table some time to populate

            rows = list(session.execute("SELECT activity FROM system_traces.events"))
            # This check assumes that the only (or at least first) thing to write to `system_traces.events.activity` is
            # the repair task triggered in the test.
            self.assertIn('job threads: {}'.format(job_thread_count),
                          rows[0][0],
                          'Expected {} job threads in repair options. Instead we saw {}'.format(job_thread_count, rows[0][0]))

    @no_vnodes()
    def test_multiple_concurrent_repairs(self):
        """
        @jira_ticket CASSANDRA-11451
        Make sure we can run sub range repairs in parallel - and verify that we actually do repair
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()
        node2.stop(wait_other_notice=True)
        node1.stress(['write', 'n=1M', 'no-warmup', '-schema', 'replication(factor=3)', '-rate', 'threads=30'])
        node2.start(wait_for_binary_proto=True)
        t1 = threading.Thread(target=node1.nodetool, args=('repair keyspace1 standard1 -st {} -et {}'.format(str(node3.initial_token), str(node1.initial_token)),))
        t2 = threading.Thread(target=node2.nodetool, args=('repair keyspace1 standard1 -st {} -et {}'.format(str(node1.initial_token), str(node2.initial_token)),))
        t3 = threading.Thread(target=node3.nodetool, args=('repair keyspace1 standard1 -st {} -et {}'.format(str(node2.initial_token), str(node3.initial_token)),))
        t1.start()
        t2.start()
        t3.start()
        t1.join()
        t2.join()
        t3.join()
        node1.stop(wait_other_notice=True)
        node3.stop(wait_other_notice=True)
        _, _, rc = node2.stress(['read', 'n=1M', 'no-warmup', '-rate', 'threads=30'], whitelist=True)
        self.assertEqual(rc, 0)

    def test_dead_coordinator(self):
        """
        @jira_ticket CASSANDRA-11824
        Make sure parent repair session is cleared out if the repair coordinator dies
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()
        node1.stress(['write', 'n=100k', '-schema', 'replication(factor=3)', '-rate', 'threads=30'])
        if cluster.version() >= "2.2":
            t1 = threading.Thread(target=node1.repair)
            t1.start()
            node2.watch_log_for('Validating ValidationRequest', filename='debug.log')
        else:
            t1 = threading.Thread(target=node1.nodetool, args=('repair keyspace1 standard1 -inc -par',))
            t1.start()
            node1.watch_log_for('requesting merkle trees', filename='system.log')
            time.sleep(2)

        debug("stopping node1")
        node1.stop(gently=False, wait_other_notice=True)
        t1.join()
        debug("starting node1 - first repair should have failed")
        node1.start(wait_for_binary_proto=True, wait_other_notice=True)
        debug("running second repair")
        if cluster.version() >= "2.2":
            node1.repair()
        else:
            node1.nodetool('repair keyspace1 standard1 -inc -par')


RepairTableContents = namedtuple('RepairTableContents',
                                 ['parent_repair_history', 'repair_history'])


@since('2.2')
@attr("resource-intensive")
class TestRepairDataSystemTable(Tester):
    """
    @jira_ticket CASSANDRA-5839

    Tests the `system_distributed.parent_repair_history` and
    `system_distributed.repair_history` tables by writing thousands of records
    to a cluster, then ensuring these tables are in valid states before and
    after running repair.
    """

    def setUp(self):
        """
        Prepares a cluster for tests of the repair history tables by starting
        a 5-node cluster, then inserting 5000 values with RF=3.
        """

        Tester.setUp(self)
        self.cluster.populate(5).start(wait_for_binary_proto=True)
        self.node1 = self.cluster.nodelist()[0]
        self.session = self.patient_cql_connection(self.node1)

        self.node1.stress(stress_options=['write', 'n=5K', 'no-warmup', 'cl=ONE', '-schema', 'replication(factor=3)'])

        self.cluster.flush()

    def repair_table_contents(self, node, include_system_keyspaces=True):
        """
        @param node the node to connect to and query
        @param include_system_keyspaces if truthy, return repair information about all keyspaces. If falsey, filter out keyspaces whose name contains 'system'

        Return a `RepairTableContents` `namedtuple` containing the rows in
        `node`'s `system_distributed.parent_repair_history` and
        `system_distributed.repair_history` tables. If `include_system_keyspaces`,
        include all results. If not `include_system_keyspaces`, filter out
        repair information about system keyspaces, or at least keyspaces with
        'system' in their names.
        """
        session = self.patient_cql_connection(node)

        def execute_with_all(stmt):
            return session.execute(SimpleStatement(stmt, consistency_level=ConsistencyLevel.ALL))

        parent_repair_history = execute_with_all('SELECT * FROM system_distributed.parent_repair_history;')
        repair_history = execute_with_all('SELECT * FROM system_distributed.repair_history;')

        if not include_system_keyspaces:
            parent_repair_history = [row for row in parent_repair_history
                                     if 'system' not in row.keyspace_name]
            repair_history = [row for row in repair_history if
                              'system' not in row.keyspace_name]
        return RepairTableContents(parent_repair_history=parent_repair_history,
                                   repair_history=repair_history)

    @skip('hangs CI')
    def initial_empty_repair_tables_test(self):
        debug('repair tables:')
        debug(self.repair_table_contents(node=self.node1, include_system_keyspaces=False))
        repair_tables_dict = self.repair_table_contents(node=self.node1, include_system_keyspaces=False)._asdict()
        for table_name, table_contents in repair_tables_dict.items():
            self.assertFalse(table_contents, '{} is non-empty'.format(table_name))

    def repair_parent_table_test(self):
        """
        Test that `system_distributed.parent_repair_history` is properly populated
        after repair by:

        - running repair on `node` and
        - checking that there are a non-zero number of entries in `parent_repair_history`.
        """
        self.node1.repair()
        parent_repair_history, _ = self.repair_table_contents(node=self.node1, include_system_keyspaces=False)
        self.assertTrue(len(parent_repair_history))

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11298',
                   flaky=True,
                   notes='windows')
    def repair_table_test(self):
        """
        Test that `system_distributed.repair_history` is properly populated
        after repair by:

        - running repair on `node` and
        - checking that there are a non-zero number of entries in `repair_history`.
        """
        self.node1.repair()
        _, repair_history = self.repair_table_contents(node=self.node1, include_system_keyspaces=False)
        self.assertTrue(len(repair_history))
