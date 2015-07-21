import time
from collections import namedtuple

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester, debug
from tools import insert_c1c2, no_vnodes, query_c1c2, require, since


class TestRepair(Tester):

    def check_rows_on_node(self, node_to_check, rows, found=None, missings=None, restart=True):
        if found is None:
            found = []
        if missings is None:
            missings = []
        stopped_nodes = []

        for node in self.cluster.nodes.values():
            if node.is_running() and node is not node_to_check:
                stopped_nodes.append(node)
                node.stop(wait_other_notice=True)

        session = self.patient_cql_connection(node_to_check, 'ks')
        result = session.execute("SELECT * FROM cf LIMIT %d" % (rows * 2))
        self.assertEqual(len(result), rows, len(result))

        for k in found:
            query_c1c2(session, k, ConsistencyLevel.ONE)

        for k in missings:
            query = SimpleStatement("SELECT c1, c2 FROM cf WHERE key='k%d'" % k, consistency_level=ConsistencyLevel.ONE)
            res = session.execute(query)
            self.assertEqual(len(filter(lambda x: len(x) != 0, res)), 0, res)

        if restart:
            for node in stopped_nodes:
                node.start(wait_other_notice=True)

    def simple_sequential_repair_test(self, ):
        self._simple_repair(sequential=True)

    def simple_parallel_repair_test(self, ):
        self._simple_repair(sequential=False)

    def empty_vs_gcable_sequential_repair_test(self):
        self._empty_vs_gcable_no_repair(sequential=True)

    def empty_vs_gcable_parallel_repair_test(self):
        self._empty_vs_gcable_no_repair(sequential=False)

    @no_vnodes()  # https://issues.apache.org/jira/browse/CASSANDRA-5220
    def simple_repair_order_preserving_test(self, ):
        self._simple_repair(order_preserving_partitioner=True)

    def _repair_options(self, ks='', cf=None, sequential=True):
        if cf is None:
            cf = []
        opts = []
        version = self.cluster.version()
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
            opts += cf
        return opts

    def _simple_repair(self, order_preserving_partitioner=False, sequential=True):
        cluster = self.cluster

        if order_preserving_partitioner:
            cluster.set_partitioner('org.apache.cassandra.dht.ByteOrderedPartitioner')

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        debug("Starting cluster..")
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
        debug("Inserting data...")
        for i in xrange(0, 1000):
            insert_c1c2(session, i, ConsistencyLevel.ALL)
        node3.flush()
        node3.stop()
        insert_c1c2(session, 1000, ConsistencyLevel.TWO)
        node3.start(wait_other_notice=True)
        for i in xrange(1001, 2001):
            insert_c1c2(session, i, ConsistencyLevel.ALL)

        cluster.flush()

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
        node1.repair(self._repair_options(ks='ks', sequential=sequential))
        debug("Repair time: {end}".format(end=time.time() - start))

        # Validate that only one range was transfered
        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        if cluster.version() > "1":
            self.assertEqual(len(out_of_sync_logs), 2, "Lines matching: " + str([elt[0] for elt in out_of_sync_logs]))
        else:
            # In pre-1.0, we should have only one line
            self.assertEqual(len(out_of_sync_logs), 1, "Lines matching: " + str([elt[0] for elt in out_of_sync_logs]))
        valid = [(node1.address(), node3.address()), (node3.address(), node1.address()),
                 (node2.address(), node3.address()), (node3.address(), node2.address())]
        for line, m in out_of_sync_logs:
            self.assertEqual(int(m.group(3)), 1, "Expecting 1 range out of sync, got " + m.group(3))
            self.assertIn((m.group(1), m.group(2)), valid, str((m.group(1), m.group(2))))
            valid.remove((m.group(1), m.group(2)))
            valid.remove((m.group(2), m.group(1)))

        # Check node3 now has the key
        self.check_rows_on_node(node3, 2001, found=[1000], restart=False)

    def _empty_vs_gcable_no_repair(self, sequential):
        """
        Repairing empty partition and tombstoned partition older than gc grace
        should be treated as the same and no repair is necessary.
        See CASSANDRA-8979.
        """
        cluster = self.cluster
        cluster.populate(2)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        cluster.start()
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        # create keyspace with RF=2 to be able to be repaired
        self.create_ks(session, 'ks', 2)
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
                    query = SimpleStatement("INSERT INTO %s (key, c1, c2) VALUES ('k%d', 'v%d', 'value')" % (cf, i, j), consistency_level=ConsistencyLevel.ONE)
                    session.execute(query)
            node1.flush()
            # delete those data, half with row tombstone, and the rest with cell range tombstones
            for i in xrange(0, 5):
                query = SimpleStatement("DELETE FROM %s WHERE key='k%d'" % (cf, i), consistency_level=ConsistencyLevel.ONE)
                session.execute(query)
            node1.flush()
            for i in xrange(5, 10):
                for j in xrange(0, 1000):
                    query = SimpleStatement("DELETE FROM %s WHERE key='k%d' AND c1='v%d'" % (cf, i, j), consistency_level=ConsistencyLevel.ONE)
                    session.execute(query)
            node1.flush()

        # sleep until gc grace seconds pass so that cf1 can be dropped
        time.sleep(2)

        # bring up node2 and repair
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node2.repair(self._repair_options(ks='ks', sequential=sequential))

        # check no rows will be returned
        for cf in ['cf1', 'cf2']:
            for i in xrange(0, 10):
                query = SimpleStatement("SELECT c1, c2 FROM %s WHERE key='k%d'" % (cf, i), consistency_level=ConsistencyLevel.ALL)
                res = session.execute(query)
                self.assertEqual(len(filter(lambda x: len(x) != 0, res)), 0, res)

        # check log for no repair happened for gcable data
        out_of_sync_logs = node2.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync for cf1")
        self.assertEqual(len(out_of_sync_logs), 0, "GC-able data does not need to be repaired with empty data: " + str([elt[0] for elt in out_of_sync_logs]))
        # check log for actual repair for non gcable data
        out_of_sync_logs = node2.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync for cf2")
        self.assertGreater(len(out_of_sync_logs), 0, "Non GC-able data should be repaired")

    def local_dc_repair_test(self):
        cluster = self._setup_multi_dc()
        node1 = cluster.nodes["node1"]
        node2 = cluster.nodes["node2"]

        debug("starting repair...")
        opts = ["-local"]
        opts += self._repair_options(ks="ks")
        node1.repair(opts)

        # Verify that only nodes in dc1 are involved in repair
        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        self.assertEqual(len(out_of_sync_logs), 1, "Lines matching: %d" % len(out_of_sync_logs))
        line, m = out_of_sync_logs[0]
        self.assertEqual(int(m.group(3)), 1, "Expecting 1 range out of sync, got " + m.group(3))
        valid = [node1.address(), node2.address()]
        self.assertIn(m.group(1), valid, "Unrelated node found in local repair: " + m.group(1))
        valid.remove(m.group(1))
        self.assertIn(m.group(2), valid, "Unrelated node found in local repair: " + m.group(2))
        # Check node2 now has the key
        self.check_rows_on_node(node2, 2001, found=[1000], restart=False)

    def dc_repair_test(self):
        cluster = self._setup_multi_dc()
        node1 = cluster.nodes["node1"]
        node2 = cluster.nodes["node2"]
        node3 = cluster.nodes["node3"]

        debug("starting repair...")
        opts = ["-dc", "dc1", "-dc", "dc2"]
        opts += self._repair_options(ks="ks")
        node1.repair(opts)

        # Verify that only nodes in dc1 and dc2 are involved in repair
        out_of_sync_logs = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        self.assertEqual(len(out_of_sync_logs),  2, "Lines matching: " + str([elt[0] for elt in out_of_sync_logs]))
        valid = [(node1.address(), node2.address()), (node2.address(), node1.address()),
                 (node2.address(), node3.address()), (node3.address(), node2.address())]
        for line, m in out_of_sync_logs:
            self.assertEqual(int(m.group(3)), 1, "Expecting 1 range out of sync, got " + m.group(3))
            self.assertIn((m.group(1), m.group(2)), valid, str((m.group(1), m.group(2))))
            valid.remove((m.group(1), m.group(2)))
            valid.remove((m.group(2), m.group(1)))
        # Check node2 now has the key
        self.check_rows_on_node(node2, 2001, found=[1000], restart=False)

    def _setup_multi_dc(self):
        """
        Sets up 3 DCs (2 nodes in 'dc1', and one each in 'dc2' and 'dc3').
        After set up, node2 in dc1 lacks some data and needs to be repaired.
        """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False}, batch_commitlog=True)
        debug("Starting cluster..")
        # populate 2 nodes in dc1, and one node each in dc2 and dc3
        cluster.populate([2, 1, 1]).start()
        version = cluster.version()

        [node1, node2, node3, node4] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1, 'dc3':1};")
        session.execute("USE ks")
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
        debug("Inserting data...")
        for i in xrange(0, 1000):
            insert_c1c2(session, i, ConsistencyLevel.ALL)
        node2.flush()
        node2.stop()
        insert_c1c2(session, 1000, ConsistencyLevel.THREE)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        node1.watch_log_for_alive(node2)
        for i in xrange(1001, 2001):
            insert_c1c2(session, i, ConsistencyLevel.ALL)

        cluster.flush()

        # Verify that only node2 has only 2000 keys and others have 2001 keys
        debug("Checking data...")
        self.check_rows_on_node(node2, 2000, missings=[1000])
        for node in [node1, node3, node4]:
            self.check_rows_on_node(node, 2001, found=[1000])
        return cluster

RepairTableContents = namedtuple('RepairTableContents',
                                 ['parent_repair_history', 'repair_history'])


@since('2.2')
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

        self.node1.stress(stress_options=['write', 'n=5000', 'cl=ONE', '-schema', 'replication(factor=3)'])

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

    def initial_empty_repair_tables_test(self):
        debug('repair tables:')
        debug(self.repair_table_contents(node=self.node1, include_system_keyspaces=False))
        repair_tables_dict = self.repair_table_contents(node=self.node1, include_system_keyspaces=False)._asdict()
        for table_name, table_contents in repair_tables_dict.items():
            self.assertFalse(table_contents, '{} is non-empty'.format(table_name))

    @require(9534)
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

    @require(9534)
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
