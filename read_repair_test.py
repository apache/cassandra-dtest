import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import PRINT_DEBUG, Tester, debug
from tools import known_failure, rows_to_list, since


class TestReadRepair(Tester):

    def setUp(self):
        Tester.setUp(self)
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        self.cluster.populate(3).start(wait_for_binary_proto=True)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11266',
                   flaky=False,
                   notes='windows')
    @since('3.0')
    def alter_rf_and_run_read_repair_test(self):
        """
        @jira_ticket CASSANDRA-10655
        @jira_ticket CASSANDRA-10657

        Test that querying only a subset of all the columns in a row doesn't confuse read-repair to avoid
        the problem described in CASSANDRA-10655.
        """

        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        session.execute("""CREATE KEYSPACE alter_rf_test
                           WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};""")
        session.execute("CREATE TABLE alter_rf_test.t1 (k int PRIMARY KEY, a int, b int);")
        session.execute("INSERT INTO alter_rf_test.t1 (k, a, b) VALUES (1, 1, 1);")
        cl_one_stmt = SimpleStatement("SELECT * FROM alter_rf_test.t1 WHERE k=1",
                                      consistency_level=ConsistencyLevel.ONE)

        # identify the initial replica and trigger a flush to ensure reads come from sstables
        initial_replica, non_replicas = self.identify_initial_placement('alter_rf_test', 't1', 1)
        debug("At RF=1 replica for data is " + initial_replica.name)
        initial_replica.flush()

        # At RF=1, it shouldn't matter which node we query, as the actual data should always come from the
        # initial replica when reading at CL ONE
        for n in self.cluster.nodelist():
            debug("Checking " + n.name)
            session = self.patient_exclusive_cql_connection(n)
            res = rows_to_list(session.execute(cl_one_stmt))
            assert res == [[1, 1, 1]], res

        # Alter so RF=n but don't repair, then execute a query which selects only a subset of the columns. Run this at
        # CL ALL on one of the nodes which doesn't currently have the data, triggering a read repair.
        # The expectation will be that every replicas will have been repaired for that column (but we make no assumptions
        # on the other columns).
        debug("Changing RF from 1 to 3")
        session.execute("""ALTER KEYSPACE alter_rf_test
                           WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};""")
        cl_all_stmt = SimpleStatement("SELECT a FROM alter_rf_test.t1 WHERE k=1",
                                      consistency_level=ConsistencyLevel.ALL)
        debug("Executing SELECT on non-initial replica to trigger read repair " + non_replicas[0].name)
        read_repair_session = self.patient_exclusive_cql_connection(non_replicas[0])
        res = read_repair_session.execute(cl_all_stmt)
        # result of the CL ALL query contains only the selected column
        assert rows_to_list(res) == [[1]], res

        # Check the results of the read repair by querying each replica again at CL ONE
        debug("Re-running SELECTs at CL ONE to verify read repair")
        for n in self.cluster.nodelist():
            debug("Checking " + n.name)
            session = self.patient_exclusive_cql_connection(n)
            res = rows_to_list(session.execute(cl_one_stmt))
            assert len(res) == 1
            # Column a must be at 1 everywhere
            assert res[0][1] == 1, res
            # Column b must be either 1 or None everywhere
            assert res[0][2] == 1 or res[0][2] is None, res

        # Now query at ALL but selecting all columns
        cl_all_stmt = SimpleStatement("SELECT * FROM alter_rf_test.t1 WHERE k=1",
                                      consistency_level=ConsistencyLevel.ALL)
        debug("Executing SELECT on non-initial replica to trigger read repair " + non_replicas[0].name)
        read_repair_session = self.patient_exclusive_cql_connection(non_replicas[0])
        res = read_repair_session.execute(cl_all_stmt)
        assert rows_to_list(res) == [[1, 1, 1]], res

        # Check all replica is fully up to date
        debug("Re-running SELECTs at CL ONE to verify read repair")
        for n in self.cluster.nodelist():
            debug("Checking " + n.name)
            session = self.patient_exclusive_cql_connection(n)
            res = rows_to_list(session.execute(cl_one_stmt))
            assert res == [[1, 1, 1]], res

    def identify_initial_placement(self, keyspace, table, key):
        nodes = self.cluster.nodelist()
        out, err = nodes[0].nodetool("getendpoints alter_rf_test t1 1")
        address = out.split('\n')[-2]
        initial_replica = None
        non_replicas = []
        for node in nodes:
            if node.address() == address:
                initial_replica = node
            else:
                non_replicas.append(node)

        assert initial_replica is not None, "Couldn't identify initial replica"

        return initial_replica, non_replicas

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11252',
                   flaky=True,
                   notes='windows')
    @since('2.0')
    def range_slice_query_with_tombstones_test(self):
        """
        @jira_ticket CASSANDRA-8989
        @jira_ticket CASSANDRA-9502

        Range-slice queries with CL>ONE do unnecessary read-repairs.
        Reading from table which contains collection type using token function and with CL > ONE causes overwhelming writes to replicas.


        It's possible to check the behavior with tracing - pattern matching in system_traces.events.activity
        """
        node1 = self.cluster.nodelist()[0]
        session1 = self.patient_exclusive_cql_connection(node1)

        session1.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2}")
        session1.execute("""
            CREATE TABLE ks.cf (
                key    int primary key,
                value  double,
                txt    text
            );
        """)

        for n in range(1, 2500):
            str = "foo bar %d iuhiu iuhiu ihi" % n
            session1.execute("INSERT INTO ks.cf (key, value, txt) VALUES (%d, %d, '%s')" % (n, n, str))

        self.cluster.flush()
        self.cluster.stop()
        self.cluster.start(wait_for_binary_proto=True)
        session1 = self.patient_exclusive_cql_connection(node1)

        for n in range(1, 1000):
            session1.execute("DELETE FROM ks.cf WHERE key = %d" % (n))

        time.sleep(1)

        node1.flush()

        time.sleep(1)

        query = SimpleStatement("SELECT * FROM ks.cf LIMIT 100", consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        future = session1.execute_async(query, trace=True)
        future.result()
        trace = future.get_query_trace(max_wait=120)
        self.pprint_trace(trace)
        for trace_event in trace.events:
            # Step 1, find coordinator node:
            activity = trace_event.description
            assert "Appending to commitlog" not in activity
            assert "Adding to cf memtable" not in activity
            assert "Acquiring switchLock read lock" not in activity

    @since('2.2')
    def test_gcable_tombstone_resurrection_on_range_slice_query(self):
        """
        @jira_ticket CASSANDRA-11427

        Range queries before the 11427 will trigger read repairs for puregable tombstones on hosts that already compacted given tombstones.
        This will result in constant transfer and compaction actions sourced by few nodes seeding purgeable tombstones and triggered e.g.
        by periodical jobs scanning data range wise.
        """

        node1, node2, _ = self.cluster.nodelist()

        session1 = self.patient_cql_connection(node1)
        self.create_ks(session1, 'gcts', 3)
        query = """
            CREATE TABLE gcts.cf1 (
                key text,
                c1 text,
                PRIMARY KEY (key, c1)
            )
            WITH gc_grace_seconds=0
            AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'};
        """
        session1.execute(query)

        # create row tombstone
        delete_stmt = SimpleStatement("DELETE FROM gcts.cf1 WHERE key = 'a'", consistency_level=ConsistencyLevel.ALL)
        session1.execute(delete_stmt)

        # flush single sstable with tombstone
        node1.flush()
        node2.flush()

        # purge tombstones from node2 (gc grace 0)
        node2.compact()

        # execute range slice query, which should not trigger read-repair for purged TS
        future = session1.execute_async(SimpleStatement("SELECT * FROM gcts.cf1", consistency_level=ConsistencyLevel.ALL), trace=True)
        future.result()
        trace = future.get_query_trace(max_wait=120)
        self.pprint_trace(trace)
        for trace_event in trace.events:
            activity = trace_event.description
            self.assertNotIn("Sending READ_REPAIR message", activity)

    def pprint_trace(self, trace):
        """Pretty print a trace"""
        if PRINT_DEBUG:
            print("-" * 40)
            for t in trace.events:
                print("%s\t%s\t%s\t%s" % (t.source, t.source_elapsed, t.description, t.thread_name))
            print("-" * 40)
