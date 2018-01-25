import time
import pytest
import logging

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester, create_ks
from tools.assertions import assert_one
from tools.data import rows_to_list
from tools.misc import retry_till_success

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestReadRepair(Tester):

    @pytest.fixture(scope='function', autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        fixture_dtest_setup.cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        fixture_dtest_setup.cluster.populate(3).start(wait_for_binary_proto=True)

    @since('3.0')
    def test_alter_rf_and_run_read_repair(self):
        """
        @jira_ticket CASSANDRA-10655
        @jira_ticket CASSANDRA-10657

        Test that querying only a subset of all the columns in a row doesn't confuse read-repair to avoid
        the problem described in CASSANDRA-10655.
        """

        # session is only used to setup & do schema modification. Actual data queries are done directly on
        # each node, using an exclusive connection and CL.ONE
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        initial_replica, non_replicas = self.do_initial_setup(session)

        # Execute a query at CL.ALL on one of the nodes which was *not* the initial replica. It should trigger a
        # read repair and propagate the data to all 3 nodes.
        # Note: result of the read repair contains only the selected column (a), not all columns
        logger.debug("Executing 'SELECT a...' on non-initial replica to trigger read repair " + non_replicas[0].name)
        read_repair_session = self.patient_exclusive_cql_connection(non_replicas[0])
        assert_one(read_repair_session, "SELECT a FROM alter_rf_test.t1 WHERE k=1", [1], cl=ConsistencyLevel.ALL)

        # The read repair should have repaired the replicas, at least partially (see CASSANDRA-10655)
        # verify by querying each replica in turn.
        self.check_data_on_each_replica(expect_fully_repaired=False, initial_replica=initial_replica)

        # Now query again at CL.ALL but this time selecting all columns, which should ensure that 'b' also gets repaired
        query = "SELECT * FROM alter_rf_test.t1 WHERE k=1"
        logger.debug("Executing 'SELECT *...' on non-initial replica to trigger read repair " + non_replicas[0].name)
        assert_one(read_repair_session, query, [1, 1, 1], cl=ConsistencyLevel.ALL)

        # Check each replica individually again now that we expect the data to be fully repaired
        self.check_data_on_each_replica(expect_fully_repaired=True, initial_replica=initial_replica)

    def test_read_repair_chance(self):
        """
        @jira_ticket CASSANDRA-12368
        """
        # session is only used to setup & do schema modification. Actual data queries are done directly on
        # each node, using an exclusive connection and CL.ONE
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        initial_replica, non_replicas = self.do_initial_setup(session)

        # To ensure read repairs are triggered, set the table property to 100%
        logger.debug("Setting table read repair chance to 1")
        session.execute("""ALTER TABLE alter_rf_test.t1 WITH read_repair_chance = 1;""")

        # Execute a query at CL.ONE on one of the nodes which was *not* the initial replica. It should trigger a
        # read repair because read_repair_chance == 1, and propagate the data to all 3 nodes.
        # Note: result of the read repair contains only the selected column (a), not all columns, so we won't expect
        # 'b' to have been fully repaired afterwards.
        logger.debug("Executing 'SELECT a...' on non-initial replica to trigger read repair " + non_replicas[0].name)
        read_repair_session = self.patient_exclusive_cql_connection(non_replicas[0])
        read_repair_session.execute(SimpleStatement("SELECT a FROM alter_rf_test.t1 WHERE k=1",
                                                    consistency_level=ConsistencyLevel.ONE))

        # Query each replica individually to ensure that read repair was triggered. We should expect that only
        # the initial replica has data for both the 'a' and 'b' columns. The read repair should only have affected
        # the selected column, so the other two replicas should only have that data.
        # Note: we need to temporarily set read_repair_chance to 0 while we perform this check.
        logger.debug("Setting table read repair chance to 0 while we verify each replica's data")
        session.execute("""ALTER TABLE alter_rf_test.t1 WITH read_repair_chance = 0;""")
        # The read repair is run in the background, so we spin while checking that the repair has completed
        retry_till_success(self.check_data_on_each_replica,
                           expect_fully_repaired=False,
                           initial_replica=initial_replica,
                           timeout=30,
                           bypassed_exception=NotRepairedException)

        # Re-enable global read repair and perform another query on a non-replica. This time the query selects all
        # columns so we also expect the value for 'b' to be repaired.
        logger.debug("Setting table read repair chance to 1")
        session.execute("""ALTER TABLE alter_rf_test.t1 WITH read_repair_chance = 1;""")
        logger.debug("Executing 'SELECT *...' on non-initial replica to trigger read repair " + non_replicas[0].name)
        read_repair_session = self.patient_exclusive_cql_connection(non_replicas[0])
        read_repair_session.execute(SimpleStatement("SELECT * FROM alter_rf_test.t1 WHERE k=1",
                                                    consistency_level=ConsistencyLevel.ONE))

        # Query each replica again to ensure that second read repair was triggered. This time, we expect the
        # data to be fully repaired (both 'a' and 'b' columns) by virtue of the query being 'SELECT *...'
        # As before, we turn off read repair before doing this check.
        logger.debug("Setting table read repair chance to 0 while we verify each replica's data")
        session.execute("""ALTER TABLE alter_rf_test.t1 WITH read_repair_chance = 0;""")
        retry_till_success(self.check_data_on_each_replica,
                           expect_fully_repaired=True,
                           initial_replica=initial_replica,
                           timeout=30,
                           bypassed_exception=NotRepairedException)

    def do_initial_setup(self, session):
        """
        Create a keyspace with rf=1 and a table containing a single row with 2 non-primary key columns.
        Insert 1 row, placing the data on a single initial replica. Then, alter the keyspace to rf=3, but don't
        repair. Tests will execute various reads on the replicas and assert the effects of read repair.
        :param session: Used to perform the schema setup & insert the data
        :return: a tuple containing the node which initially acts as the replica, and a list of the other two nodes
        """
        # Disable speculative retry to make it clear that we only query additional nodes because of read_repair_chance
        session.execute("""CREATE KEYSPACE alter_rf_test
                           WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};""")
        session.execute("CREATE TABLE alter_rf_test.t1 (k int PRIMARY KEY, a int, b int) WITH speculative_retry='NONE';")
        session.execute("INSERT INTO alter_rf_test.t1 (k, a, b) VALUES (1, 1, 1);")

        # identify the initial replica and trigger a flush to ensure reads come from sstables
        initial_replica, non_replicas = self.identify_initial_placement()
        logger.debug("At RF=1 replica for data is " + initial_replica.name)
        initial_replica.flush()

        # Just some basic validation.
        # At RF=1, it shouldn't matter which node we query, as the actual data should always come from the
        # initial replica when reading at CL ONE
        for n in self.cluster.nodelist():
            logger.debug("Checking " + n.name)
            session = self.patient_exclusive_cql_connection(n)
            assert_one(session, "SELECT * FROM alter_rf_test.t1 WHERE k=1", [1, 1, 1], cl=ConsistencyLevel.ONE)

        # Alter so RF=n but don't repair, calling tests will execute queries to exercise read repair,
        # either at CL.ALL or after setting read_repair_chance to 100%.
        logger.debug("Changing RF from 1 to 3")
        session.execute("""ALTER KEYSPACE alter_rf_test
                           WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};""")

        return initial_replica, non_replicas

    def identify_initial_placement(self):
        """
        Identify which node in the 3 node cluster contains the specific key at the point that the test keyspace has
        rf=1.
        :return: tuple containing the initial replica, plus a list of the other 2 replicas.
        """
        nodes = self.cluster.nodelist()
        out, _, _ = nodes[0].nodetool("getendpoints alter_rf_test t1 1")
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

    def check_data_on_each_replica(self, expect_fully_repaired, initial_replica):
        """
        Perform a SELECT * query at CL.ONE on each replica in turn. If expect_fully_repaired is True, we verify that
        each replica returns the full row being queried. If not, then we only verify that the 'a' column has been
        repaired.
        """
        stmt = SimpleStatement("SELECT * FROM alter_rf_test.t1 WHERE k=1", consistency_level=ConsistencyLevel.ONE)
        logger.debug("Checking all if read repair has completed on all replicas")
        for n in self.cluster.nodelist():
            logger.debug("Checking {n}, {x}expecting all columns"
                         .format(n=n.name, x="" if expect_fully_repaired or n == initial_replica else "not "))
            session = self.patient_exclusive_cql_connection(n)
            res = rows_to_list(session.execute(stmt))
            logger.debug("Actual result: " + str(res))
            expected = [[1, 1, 1]] if expect_fully_repaired or n == initial_replica else [[1, 1, None]]
            if res != expected:
                raise NotRepairedException()


    @since('2.0')
    def test_range_slice_query_with_tombstones(self):
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

    @since('3.0')
    def test_gcable_tombstone_resurrection_on_range_slice_query(self):
        """
        @jira_ticket CASSANDRA-11427

        Range queries before the 11427 will trigger read repairs for puregable tombstones on hosts that already compacted given tombstones.
        This will result in constant transfer and compaction actions sourced by few nodes seeding purgeable tombstones and triggered e.g.
        by periodical jobs scanning data range wise.
        """

        node1, node2, _ = self.cluster.nodelist()

        session1 = self.patient_cql_connection(node1)
        create_ks(session1, 'gcts', 3)
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
            assert "Sending READ_REPAIR message" not in activity

    def pprint_trace(self, trace):
        """Pretty print a trace"""
        if logging.root.level == logging.DEBUG:
            print(("-" * 40))
            for t in trace.events:
                print(("%s\t%s\t%s\t%s" % (t.source, t.source_elapsed, t.description, t.thread_name)))
            print(("-" * 40))


class NotRepairedException(Exception):
    """
    Thrown to indicate that the data on a replica hasn't been doesn't match what we'd expect if a
    specific read repair has run. See check_data_on_each_replica.
    """
    pass

