import time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from dtest import Tester, PRINT_DEBUG
from tools import since


@since('2.0')
class TestRepairDataSystemTable(Tester):
    """
    @jira_ticket CASSANDRA-8989
    @jira_ticket CASSANDRA-9502

    Range-slice queries with CL>ONE do unnecessary read-repairs.
    Reading from table which contains collection type using token function and with CL > ONE causes overwhelming writes to replicas.


    It's possible to check the behavior with tracing - pattern matching in system_traces.events.activity
    """
    def setUp(self):
        Tester.setUp(self)
        self.cluster.populate(3).start(wait_for_binary_proto=True)
        self.node1 = self.cluster.nodelist()[0]
        self.node2 = self.cluster.nodelist()[1]
        self.node3 = self.cluster.nodelist()[2]
        self.session = self.patient_cql_connection(self.node1)

        session = self.patient_exclusive_cql_connection(self.node1)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2}")
        session.execute("""
            CREATE TABLE ks.cf (
                key    int primary key,
                value  double,
                txt    text
            );
        """)

    def range_slice_query_with_tombstones_test(self):
        session1 = self.patient_exclusive_cql_connection(self.node1)

        for n in range(1, 2500):
            str = "foo bar %d iuhiu iuhiu ihi" % n
            session1.execute("INSERT INTO ks.cf (key, value, txt) VALUES (%d, %d, '%s')" % (n, n, str))

        self.cluster.stop()
        self.cluster.start(wait_for_binary_proto=True)
        session1 = self.patient_exclusive_cql_connection(self.node1)

        for n in range(1, 1000):
            session1.execute("DELETE FROM ks.cf WHERE key = %d" % (n))

        time.sleep(1)

        self.node1.flush()

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

    def pprint_trace(self, trace):
        """Pretty print a trace"""
        if PRINT_DEBUG:
            print("-" * 40)
            for t in trace.events:
                print("%s\t%s\t%s\t%s" % (t.source, t.source_elapsed, t.description, t.thread_name))
            print("-" * 40)
