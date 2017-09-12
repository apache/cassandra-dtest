import time
import pytest
import logging

from cassandra.concurrent import execute_concurrent_with_args

from tools.assertions import assert_invalid, assert_all, assert_one, assert_none, assert_some
from dtest import Tester, create_ks, create_cf

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestSchema(Tester):

    def test_table_alteration(self):
        """
        Tests that table alters return as expected with many sstables at different schema points
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute("use ks;")
        session.execute("create table tbl_o_churn (id int primary key, c0 text, c1 text) "
                        "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': 1024, 'max_threshold': 1024 };")

        stmt1 = session.prepare("insert into tbl_o_churn (id, c0, c1) values (?, ?, ?)")
        rows_to_insert = 50

        for n in range(5):
            parameters = [(x, 'aaa', 'bbb') for x in range(n * rows_to_insert, (n * rows_to_insert) + rows_to_insert)]
            execute_concurrent_with_args(session, stmt1, parameters, concurrency=rows_to_insert)
            node1.flush()

        session.execute("alter table tbl_o_churn add c2 text")
        session.execute("alter table tbl_o_churn drop c0")
        stmt2 = session.prepare("insert into tbl_o_churn (id, c1, c2) values (?, ?, ?);")

        for n in range(5, 10):
            parameters = [(x, 'ccc', 'ddd') for x in range(n * rows_to_insert, (n * rows_to_insert) + rows_to_insert)]
            execute_concurrent_with_args(session, stmt2, parameters, concurrency=rows_to_insert)
            node1.flush()

        rows = session.execute("select * from tbl_o_churn")
        for row in rows:
            if row.id < rows_to_insert * 5:
                assert row.c1 == 'bbb'
                assert row.c2 is None
                assert not hasattr(row, 'c0')
            else:
                assert row.c1 == 'ccc'
                assert row.c2 == 'ddd'
                assert not hasattr(row, 'c0')

    @since("2.0", max_version="3.X")  # Compact Storage
    def test_drop_column_compact(self):
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int) WITH COMPACT STORAGE")

        assert_invalid(session, "ALTER TABLE cf DROP c1", "Cannot drop columns from a")

    def test_drop_column_compaction(self):
        session = self.prepare()
        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")

        # insert some data.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        session.execute("ALTER TABLE cf DROP c1")
        session.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        node = self.cluster.nodelist()[0]
        node.flush()
        node.compact()

        # test that c1 values have been compacted away.
        session = self.patient_cql_connection(node)
        assert_all(session, "SELECT c1 FROM ks.cf", [[None], [None], [None], [4]], ignore_order=True)

    def test_drop_column_queries(self):
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")
        session.execute("CREATE INDEX ON cf(c2)")

        # insert some data.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        session.execute("ALTER TABLE cf DROP c1")
        session.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        session.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        # test that old (pre-drop) c1 values aren't returned and new ones are.
        assert_all(session, "SELECT c1 FROM cf", [[None], [None], [None], [4]], ignore_order=True)

        assert_all(session, "SELECT * FROM cf", [[0, None, 2], [1, None, 3], [2, None, 4], [3, 4, 5]], ignore_order=True)

        assert_one(session, "SELECT c1 FROM cf WHERE key = 0", [None])

        assert_one(session, "SELECT c1 FROM cf WHERE key = 3", [4])

        assert_one(session, "SELECT * FROM cf WHERE c2 = 2", [0, None, 2])

        assert_one(session, "SELECT * FROM cf WHERE c2 = 5", [3, 4, 5])

    def test_drop_column_and_restart(self):
        """
        Simply insert data in a table, drop a column involved in the insert and restart the node afterwards.
        This ensures that the dropped_columns system table is properly flushed on the alter or the restart
        fails as in CASSANDRA-11050.

        @jira_ticket CASSANDRA-11050
        """
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE t (k int PRIMARY KEY, c1 int, c2 int)")

        session.execute("INSERT INTO t (k, c1, c2) VALUES (0, 0, 0)")
        session.execute("ALTER TABLE t DROP c2")

        assert_one(session, "SELECT * FROM t", [0, 0])

        self.cluster.stop()
        self.cluster.start()

        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        session.execute("USE ks")
        assert_one(session, "SELECT * FROM t", [0, 0])

    def test_drop_static_column_and_restart(self):
        """
        Dropping a static column caused an sstable corrupt exception after restarting, here
        we test that we can drop a static column and restart safely.

        @jira_ticket CASSANDRA-12582
        """
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE ts (id1 int, id2 int, id3 int static, val text, PRIMARY KEY (id1, id2))")

        session.execute("INSERT INTO ts (id1, id2, id3, val) VALUES (1, 1, 0, 'v1')")
        session.execute("INSERT INTO ts (id1, id2, id3, val) VALUES (1, 2, 0, 'v2')")
        session.execute("INSERT INTO ts (id1, id2, id3, val) VALUES (2, 1, 1, 'v3')")

        self.cluster.nodelist()[0].nodetool('flush ks ts')
        assert_all(session, "SELECT * FROM ts", [[1, 1, 0, 'v1'], [1, 2, 0, 'v2'], [2, 1, 1, 'v3']])

        session.execute("alter table ts drop id3")
        assert_all(session, "SELECT * FROM ts", [[1, 1, 'v1'], [1, 2, 'v2'], [2, 1, 'v3']])

        self.cluster.stop()
        self.cluster.start()

        session = self.patient_cql_connection(self.cluster.nodelist()[0])

        session.execute("USE ks")
        assert_all(session, "SELECT * FROM ts", [[1, 1, 'v1'], [1, 2, 'v2'], [2, 1, 'v3']])

    @since('3.0')
    def drop_table_reflected_in_size_estimates_test(self):
        """
        A dropped table should result in its entries being removed from size estimates, on both
        nodes that are up and down at the time of the drop.

        @jira_ticket CASSANDRA-14905
        """
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks1', 2)
        create_ks(session, 'ks2', 2)
        create_cf(session, 'ks1.cf1', columns={'c1': 'text', 'c2': 'text'})
        create_cf(session, 'ks2.cf1', columns={'c1': 'text', 'c2': 'text'})
        create_cf(session, 'ks2.cf2', columns={'c1': 'text', 'c2': 'text'})

        node1.nodetool('refreshsizeestimates')
        node2.nodetool('refreshsizeestimates')
        node2.stop()
        session.execute('DROP TABLE ks2.cf1')
        session.execute('DROP KEYSPACE ks1')
        node2.start(wait_for_binary_proto=True)
        session2 = self.patient_exclusive_cql_connection(node2)

        session.cluster.control_connection.wait_for_schema_agreement()

        assert_none(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks1'")
        assert_none(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks2' AND table_name='cf1'")
        assert_some(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks2' AND table_name='cf2'")
        assert_none(session2, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks1'")
        assert_none(session2, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks2' AND table_name='cf1'")
        assert_some(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='ks2' AND table_name='cf2'")

    @since('3.0')
    def invalid_entries_removed_from_size_estimates_on_restart_test(self):
        """
        Entries for dropped tables/keyspaces should be cleared from size_estimates on restart.

        @jira_ticket CASSANDRA-14905
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        session.execute("USE system;")
        session.execute("INSERT INTO size_estimates (keyspace_name, table_name, range_start, range_end, mean_partition_size, partitions_count) VALUES ( 'system_auth', 'bad_table', '-5', '5', 0, 0);")
        # Invalid keyspace and table
        session.execute("INSERT INTO size_estimates (keyspace_name, table_name, range_start, range_end, mean_partition_size, partitions_count) VALUES ( 'bad_keyspace', 'bad_table', '-5', '5', 0, 0);")
        node.stop()
        node.start()
        session = self.patient_cql_connection(node)
        assert_none(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='system_auth' AND table_name='bad_table'")
        assert_none(session, "SELECT * FROM system.size_estimates WHERE keyspace_name='bad_keyspace'")


    def prepare(self):
        cluster = self.cluster
        cluster.populate(1).start()
        time.sleep(.5)
        nodes = cluster.nodelist()
        session = self.patient_cql_connection(nodes[0])
        create_ks(session, 'ks', 1)
        return session
