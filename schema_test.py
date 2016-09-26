import time

from cassandra.concurrent import execute_concurrent_with_args

from tools.assertions import assert_invalid, assert_all, assert_one
from dtest import Tester, create_ks


class TestSchema(Tester):

    def table_alteration_test(self):
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
                self.assertEqual(row.c1, 'bbb')
                self.assertIsNone(row.c2)
                self.assertFalse(hasattr(row, 'c0'))
            else:
                self.assertEqual(row.c1, 'ccc')
                self.assertEqual(row.c2, 'ddd')
                self.assertFalse(hasattr(row, 'c0'))

    def drop_column_compact_test(self):
        session = self.prepare()

        session.execute("USE ks")
        session.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int) WITH COMPACT STORAGE")

        assert_invalid(session, "ALTER TABLE cf DROP c1", "Cannot drop columns from a")

    def drop_column_compaction_test(self):
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

    def drop_column_queries_test(self):
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

    def drop_column_and_restart_test(self):
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

    def prepare(self):
        cluster = self.cluster
        cluster.populate(1).start()
        time.sleep(.5)
        nodes = cluster.nodelist()
        session = self.patient_cql_connection(nodes[0])
        create_ks(session, 'ks', 1)
        return session
