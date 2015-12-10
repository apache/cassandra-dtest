import time

from assertions import assert_invalid
from cassandra.concurrent import execute_concurrent_with_args
from dtest import Tester
from tools import rows_to_list


class TestSchema(Tester):

    def table_alteration_test(self):
        """
        Tests that table alters return as expected with many sstables at different schema points
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
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
        rows = session.execute("SELECT c1 FROM ks.cf")
        self.assertEqual([[None], [None], [None], [4]], sorted(rows_to_list(rows)))

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
        rows = session.execute("SELECT c1 FROM cf")
        self.assertEqual([[None], [None], [None], [4]], sorted(rows_to_list(rows)))

        rows = session.execute("SELECT * FROM cf")
        self.assertEqual([[0, None, 2], [1, None, 3], [2, None, 4], [3, 4, 5]], sorted(rows_to_list(rows)))

        rows = session.execute("SELECT c1 FROM cf WHERE key = 0")
        self.assertEqual([[None]], rows_to_list(rows))

        rows = session.execute("SELECT c1 FROM cf WHERE key = 3")
        self.assertEqual([[4]], rows_to_list(rows))

        rows = session.execute("SELECT * FROM cf WHERE c2 = 2")
        self.assertEqual([[0, None, 2]], rows_to_list(rows))

        rows = session.execute("SELECT * FROM cf WHERE c2 = 5")
        self.assertEqual([[3, 4, 5]], rows_to_list(rows))

    def prepare(self):
        cluster = self.cluster
        cluster.populate(1).start()
        time.sleep(.5)
        nodes = cluster.nodelist()
        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 1)
        return session
