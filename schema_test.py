from cql import ProgrammingError
from dtest import Tester
from tools import *
import time

class TestSchema(Tester):
    @since('2.0')
    def drop_column_compact_test(self):
        cursor = self.prepare()

        cursor.execute("USE ks")
        cursor.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int) WITH COMPACT STORAGE")

        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute("ALTER TABLE cf DROP c1")
        self.assertTrue(cm.exception.message.startswith("Bad Request: Cannot drop columns from a"))

    @since('2.0')
    def drop_column_compaction_test(self):
        cursor = self.prepare()
        cursor.execute("USE ks")
        cursor.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")

        # insert some data.
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        cursor.execute("ALTER TABLE cf DROP c1")
        cursor.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        node = self.cluster.nodelist()[0]
        node.flush()
        node.compact()

        # erase info on dropped 'c1' column and restart.
        cursor.execute("""UPDATE system.schema_columnfamilies
                          SET dropped_columns = null
                          WHERE keyspace_name = 'ks' AND columnfamily_name = 'cf'""")
        node.stop(gently=False)
        node.start()
        time.sleep(.5)

        # test that c1 values have been compacted away.
        cursor = self.patient_cql_connection(node, version='3.0.10').cursor()
        cursor.execute("SELECT c1 FROM ks.cf")
        self.assertEqual([[None], [None], [None], [4]], sorted(cursor.fetchall()))

    @since('2.0')
    def drop_column_queries_test(self):
        cursor = self.prepare()

        cursor.execute("USE ks")
        cursor.execute("CREATE TABLE cf (key int PRIMARY KEY, c1 int, c2 int)")
        cursor.execute("CREATE INDEX ON cf(c2)")

        # insert some data.
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (0, 1, 2)")
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (1, 2, 3)")
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (2, 3, 4)")

        # drop and readd c1.
        cursor.execute("ALTER TABLE cf DROP c1")
        cursor.execute("ALTER TABLE cf ADD c1 int")

        # add another row.
        cursor.execute("INSERT INTO cf (key, c1, c2) VALUES (3, 4, 5)")

        # test that old (pre-drop) c1 values aren't returned and new ones are.
        cursor.execute("SELECT c1 FROM cf")
        self.assertEqual([[None], [None], [None], [4]], sorted(cursor.fetchall()))

        cursor.execute("SELECT * FROM cf")
        self.assertEqual([[0,None,2], [1,None,3], [2,None,4], [3,4,5]], sorted(cursor.fetchall()))

        cursor.execute("SELECT c1 FROM cf WHERE key = 0")
        self.assertEqual([None], cursor.fetchone())

        cursor.execute("SELECT c1 FROM cf WHERE key = 3")
        self.assertEqual([4], cursor.fetchone())

        cursor.execute("SELECT * FROM cf WHERE c2 = 2")
        self.assertEqual([0,None,2], cursor.fetchone())

        cursor.execute("SELECT * FROM cf WHERE c2 = 5")
        self.assertEqual([3,4,5], cursor.fetchone())

    def prepare(self):
        cluster = self.cluster
        cluster.populate(1).start()
        time.sleep(.5)
        nodes = cluster.nodelist()
        cursor = self.patient_cql_connection(nodes[0], version='3.0.10').cursor()
        self.create_ks(cursor, 'ks', 1)
        return cursor
