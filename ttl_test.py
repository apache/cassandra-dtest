import time
from collections import OrderedDict
from cassandra.util import sortedset
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from dtest import Tester
from tools import since
from assertions import (
    assert_all,
    assert_none,
    assert_row_count,
    assert_almost_equal,
    assert_unavailable
)

@since('2.0')
class TestTTL(Tester):
    """ Test Time To Live Feature """

    def setUp(self):
        super(TestTTL, self).setUp()
        self.cluster.populate(1).start()
        [node1] = self.cluster.nodelist()
        self.cursor1 = self.patient_cql_connection(node1)
        self.create_ks(self.cursor1, 'ks', 1)

    def prepare(self, default_time_to_live=None):
        self.cursor1.execute("DROP TABLE IF EXISTS ttl_table;")
        query = """
            CREATE TABLE ttl_table (
                key int primary key,
                col1 int,
                col2 int,
                col3 int,
            )
        """
        if default_time_to_live:
            query += " WITH default_time_to_live = {};".format(default_time_to_live)

        self.cursor1.execute(query)

    def smart_sleep(self, start_time, time_to_wait):
        """ Function that sleep smartly based on the start_time.
            Useful when tests are slower than expected.

            start_time: The start time of the timed operations
            time_to_wait: The time to wait in seconds from the start_time
        """

        now = time.time()
        real_time_to_wait = time_to_wait - (now - start_time)

        if real_time_to_wait > 0:
            time.sleep(real_time_to_wait)

    def default_ttl_test(self):
        """ Test default_time_to_live specified on a table """

        self.prepare(default_time_to_live=1)
        start = time.time()
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (1, 1))
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (2, 2))
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (3, 3))
        self.smart_sleep(start, 1.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    def insert_ttl_has_priority_on_defaut_ttl_test(self):
        """ Test that a ttl specified during an insert has priority on the default table ttl """

        self.prepare(default_time_to_live=1)

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 3;
        """ % (1, 1))
        self.smart_sleep(start, 1)
        assert_row_count(self.cursor1, 'ttl_table', 1)  # should still exist
        self.smart_sleep(start, 3.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    def insert_ttl_works_without_defaut_ttl_test(self):
        """ Test that a ttl specified during an insert works even if a table has no default ttl """

        self.prepare()

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 1;
        """ % (1, 1))
        self.smart_sleep(start, 1.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    def default_ttl_can_be_removed_test(self):
        """ Test that default_time_to_live can be removed """

        self.prepare(default_time_to_live=1)

        start = time.time()
        self.cursor1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 0;")
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d);
        """ % (1, 1))
        self.smart_sleep(start, 1.5)
        assert_row_count(self.cursor1, 'ttl_table', 1)

    def removing_default_ttl_does_not_affect_existing_rows_test(self):
        """ Test that removing a default_time_to_live doesn't affect the existings rows """

        self.prepare(default_time_to_live=1)

        self.cursor1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 3;")
        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d);
        """ % (1, 1))
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 5;
        """ % (2, 1))
        self.cursor1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 0;")
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d);" % (3, 1))
        self.smart_sleep(start, 1)
        assert_row_count(self.cursor1, 'ttl_table', 3)
        self.smart_sleep(start, 3.5)
        assert_row_count(self.cursor1, 'ttl_table', 2)
        self.smart_sleep(start, 5.5)
        assert_row_count(self.cursor1, 'ttl_table', 1)

    def update_single_column_ttl_test(self):
        """ Test that specifying a TTL on a single column works """

        self.prepare()

        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """  % (1, 1, 1, 1))
        start = time.time()
        self.cursor1.execute("UPDATE ttl_table USING TTL 2 set col1=42 where key=%s;" % (1,))
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, 42, 1, 1]])
        self.smart_sleep(start, 2.5)
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, None, 1, 1]])

    def update_multiple_columns_ttl_test(self):
        """ Test that specifying a TTL on multiple columns works """

        self.prepare()

        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """  % (1, 1, 1, 1))
        start = time.time()
        self.cursor1.execute("""
            UPDATE ttl_table USING TTL 2 set col1=42, col2=42, col3=42 where key=%s;
        """ % (1,))
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, 42, 42, 42]])
        self.smart_sleep(start, 2.5)
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, None, None, None]])

    def update_column_ttl_with_default_ttl_test(self):
        """
        Test that specifying a column ttl works when a default ttl is set.
        This test specify a lower ttl for the column than the default ttl.
        """

        self.prepare(default_time_to_live=4)

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """  % (1, 1, 1, 1))
        self.cursor1.execute("UPDATE ttl_table USING TTL 2 set col1=42 where key=%s;" % (1,))
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, 42, 1, 1]])
        self.smart_sleep(start, 2.5)
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, None, 1, 1]])
        self.smart_sleep(start, 4.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    def update_column_ttl_with_default_ttl_test2(self):
        """
        Test that specifying a column ttl works when a default ttl is set.
        This test specify a higher column ttl than the default ttl.
        """

        self.prepare(default_time_to_live=2)

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """  % (1, 1, 1, 1))
        self.cursor1.execute("UPDATE ttl_table USING TTL 4 set col1=42 where key=%s;" % (1,))
        self.smart_sleep(start, 2.5)
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, 42, None, None]])
        self.smart_sleep(start, 4.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    def remove_column_ttl_test(self):
        """
        Test that removing a column ttl works.
        """

        self.prepare()

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d) USING TTL 2;
        """  % (1, 1, 1, 1))
        self.cursor1.execute("UPDATE ttl_table set col1=42 where key=%s;" % (1,))
        self.smart_sleep(start, 2.5)
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, 42, None, None]])

    def remove_column_ttl_with_default_ttl_test(self):
        """
        Test that we cannot remove a column ttl when a default ttl is set.
        """

        self.prepare(default_time_to_live=2)

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """  % (1, 1, 1, 1))
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """  % (2, 1, 1, 1))
        self.cursor1.execute("UPDATE ttl_table using ttl 0 set col1=42 where key=%s;" % (1,))
        self.cursor1.execute("UPDATE ttl_table using ttl 4 set col1=42 where key=%s;" % (2,))
        self.smart_sleep(start, 2.5)
        # The first row should be deleted, using ttl 0 should fallback to default_time_to_live
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[2, 42, None, None]])
        self.smart_sleep(start, 4.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    def collection_list_ttl_test(self):
        """
        Test that ttl has a granularity of elements using a list collection.
        """

        self.prepare(default_time_to_live=5)

        self.cursor1.execute("ALTER TABLE ttl_table ADD mylist list<int>;""")
        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, mylist) VALUES (%d, %d, %s);
        """  % (1, 1, [1, 2, 3, 4, 5]))
        self.cursor1.execute("""
            UPDATE ttl_table USING TTL 2 SET mylist[0] = 42, mylist[4] = 42 WHERE key=1;
        """)
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, 1, None, None, [42, 2, 3, 4, 42]]])
        self.smart_sleep(start, 3)
        assert_all(self.cursor1, "SELECT * FROM ttl_table;", [[1, 1, None, None, [2, 3, 4]]])
        self.smart_sleep(start, 5.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    def collection_set_ttl_test(self):
        """
        Test that ttl has a granularity of elements using a set collection.
        """

        self.prepare(default_time_to_live=5)

        self.cursor1.execute("ALTER TABLE ttl_table ADD myset set<int>;""")
        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, myset) VALUES (%d, %d, %s);
        """  % (1, 1, '{1,2,3,4,5}'))
        self.cursor1.execute("""
            UPDATE ttl_table USING TTL 2 SET myset = myset + {42} WHERE key=1;
        """)
        assert_all(
            self.cursor1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, sortedset([1, 2, 3, 4, 5, 42])]]
        )
        self.smart_sleep(start, 3)
        assert_all(
            self.cursor1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, sortedset([1, 2, 3, 4, 5])]]
        )
        self.smart_sleep(start, 5.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    def collection_map_ttl_test(self):
        """
        Test that ttl has a granularity of elements using a map collection.
        """

        self.prepare(default_time_to_live=5)

        self.cursor1.execute("ALTER TABLE ttl_table ADD mymap map<int, int>;""")
        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1, mymap) VALUES (%d, %d, %s);
        """  % (1, 1, '{1:1,2:2,3:3,4:4,5:5}'))
        self.cursor1.execute("""
            UPDATE ttl_table USING TTL 2 SET mymap[1] = 42, mymap[5] = 42 WHERE key=1;
        """)
        assert_all(
            self.cursor1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, OrderedDict([(1, 42), (2, 2), (3, 3), (4, 4), (5, 42)])]]
        )
        self.smart_sleep(start, 3)
        assert_all(
            self.cursor1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, OrderedDict([(2, 2), (3, 3), (4, 4)])]]
        )
        self.smart_sleep(start, 5.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)


class TestDistributedTTL(Tester):
    """ Test Time To Live Feature in a distributed environment """

    def setUp(self):
        super(TestDistributedTTL, self).setUp()
        self.cluster.populate(2).start()
        [self.node1, self.node2] = self.cluster.nodelist()
        self.cursor1 = self.patient_cql_connection(self.node1)
        self.create_ks(self.cursor1, 'ks', 2)

    def prepare(self, default_time_to_live=None):
        self.cursor1.execute("DROP TABLE IF EXISTS ttl_table;")
        query = """
            CREATE TABLE ttl_table (
                key int primary key,
                col1 int,
                col2 int,
                col3 int,
            )
        """
        if default_time_to_live:
            query += " WITH default_time_to_live = {};".format(default_time_to_live)

        self.cursor1.execute(query)

    def ttl_is_replicated_test(self):
        """
        Test that the ttl setting is replicated properly on all nodes
        """

        self.prepare(default_time_to_live=3)
        cursor2 = self.patient_cql_connection(self.node2)
        cursor2.execute("USE ks;")
        query = SimpleStatement(
            "INSERT INTO ttl_table (key, col1) VALUES (1, 1);",
            consistency_level=ConsistencyLevel.ALL
        )
        self.cursor1.execute(query)
        assert_all(
            self.cursor1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None]],
            cl=ConsistencyLevel.ALL
        )
        ttl_cursor1 = self.cursor1.execute('SELECT ttl(col1) FROM ttl_table;')
        ttl_cursor2 = cursor2.execute('SELECT ttl(col1) FROM ttl_table;')
        assert_almost_equal(ttl_cursor1[0][0], ttl_cursor2[0][0], error=0.05)

        time.sleep(3.5)

        assert_none(self.cursor1, "SELECT * FROM ttl_table;", cl=ConsistencyLevel.ALL)

    def ttl_is_respected_on_delayed_replication_test(self):
        """ Test that ttl is respected on delayed replication """

        self.prepare()
        self.node2.stop()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (1, 1) USING TTL 3;
        """)
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (2, 2) USING TTL 60;
        """)
        assert_all(
            self.cursor1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None], [2, 2, None, None]]
        )
        time.sleep(3.5)
        self.node1.stop()
        self.node2.start()
        self.node2.watch_log_for("Listening for thrift clients...")
        cursor2 = self.patient_exclusive_cql_connection(self.node2)
        cursor2.execute("USE ks;")
        assert_row_count(cursor2, 'ttl_table', 0)  # should be 0 since node1 is down, no replica yet
        self.node1.start()
        self.node1.watch_log_for("Listening for thrift clients...")
        self.cursor1 = self.patient_exclusive_cql_connection(self.node1)
        self.cursor1.execute("USE ks;")
        self.node1.cleanup()

        # Check that the expired data has not been replicated
        assert_row_count(cursor2, 'ttl_table', 1)
        assert_all(
            cursor2,
            "SELECT * FROM ttl_table;",
            [[2, 2, None, None]],
            cl=ConsistencyLevel.ALL
        )

        # Check that the TTL on both server are the same
        ttl_cursor1 = self.cursor1.execute('SELECT ttl(col1) FROM ttl_table;')
        ttl_cursor2 = cursor2.execute('SELECT ttl(col1) FROM ttl_table;')
        assert_almost_equal(ttl_cursor1[0][0], ttl_cursor2[0][0], error=0.05)

    def ttl_is_respected_on_repair_test(self):
        """ Test that ttl is respected on repair """

        self.prepare()
        self.cursor1.execute("""
            ALTER KEYSPACE ks WITH REPLICATION =
            {'class' : 'SimpleStrategy', 'replication_factor' : 1};
        """)
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (1, 1) USING TTL 3;
        """)
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (2, 2) USING TTL 1000;
        """)

        assert_all(
            self.cursor1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None], [2, 2, None, None]]
        )
        time.sleep(3.5)
        self.node1.stop()
        cursor2 = self.patient_exclusive_cql_connection(self.node2)
        cursor2.execute("USE ks;")
        assert_unavailable(cursor2.execute, "SELECT * FROM ttl_table;")
        self.node1.start()
        self.node1.watch_log_for("Listening for thrift clients...")
        self.cursor1 = self.patient_exclusive_cql_connection(self.node1)
        self.cursor1.execute("USE ks;")
        self.cursor1.execute("""
            ALTER KEYSPACE ks WITH REPLICATION =
            {'class' : 'SimpleStrategy', 'replication_factor' : 2};
        """)
        self.node1.repair(['ks'])
        ttl_start = time.time()
        ttl_cursor1 = self.cursor1.execute('SELECT ttl(col1) FROM ttl_table;')
        self.node1.stop()

        assert_row_count(cursor2, 'ttl_table', 1)
        assert_all(
            cursor2,
            "SELECT * FROM ttl_table;",
            [[2, 2, None, None]]
        )

        # Check that the TTL on both server are the same
        ttl_cursor2 = cursor2.execute('SELECT ttl(col1) FROM ttl_table;')
        ttl_cursor1 = ttl_cursor1[0][0] - (time.time() - ttl_start)
        assert_almost_equal(ttl_cursor1, ttl_cursor2[0][0], error=0.005)
