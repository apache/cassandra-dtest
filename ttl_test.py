import time
from collections import OrderedDict
from cassandra.util import sortedset
from dtest import Tester
from pytools import since
from pyassertions import assert_all, assert_row_count


class TestTTL(Tester):
    """ Test Time To Live Feature """

    def setUp(self):
        super(TestTTL, self).setUp()
        self.cluster.populate(3).start()
        [node1, node2, node3] = self.cluster.nodelist()
        self.cursor1 = self.patient_cql_connection(node1)
        self.create_ks(self.cursor1, 'ks', 3)

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

    @since('2.0')
    def default_ttl_test(self):
        """ Test default_time_to_live specified on a table """

        self.prepare(default_time_to_live=1)
        start = time.time()
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (1, 1))
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (2, 2))
        self.cursor1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (3, 3))
        self.smart_sleep(start, 1.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    @since('2.0')
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

    @since('2.0')
    def insert_ttl_works_without_defaut_ttl_test(self):
        """ Test that a ttl specified during an insert works even if a table has no default ttl """

        self.prepare()

        start = time.time()
        self.cursor1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 1;
        """ % (1, 1))
        self.smart_sleep(start, 1.5)
        assert_row_count(self.cursor1, 'ttl_table', 0)

    @since('2.0')
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

    @since('2.0')
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

    @since('2.0')
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

    @since('2.0')
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

    @since('2.0')
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

    @since('2.0')
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

    @since('2.0')
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

    @since('2.0')
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

    @since('2.0')
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

    @since('2.0')
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

    @since('2.0')
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
