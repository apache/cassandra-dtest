import time
import pytest
import logging

from collections import OrderedDict

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.util import sortedset

from dtest import Tester, create_ks
from tools.assertions import (assert_all, assert_almost_equal, assert_none,
                              assert_row_count, assert_unavailable)

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('2.0')
class TestTTL(Tester):
    """ Test Time To Live Feature """

    @pytest.fixture(scope='function', autouse=True)
    def fixture_ttl_test_setup(self, fixture_dtest_setup):
        self.cluster = fixture_dtest_setup.cluster
        self.fixture_dtest_setup = fixture_dtest_setup
        self.cluster.populate(1).start()
        [node1] = self.cluster.nodelist()
        self.session1 = self.patient_cql_connection(node1)
        create_ks(self.session1, 'ks', 1)

    def prepare(self, default_time_to_live=None):
        self.session1.execute("DROP TABLE IF EXISTS ttl_table;")
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

        self.session1.execute(query)

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

    def test_default_ttl(self):
        """ Test default_time_to_live specified on a table """
        self.prepare(default_time_to_live=1)
        start = time.time()
        self.session1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (1, 1))
        self.session1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (2, 2))
        self.session1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d)" % (3, 3))
        self.smart_sleep(start, 3)
        assert_row_count(self.session1, 'ttl_table', 0)

    def test_insert_ttl_has_priority_on_defaut_ttl(self):
        """ Test that a ttl specified during an insert has priority on the default table ttl """
        self.prepare(default_time_to_live=1)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 5;
        """ % (1, 1))
        self.smart_sleep(start, 2)
        assert_row_count(self.session1, 'ttl_table', 1)  # should still exist
        self.smart_sleep(start, 7)
        assert_row_count(self.session1, 'ttl_table', 0)

    def test_insert_ttl_works_without_default_ttl(self):
        """ Test that a ttl specified during an insert works even if a table has no default ttl """
        self.prepare()

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 1;
        """ % (1, 1))
        self.smart_sleep(start, 3)
        assert_row_count(self.session1, 'ttl_table', 0)

    def test_default_ttl_can_be_removed(self):
        """ Test that default_time_to_live can be removed """
        self.prepare(default_time_to_live=1)

        start = time.time()
        self.session1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 0;")
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d);
        """ % (1, 1))
        self.smart_sleep(start, 1.5)
        assert_row_count(self.session1, 'ttl_table', 1)

    def test_removing_default_ttl_does_not_affect_existing_rows(self):
        """ Test that removing a default_time_to_live doesn't affect the existings rows """
        self.prepare(default_time_to_live=1)

        self.session1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 10;")
        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d);
        """ % (1, 1))
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (%d, %d) USING TTL 15;
        """ % (2, 1))
        self.session1.execute("ALTER TABLE ttl_table WITH default_time_to_live = 0;")
        self.session1.execute("INSERT INTO ttl_table (key, col1) VALUES (%d, %d);" % (3, 1))
        self.smart_sleep(start, 5)
        assert_row_count(self.session1, 'ttl_table', 3)
        self.smart_sleep(start, 12)
        assert_row_count(self.session1, 'ttl_table', 2)
        self.smart_sleep(start, 20)
        assert_row_count(self.session1, 'ttl_table', 1)

    def test_update_single_column_ttl(self):
        """ Test that specifying a TTL on a single column works """
        self.prepare()

        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        start = time.time()
        self.session1.execute("UPDATE ttl_table USING TTL 3 set col1=42 where key=%s;" % (1,))
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, 1, 1]])
        self.smart_sleep(start, 5)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, None, 1, 1]])

    def test_update_multiple_columns_ttl(self):
        """ Test that specifying a TTL on multiple columns works """
        self.prepare()

        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        start = time.time()
        self.session1.execute("""
            UPDATE ttl_table USING TTL 2 set col1=42, col2=42, col3=42 where key=%s;
        """ % (1,))
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, 42, 42]])
        self.smart_sleep(start, 4)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, None, None, None]])

    def test_update_column_ttl_with_default_ttl(self):
        """
        Test that specifying a column ttl works when a default ttl is set.
        This test specify a lower ttl for the column than the default ttl.
        """
        self.prepare(default_time_to_live=8)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        self.session1.execute("UPDATE ttl_table USING TTL 3 set col1=42 where key=%s;" % (1,))
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, 1, 1]])
        self.smart_sleep(start, 5)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, None, 1, 1]])
        self.smart_sleep(start, 10)
        assert_row_count(self.session1, 'ttl_table', 0)

    def update_column_ttl_with_default_ttl_test2(self):
        """
        Test that specifying a column ttl works when a default ttl is set.
        This test specify a higher column ttl than the default ttl.
        """

        self.prepare(default_time_to_live=2)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        self.session1.execute("UPDATE ttl_table USING TTL 6 set col1=42 where key=%s;" % (1,))
        self.smart_sleep(start, 4)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, None, None]])
        self.smart_sleep(start, 8)
        assert_row_count(self.session1, 'ttl_table', 0)

    def test_remove_column_ttl(self):
        """
        Test that removing a column ttl works.
        """
        self.prepare()

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d) USING TTL 2;
        """ % (1, 1, 1, 1))
        self.session1.execute("UPDATE ttl_table set col1=42 where key=%s;" % (1,))
        self.smart_sleep(start, 4)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, None, None]])

    @since('3.6')
    def test_set_ttl_to_zero_to_default_ttl(self):
        """
        Test that we can remove the default ttl by setting the ttl explicitly to zero.
        CASSANDRA-11207
        """
        self.prepare(default_time_to_live=2)

        start = time.time()
        self.session1.execute("INSERT INTO ttl_table (key, col1, col2, col3) VALUES ({}, {}, {}, {});".format(1, 1, 1, 1))
        self.session1.execute("INSERT INTO ttl_table (key, col1, col2, col3) VALUES ({}, {}, {}, {});".format(2, 1, 1, 1))
        self.session1.execute("UPDATE ttl_table using ttl 0 set col1=42 where key={};".format(1))
        self.session1.execute("UPDATE ttl_table using ttl 3 set col1=42 where key={};".format(2))
        self.smart_sleep(start, 5)

        # The first row should be deleted, using ttl 0 should fallback to default_time_to_live
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 42, None, None]])

    @since('2.1', max_version='3.5')
    def test_remove_column_ttl_with_default_ttl(self):
        """
        Test that we cannot remove a column ttl when a default ttl is set.
        """
        self.prepare(default_time_to_live=2)

        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (1, 1, 1, 1))
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, col2, col3) VALUES (%d, %d, %d, %d);
        """ % (2, 1, 1, 1))
        self.session1.execute("UPDATE ttl_table using ttl 0 set col1=42 where key=%s;" % (1,))
        self.session1.execute("UPDATE ttl_table using ttl 8 set col1=42 where key=%s;" % (2,))
        self.smart_sleep(start, 5)
        # The first row should be deleted, using ttl 0 should fallback to default_time_to_live
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[2, 42, None, None]])
        self.smart_sleep(start, 10)
        assert_row_count(self.session1, 'ttl_table', 0)

    def test_collection_list_ttl(self):
        """
        Test that ttl has a granularity of elements using a list collection.
        """
        self.prepare(default_time_to_live=10)

        self.session1.execute("ALTER TABLE ttl_table ADD mylist list<int>;""")
        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, mylist) VALUES (%d, %d, %s);
        """ % (1, 1, [1, 2, 3, 4, 5]))
        self.session1.execute("""
            UPDATE ttl_table USING TTL 5 SET mylist[0] = 42, mylist[4] = 42 WHERE key=1;
        """)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 1, None, None, [42, 2, 3, 4, 42]]])
        self.smart_sleep(start, 7)
        assert_all(self.session1, "SELECT * FROM ttl_table;", [[1, 1, None, None, [2, 3, 4]]])
        self.smart_sleep(start, 12)
        assert_row_count(self.session1, 'ttl_table', 0)

    def test_collection_set_ttl(self):
        """
        Test that ttl has a granularity of elements using a set collection.
        """
        self.prepare(default_time_to_live=10)

        self.session1.execute("ALTER TABLE ttl_table ADD myset set<int>;""")
        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, myset) VALUES (%d, %d, %s);
        """ % (1, 1, '{1,2,3,4,5}'))
        self.session1.execute("""
            UPDATE ttl_table USING TTL 3 SET myset = myset + {42} WHERE key=1;
        """)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, sortedset([1, 2, 3, 4, 5, 42])]]
        )
        self.smart_sleep(start, 5)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, sortedset([1, 2, 3, 4, 5])]]
        )
        self.smart_sleep(start, 12)
        assert_row_count(self.session1, 'ttl_table', 0)

    def test_collection_map_ttl(self):
        """
        Test that ttl has a granularity of elements using a map collection.
        """
        self.prepare(default_time_to_live=6)

        self.session1.execute("ALTER TABLE ttl_table ADD mymap map<int, int>;""")
        start = time.time()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1, mymap) VALUES (%d, %d, %s);
        """ % (1, 1, '{1:1,2:2,3:3,4:4,5:5}'))
        self.session1.execute("""
            UPDATE ttl_table USING TTL 2 SET mymap[1] = 42, mymap[5] = 42 WHERE key=1;
        """)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, OrderedDict([(1, 42), (2, 2), (3, 3), (4, 4), (5, 42)])]]
        )
        self.smart_sleep(start, 4)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None, OrderedDict([(2, 2), (3, 3), (4, 4)])]]
        )
        self.smart_sleep(start, 8)
        assert_row_count(self.session1, 'ttl_table', 0)

    def test_delete_with_ttl_expired(self):
        """
        Updating a row with a ttl does not prevent deletion, test for CASSANDRA-6363
        """
        self.session1.execute("DROP TABLE IF EXISTS session")
        self.session1.execute("CREATE TABLE session (id text, usr text, valid int, PRIMARY KEY (id))")

        self.session1.execute("insert into session (id, usr) values ('abc', 'abc')")
        self.session1.execute("update session using ttl 1 set valid = 1 where id = 'abc'")
        self.smart_sleep(time.time(), 2)

        self.session1.execute("delete from session where id = 'abc' if usr ='abc'")
        assert_row_count(self.session1, 'session', 0)


class TestDistributedTTL(Tester):
    """ Test Time To Live Feature in a distributed environment """

    @pytest.fixture(scope='function', autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        fixture_dtest_setup.cluster.populate(2).start()
        [self.node1, self.node2] = fixture_dtest_setup.cluster.nodelist()
        self.session1 = fixture_dtest_setup.patient_cql_connection(self.node1)
        create_ks(self.session1, 'ks', 2)


    def prepare(self, default_time_to_live=None):
        self.session1.execute("DROP TABLE IF EXISTS ttl_table;")
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

        self.session1.execute(query)

    def test_ttl_is_replicated(self):
        """
        Test that the ttl setting is replicated properly on all nodes
        """
        self.prepare(default_time_to_live=5)
        session1 = self.patient_exclusive_cql_connection(self.node1)
        session2 = self.patient_exclusive_cql_connection(self.node2)
        session1.execute("USE ks;")
        session2.execute("USE ks;")
        query = SimpleStatement(
            "INSERT INTO ttl_table (key, col1) VALUES (1, 1);",
            consistency_level=ConsistencyLevel.ALL
        )
        session1.execute(query)
        assert_all(
            session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None]],
            cl=ConsistencyLevel.ALL
        )
        ttl_session1 = session1.execute('SELECT ttl(col1) FROM ttl_table;')
        ttl_session2 = session2.execute('SELECT ttl(col1) FROM ttl_table;')

        # since the two queries are not executed simultaneously, the remaining
        # TTLs can differ by one second
        assert abs(ttl_session1[0][0] - ttl_session2[0][0]) <= 1

        time.sleep(7)

        assert_none(session1, "SELECT * FROM ttl_table;", cl=ConsistencyLevel.ALL)

    def test_ttl_is_respected_on_delayed_replication(self):
        """ Test that ttl is respected on delayed replication """
        self.prepare()
        self.node2.stop()
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (1, 1) USING TTL 5;
        """)
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (2, 2) USING TTL 1000;
        """)
        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None], [2, 2, None, None]]
        )
        time.sleep(7)
        self.node1.stop()
        self.node2.start(wait_for_binary_proto=True)
        session2 = self.patient_exclusive_cql_connection(self.node2)
        session2.execute("USE ks;")
        assert_row_count(session2, 'ttl_table', 0)  # should be 0 since node1 is down, no replica yet
        self.node1.start(wait_for_binary_proto=True)
        self.session1 = self.patient_exclusive_cql_connection(self.node1)
        self.session1.execute("USE ks;")
        self.node1.cleanup()

        assert_all(session2, "SELECT count(*) FROM ttl_table", [[1]], cl=ConsistencyLevel.ALL)
        assert_all(
            session2,
            "SELECT * FROM ttl_table;",
            [[2, 2, None, None]],
            cl=ConsistencyLevel.ALL
        )

        # Check that the TTL on both server are the same
        ttl_1 = self.session1.execute('SELECT ttl(col1) FROM ttl_table;')[0][0]
        ttl_2 = session2.execute('SELECT ttl(col1) FROM ttl_table;')[0][0]

        logger.debug("ttl_1 is {}:".format(ttl_1))
        logger.debug("ttl_2 is {}:".format(ttl_2))
        assert abs(ttl_1 - ttl_2) <= 1

    def test_ttl_is_respected_on_repair(self):
        """ Test that ttl is respected on repair """
        self.prepare()
        self.session1.execute("""
            ALTER KEYSPACE ks WITH REPLICATION =
            {'class' : 'SimpleStrategy', 'replication_factor' : 1};
        """)
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (1, 1) USING TTL 5;
        """)
        self.session1.execute("""
            INSERT INTO ttl_table (key, col1) VALUES (2, 2) USING TTL 1000;
        """)

        assert_all(
            self.session1,
            "SELECT * FROM ttl_table;",
            [[1, 1, None, None], [2, 2, None, None]]
        )
        time.sleep(7)
        self.node1.stop()
        session2 = self.patient_exclusive_cql_connection(self.node2)
        session2.execute("USE ks;")
        assert_unavailable(session2.execute, "SELECT * FROM ttl_table;")
        self.node1.start(wait_for_binary_proto=True)
        self.session1 = self.patient_exclusive_cql_connection(self.node1)
        self.session1.execute("USE ks;")
        self.session1.execute("""
            ALTER KEYSPACE ks WITH REPLICATION =
            {'class' : 'SimpleStrategy', 'replication_factor' : 2};
        """)
        self.node1.repair(['ks'])
        ttl_start = time.time()
        ttl_session1 = self.session1.execute('SELECT ttl(col1) FROM ttl_table;')
        self.node1.stop()

        assert_row_count(session2, 'ttl_table', 1)
        assert_all(
            session2,
            "SELECT * FROM ttl_table;",
            [[2, 2, None, None]]
        )

        # Check that the TTL on both server are the same
        ttl_session2 = session2.execute('SELECT ttl(col1) FROM ttl_table;')
        ttl_session1 = ttl_session1[0][0] - (time.time() - ttl_start)
        assert_almost_equal(ttl_session1, ttl_session2[0][0], error=0.005)
