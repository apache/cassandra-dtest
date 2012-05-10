from dtest import Tester
from assertions import *
from tools import *

import os, sys, time, tools
from uuid import UUID
from ccmlib.cluster import Cluster

cql_version="3.0.0-beta1"

class TestCQL(Tester):

    def prepare(self):
        cluster = self.cluster

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1, version=cql_version).cursor()
        self.create_ks(cursor, 'ks', 1)
        return cursor

    @since('1.1')
    def static_cf_test(self):
        """ Test non-composite static CF syntax """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        # Inserts
        cursor.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        cursor.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        cursor.execute("SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000");
        res = cursor.fetchall()
        assert res == [[ 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000");
        res = cursor.fetchall()
        assert res == [[ UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users");
        res = cursor.fetchall()
        assert res == [
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee' ],
            [ UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins' ],
        ], res

        # Test batch inserts
        cursor.execute("""
            BEGIN BATCH
                INSERT INTO users (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                UPDATE users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                DELETE firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                DELETE firstname, lastname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
            APPLY BATCH
        """)

        cursor.execute("SELECT * FROM users")
        res = cursor.fetchall()
        assert res == [
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 37, None, None ],
            [ UUID('550e8400-e29b-41d4-a716-446655440000'), 36, None, None ],
        ], res

    @since('1.1')
    def dynamic_cf_test(self):
        """ Test non-composite dynamic CF syntax """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid uuid,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo.bar', 42)")
        cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo-2.bar', 24)")
        cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://bar.bar', 128)")
        cursor.execute("UPDATE clicks SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 and url = 'http://bar.foo'")
        cursor.execute("UPDATE clicks SET time = 12 WHERE userid IN (f47ac10b-58cc-4372-a567-0e02b2c3d479, 550e8400-e29b-41d4-a716-446655440000) and url = 'http://foo-3'")

        # Queries
        cursor.execute("SELECT url, time FROM clicks WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ 'http://bar.bar', 128 ], [ 'http://foo-2.bar', 24 ], [ 'http://foo-3', 12 ], [ 'http://foo.bar', 42 ]], res

        cursor.execute("SELECT * FROM clicks WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")
        res = cursor.fetchall()
        assert res == [
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://bar.foo', 24 ],
            [ UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://foo-3', 12 ]
        ], res

        cursor.execute("SELECT time FROM clicks")
        res = cursor.fetchall()
        # Result from 'f47ac10b-58cc-4372-a567-0e02b2c3d479' are first
        assert res == [[24], [12], [128], [24], [12], [42]], res

    @since('1.1')
    def dense_cf_test(self):
        """ Test composite 'dense' CF syntax """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE connections (
                userid uuid,
                ip text,
                port int,
                time bigint,
                PRIMARY KEY (userid, ip, port)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        cursor.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.1', 80, 42)")
        cursor.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 80, 24)")
        cursor.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 90, 42)")
        cursor.execute("UPDATE connections SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.2' AND port = 80")

        # Queries
        cursor.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ '192.168.0.1', 80, 42 ], [ '192.168.0.2', 80, 24 ], [ '192.168.0.2', 90, 42 ]], res

        cursor.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip >= '192.168.0.2'")
        res = cursor.fetchall()
        assert res == [[ '192.168.0.2', 80, 24 ], [ '192.168.0.2', 90, 42 ]], res

        cursor.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip = '192.168.0.2'")
        res = cursor.fetchall()
        assert res == [[ '192.168.0.2', 80, 24 ], [ '192.168.0.2', 90, 42 ]], res

        cursor.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip > '192.168.0.2'")
        res = cursor.fetchall()
        assert res == [], res

        # Deletion
        cursor.execute("DELETE time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND ip = '192.168.0.2' AND port = 80")
        cursor.execute("SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert len(res) == 2, res

        cursor.execute("DELETE FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        cursor.execute("SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert len(res) == 0, res

    @since('1.1')
    def sparse_cf_test(self):
        """ Test composite 'sparse' CF syntax """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE timeline (
                userid uuid,
                posted_month int,
                posted_day int,
                body text,
                posted_by text,
                PRIMARY KEY (userid, posted_month, posted_day)
            );
        """)

        # Inserts
        cursor.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 12, 'Something else', 'Frodo Baggins')")
        cursor.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 24, 'Something something', 'Frodo Baggins')")
        cursor.execute("UPDATE timeline SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND posted_month = 1 AND posted_day = 3")
        cursor.execute("UPDATE timeline SET body = 'Yet one more message' WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 and posted_day = 30")

        # Queries
        cursor.execute("SELECT body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day = 24")
        res = cursor.fetchall()
        assert res == [[ 'Something something', 'Frodo Baggins' ]], res

        cursor.execute("SELECT posted_day, body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day > 12")
        res = cursor.fetchall()
        assert res == [
            [ 24, 'Something something', 'Frodo Baggins' ],
            [ 30, 'Yet one more message', None ]
        ], res

        cursor.execute("SELECT posted_day, body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1")
        res = cursor.fetchall()
        assert res == [
            [ 12, 'Something else', 'Frodo Baggins' ],
            [ 24, 'Something something', 'Frodo Baggins' ],
            [ 30, 'Yet one more message', None ]
        ], res

    @since('1.1')
    def create_invalid_test(self):
        """ Check invalid CREATE TABLE requests """

        cursor = self.prepare()

        assert_invalid(cursor, "CREATE TABLE test ()")
        assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY)")
        assert_invalid(cursor, "CREATE TABLE test (c1 text, c2 text, c3 text)")
        assert_invalid(cursor, "CREATE TABLE test (key1 text PRIMARY KEY, key2 text PRIMARY KEY)")

        assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY, key int)")
        assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY, c int, c text)")

        assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY, c int, d text) WITH COMPACT STORAGE")
        assert_invalid(cursor, "CREATE TABLE test (key text, key2 text, c int, d text, PRIMARY KEY (key, key2)) WITH COMPACT STORAGE")

    @since('1.1')
    def limit_ranges_test(self):
        """ Validate LIMIT option for 'range queries' in SELECT statements """

        cluster = self.cluster
        # We don't yet support paging for RP
        cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1, version=cql_version).cursor()
        self.create_ks(cursor, 'ks', 1)

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for id in xrange(0, 100):
            for tld in [ 'com', 'org', 'net' ]:
                cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (%i, 'http://foo.%s', 42)" % (id, tld))

        # Queries
        cursor.execute("SELECT * FROM clicks WHERE userid >= 2 LIMIT 1")
        res = cursor.fetchall()
        assert res == [[ 2, 'http://foo.com', 42 ]], res

        cursor.execute("SELECT * FROM clicks WHERE userid > 2 LIMIT 1")
        res = cursor.fetchall()
        assert res == [[ 3, 'http://foo.com', 42 ]], res

    @since('1.1')
    def limit_multiget_test(self):
        """ Validate LIMIT option for 'multiget' in SELECT statements """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for id in xrange(0, 100):
            for tld in [ 'com', 'org', 'net' ]:
                cursor.execute("INSERT INTO clicks (userid, url, time) VALUES (%i, 'http://foo.%s', 42)" % (id, tld))

        # Queries
        # That that we do limit the output to 1 *and* that we respect query
        # order of keys (even though 48 is after 2)
        cursor.execute("SELECT * FROM clicks WHERE userid IN (48, 2) LIMIT 1")
        res = cursor.fetchall()
        assert res == [[ 48, 'http://foo.com', 42 ]], res

    @since('1.1')
    def limit_sparse_test(self):
        """ Validate LIMIT option for sparse table in SELECT statements """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                day int,
                month text,
                year int,
                PRIMARY KEY (userid, url)
            );
        """)

        # Inserts
        for id in xrange(0, 100):
            for tld in [ 'com', 'org', 'net' ]:
                cursor.execute("INSERT INTO clicks (userid, url, day, month, year) VALUES (%i, 'http://foo.%s', 1, 'jan', 2012)" % (id, tld))

        # Queries
        # Check we do get as many rows as requested
        cursor.execute("SELECT * FROM clicks LIMIT 4")
        res = cursor.fetchall()
        assert len(res) == 4, res

    @since('1.1')
    def counters_test(self):
        """ Validate counter support """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                total counter,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        cursor.execute("UPDATE clicks SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'")
        cursor.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        res = cursor.fetchall()
        assert res == [[ 1 ]], res

        cursor.execute("UPDATE clicks SET total = total - 4 WHERE userid = 1 AND url = 'http://foo.com'")
        cursor.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        res = cursor.fetchall()
        assert res == [[ -3 ]], res

        cursor.execute("UPDATE clicks SET total = total+1 WHERE userid = 1 AND url = 'http://foo.com'")
        cursor.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        res = cursor.fetchall()
        assert res == [[ -2 ]], res

        cursor.execute("UPDATE clicks SET total = total -2 WHERE userid = 1 AND url = 'http://foo.com'")
        cursor.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        res = cursor.fetchall()
        assert res == [[ -4 ]], res

    @since('1.1')
    def indexed_with_eq_test(self):
        """ Check that you can query for an indexed column even with a key EQ clause """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        cursor.execute("CREATE INDEX byAge ON users(age)")

        # Inserts
        cursor.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        cursor.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        cursor.execute("SELECT firstname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND age = 33");
        res = cursor.fetchall()
        assert res == [], res

        cursor.execute("SELECT firstname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND age = 33");
        res = cursor.fetchall()
        assert res == [[ 'Samwise' ]], res

    @since('1.1')
    def select_key_in_test(self):
        """ Query for KEY IN (...) """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        # Inserts
        cursor.execute("""
                INSERT INTO users (userid, firstname, lastname, age)
                VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)
        """)
        cursor.execute("""
                INSERT INTO users (userid, firstname, lastname, age)
                VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, 'Samwise', 'Gamgee', 33)
        """)

        # Select
        cursor.execute("""
                SELECT firstname, lastname FROM users
                WHERE userid IN (550e8400-e29b-41d4-a716-446655440000, f47ac10b-58cc-4372-a567-0e02b2c3d479)
        """);

        res = cursor.fetchall()
        assert len(res) == 2, res

    @since('1.1')
    def exclusive_slice_test(self):
        """ Test SELECT respects inclusive and exclusive bounds """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test (k, c, v) VALUES (0, %i, %i)" % (x, x))

        # Queries
        cursor.execute("SELECT v FROM test WHERE k = 0")
        res = cursor.fetchall()
        assert len(res) == 10, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c >= '' AND c <= ''")
        res = cursor.fetchall()
        assert len(res) == 10, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c > '' AND c < ''")
        res = cursor.fetchall()
        assert len(res) == 10, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c <= 6")
        res = cursor.fetchall()
        assert len(res) == 5 and res[0][0] == 2 and res[len(res) - 1][0] == 6, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c <= 6")
        res = cursor.fetchall()
        assert len(res) == 4 and res[0][0] == 3 and res[len(res) - 1][0] == 6, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c < 6")
        res = cursor.fetchall()
        assert len(res) == 4 and res[0][0] == 2 and res[len(res) - 1][0] == 5, res

        cursor.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c < 6")
        res = cursor.fetchall()
        assert len(res) == 3 and res[0][0] == 3 and res[len(res) - 1][0] == 5, res

    @since('1.1')
    def in_clause_wide_rows_test(self):
        """ Check IN support for 'wide rows' in SELECT statement """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test1 (k, c, v) VALUES (0, %i, %i)" % (x, x))

        cursor.execute("SELECT v FROM test1 WHERE k = 0 AND c IN (5, 2, 8)")
        res = cursor.fetchall()
        assert res == [[5], [2], [8]], res

        # composites
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, 0, %i, %i)" % (x, x))

        # Check first we don't allow IN everywhere
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3")

        cursor.execute("SELECT v FROM test2 WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)")
        res = cursor.fetchall()
        assert res == [[5], [2], [8]], res

    @since('1.1')
    def order_by_test(self):
        """ Check ORDER BY support in SELECT statement """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test1 (k, c, v) VALUES (0, %i, %i)" % (x, x))

        cursor.execute("SELECT v FROM test1 WHERE k = 0 ORDER BY c DESC")
        res = cursor.fetchall()
        assert res == [[x] for x in range(9, -1, -1)], res

        # composites
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            );
        """)

        # Inserts
        for x in range(0, 4):
            for y in range(0, 2):
                cursor.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, %i, %i, %i)" % (x, y, x * 2 + y))

        # Check first we don't always ORDER BY 
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c DESC")
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c2 DESC")
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY k DESC")

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 DESC")
        res = cursor.fetchall()
        assert res == [[x] for x in range(7, -1, -1)], res

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1")
        res = cursor.fetchall()
        assert res == [[x] for x in range(0, 8)], res

    @since('1.1')
    def more_order_by_test(self):
        """ More ORDER BY checks (#4160) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE COLUMNFAMILY Test (
                row text,
                number int,
                string text,
                PRIMARY KEY (row, number)
            ) WITH COMPACT STORAGE
        """)

        cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 1, 'one');")
        cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 2, 'two');")
        cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 3, 'three');")
        cursor.execute("INSERT INTO Test (row, number, string) VALUES ('row', 4, 'four');")

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number ASC;")
        res = cursor.fetchall()
        assert res == [[1], [2]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number ASC;")
        res = cursor.fetchall()
        assert res == [[3], [4]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number DESC;")
        res = cursor.fetchall()
        assert res == [[2], [1]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number DESC;")
        res = cursor.fetchall()
        assert res == [[4], [3]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number > 3 ORDER BY number DESC;")
        res = cursor.fetchall()
        assert res == [[4]], res

        cursor.execute("SELECT number FROM Test WHERE row='row' AND number <= 3 ORDER BY number DESC;")
        res = cursor.fetchall()
        assert res == [[3], [2], [1]], res

    #@require('#4004')
    #def reversed_comparator_test(self):
    #    cluster = self.cluster

    #    cluster.populate(1).start()
    #    node1 = cluster.nodelist()[0]
    #    time.sleep(0.2)

    #    cursor = self.cql_connection(node1, version=cql_version).cursor()
    #    self.create_ks(cursor, 'ks', 1)

    #    cursor.execute("""
    #        CREATE TABLE test (
    #            k int,
    #            c int,
    #            v int,
    #            PRIMARY KEY (k, c DESC)
    #        );
    #    """)

    #    # Inserts
    #    for x in range(0, 10):
    #        cursor.execute("INSERT INTO test (k, c, v) VALUES (0, %i, %i)" % (x, x))

    #    cursor.execute("SELECT v FROM test WHERE k = 0")
    #    res = cursor.fetchall()
    #    assert res == [[x] for x in range(9, -1, -1)], res

    @require('#4004')
    def reversed_comparator_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH CLUSTERING ORDER BY (c DESC);
        """)

        # Inserts
        for x in range(0, 10):
            cursor.execute("INSERT INTO test (k, c, v) VALUES (0, %i, %i)" % (x, x))

        cursor.execute("SELECT v FROM test WHERE k = 0 ORDER BY c ASC")
        res = cursor.fetchall()
        assert res == [[x] for x in range(0, 10)], res

        cursor.execute("SELECT v FROM test WHERE k = 0 ORDER BY c DESC")
        res = cursor.fetchall()
        assert res == [[x] for x in range(9, -1, -1)], res

        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                v text,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """)

        # Inserts
        for x in range(0, 10):
            for y in range(0, 10):
                cursor.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, %i, %i, '%i%i')" % (x, y, x, y))

        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 ASC")
        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 DESC")

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 ASC")
        res = cursor.fetchall()
        assert res == [['%i%i' % (x, y)] for x in range(0, 10) for y in range(9, -1, -1)], res

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 DESC")
        res = cursor.fetchall()
        assert res == [['%i%i' % (x, y)] for x in range(0, 10) for y in range(9, -1, -1)], res

        cursor.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 ASC")
        res = cursor.fetchall()
        assert res == [['%i%i' % (x, y)] for x in range(9, -1, -1) for y in range(0, 10)], res

        assert_invalid(cursor, "SELECT v FROM test2 WHERE k = 0 ORDER BY c2 DESC, c1 ASC")

    @since('1.1')
    def invalid_old_property_test(self):
        """ Check obsolete properties from CQL2 are rejected """
        cursor = self.prepare()

        assert_invalid(cursor, "CREATE TABLE test (foo text PRIMARY KEY, c int) WITH default_validation=timestamp")

        cursor.execute("CREATE TABLE test (foo text PRIMARY KEY, c int)")
        assert_invalid(cursor, "ALTER TABLE test WITH default_validation=int;")

    @since('1.1')
    def alter_type_test(self):
        """ Validate ALTER TYPE behavior """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """)

        cursor.execute("ALTER TABLE test ALTER v TYPE float")
        cursor.execute("INSERT INTO test (k, v) VALUES (0, 2.4)")

    @require('#3783')
    def null_support_test(self):
        """ Test support for nulls """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                c3 int,
                v int,
                PRIMARY KEY (k, c1, c2, c3)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 0, 0, 0)")
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 0, 1, 1)")
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 1, 0, 2)")
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 1, 1, 3)")

        cursor.execute("INSERT INTO test1 (k, c1, c2, v) VALUES (0, 0, 0, 10)")
        cursor.execute("INSERT INTO test1 (k, c1, c2, c3, v) VALUES (0, 0, 1, null, 11)")

        #cursor.execute("SELECT v FROM test1 WHERE k = 0")
        #res = cursor.fetchall()
        #assert res == [[10], [0], [1], [11], [2], [3]], res

        #cursor.execute("SELECT v FROM test1 WHERE k = 0 AND c1 = 0 AND c2 = 0")
        #res = cursor.fetchall()
        #assert res == [[10], [0], [1]], res

        cursor.execute("SELECT v FROM test1 WHERE k = 0 AND c1 = 0 AND c2 = 0 AND c3 = null")
        res = cursor.fetchall()
        assert res == [[10]], res

    @require('#4192')
    def nameless_index_test(self):
        """ Test CREATE INDEX without name and validate the index can be dropped """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE users (
                id text PRIMARY KEY,
                birth_year int,
            )
        """)

        cursor.execute("CREATE INDEX on users(birth_year)")

        cursor.execute("INSERT INTO users (id, birth_year) VALUES ('Tom', 42)")
        cursor.execute("INSERT INTO users (id, birth_year) VALUES ('Paul', 24)")
        cursor.execute("INSERT INTO users (id, birth_year) VALUES ('Bob', 42)")

        cursor.execute("SELECT id FROM users WHERE birth_year = 42")
        res = cursor.fetchall()
        assert res == [['Tom'], ['Bob']]

        cursor.execute("DROP INDEX users_birth_year_idx")

        assert_invalid(cursor, "SELECT id FROM users WHERE birth_year = 42")

    @since('1.1')
    def deletion_test(self):
        """ Test simple deletion and in particular check for #4193 bug """

        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE testcf (
                username varchar,
                id int,
                name varchar,
                stuff varchar,
                PRIMARY KEY(username, id)
            );
        """)

        q = "INSERT INTO testcf (username, id, name, stuff) VALUES ('%s', %d, '%s', '%s');"
        row1 = ('abc', 2, 'rst', 'some value')
        row2 = ('abc', 4, 'xyz', 'some other value')
        cursor.execute(q % row1)
        cursor.execute(q % row2)

        cursor.execute("SELECT * FROM testcf")
        res = cursor.fetchall()
        assert res == [ list(row1), list(row2) ], res

        cursor.execute("DELETE FROM testcf WHERE username='abc' AND id=2");

        cursor.execute("SELECT * FROM testcf")
        res = cursor.fetchall()
        assert res == [ list(row2) ], res

        # Compact case
        cursor.execute("""
            CREATE TABLE testcf2 (
                username varchar,
                id int,
                name varchar,
                stuff varchar,
                PRIMARY KEY(username, id, name)
            ) WITH COMPACT STORAGE;
        """)

        q = "INSERT INTO testcf2 (username, id, name, stuff) VALUES ('%s', %d, '%s', '%s');"
        row1 = ('abc', 2, 'rst', 'some value')
        row2 = ('abc', 4, 'xyz', 'some other value')
        cursor.execute(q % row1)
        cursor.execute(q % row2)

        cursor.execute("SELECT * FROM testcf2")
        res = cursor.fetchall()
        assert res == [ list(row1), list(row2) ], res

        # Won't be allowed until #3708 is in
        assert_invalid(cursor, "DELETE FROM testcf2 WHERE username='abc' AND id=2");

    @since('1.1')
    def count_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE events (
                kind text,
                time int,
                value1 int,
                value2 int,
                PRIMARY KEY(kind, time)
            )
        """)

        full = "INSERT INTO events (kind, time, value1, value2) VALUES ('ev1', %d, %d, %d)"
        no_v2 = "INSERT INTO events (kind, time, value1) VALUES ('ev1', %d, %d)"

        cursor.execute(full  % (0, 0, 0));
        cursor.execute(full  % (1, 1, 1));
        cursor.execute(no_v2 % (2, 2));
        cursor.execute(full  % (3, 3, 3));
        cursor.execute(no_v2 % (4, 4));
        cursor.execute("INSERT INTO events (kind, time, value1, value2) VALUES ('ev2', 0, 0, 0)")

        cursor.execute("SELECT COUNT(*) FROM events WHERE kind = 'ev1'")
        res = cursor.fetchall()
        assert res == [[5]], res

        cursor.execute("SELECT COUNT(1) FROM events WHERE kind IN ('ev1', 'ev2') AND time=0")
        res = cursor.fetchall()
        assert res == [[2]], res

    @since('1.1')
    def reserved_keyword_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                key text PRIMARY KEY,
                count counter,
            )
        """)

        assert_invalid(cursor, "CREATE TABLE test2 ( select text PRIMARY KEY, x int)")

    @since('1.1')
    def timeuuid_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE timeline (
                k text,
                time timeuuid,
                value int,
                PRIMARY KEY(k, time)
            )
        """)

        q = "INSERT INTO timeline (k, time, value) VALUES ('k', '%s', %d)"
        cursor.execute(q % ('2012-04-10', 0))
        cursor.execute(q % ('2012-04-15', 1))
        cursor.execute(q % ('2012-04-22', 2))
        cursor.execute(q % ('now', 3))

        cursor.execute("SELECT value FROM timeline WHERE k='k' AND time > '2012-04-15'")
        res = cursor.fetchall()
        assert res == [[2], [3]], res

    @since('1.1')
    def identifier_test(self):
        cursor = self.prepare()

        # Test case insensitivity
        cursor.execute("CREATE TABLE test1 (key_23 int PRIMARY KEY, CoLuMn int)")

        # Should work
        cursor.execute("INSERT INTO test1 (Key_23, Column) VALUES (0, 0)")
        cursor.execute("INSERT INTO test1 (KEY_23, COLUMN) VALUES (0, 0)")

        # Reserved keywords
        assert_invalid(cursor, "CREATE TABLE test1 (select int PRIMARY KEY, column int)")

    @since('1.1')
    def keyspace_test(self):
        cursor = self.prepare()

        assert_invalid(cursor, "CREATE KEYSPACE test1");
        cursor.execute("CREATE KEYSPACE test2 WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1");
        assert_invalid(cursor, "CREATE KEYSPACE My_much_much_too_long_identifier_that_should_not_work WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1");

        cursor.execute("DROP KEYSPACE test2");
        assert_invalid(cursor, "DROP KEYSPACE non_existing");
        cursor.execute("CREATE KEYSPACE test2 WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1");

    @since('1.1')
    def table_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c int
            )
        """)

        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                name int,
                value int,
                PRIMARY KEY(k, name)
            ) WITH COMPACT STORAGE
        """)

        cursor.execute("""
            CREATE TABLE test3 (
                k int,
                c int,
                PRIMARY KEY (k),
            )
        """)

        # existing table
        assert_invalid(cursor, "CREATE TABLE test3 (k int PRIMARY KEY, c int)")
        # repeated column
        assert_invalid(cursor, "CREATE TABLE test4 (k int PRIMARY KEY, c int, k text)")

        # compact storage limitations
        assert_invalid(cursor, "CREATE TABLE test4 (k int PRIMARY KEY, c int) WITH COMPACT STORAGE")
        assert_invalid(cursor, "CREATE TABLE test4 (k int, name, int, c1 int, c2 int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE")

        cursor.execute("DROP TABLE test1")
        cursor.execute("TRUNCATE test2")

    @since('1.1')
    def batch_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE users (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """)

        cursor.execute("""
            BEGIN BATCH USING CONSISTENCY QUORUM
                INSERT INTO users (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE users SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO users (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM users WHERE userid = 'user1';
            APPLY BATCH;
        """)

    @require('#3771')
    def token_range_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c int,
                v int
            )
        """)

        c = 100
        for i in range(0, c):
            cursor.execute("INSERT INTO test (k, c, v) VALUES (%d, %d, %d)" % (i, i, i))

        cursor.execute("SELECT k FROM test")
        inOrder = [ x[0] for x in cursor.fetchall() ]
        assert len(inOrder) == c, 'Expecting %d elements, got %d' % (c, len(inOrder))

        cursor.execute("SELECT k FROM test WHERE token(k) >= '0'")
        res = cursor.fetchall()
        assert len(res) == c, "%s [all: %s]" % (str(res), str(inOrder))

        assert_invalid(cursor, "SELECT k FROM test WHERE token(k) >= 0")

        cursor.execute("SELECT k FROM test WHERE token(k) >= token(%d) AND token(k) < token(%d)" % (inOrder[32], inOrder[65]))
        res = cursor.fetchall()
        assert res == [ [inOrder[x]] for x in range(32, 65) ], "%s [all: %s]" % (str(res), str(inOrder))

    @since('1.1')
    def table_options_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c int
            ) WITH comment = 'My comment'
               AND read_repair_chance = 0.5
               AND dclocal_read_repair_chance = 0.5
               AND gc_grace_seconds = 4
               AND bloom_filter_fp_chance = 0.01
               AND compaction_strategy_class = LeveledCompactionStrategy
               AND compaction_strategy_options:sstable_size_in_mb = 10
               AND compression_parameters:sstable_compression = ''
               AND caching = all
        """)

        cursor.execute("""
            ALTER TABLE test
            WITH comment = 'other comment'
             AND read_repair_chance = 0.3
             AND dclocal_read_repair_chance = 0.3
             AND gc_grace_seconds = 100
             AND bloom_filter_fp_chance = 0.1
             AND compaction_strategy_class = SizeTieredCompactionStrategy
             AND compaction_strategy_options:min_sstable_size = 42
             AND compression_parameters:sstable_compression = SnappyCompressor
             AND caching = rows_only
        """)

    @require('#4217')
    def timestamp_and_ttl_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c text
            )
        """)

        cursor.execute("INSERT INTO test (k, c) VALUES (1, 'test')")
        cursor.execute("INSERT INTO test (k, c) VALUES (2, 'test') USING TTL 400")

        cursor.execute("SELECT k, c, writetime(c), ttl(c) FROM test")
        res = cursor.fetchall()
        assert len(res) == 2, res
        for r in res:
            assert isinstance(r[2], (int, long))
            if r[0] == 1:
                assert r[3] == None, res
            else:
                assert isinstance(r[3], (int, long)), res

        assert_invalid(cursor, "SELECT k, c, writetime(k) FROM test")

    @require('3982')
    def no_range_ghost_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """)

        for k in range(0, 5):
            cursor.execute("INSERT INTO test (k, v) VALUES (%d, 0)" % k)

        cursor.execute("SELECT k FROM test")
        res = sorted(cursor.fetchall())
        assert res == [[k] for k in range(0, 5)], res

        cursor.execute("DELETE FROM test WHERE k=2")

        cursor.execute("SELECT k FROM test")
        res = sorted(cursor.fetchall())
        assert res == [[k] for k in range(0, 5) if k is not 2], res

    @require('3982')
    def undefined_column_handling_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 int,
            )
        """)

        cursor.execute("INSERT INTO test (k, v1, v2) VALUES (0, 0, 0)")
        cursor.execute("INSERT INTO test (k, v1) VALUES (1, 1)")
        cursor.execute("INSERT INTO test (k, v1, v2) VALUES (2, 2, 2)")

        cursor.execute("SELECT v2 FROM test")
        res = cursor.fetchall()
        assert res == [[0], [None], [2]], res

        cursor.execute("SELECT v2 FROM test WHERE k = 1")
        res = cursor.fetchall()
        assert res == [[None]], res
