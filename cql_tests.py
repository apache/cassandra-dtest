# coding: utf-8

from dtest import Tester
from assertions import *
from cql import ProgrammingError
from tools import *

import os, sys, time, tools, json, random
from uuid import UUID
from ccmlib.cluster import Cluster

cql_version="3.0.0"

class TestCQL(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False):
        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={ 'row_cache_size_in_mb' : 100 })

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1, version=cql_version).cursor()
        if create_keyspace:
            self.create_ks(cursor, 'ks', 1)
        return cursor

    @since('1.1')
    def static_cf_test(self):
        """ Test static CF syntax """
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
        cursor.execute("SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users")
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

    @since('1.2')
    def noncomposite_static_cf_test(self):
        """ Test non-composite static CF syntax """
        cursor = self.prepare()

        # Create
        cursor.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        cursor.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        cursor.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        cursor.execute("SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = cursor.fetchall()
        assert res == [[ UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins' ]], res

        cursor.execute("SELECT * FROM users")
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
        if self.cluster.version() < "1.2":
            assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY)")
        assert_invalid(cursor, "CREATE TABLE test (c1 text, c2 text, c3 text)")
        assert_invalid(cursor, "CREATE TABLE test (key1 text PRIMARY KEY, key2 text PRIMARY KEY)")

        assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY, key int)")
        assert_invalid(cursor, "CREATE TABLE test (key text PRIMARY KEY, c int, c text)")

        assert_invalid(cursor, "CREATE TABLE test (key text, key2 text, c int, d text, PRIMARY KEY (key, key2)) WITH COMPACT STORAGE")

    @since('1.1')
    def limit_ranges_test(self):
        """ Validate LIMIT option for 'range queries' in SELECT statements """
        cursor = self.prepare(ordered=True)

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
        cursor.execute("SELECT * FROM clicks WHERE token(userid) >= token(2) LIMIT 1")
        res = cursor.fetchall()
        assert res == [[ 2, 'http://foo.com', 42 ]], res

        cursor.execute("SELECT * FROM clicks WHERE token(userid) > token(2) LIMIT 1")
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

        # Check that we do limit the output to 1 *and* that we respect query
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

    @since('1.2')
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
        cursor.execute("SELECT firstname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND age = 33")
        res = cursor.fetchall()
        assert res == [], res

        cursor.execute("SELECT firstname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND age = 33")
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
        """)

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

    @since('1.1')
    def order_by_validation_test(self):
        """ Check we don't allow order by on row key (#4246) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY (k1, k2)
            )
        """)

        q = "INSERT INTO test (k1, k2, v) VALUES (%d, %d, %d)"
        cursor.execute(q % (0, 0, 0))
        cursor.execute(q % (1, 1, 1))
        cursor.execute(q % (2, 2, 2))

        assert_invalid(cursor, "SELECT * FROM test ORDER BY k2")

    @since('1.1.3')
    def order_by_with_in_test(self):
        """ Check that order-by works with IN (#4327) """
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test(
                my_id varchar,
                col1 int,
                value varchar,
                PRIMARY KEY (my_id, col1)
            )
        """)
        cursor.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key1', 1, 'a')")
        cursor.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key2', 3, 'c')")
        cursor.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key3', 2, 'b')")
        cursor.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key4', 4, 'd')")
        # Currently this breaks due to CASSANDRA-4612
        cursor.execute("SELECT col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1")

        res = cursor.fetchall()
        assert res == [[1], [2], [3]], res


    @since('1.1')
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

        cursor.execute("SELECT c, v FROM test WHERE k = 0 ORDER BY c ASC")
        res = cursor.fetchall()
        assert res == [[x, x] for x in range(0, 10)], res

        cursor.execute("SELECT c, v FROM test WHERE k = 0 ORDER BY c DESC")
        res = cursor.fetchall()
        assert res == [[x, x] for x in range(9, -1, -1)], res

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

        assert_invalid(cursor, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 ASC")
        assert_invalid(cursor, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 DESC")

        cursor.execute("SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC")
        res = cursor.fetchall()
        assert res == [[x, y, '%i%i' % (x, y)] for x in range(0, 10) for y in range(9, -1, -1)], res

        cursor.execute("SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 DESC")
        res = cursor.fetchall()
        assert res == [[x, y, '%i%i' % (x, y)] for x in range(0, 10) for y in range(9, -1, -1)], res

        cursor.execute("SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 ASC")
        res = cursor.fetchall()
        assert res == [[x, y, '%i%i' % (x, y)] for x in range(9, -1, -1) for y in range(0, 10)], res

        assert_invalid(cursor, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c2 DESC, c1 ASC")

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

    @since('1.2')
    def null_support_test(self):
        """ Test support for nulls """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v1 int,
                v2 set<text>,
                PRIMARY KEY (k, c)
            );
        """)

        # Inserts
        cursor.execute("INSERT INTO test (k, c, v1, v2) VALUES (0, 0, null, {'1', '2'})")
        cursor.execute("INSERT INTO test (k, c, v1) VALUES (0, 1, 1)")

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [ [0, 0, None, set(['1', '2'])], [0, 1, 1, None]], res

        cursor.execute("INSERT INTO test (k, c, v1) VALUES (0, 1, null)")
        cursor.execute("INSERT INTO test (k, c, v2) VALUES (0, 0, null)")

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [ [0, 0, None, None], [0, 1, None, None]], res

        assert_invalid(cursor, "INSERT INTO test (k, c, v2) VALUES (0, 2, {1, null})")
        assert_invalid(cursor, "SELECT * FROM test WHER k = null")
        assert_invalid(cursor, "INSERT INTO test (k, c, v2) VALUES (0, 0, { 'foo', 'bar', null })")


    @since('1.2')
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

        cursor.execute("DELETE FROM testcf WHERE username='abc' AND id=2")

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
        if self.cluster.version() < "1.2":
            assert_invalid(cursor, "DELETE FROM testcf2 WHERE username='abc' AND id=2")

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

        cursor.execute(full  % (0, 0, 0))
        cursor.execute(full  % (1, 1, 1))
        cursor.execute(no_v2 % (2, 2))
        cursor.execute(full  % (3, 3, 3))
        cursor.execute(no_v2 % (4, 4))
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
    def identifier_test(self):
        cursor = self.prepare()

        # Test case insensitivity
        cursor.execute("CREATE TABLE test1 (key_23 int PRIMARY KEY, CoLuMn int)")

        # Should work
        cursor.execute("INSERT INTO test1 (Key_23, Column) VALUES (0, 0)")
        cursor.execute("INSERT INTO test1 (KEY_23, COLUMN) VALUES (0, 0)")

        # Reserved keywords
        assert_invalid(cursor, "CREATE TABLE test1 (select int PRIMARY KEY, column int)")

    @since('1.2')
    def keyspace_test(self):
        cursor = self.prepare()

        assert_invalid(cursor, "CREATE KEYSPACE test1")
        cursor.execute("CREATE KEYSPACE test2 WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
        assert_invalid(cursor, "CREATE KEYSPACE My_much_much_too_long_identifier_that_should_not_work WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

        cursor.execute("DROP KEYSPACE test2")
        assert_invalid(cursor, "DROP KEYSPACE non_existing")
        cursor.execute("CREATE KEYSPACE test2 WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

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
        assert_invalid(cursor, "CREATE TABLE test4 (k int, name, int, c1 int, c2 int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE")

        cursor.execute("DROP TABLE test1")
        cursor.execute("TRUNCATE test2")

        cursor.execute("""
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c1 int,
                c2 int,
            )
        """)

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

        if self.cluster.version() >= '1.2':
            cursor.execute("""
                BEGIN BATCH
                    INSERT INTO users (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                    UPDATE users SET password = 'ps22dhds' WHERE userid = 'user3';
                    INSERT INTO users (userid, password) VALUES ('user4', 'ch@ngem3c');
                    DELETE name FROM users WHERE userid = 'user1';
                APPLY BATCH;
            """, consistency_level='QUORUM')
        else:
            cursor.execute("""
                BEGIN BATCH USING CONSISTENCY QUORUM
                    INSERT INTO users (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                    UPDATE users SET password = 'ps22dhds' WHERE userid = 'user3';
                    INSERT INTO users (userid, password) VALUES ('user4', 'ch@ngem3c');
                    DELETE name FROM users WHERE userid = 'user1';
                APPLY BATCH;
            """)

    @since('1.2')
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

        if self.cluster.version() < '1.2':
            cursor.execute("SELECT k FROM test WHERE token(k) >= 0")
        else:
            min_token = -2**63
            cursor.execute("SELECT k FROM test WHERE token(k) >= %d" % min_token)
        res = cursor.fetchall()
        assert len(res) == c, "%s [all: %s]" % (str(res), str(inOrder))

        #assert_invalid(cursor, "SELECT k FROM test WHERE token(k) >= 0")
        #cursor.execute("SELECT k FROM test WHERE token(k) >= 0")

        cursor.execute("SELECT k FROM test WHERE token(k) >= token(%d) AND token(k) < token(%d)" % (inOrder[32], inOrder[65]))
        res = cursor.fetchall()
        assert res == [ [inOrder[x]] for x in range(32, 65) ], "%s [all: %s]" % (str(res), str(inOrder))

    @since('1.2')
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
               AND compaction = { 'class' : 'LeveledCompactionStrategy',
                                  'sstable_size_in_mb' : 10 }
               AND compression = { 'sstable_compression' : '' }
               AND caching = 'all'
        """)

        cursor.execute("""
            ALTER TABLE test
            WITH comment = 'other comment'
             AND read_repair_chance = 0.3
             AND dclocal_read_repair_chance = 0.3
             AND gc_grace_seconds = 100
             AND bloom_filter_fp_chance = 0.1
             AND compaction = { 'class' : 'SizeTieredCompactionStrategy',
                                'min_sstable_size' : 42 }
             AND compression = { 'sstable_compression' : 'SnappyCompressor' }
             AND caching = 'rows_only'
        """)

    @since('1.1')
    def timestamp_and_ttl_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c text,
                d text
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

        cursor.execute("SELECT k, d, writetime(d) FROM test WHERE k = 1")
        res = cursor.fetchall()
        assert res == [[1, None, None]]

    @since('1.2')
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

        # Example from #3505
        cursor.execute("CREATE KEYSPACE ks1 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        cursor.execute("USE ks1")
        cursor.execute("""
            CREATE COLUMNFAMILY users (
                KEY varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                birth_year bigint)
        """)

        cursor.execute("INSERT INTO users (KEY, password) VALUES ('user1', 'ch@ngem3a')")
        cursor.execute("UPDATE users SET gender = 'm', birth_year = 1980 WHERE KEY = 'user1'")
        cursor.execute("SELECT * FROM users WHERE KEY='user1'")
        res = cursor.fetchall()
        assert res == [[ 'user1', 1980, 'm', 'ch@ngem3a' ]], res

        cursor.execute("TRUNCATE users")

        cursor.execute("SELECT * FROM users")
        res = cursor.fetchall()
        assert res == [], res

        cursor.execute("SELECT * FROM users WHERE KEY='user1'")
        res = cursor.fetchall()
        assert res == [], res


    @since('1.1')
    def undefined_column_handling_test(self):
        cursor = self.prepare(ordered=True)

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

    @since('1.2')
    def range_tombstones_test(self):
        """ Test deletion by 'composite prefix' (range tombstones) """
        cluster = self.cluster

        # Uses 3 nodes just to make sure RowMutation are correctly serialized
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1, version=cql_version).cursor()
        self.create_ks(cursor, 'ks', 1)

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                v1 int,
                v2 int,
                PRIMARY KEY (k, c1, c2)
            );
        """)
        time.sleep(1)

        rows = 5
        col1 = 2
        col2 = 2
        cpr = col1 * col2
        for i in xrange(0, rows):
            for j in xrange(0, col1):
                for k in xrange(0, col2):
                    n = (i * cpr) + (j * col2) + k
                    cursor.execute("INSERT INTO test1 (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)" % (i, j, k, n, n))

        for i in xrange(0, rows):
            cursor.execute("SELECT v1, v2 FROM test1 where k = %d" % i)
            res = cursor.fetchall()
            assert res == [[x, x] for x in xrange(i * cpr, (i + 1) * cpr)], res

        for i in xrange(0, rows):
            cursor.execute("DELETE FROM test1 WHERE k = %d AND c1 = 0" % i)

        for i in xrange(0, rows):
            cursor.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            res = cursor.fetchall()
            assert res == [[x, x] for x in xrange(i * cpr + col1, (i + 1) * cpr)], res

        cluster.flush()
        time.sleep(0.2)

        for i in xrange(0, rows):
            cursor.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            res = cursor.fetchall()
            assert res == [[x, x] for x in xrange(i * cpr + col1, (i + 1) * cpr)], res

    @since('1.2')
    def range_tombstones_compaction_test(self):
        """ Test deletion by 'composite prefix' (range tombstones) with compaction """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test1 (
                k int,
                c1 int,
                c2 int,
                v1 text,
                PRIMARY KEY (k, c1, c2)
            );
        """)


        for c1 in range(0, 4):
            for c2 in range(0, 2):
                cursor.execute("INSERT INTO test1 (k, c1, c2, v1) VALUES (0, %d, %d, '%s')" % (c1, c2, '%i%i' % (c1, c2)))

        self.cluster.flush()

        cursor.execute("DELETE FROM test1 WHERE k = 0 AND c1 = 1")

        self.cluster.flush()
        self.cluster.compact()

        cursor.execute("SELECT v1 FROM test1 WHERE k = 0")
        res = cursor.fetchall()
        assert res == [ ['%i%i' % (c1, c2)] for c1 in xrange(0, 4) for c2 in xrange(0, 2) if c1 != 1], res

    @since('1.2')
    def delete_row_test(self):
        """ Test deletion of rows """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                 k int,
                 c1 int,
                 c2 int,
                 v1 int,
                 v2 int,
                 PRIMARY KEY (k, c1, c2)
            );
        """)

        q = "INSERT INTO test (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)"
        cursor.execute(q % (0, 0, 0, 0, 0))
        cursor.execute(q % (0, 0, 1, 1, 1))
        cursor.execute(q % (0, 0, 2, 2, 2))
        cursor.execute(q % (0, 1, 0, 3, 3))

        cursor.execute("DELETE FROM test WHERE k = 0 AND c1 = 0 AND c2 = 0")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert len(res) == 3, res

    @since('1.2')
    def range_query_2ndary_test(self):
        """ Test range queries with 2ndary indexes (#4257) """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE indextest (id int primary key, row int, setid int);")
        cursor.execute("CREATE INDEX indextest_setid_idx ON indextest (setid)")

        q =  "INSERT INTO indextest (id, row, setid) VALUES (%d, %d, %d);"
        cursor.execute(q % (0, 0, 0))
        cursor.execute(q % (1, 1, 0))
        cursor.execute(q % (2, 2, 0))
        cursor.execute(q % (3, 3, 0))

        assert_invalid(cursor, "SELECT * FROM indextest WHERE setid = 0 AND row < 1;")
        cursor.execute("SELECT * FROM indextest WHERE setid = 0 AND row < 1 ALLOW FILTERING;")
        res = cursor.fetchall()
        assert res == [[0, 0, 0]], res

    @since('1.1')
    def compression_option_validation_test(self):
        """ Check for unknown compression parameters options (#4266) """
        cursor = self.prepare()

        assert_invalid(cursor, """
          CREATE TABLE users (key varchar PRIMARY KEY, password varchar, gender varchar)
          WITH compression_parameters:sstable_compressor = 'DeflateCompressor';
        """)

        if self.cluster.version() >= '1.2':
            assert_invalid(cursor, """
              CREATE TABLE users (key varchar PRIMARY KEY, password varchar, gender varchar)
              WITH compression = { 'sstable_compressor' : 'DeflateCompressor' };
            """)

    @since('1.1')
    def keyspace_creation_options_test(self):
        """ Check one can use arbitrary name for datacenter when creating keyspace (#4278) """
        cursor = self.prepare()

        # we just want to make sure the following is valid
        if self.cluster.version() >= '1.2':
            cursor.execute("""
                CREATE KEYSPACE Foo
                    WITH replication = { 'class' : 'NetworkTopologyStrategy',
                                         'us-east' : 1,
                                         'us-west' : 1 };
            """)
        else:
            cursor.execute("""
                CREATE KEYSPACE Foo
                    WITH strategy_class='NetworkTopologyStrategy'
                     AND strategy_options:"us-east"=1
                     AND strategy_options:"us-west"=1;
            """)

    @since('1.2')
    def set_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                tags set<text>,
                PRIMARY KEY (fn, ln)
            )
        """)

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        cursor.execute(q % "tags = tags + { 'foo' }")
        cursor.execute(q % "tags = tags + { 'bar' }")
        cursor.execute(q % "tags = tags + { 'foo' }")
        cursor.execute(q % "tags = tags + { 'foobar' }")
        cursor.execute(q % "tags = tags - { 'bar' }")

        cursor.execute("SELECT tags FROM user")
        res = cursor.fetchall()
        assert res == [[set(['foo', 'foobar'])]], res

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        cursor.execute(q % "tags = { 'a', 'c', 'b' }")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[set(['a', 'b', 'c'])]], res

        time.sleep(.01)

        cursor.execute(q % "tags = { 'm', 'n' }")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[set(['m', 'n'])]], res

        cursor.execute("DELETE tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[None]], re


    @since('1.2')
    def map_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                m map<text, int>,
                PRIMARY KEY (fn, ln)
            )
        """)

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        cursor.execute(q % "m['foo'] = 3")
        cursor.execute(q % "m['bar'] = 4")
        cursor.execute(q % "m['woot'] = 5")
        cursor.execute(q % "m['bar'] = 6")
        cursor.execute("DELETE m['foo'] FROM user WHERE fn='Tom' AND ln='Bombadil'")

        cursor.execute("SELECT m FROM user")
        res = cursor.fetchall()
        assert res == [[{ 'woot': 5, 'bar' : 6 }]], res

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        cursor.execute(q % "m = { 'a' : 4 , 'c' : 3, 'b' : 2 }")
        cursor.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[ {'a' : 4, 'b' : 2, 'c' : 3 } ]], res

        time.sleep(.01)

        # Check we correctly overwrite
        cursor.execute(q % "m = { 'm' : 4 , 'n' : 1, 'o' : 2 }")
        cursor.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[ {'m' : 4, 'n' : 1, 'o' : 2 } ]], res

        cursor.execute(q % "m = {}")
        cursor.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[ None ]], res

    @since('1.2')
    def list_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                tags list<text>,
                PRIMARY KEY (fn, ln)
            )
        """)

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        cursor.execute(q % "tags = tags + [ 'foo' ]")
        cursor.execute(q % "tags = tags + [ 'bar' ]")
        cursor.execute(q % "tags = tags + [ 'foo' ]")
        cursor.execute(q % "tags = tags + [ 'foobar' ]")

        cursor.execute("SELECT tags FROM user")
        res = cursor.fetchall()
        assert res == [[ ('foo', 'bar', 'foo', 'foobar') ]], res

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        cursor.execute(q % "tags = [ 'a', 'c', 'b', 'c' ]")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[ ('a', 'c', 'b', 'c') ]], res

        cursor.execute(q % "tags = [ 'm', 'n' ] + tags")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[ ('n', 'm', 'a', 'c', 'b', 'c') ]], res

        cursor.execute(q % "tags[2] = 'foo', tags[4] = 'bar'")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[ ('n', 'm', 'foo', 'c', 'bar', 'c') ]], res

        cursor.execute("DELETE tags[2] FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[ ('n', 'm', 'c', 'bar', 'c') ]], res

        cursor.execute(q % "tags = tags - [ 'bar' ]")
        cursor.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = cursor.fetchall()
        assert res == [[ ('n', 'm', 'c', 'c') ]], res

    @since('1.2')
    def multi_collection_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE foo(
                k uuid PRIMARY KEY,
                L list<int>,
                M map<text, int>,
                S set<int>
            );
        """)

        cursor.execute("UPDATE ks.foo SET L = [1, 3, 5] WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        cursor.execute("UPDATE ks.foo SET L = L + [7, 11, 13] WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        cursor.execute("UPDATE ks.foo SET S = {1, 3, 5} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        cursor.execute("UPDATE ks.foo SET S = S + {7, 11, 13} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        cursor.execute("UPDATE ks.foo SET M = {'foo': 1, 'bar' : 3} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        cursor.execute("UPDATE ks.foo SET M = M + {'foobar' : 4} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")

        cursor.execute("SELECT L, M, S FROM foo WHERE k = b017f48f-ae67-11e1-9096-005056c00008")
        res = cursor.fetchall()
        assert res == [[
            (1, 3, 5, 7, 11, 13),
            {'foo' : 1, 'bar' : 3, 'foobar' : 4},
            set([1, 3, 5, 7, 11, 13]),
        ]], res

    @since('1.1')
    def range_query_test(self):
        """ Range test query from #4372 """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (a int, b int, c int, d int, e int, f text, PRIMARY KEY (a, b, c, d, e) )")

        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 2, '2');")
        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, '1');")
        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 2, 1, '1');")
        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 3, '3');")
        cursor.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 5, '5');")

        cursor.execute("SELECT a, b, c, d, e, f FROM test WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 AND e >= 2;")
        res = cursor.fetchall()
        assert res == [[1, 1, 1, 1, 2, u'2'], [1, 1, 1, 1, 3, u'3'], [1, 1, 1, 1, 5, u'5']], res

    @since('1.1.2')
    def update_type_test(self):
        """ Test altering the type of a column, including the one in the primary key (#4041) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k text,
                c text,
                v text,
                PRIMARY KEY (k, c)
            )
        """)

        req = "INSERT INTO test (k, c, v) VALUES ('%s', '%s', '%s')"
        # using utf8 character so that we can see the transition to BytesType
        cursor.execute(req % ('ɸ', 'ɸ', 'ɸ'))

        cursor.execute("SELECT * FROM test")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[u'ɸ', u'ɸ', u'ɸ']], res

        cursor.execute("ALTER TABLE test ALTER v TYPE blob")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        # the last should not be utf8 but a raw string
        assert res == [[u'ɸ', u'ɸ', 'ɸ']], res

        cursor.execute("ALTER TABLE test ALTER k TYPE blob")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [['ɸ', u'ɸ', 'ɸ']], res

        cursor.execute("ALTER TABLE test ALTER c TYPE blob")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [['ɸ', 'ɸ', 'ɸ']], res

    @since('1.2')
    def composite_row_key_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k1 int,
                k2 int,
                c int,
                v int,
                PRIMARY KEY ((k1, k2), c)
            )
        """)

        req = "INSERT INTO test (k1, k2, c, v) VALUES (%d, %d, %d, %d)"
        for i in range(0, 4):
            cursor.execute(req % (0, i, i, i))

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]], res

        cursor.execute("SELECT * FROM test WHERE k1 = 0 and k2 IN (1, 3)")
        res = cursor.fetchall()
        assert res == [[0, 1, 1, 1], [0, 3, 3, 3]], res

        assert_invalid(cursor, "SELECT * FROM test WHERE k2 = 3")
        assert_invalid(cursor, "SELECT * FROM test WHERE k1 IN (0, 1) and k2 = 3")

        cursor.execute("SELECT * FROM test WHERE token(k1, k2) = token(0, 1)")
        res = cursor.fetchall()
        assert res == [[0, 1, 1, 1]], res

        cursor.execute("SELECT * FROM test WHERE token(k1, k2) > " + str(-((2**63)-1)))
        res = cursor.fetchall()
        assert res == [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]], res

    @since('1.2')
    def cql3_insert_thrift_test(self):
        """ Check that we can insert from thrift into a CQL3 table (#4377) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        cli = self.cluster.nodelist()[0].cli()
        cli.do("use ks")
        cli.do("set test[2]['4:v'] = int(200)")
        assert not cli.has_errors(), cli.errors()

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [ [2, 4, 200] ], res

    @since('1.2')
    def row_existence_test(self):
        """ Check the semantic of CQL row existence (part of #4361) """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v1 int,
                v2 int,
                PRIMARY KEY (k, c)
            )
        """)

        cursor.execute("INSERT INTO test (k, c, v1, v2) VALUES (1, 1, 1, 1)")

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[1, 1, 1, 1]], res

        assert_invalid(cursor, "DELETE c FROM test WHERE k = 1 AND c = 1")

        cursor.execute("DELETE v2 FROM test WHERE k = 1 AND c = 1")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[1, 1, 1, None]], res

        cursor.execute("DELETE v1 FROM test WHERE k = 1 AND c = 1")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[1, 1, None, None]], res

        cursor.execute("DELETE FROM test WHERE k = 1 AND c = 1")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [], res

        cursor.execute("INSERT INTO test (k, c) VALUES (2, 2)")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[2, 2, None, None]], res

    @since('1.2')
    def only_pk_test(self):
        """ Check table with only a PK (#4361) """
        cursor = self.prepare(ordered=True)

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                PRIMARY KEY (k, c)
            )
        """)

        q = "INSERT INTO test (k, c) VALUES (%d, %d)"
        for k in range(0, 2):
            for c in range(0, 2):
                cursor.execute(q % (k, c))

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[x, y] for x in range(0, 2) for y in range(0, 2)], res

        # Check for dense tables too
        cursor.execute("""
            CREATE TABLE test2 (
                k int,
                c int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE
        """)

        q = "INSERT INTO test2 (k, c) VALUES (%d, %d)"
        for k in range(0, 2):
            for c in range(0, 2):
                cursor.execute(q % (k, c))

        cursor.execute("SELECT * FROM test2")
        res = cursor.fetchall()
        assert res == [[x, y] for x in range(0, 2) for y in range(0, 2)], res

    @since('1.2')
    def date_test(self):
        """ Check dates are correctly recognized and validated """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                t timestamp
            )
        """)

        cursor.execute("INSERT INTO test (k, t) VALUES (0, '2011-02-03')")
        assert_invalid(cursor, "INSERT INTO test (k, t) VALUES (0, '2011-42-42')")

    @since('1.1')
    def range_slice_test(self):
        """ Test a regression from #1337 """

        cluster = self.cluster

        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1, version=cql_version).cursor()
        self.create_ks(cursor, 'ks', 1)

        cursor.execute("""
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int
            );
        """)
        time.sleep(1)

        cursor.execute("INSERT INTO test (k, v) VALUES ('foo', 0)")
        cursor.execute("INSERT INTO test (k, v) VALUES ('bar', 1)")

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert len(res) == 2, res

    @since('1.2')
    def composite_index_with_pk_test(self):

        cursor = self.prepare(ordered=True)
        cursor.execute("""
            CREATE TABLE blogs (
                blog_id int,
                time1 int,
                time2 int,
                author text,
                content text,
                PRIMARY KEY (blog_id, time1, time2)
            )
        """)

        cursor.execute("CREATE INDEX ON blogs(author)")

        req = "INSERT INTO blogs (blog_id, time1, time2, author, content) VALUES (%d, %d, %d, '%s', '%s')"
        cursor.execute(req % (1, 0, 0, 'foo', 'bar1'))
        cursor.execute(req % (1, 0, 1, 'foo', 'bar2'))
        cursor.execute(req % (2, 1, 0, 'foo', 'baz'))
        cursor.execute(req % (3, 0, 1, 'gux', 'qux'))


        cursor.execute("SELECT blog_id, content FROM blogs WHERE author='foo'")
        res = cursor.fetchall()
        assert res == [[1, 'bar1'], [1, 'bar2'], [2, 'baz']], res

        cursor.execute("SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo'")
        res = cursor.fetchall()
        assert res == [[2, 'baz']], res

        cursor.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo'")
        res = cursor.fetchall()
        assert res == [[2, 'baz']], res

        cursor.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo'")
        res = cursor.fetchall()
        assert res == [[2, 'baz']], res

        cursor.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo'")
        res = cursor.fetchall()
        assert res == [], res

        cursor.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo'")
        res = cursor.fetchall()
        assert res == [], res

        assert_invalid(cursor, "SELECT content FROM blogs WHERE time2 >= 0 AND author='foo'")

    @since('1.2')
    def limit_bugs_test(self):
        """ Test for LIMIT bugs from 4579 """

        cursor = self.prepare(ordered=True)
        cursor.execute("""
            CREATE TABLE testcf (
                a int,
                b int,
                c int,
                d int,
                e int,
                PRIMARY KEY (a, b)
            );
        """)

        cursor.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (1, 1, 1, 1, 1);")
        cursor.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (2, 2, 2, 2, 2);")
        cursor.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (3, 3, 3, 3, 3);")
        cursor.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (4, 4, 4, 4, 4);")

        cursor.execute("SELECT * FROM testcf;")
        res = cursor.fetchall()
        assert res == [[1, 1, 1, 1, 1], [2, 2, 2, 2, 2], [3, 3, 3, 3, 3], [4, 4, 4, 4, 4]], res

        cursor.execute("SELECT * FROM testcf LIMIT 1;") # columns d and e in result row are null
        res = cursor.fetchall()
        assert res == [[1, 1, 1, 1, 1]], res

        cursor.execute("SELECT * FROM testcf LIMIT 2;") # columns d and e in last result row are null
        res = cursor.fetchall()
        assert res == [[1, 1, 1, 1, 1], [2, 2, 2, 2, 2]], res

        cursor.execute("""
            CREATE TABLE testcf2 (
                a int primary key,
                b int,
                c int,
            );
        """)

        cursor.execute("INSERT INTO testcf2 (a, b, c) VALUES (1, 1, 1);")
        cursor.execute("INSERT INTO testcf2 (a, b, c) VALUES (2, 2, 2);")
        cursor.execute("INSERT INTO testcf2 (a, b, c) VALUES (3, 3, 3);")
        cursor.execute("INSERT INTO testcf2 (a, b, c) VALUES (4, 4, 4);")

        cursor.execute("SELECT * FROM testcf2;")
        res = cursor.fetchall()
        assert res == [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]], res

        cursor.execute("SELECT * FROM testcf2 LIMIT 1;") # gives 1 row
        res = cursor.fetchall()
        assert res == [[1, 1, 1]], res

        cursor.execute("SELECT * FROM testcf2 LIMIT 2;") # gives 1 row
        res = cursor.fetchall()
        assert res == [[1, 1, 1], [2, 2, 2]], res

        cursor.execute("SELECT * FROM testcf2 LIMIT 3;") # gives 2 rows
        res = cursor.fetchall()
        assert res == [[1, 1, 1], [2, 2, 2], [3, 3, 3]], res

        cursor.execute("SELECT * FROM testcf2 LIMIT 4;") # gives 2 rows
        res = cursor.fetchall()
        assert res == [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]], res

        cursor.execute("SELECT * FROM testcf2 LIMIT 5;") # gives 3 rows
        res = cursor.fetchall()
        assert res == [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]], res

    @since('1.1')
    def bug_4532_test(self):

        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE compositetest(
                status ascii,
                ctime bigint,
                key ascii,
                nil ascii,
                PRIMARY KEY (status, ctime, key)
            )
        """)

        cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345678,'key1','')")
        cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345678,'key2','')")
        cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key3','')")
        cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key4','')")
        cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key5','')")
        cursor.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345680,'key6','')")

        assert_invalid(cursor, "SELECT * FROM compositetest WHERE ctime>=12345679 AND key='key3' AND ctime<=12345680 LIMIT 3;")
        assert_invalid(cursor, "SELECT * FROM compositetest WHERE ctime=12345679  AND key='key3' AND ctime<=12345680 LIMIT 3")

    @since('1.1')
    def order_by_multikey_test(self):
        """ Test for #4612 bug and more generaly order by when multiple C* rows are queried """

        cursor = self.prepare(ordered=True)
        cursor.execute("""
            CREATE TABLE test(
                my_id varchar,
                col1 int,
                col2 int,
                value varchar,
                PRIMARY KEY (my_id, col1, col2)
            );
        """)

        cursor.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key1', 1, 1, 'a');")
        cursor.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key2', 3, 3, 'a');")
        cursor.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key3', 2, 2, 'b');")
        cursor.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key4', 2, 1, 'b');")

        cursor.execute("SELECT col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1;")
        res = cursor.fetchall()
        assert res == [[1], [2], [3]], res

        cursor.execute("SELECT col1, value, my_id, col2 FROM test WHERE my_id in('key3', 'key4') ORDER BY col1, col2;")
        res = cursor.fetchall()
        assert res == [[2, 'b', 'key4', 1], [2, 'b', 'key3', 2]], res

        assert_invalid(cursor, "SELECT col1 FROM test ORDER BY col1;")
        assert_invalid(cursor, "SELECT col1 FROM test WHERE my_id > 'key1' ORDER BY col1;")

    @since('1.2')
    def create_alter_options_test(self):
        cursor = self.prepare(create_keyspace=False)

        assert_invalid(cursor, "CREATE KEYSPACE ks1")
        assert_invalid(cursor, "CREATE KEYSPACE ks1 WITH replication= { 'replication_factor' : 1 }")

        cursor.execute("CREATE KEYSPACE ks1 WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
        cursor.execute("CREATE KEYSPACE ks2 WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes=false")

        cursor.execute("SELECT keyspace_name, durable_writes FROM system.schema_keyspaces")
        res = cursor.fetchall()
        assert res == [ ['system_auth', True], ['ks1', True], ['system', True], ['system_traces', True], ['ks2', False] ], res

        cursor.execute("ALTER KEYSPACE ks1 WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND durable_writes=False")
        cursor.execute("ALTER KEYSPACE ks2 WITH durable_writes=true")
        cursor.execute("SELECT keyspace_name, durable_writes, strategy_class FROM system.schema_keyspaces")
        res = cursor.fetchall()
        assert res == [ [u'system_auth', True, u'org.apache.cassandra.locator.SimpleStrategy'],
                      [u'ks1', False, u'org.apache.cassandra.locator.NetworkTopologyStrategy'],
                      [u'system', True, u'org.apache.cassandra.locator.LocalStrategy'],
                      [u'system_traces', True, u'org.apache.cassandra.locator.SimpleStrategy'],
                      [u'ks2', True, u'org.apache.cassandra.locator.SimpleStrategy'] ]

        cursor.execute("USE ks1")

        assert_invalid(cursor, "CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'min_threshold' : 4 }")
        cursor.execute("CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }")
        cursor.execute("SELECT columnfamily_name, min_compaction_threshold FROM system.schema_columnfamilies WHERE keyspace_name='ks1'")
        res = cursor.fetchall()
        assert res == [ ['cf1', 7] ], res

    @since('1.1')
    def remove_range_slice_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """)

        for i in range(0, 3):
            cursor.execute("INSERT INTO test (k, v) VALUES (%d, %d)" % (i, i))

        cursor.execute("DELETE FROM test WHERE k = 1")
        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert res == [[0, 0], [2, 2]], res

    @since('1.2')
    def indexes_composite_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                blog_id int,
                timestamp int,
                author text,
                content text,
                PRIMARY KEY (blog_id, timestamp)
            )
        """)

        req = "INSERT INTO test (blog_id, timestamp, author, content) VALUES (%d, %d, '%s', '%s')"
        cursor.execute(req % (0, 0, "bob", "1st post"))
        cursor.execute(req % (0, 1, "tom", "2nd post"))
        cursor.execute(req % (0, 2, "bob", "3rd post"))
        cursor.execute(req % (0, 3, "tom", "4nd post"))
        cursor.execute(req % (1, 0, "bob", "5th post"))

        cursor.execute("CREATE INDEX ON test(author)")
        time.sleep(1)

        cursor.execute("SELECT blog_id, timestamp FROM test WHERE author = 'bob'")
        res = cursor.fetchall()
        assert res == [[1, 0], [0, 0], [0, 2]], res

        cursor.execute(req % (1, 1, "tom", "6th post"))
        cursor.execute(req % (1, 2, "tom", "7th post"))
        cursor.execute(req % (1, 3, "bob", "8th post"))

        cursor.execute("SELECT blog_id, timestamp FROM test WHERE author = 'bob'")
        res = cursor.fetchall()
        assert res == [[1, 0], [1, 3], [0, 0], [0, 2]], res

    @since('1.1')
    def refuse_in_with_indexes_test(self):
        """ Test for the validation bug of #4709 """

        cursor = self.prepare()
        cursor.execute("create table t1 (pk varchar primary key, col1 varchar, col2 varchar);")
        cursor.execute("create index t1_c1 on t1(col1);")
        cursor.execute("create index t1_c2 on t1(col2);")
        cursor.execute("insert into t1  (pk, col1, col2) values ('pk1','foo1','bar1');")
        cursor.execute("insert into t1  (pk, col1, col2) values ('pk1a','foo1','bar1');")
        cursor.execute("insert into t1  (pk, col1, col2) values ('pk1b','foo1','bar1');")
        cursor.execute("insert into t1  (pk, col1, col2) values ('pk1c','foo1','bar1');")
        cursor.execute("insert into t1  (pk, col1, col2) values ('pk2','foo2','bar2');")
        cursor.execute("insert into t1  (pk, col1, col2) values ('pk3','foo3','bar3');")
        assert_invalid(cursor, "select * from t1 where col2 in ('bar1', 'bar2');")

    @since('1.2')
    def validate_counter_regular_test(self):
        """ Test for the validation bug of #4706 """

        cursor = self.prepare()
        assert_invalid(cursor, "CREATE TABLE test (id bigint PRIMARY KEY, count counter, things set<text>)")

    @since('1.1')
    def reversed_compact_test(self):
        """ Test for #4716 bug and more generally for good behavior of ordering"""

        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test1 (
                k text,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE
              AND CLUSTERING ORDER BY (c DESC);
        """)

        for i in range(0, 10):
            cursor.execute("INSERT INTO test1(k, c, v) VALUES ('foo', %i, %i)" % (i, i))

        cursor.execute("SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo'")
        res = cursor.fetchall()
        assert res == [[5], [4], [3]], res

        cursor.execute("SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo'")
        res = cursor.fetchall()
        assert res == [[6], [5], [4], [3], [2]], res

        cursor.execute("SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC")
        res = cursor.fetchall()
        assert res == [[3], [4], [5]], res

        cursor.execute("SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC")
        res = cursor.fetchall()
        assert res == [[2], [3], [4], [5], [6]], res

        cursor.execute("SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC")
        res = cursor.fetchall()
        assert res == [[5], [4], [3]], res

        cursor.execute("SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC")
        res = cursor.fetchall()
        assert res == [[6], [5], [4], [3], [2]], res

        cursor.execute("""
            CREATE TABLE test2 (
                k text,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        for i in range(0, 10):
            cursor.execute("INSERT INTO test2(k, c, v) VALUES ('foo', %i, %i)" % (i, i))

        cursor.execute("SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo'")
        res = cursor.fetchall()
        assert res == [[3], [4], [5]], res

        cursor.execute("SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo'")
        res = cursor.fetchall()
        assert res == [[2], [3], [4], [5], [6]], res

        cursor.execute("SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC")
        res = cursor.fetchall()
        assert res == [[3], [4], [5]], res

        cursor.execute("SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC")
        res = cursor.fetchall()
        assert res == [[2], [3], [4], [5], [6]], res

        cursor.execute("SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC")
        res = cursor.fetchall()
        assert res == [[5], [4], [3]], res

        cursor.execute("SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC")
        res = cursor.fetchall()
        assert res == [[6], [5], [4], [3], [2]], res

    @since('1.1')
    def unescaped_string_test(self):

        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test (
                k text PRIMARY KEY,
                c text,
            )
        """)

        assert_invalid(cursor, "INSERT INTO test (k, c) VALUES ('foo', 'CQL is cassandra's best friend')")

    @since('1.1')
    def reversed_compact_multikey_test(self):
        """ Test for the bug from #4760 and #4759 """

        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test (
                key text,
                c1 int,
                c2 int,
                value text,
                PRIMARY KEY(key, c1, c2)
                ) WITH COMPACT STORAGE
                  AND CLUSTERING ORDER BY(c1 DESC, c2 DESC);
        """)

        for i in range(0, 3):
            for j in range(0, 3):
                cursor.execute("INSERT INTO test(key, c1, c2, value) VALUES ('foo', %i, %i, 'bar');" % (i, j))

        # Equalities

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1")
        res = cursor.fetchall()
        assert res == [[1, 2], [1, 1], [1, 0]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1 ORDER BY c1 ASC, c2 ASC")
        res = cursor.fetchall()
        assert res == [[1, 0], [1, 1], [1, 2]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1 ORDER BY c1 DESC, c2 DESC")
        res = cursor.fetchall()
        assert res == [[1, 2], [1, 1], [1, 0]], res

        # GT

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1")
        res = cursor.fetchall()
        assert res == [[2, 2], [2, 1], [2, 0]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1 ORDER BY c1 ASC, c2 ASC")
        res = cursor.fetchall()
        assert res == [[2, 0], [2, 1], [2, 2]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1 ORDER BY c1 DESC, c2 DESC")
        res = cursor.fetchall()
        assert res == [[2, 2], [2, 1], [2, 0]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1")
        res = cursor.fetchall()
        assert res == [[2, 2], [2, 1], [2, 0], [1, 2], [1, 1], [1, 0]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC, c2 ASC")
        res = cursor.fetchall()
        assert res == [[1, 0], [1, 1], [1, 2], [2, 0], [2, 1], [2, 2]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC")
        res = cursor.fetchall()
        assert res == [[1, 0], [1, 1], [1, 2], [2, 0], [2, 1], [2, 2]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 DESC, c2 DESC")
        res = cursor.fetchall()
        assert res == [[2, 2], [2, 1], [2, 0], [1, 2], [1, 1], [1, 0]], res

        # LT

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1")
        res = cursor.fetchall()
        assert res == [[0, 2], [0, 1], [0, 0]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1 ORDER BY c1 ASC, c2 ASC")
        res = cursor.fetchall()
        assert res == [[0, 0], [0, 1], [0, 2]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1 ORDER BY c1 DESC, c2 DESC")
        res = cursor.fetchall()
        assert res == [[0, 2], [0, 1], [0, 0]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1")
        res = cursor.fetchall()
        assert res == [[1, 2], [1, 1], [1, 0], [0, 2], [0, 1], [0, 0]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC, c2 ASC")
        res = cursor.fetchall()
        assert res == [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC")
        res = cursor.fetchall()
        assert res == [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 DESC, c2 DESC")
        res = cursor.fetchall()
        assert res == [[1, 2], [1, 1], [1, 0], [0, 2], [0, 1], [0, 0]], res

    @since('1.2')
    def collection_and_regular_test(self):

        cursor = self.prepare()

        cursor.execute("""
          CREATE TABLE test (
            k int PRIMARY KEY,
            l list<int>,
            c int
          )
        """)

        cursor.execute("INSERT INTO test(k, l, c) VALUES(3, [0, 1, 2], 4)")
        cursor.execute("UPDATE test SET l[0] = 1, c = 42 WHERE k = 3")
        cursor.execute("SELECT l, c FROM test WHERE k = 3")
        res = cursor.fetchall()
        assert res == [[(1, 1, 2), 42]], res

    @since('1.2')
    def batch_and_list_test(self):
        cursor = self.prepare()

        cursor.execute("""
          CREATE TABLE test (
            k int PRIMARY KEY,
            l list<int>
          )
        """)

        cursor.execute("""
          BEGIN BATCH
            UPDATE test SET l = l + [ 1 ] WHERE k = 0;
            UPDATE test SET l = l + [ 2 ] WHERE k = 0;
            UPDATE test SET l = l + [ 3 ] WHERE k = 0;
          APPLY BATCH
        """)

        cursor.execute("SELECT l FROM test WHERE k = 0")
        res = cursor.fetchall()
        assert res == [[(1, 2, 3)]], res

        cursor.execute("""
          BEGIN BATCH
            UPDATE test SET l = [ 1 ] + l WHERE k = 1;
            UPDATE test SET l = [ 2 ] + l WHERE k = 1;
            UPDATE test SET l = [ 3 ] + l WHERE k = 1;
          APPLY BATCH
        """)

        cursor.execute("SELECT l FROM test WHERE k = 1")
        res = cursor.fetchall()
        assert res == [[(3, 2, 1)]], res

    @since('1.2')
    def boolean_test(self):
        cursor = self.prepare()

        cursor.execute("""
          CREATE TABLE test (
            k boolean PRIMARY KEY,
            b boolean
          )
        """)

        cursor.execute("INSERT INTO test (k, b) VALUES (true, false)")
        cursor.execute("SELECT * FROM test WHERE k = true")
        res = cursor.fetchall()
        assert res == [[True, False]], res

    @since('1.2')
    def multiordering_test(self):
        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test (
                k text,
                c1 int,
                c2 int,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """)

        for i in range(0, 2):
            for j in range(0, 2):
                cursor.execute("INSERT INTO test(k, c1, c2) VALUES ('foo', %i, %i)" % (i, j))

        cursor.execute("SELECT c1, c2 FROM test WHERE k = 'foo'")
        res = cursor.fetchall()
        assert res == [[0, 1], [0, 0], [1, 1], [1, 0]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 ASC, c2 DESC")
        res = cursor.fetchall()
        assert res == [[0, 1], [0, 0], [1, 1], [1, 0]], res

        cursor.execute("SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 DESC, c2 ASC")
        res = cursor.fetchall()
        assert res == [[1, 0], [1, 1], [0, 0], [0, 1]], res

        assert_invalid(cursor, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c2 DESC")
        assert_invalid(cursor, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c2 ASC")
        assert_invalid(cursor, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 ASC, c2 ASC")

    @since('1.2')
    def multiordering_validation_test(self):
        cursor = self.prepare()

        assert_invalid(cursor, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 DESC)")
        assert_invalid(cursor, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC)")
        assert_invalid(cursor, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC, c3 DESC)")

        cursor.execute("CREATE TABLE test1 (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)")
        cursor.execute("CREATE TABLE test2 (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)")

    @since('1.2')
    def bug_4882_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """)

        cursor.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 0, 0, 0);")
        cursor.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 1, 1, 1);")
        cursor.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 0, 2, 2);")
        cursor.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 1, 3, 3);")

        cursor.execute("select * from test where k = 0 limit 1;")
        res = cursor.fetchall()
        assert res == [[0, 0, 2, 2]], res

    @since("1.2")
    def multi_list_set_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                l1 list<int>,
                l2 list<int>
            )
        """)

        cursor.execute("INSERT INTO test (k, l1, l2) VALUES (0, [1, 2, 3], [4, 5, 6])")
        cursor.execute("UPDATE test SET l2[1] = 42, l1[1] = 24  WHERE k = 0")

        cursor.execute("SELECT l1, l2 FROM test WHERE k = 0")
        res = cursor.fetchall()
        assert res == [[(1, 24, 3), (4, 42, 6)]], res

    @since("1.2")
    def buggy_prepare(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                l list<int>,
            )
        """)

        from cql import query
        p = query.prepare_query("INSERT INTO test (k, l) VALUES (0, [?, ?])")
        print p

    @since("1.2")
    def composite_index_collections_test(self):
        cursor = self.prepare(ordered=True)
        cursor.execute("""
            CREATE TABLE blogs (
                blog_id int,
                time1 int,
                time2 int,
                author text,
                content set<text>,
                PRIMARY KEY (blog_id, time1, time2)
            )
        """)

        cursor.execute("CREATE INDEX ON blogs(author)")

        req = "INSERT INTO blogs (blog_id, time1, time2, author, content) VALUES (%d, %d, %d, '%s', %s)"
        cursor.execute(req % (1, 0, 0, 'foo', "{ 'bar1', 'bar2' }"))
        cursor.execute(req % (1, 0, 1, 'foo', "{ 'bar2', 'bar3' }"))
        cursor.execute(req % (2, 1, 0, 'foo', "{ 'baz' }"))
        cursor.execute(req % (3, 0, 1, 'gux', "{ 'qux' }"))

        cursor.execute("SELECT blog_id, content FROM blogs WHERE author='foo'")
        res = cursor.fetchall()
        assert res == [[1, set(['bar1', 'bar2'])], [1, set(['bar2', 'bar3'])], [2, set(['baz'])]], res

        assert_invalid(cursor, "CREATE INDEX ON blogs(content)")

    def truncate_clean_cache_test(self):
        cursor = self.prepare(ordered=True, use_cache=True)

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 int,
            ) WITH CACHING = ALL;
        """)

        for i in range(0, 3):
            cursor.execute("INSERT INTO test(k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i*2))

        cursor.execute("SELECT v1, v2 FROM test WHERE k IN (0, 1, 2)")
        res = cursor.fetchall()
        assert res == [[0, 0], [1, 2], [2, 4]], res

        cursor.execute("TRUNCATE test")

        cursor.execute("SELECT v1, v2 FROM test WHERE k IN (0, 1, 2)")
        res = cursor.fetchall()
        assert res == [], res

    @since('1.2')
    def allow_filtering_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        for i in range(0, 3):
            for j in range(0, 3):
                cursor.execute("INSERT INTO test(k, c, v) VALUES(%d, %d, %d)" % (i, j, j))

        # Don't require filtering, always allowed
        queries = [ "SELECT * FROM test WHERE k = 1",
                    "SELECT * FROM test WHERE k = 1 AND c > 2",
                    "SELECT * FROM test WHERE k = 1 AND c = 2" ]
        for q in queries:
            cursor.execute(q)
            cursor.execute(q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = [ "SELECT * FROM test WHERE c = 2",
                    "SELECT * FROM test WHERE c > 2 AND c <= 4" ]
        for q in queries:
            assert_invalid(cursor, q)
            cursor.execute(q + " ALLOW FILTERING")

        cursor.execute("""
            CREATE TABLE indexed (
                k int PRIMARY KEY,
                a int,
                b int,
            )
        """)

        cursor.execute("CREATE INDEX ON indexed(a)")

        for i in range(0, 5):
            cursor.execute("INSERT INTO indexed(k, a, b) VALUES(%d, %d, %d)" % (i, i * 10, i * 100))

        # Don't require filtering, always allowed
        queries = [ "SELECT * FROM indexed WHERE k = 1",
                    "SELECT * FROM indexed WHERE a = 20" ]
        for q in queries:
            cursor.execute(q)
            cursor.execute(q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = [ "SELECT * FROM indexed WHERE a = 20 AND b = 200" ]
        for q in queries:
            assert_invalid(cursor, q)
            cursor.execute(q + " ALLOW FILTERING")

    @since('1.2')
    def range_with_deletes_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int,
            )
        """)

        nb_keys = 30
        nb_deletes = 5

        for i in range(0, nb_keys):
            cursor.execute("INSERT INTO test(k, v) VALUES (%d, %d)" % (i, i))

        for i in random.sample(xrange(nb_keys), nb_deletes):
            cursor.execute("DELETE FROM test WHERE k = %d" % i)

        cursor.execute("SELECT * FROM test LIMIT %d" % (nb_keys/2))
        res = cursor.fetchall()
        assert len(res) == nb_keys/2, "Expected %d but got %d" % (nb_keys/2, len(res))

    @since('1.2')
    def alter_with_collections_test(self):
        """ Test you can add columns in a table with collections (#4982 bug) """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE collections (key int PRIMARY KEY, aset set<text>)")
        cursor.execute("ALTER TABLE collections ADD c text")
        cursor.execute("ALTER TABLE collections ADD alist list<text>")

    @since('1.2')
    def collection_compact_test(self):
        cursor = self.prepare()

        assert_invalid(cursor, """
            CREATE TABLE test (
                user ascii PRIMARY KEY,
                mails list<text>
            ) WITH COMPACT STORAGE;
        """)

    @since('1.2')
    def collection_function_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                l set<int>
            )
        """)

        assert_invalid(cursor, "SELECT ttl(l) FROM test WHERE k = 0")
        assert_invalid(cursor, "SELECT writetime(l) FROM test WHERE k = 0")

    @since('1.2')
    def collection_counter_test(self):
        cursor = self.prepare()

        assert_invalid(cursor, """
            CREATE TABLE test (
                k int PRIMARY KEY,
                l list<counter>
            )
        """)

        assert_invalid(cursor, """
            CREATE TABLE test (
                k int PRIMARY KEY,
                s set<counter>
            )
        """)

        assert_invalid(cursor, """
            CREATE TABLE test (
                k int PRIMARY KEY,
                m map<text, counter>
            )
        """)

    @since('1.2')
    def composite_partition_key_validation_test(self):
        """ Test for bug from #5122 """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE foo (a int, b text, c uuid, PRIMARY KEY ((a, b)));")

        cursor.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'aze', 4d481800-4c5f-11e1-82e0-3f484de45426)")
        cursor.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'ert', 693f5800-8acb-11e3-82e0-3f484de45426)")
        cursor.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'opl', d4815800-2d8d-11e0-82e0-3f484de45426)")

        cursor.execute("SELECT * FROM foo")
        res = cursor.fetchall()
        assert len(res) == 3, res

        assert_invalid(cursor, "SELECT * FROM foo WHERE a=1")

    @require('4762')
    def multi_in_test(self):
        self.__multi_in(False)

    @require('4762')
    def multi_in_compact_test(self):
        self.__multi_in(True)

    def __multi_in(self, compact):
        cursor = self.prepare()

        data = [
            ( 'test', '06029', 'CT',  9, 'Ellington'     ),
            ( 'test', '06031', 'CT',  9, 'Falls Village' ),
            ( 'test', '06902', 'CT',  9, 'Stamford'      ),
            ( 'test', '06927', 'CT',  9, 'Stamford'      ),
            ( 'test', '10015', 'NY', 36, 'New York'      ),
            ( 'test', '07182', 'NJ', 34, 'Newark'        ),
            ( 'test', '73301', 'TX', 48, 'Austin'        ),
            ( 'test', '94102', 'CA', 06, 'San Francisco' ),

            ( 'test2', '06029', 'CT',  9, 'Ellington'     ),
            ( 'test2', '06031', 'CT',  9, 'Falls Village' ),
            ( 'test2', '06902', 'CT',  9, 'Stamford'      ),
            ( 'test2', '06927', 'CT',  9, 'Stamford'      ),
            ( 'test2', '10015', 'NY', 36, 'New York'      ),
            ( 'test2', '07182', 'NJ', 34, 'Newark'        ),
            ( 'test2', '73301', 'TX', 48, 'Austin'        ),
            ( 'test2', '94102', 'CA', 06, 'San Francisco' ),
        ]

        create = """
            CREATE TABLE zipcodes (
                group text,
                zipcode text,
                state text,
                fips_regions int,
                city text,
                PRIMARY KEY(group,zipcode,state,fips_regions)
            )"""

        if compact:
            create = create + " WITH COMPACT STORAGE"

        cursor.execute(create)

        for d in data:
            cursor.execute("INSERT INTO zipcodes (group, zipcode, state, fips_regions, city) VALUES ('%s', '%s', '%s', %i, '%s')" % d)

        cursor.execute("select zipcode from zipcodes")
        res = cursor.fetchall()
        assert len(res) == 16, res

        cursor.execute("select zipcode from zipcodes where group='test'")
        res = cursor.fetchall()
        assert len(res) == 8, res

        assert_invalid(cursor, "select zipcode from zipcodes where zipcode='06902'")

        cursor.execute("select zipcode from zipcodes where zipcode='06902' ALLOW FILTERING")
        res = cursor.fetchall()
        assert len(res) == 2, res

        cursor.execute("select zipcode from zipcodes where group='test' and zipcode='06902'")
        res = cursor.fetchall()
        assert len(res) == 1, res

        cursor.execute("select zipcode from zipcodes where group='test' and zipcode IN ('06902','73301','94102')")
        res = cursor.fetchall()
        assert len(res) == 3, res

        cursor.execute("select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA')")
        res = cursor.fetchall()
        assert len(res) == 2, res

        cursor.execute("select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions = 9")
        res = cursor.fetchall()
        assert len(res) == 1, res

        cursor.execute("select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') ORDER BY zipcode DESC")
        res = cursor.fetchall()
        assert len(res) == 2, res

        cursor.execute("select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions > 0")
        res = cursor.fetchall()
        assert len(res) == 2, res

        cursor.execute("select zipcode from zipcodes where group='test' AND zipcode IN ('06902','73301','94102') and state IN ('CT','CA') and fips_regions < 0")
        res = cursor.fetchall()
        assert len(res) == 0, res

    @require('4762')
    def multi_in_compact_non_composite_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                key int,
                c int,
                v int,
                PRIMARY KEY (key, c)
            ) WITH COMPACT STORAGE
        """)

        cursor.execute("INSERT INTO test (key, c, v) VALUES (0, 0, 0)")
        cursor.execute("INSERT INTO test (key, c, v) VALUES (0, 1, 1)")
        cursor.execute("INSERT INTO test (key, c, v) VALUES (0, 2, 2)")

        cursor.execute("SELECT * FROM test WHERE key=0 AND c IN (0, 2)")
        res = cursor.fetchall()
        assert res == [[0, 0, 0], [0, 2, 2]], res

    @since('1.2.1')
    def timeuuid_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                t timeuuid,
                PRIMARY KEY (k, t)
            )
        """)

        assert_invalid(cursor, "INSERT INTO test (k, t) VALUES (0, 2012-11-07 18:18:22-0800)")

        for i in range(4):
            cursor.execute("INSERT INTO test (k, t) VALUES (0, now())")
            time.sleep(1)

        cursor.execute("SELECT * FROM test")
        res = cursor.fetchall()
        assert len(res) == 4, res
        dates = [ d[1] for d in res ]

        cursor.execute("SELECT * FROM test WHERE k = 0 AND t >= %s" % dates[0])
        res = cursor.fetchall()
        assert len(res) == 4, res

        cursor.execute("SELECT * FROM test WHERE k = 0 AND t < %s" % dates[0])
        res = cursor.fetchall()
        assert len(res) == 0, res

        cursor.execute("SELECT * FROM test WHERE k = 0 AND t > %s AND t <= %s" % (dates[0], dates[2]))
        res = cursor.fetchall()
        assert len(res) == 2, res

        cursor.execute("SELECT * FROM test WHERE k = 0 AND t = %s" % dates[0])
        res = cursor.fetchall()
        assert len(res) == 1, res

        assert_invalid(cursor, "SELECT dateOf(k) FROM test WHERE k = 0 AND t = %s" % dates[0])

        cursor.execute("SELECT dateOf(t), unixTimestampOf(t) FROM test WHERE k = 0 AND t = %s" % dates[0])
        cursor.execute("SELECT t FROM test WHERE k = 0 AND t > maxTimeuuid(1234567) AND t < minTimeuuid('2012-11-07 18:18:22-0800')")
        # not sure what to check exactly so just checking the query returns

    @since('1.2')
    def float_with_exponent_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                d double,
                f float
            )
        """)

        cursor.execute("INSERT INTO test(k, d, f) VALUES (0, 3E+10, 3.4E3)")
        cursor.execute("INSERT INTO test(k, d, f) VALUES (1, 3.E10, -23.44E-3)")
        cursor.execute("INSERT INTO test(k, d, f) VALUES (2, 3, -2)")

    @since('1.2')
    def compact_metadata_test(self):
        """ Test regression from #5189 """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE bar (
                id int primary key,
                i int
            ) WITH COMPACT STORAGE;
        """)

        cursor.execute("INSERT INTO bar (id, i) VALUES (1, 2);")
        cursor.execute("SELECT * FROM bar")
        res = cursor.fetchall()
        assert res == [[1, 2]], res

    @since('2.0')
    def clustering_indexing_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE posts (
                id1 int,
                id2 int,
                author text,
                time bigint,
                content text,
                PRIMARY KEY ((id1, id2), author, time)
            )
        """)

        cursor.execute("CREATE INDEX ON posts(time)")
        cursor.execute("CREATE INDEX ON posts(id2)")

        cursor.execute("INSERT INTO posts(id1, id2, author, time, content) VALUES(0, 0, 'bob', 0, 'A')")
        cursor.execute("INSERT INTO posts(id1, id2, author, time, content) VALUES(0, 0, 'bob', 1, 'B')")
        cursor.execute("INSERT INTO posts(id1, id2, author, time, content) VALUES(0, 1, 'bob', 2, 'C')")
        cursor.execute("INSERT INTO posts(id1, id2, author, time, content) VALUES(0, 0, 'tom', 0, 'D')")
        cursor.execute("INSERT INTO posts(id1, id2, author, time, content) VALUES(0, 1, 'tom', 1, 'E')")

        cursor.execute("SELECT content FROM posts WHERE time = 1")
        res = cursor.fetchall()
        assert res == [ ['B'], ['E'] ], res

        cursor.execute("SELECT content FROM posts WHERE id2 = 1")
        res = cursor.fetchall()
        assert res == [ ['C'], ['E'] ], res

        cursor.execute("SELECT content FROM posts WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0")
        res = cursor.fetchall()
        assert res == [ ['A'] ], res

    @since('1.2')
    def bug_5240_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test(
                interval text,
                seq int,
                id int,
                severity int,
                PRIMARY KEY ((interval, seq), id)
            ) WITH CLUSTERING ORDER BY (id DESC);
        """)

        cursor.execute("CREATE INDEX ON test(severity);")

        cursor.execute("insert into test(interval, seq, id , severity) values('t',1, 1, 1);")
        cursor.execute("insert into test(interval, seq, id , severity) values('t',1, 2, 1);")
        cursor.execute("insert into test(interval, seq, id , severity) values('t',1, 3, 2);")
        cursor.execute("insert into test(interval, seq, id , severity) values('t',1, 4, 3);")
        cursor.execute("insert into test(interval, seq, id , severity) values('t',2, 1, 3);")
        cursor.execute("insert into test(interval, seq, id , severity) values('t',2, 2, 3);")
        cursor.execute("insert into test(interval, seq, id , severity) values('t',2, 3, 1);")
        cursor.execute("insert into test(interval, seq, id , severity) values('t',2, 4, 2);")

        cursor.execute("select * from test where severity = 3 and interval = 't' and seq =1;")
        res = cursor.fetchall()
        assert res == [['t', 1, 4, 3]], res

    @since('1.2')
    def ticket_5230_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE foo (
                key text,
                c text,
                v text,
                PRIMARY KEY (key, c)
            )
        """)

        cursor.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '1', '1')")
        cursor.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '2', '2')")
        cursor.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '3', '3')")

        cursor.execute("SELECT c FROM foo WHERE key = 'foo' AND c IN ('1', '2');")
        res = cursor.fetchall()
        assert res == [['1'], ['2']], res

    @since('1.2')
    def conversion_functions_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                i varint,
                b blob
            )
        """)

        cursor.execute("INSERT INTO test(k, i, b) VALUES (0, blobAsVarint(bigintAsBlob(3)), textAsBlob('foobar'))")
        cursor.execute("SELECT i, blobAsText(b) FROM test WHERE k = 0")
        res = cursor.fetchall()
        assert res == [[3, 'foobar']], res

    @since('1.2')
    def alter_bug_test(self):
        """ Test for bug of 5232 """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE t1 (id int PRIMARY KEY, t text);")

        cursor.execute("UPDATE t1 SET t = '111' WHERE id = 1;")
        cursor.execute("ALTER TABLE t1 ADD l list<text>;")

        time.sleep(.5)

        cursor.execute("SELECT * FROM t1;")
        res = cursor.fetchall()
        assert res == [[1, None, '111']], res

        cursor.execute("ALTER TABLE t1 ADD m map<int, text>;")
        time.sleep(.5)
        cursor.execute("SELECT * FROM t1;")
        res = cursor.fetchall()
        assert res == [[1, None, None, '111']], res

    @since('1.2')
    def validation_bug_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                l list<int>,
                s set<int>
            )
        """)

        assert_invalid(cursor, "INSERT INTO test(k, s) VALUES (0, {1, 1})")

    @since('1.2')
    def bug_5376(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                key text,
                c bigint,
                v text,
                x set<text>,
                PRIMARY KEY (key, c)
            );
        """)

        assert_invalid(cursor, "select * from test where key = 'foo' and c in (1,3,4);")

    @since('1.2')
    def function_and_reverse_type_test(self):
        """ Test for #5386 """

        cursor = self.prepare()
        cursor.execute("""
            CREATE TABLE test (
                k int,
                c timeuuid,
                v int,
                PRIMARY KEY (k, c)
            ) WITH CLUSTERING ORDER BY (c DESC)
        """)

        cursor.execute("INSERT INTO test (k, c, v) VALUES (0, now(), 0);")

    @since('1.2')
    def bug_5404(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (key text PRIMARY KEY)")
        # We just want to make sure this doesn't NPE server side
        assert_invalid(cursor, "select * from test where token(key) > token(int(3030343330393233)) limit 1;")

    @since('1.2')
    def empty_blob_test(self):
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, b blob)")
        cursor.execute("INSERT INTO test (k, b) VALUES (0, 0x)");
        cursor.execute("SELECT * FROM test");
        res = cursor.fetchall()
        assert res == [[ 0, '' ]], res

    @since('2.0')
    def rename_test(self):
        cursor = self.prepare()

        # The goal is to test renaming from an old cli value
        cli = self.cluster.nodelist()[0].cli()
        cli.do("use ks")
        cli.do("create column family test with comparator='CompositeType(Int32Type, Int32Type, Int32Type)' "
                + "and key_validation_class=UTF8Type and default_validation_class=UTF8Type");
        cli.do("set test['foo']['4:3:2'] = 'bar'")
        assert not cli.has_errors(), cli.errors()

        cursor.execute("ALTER TABLE test RENAME column1 TO foo1 AND column2 TO foo2 AND column3 TO foo3")
        assert_one(cursor, "SELECT foo1, foo2, foo3 FROM test", [4, 3, 2])

    @since('1.2')
    def clustering_order_and_functions_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int,
                t timeuuid,
                PRIMARY KEY (k, t)
            ) WITH CLUSTERING ORDER BY (t DESC)
        """)

        for i in range(0, 5):
            cursor.execute("INSERT INTO test (k, t) VALUES (%d, now())" % i)

        cursor.execute("SELECT dateOf(t) FROM test");

    @since('2.0')
    def conditional_update_test(self):
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 text,
                v3 int
            )
        """)

        # Shouldn't apply
        assert_one(cursor, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4", [ False ])

        # Should apply
        assert_one(cursor, "INSERT INTO test (k, v1, v2) VALUES (0, 2, 'foo') IF NOT EXISTS", [ True ])

        # Shouldn't apply
        assert_one(cursor, "INSERT INTO test (k, v1, v2) VALUES (0, 5, 'bar') IF NOT EXISTS", [ False, 0, 2, 'foo', None ])
        assert_one(cursor, "SELECT * FROM test", [ 0, 2, 'foo', None ])

        # Should not apply
        assert_one(cursor, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4", [ False, 2 ])
        assert_one(cursor, "SELECT * FROM test", [ 0, 2, 'foo', None ])

        # Should apply (note: we want v2 before v1 in the statement order to exercise #5786)
        assert_one(cursor, "UPDATE test SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 = 2", [ True ])
        assert_one(cursor, "SELECT * FROM test", [ 0, 3, 'bar', None ])

        # Shouldn't apply, only one condition is ok
        assert_one(cursor, "UPDATE test SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'foo'", [ False, 3, 'bar' ])
        assert_one(cursor, "SELECT * FROM test", [ 0, 3, 'bar', None ])

        # Should apply
        assert_one(cursor, "UPDATE test SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'bar'", [ True ])
        assert_one(cursor, "SELECT * FROM test", [ 0, 5, 'foobar', None ])

        # Shouldn't apply
        assert_one(cursor, "DELETE v2 FROM test WHERE k = 0 IF v1 = 3", [ False, 5 ])
        assert_one(cursor, "SELECT * FROM test", [ 0, 5, 'foobar', None ])

        # Shouldn't apply
        assert_one(cursor, "DELETE v2 FROM test WHERE k = 0 IF v1 = null", [ False, 5 ])
        assert_one(cursor, "SELECT * FROM test", [ 0, 5, 'foobar', None ])

        # Should apply
        assert_one(cursor, "DELETE v2 FROM test WHERE k = 0 IF v1 = 5", [ True ])
        assert_one(cursor, "SELECT * FROM test", [ 0, 5, None, None ])

        # Shouln't apply
        assert_one(cursor, "DELETE v1 FROM test WHERE k = 0 IF v3 = 4", [ False, None ])

        # Should apply
        assert_one(cursor, "DELETE v1 FROM test WHERE k = 0 IF v3 = null", [ True ])
        assert_one(cursor, "SELECT * FROM test", [ 0, None, None, None ])

        # Should apply
        assert_one(cursor, "DELETE FROM test WHERE k = 0 IF v1 = null", [ True ])
        assert_none(cursor, "SELECT * FROM test")


    @since('1.2')
    def range_key_ordered_test(self):
        cursor = self.prepare(ordered=True)

        cursor.execute("CREATE TABLE test ( k int PRIMARY KEY)")

        cursor.execute("INSERT INTO test(k) VALUES (-1)")
        cursor.execute("INSERT INTO test(k) VALUES ( 0)")
        cursor.execute("INSERT INTO test(k) VALUES ( 1)")

        assert_all(cursor, "SELECT * FROM test", [[0], [1], [-1]])
        assert_invalid(cursor, "SELECT * FROM test WHERE k >= -1 AND k < 1;")

    @since('2.0')
    def select_with_alias_test(self):
        cursor = self.prepare()
        cursor.execute('CREATE TABLE users (id int PRIMARY KEY, name text)')

        for id in range(0, 5):
            cursor.execute("INSERT INTO users (id, name) VALUES (%d, 'name%d') USING TTL 10 AND TIMESTAMP 0" % (id, id))

        # test aliasing count(*)
        cursor.execute('SELECT count(*) AS user_count FROM users')
        self.assertEqual('user_count', cursor.name_info[0][0])
        self.assertEqual([5], cursor.fetchone())

        # test aliasing regular value
        cursor.execute('SELECT name AS user_name FROM users WHERE id = 0')
        self.assertEqual('user_name', cursor.name_info[0][0])
        self.assertEqual(['name0'], cursor.fetchone())

        # test aliasing writetime
        cursor.execute('SELECT writeTime(name) AS name_writetime FROM users WHERE id = 0')
        self.assertEqual('name_writetime', cursor.name_info[0][0])
        self.assertEqual([0], cursor.fetchone())

        # test aliasing ttl
        cursor.execute('SELECT ttl(name) AS name_ttl FROM users WHERE id = 0')
        self.assertEqual('name_ttl', cursor.name_info[0][0])
        assert cursor.fetchone()[0] in (9, 10)

        # test aliasing a regular function
        cursor.execute('SELECT intAsBlob(id) AS id_blob FROM users WHERE id = 0')
        self.assertEqual('id_blob', cursor.name_info[0][0])
        self.assertEqual(['\x00\x00\x00\x00'], cursor.fetchone())

        # test that select throws a meaningful exception for aliases in where clause
        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute('SELECT id AS user_id, name AS user_name FROM users WHERE user_id = 0')
        self.assertEqual("Bad Request: Aliases aren't allowed in where clause ('user_id EQ 0')",
                         cm.exception.message)

        # test that select throws a meaningful exception for aliases in order by clause
        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute('SELECT id AS user_id, name AS user_name FROM users WHERE id IN (0) ORDER BY user_name')
        self.assertEqual("Bad Request: Aliases are not allowed in order by clause ('user_name')",
                         cm.exception.message)

    @since('2.0')
    def only_key_update(self):

        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (k int PRIMARY KEY)")

        cursor.execute("UPDATE test SET PRIMARY KEY WHERE k = 0")
        assert_one(cursor, "SELECT * FROM test", [ 0 ])

    @since('1.2')
    def nonpure_function_collection(self):
        """ Test for bug #5795 """

        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, v list<timeuuid>)")

        # we just want to make sure this doesn't throw
        cursor.execute("INSERT INTO test(k, v) VALUES (0, [now()])")

    @require('5626')
    def empty_in(self):
        cursor = self.prepare()
        cursor.execute("CREATE TABLE test (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))")

        # Inserts a few rows to make sure we don't actually query something
        for i in range(0, 1):
            for j in range(0, 1):
                cursor.execute("INSERT INTO test(k1, k2, v) VALUES (%d, %d, %d)" % (i, j, i+j))

        assert_none(cursor, "SELECT v FROM test WHERE k1 IN ()")
        assert_none(cursor, "SELECT v FROM test WHERE k1 = 0 AND k2 IN ()")

        # Same test, but for compact
        cursor.execute("CREATE TABLE test_compact (k1 int, k2 int, v int, PRIMARY KEY (k1, k2)) WITH COMPACT STORAGE")

        for i in range(0, 1):
            for j in range(0, 1):
                cursor.execute("INSERT INTO test_compact(k1, k2, v) VALUES (%d, %d, %d)" % (i, j, i+j))

        assert_none(cursor, "SELECT v FROM test_compact WHERE k1 IN ()")
        assert_none(cursor, "SELECT v FROM test_compact WHERE k1 = 0 AND k2 IN ()")

