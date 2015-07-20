# coding: utf-8

import math
import random
import struct
import time
from collections import OrderedDict
from uuid import UUID

from cassandra import AlreadyExists, ConsistencyLevel, InvalidRequest
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.protocol import (ConfigurationException, ProtocolException,
                                SyntaxException)
from cassandra.query import SimpleStatement
from cassandra.util import sortedset

from assertions import assert_all, assert_invalid, assert_none, assert_one
from dtest import Tester, canReuseCluster, freshCluster
from thrift_bindings.v22.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.v22.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                        Mutation)
from thrift_tests import get_thrift_client
from tools import require, rows_to_list, since


@since('1.0.x', max_version='2.0.x')
@canReuseCluster
class TestCQL(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, nodes=1, rf=1, protocol_version=None, **kwargs):
        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        start_rpc = kwargs.pop('start_rpc', False)
        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        if not cluster.nodelist():
            cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            if self._preserve_cluster:
                session.execute("DROP KEYSPACE IF EXISTS ks")
            self.create_ks(session, 'ks', rf)
        return session

    def static_cf_test(self):
        """
        Test static CF syntax.
        """
        session = self.prepare()

        # Create
        session.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        # Inserts
        session.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        session.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        res = session.execute("SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [['Frodo', 'Baggins']], res

        res = session.execute("SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [[UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins']], res

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [
            [UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee'],
            [UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins'],
        ], res

        # Test batch inserts
        session.execute("""
            BEGIN BATCH
                INSERT INTO users (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                UPDATE users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                DELETE firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                DELETE firstname, lastname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
            APPLY BATCH
        """)

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [
            [UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 37, None, None],
            [UUID('550e8400-e29b-41d4-a716-446655440000'), 36, None, None],
        ], res

    def large_collection_errors(self):
        """
        For large collections, make sure that we are printing warnings.
        """

        # We only warn with protocol 2
        session = self.prepare(protocol_version=2)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]
        self.ignore_log_patterns = ["Detected collection for table"]

        session.execute("""
            CREATE TABLE maps (
                userid text PRIMARY KEY,
                properties map<int, text>
            );
        """)

        # Insert more than the max, which is 65535
        for i in range(70000):
            session.execute("UPDATE maps SET properties[%i] = 'x' WHERE userid = 'user'" % i)

        # Query for the data and throw exception
        session.execute("SELECT properties FROM maps WHERE userid = 'user'")
        node1.watch_log_for("Detected collection for table ks.maps with 70000 elements, more than the 65535 limit. Only the first 65535 elements will be returned to the client. Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.")

    def noncomposite_static_cf_test(self):
        """
        Test non-composite static CF syntax.
        """
        session = self.prepare()

        # Create
        session.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        session.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        session.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        res = session.execute("SELECT firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [['Frodo', 'Baggins']], res

        res = session.execute("SELECT * FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [[UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins']], res

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [
            [UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee'],
            [UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins'],
        ], res

        # Test batch inserts
        session.execute("""
            BEGIN BATCH
                INSERT INTO users (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                UPDATE users SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                DELETE firstname, lastname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                DELETE firstname, lastname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
            APPLY BATCH
        """)

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [
            [UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 37, None, None],
            [UUID('550e8400-e29b-41d4-a716-446655440000'), 36, None, None],
        ], res

    def dynamic_cf_test(self):
        """
        Test non-composite dynamic CF syntax.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE clicks (
                userid uuid,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        session.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo.bar', 42)")
        session.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo-2.bar', 24)")
        session.execute("INSERT INTO clicks (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://bar.bar', 128)")
        session.execute("UPDATE clicks SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 and url = 'http://bar.foo'")
        session.execute("UPDATE clicks SET time = 12 WHERE userid IN (f47ac10b-58cc-4372-a567-0e02b2c3d479, 550e8400-e29b-41d4-a716-446655440000) and url = 'http://foo-3'")

        # Queries
        res = session.execute("SELECT url, time FROM clicks WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [['http://bar.bar', 128], ['http://foo-2.bar', 24], ['http://foo-3', 12], ['http://foo.bar', 42]], res

        res = session.execute("SELECT * FROM clicks WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")
        assert rows_to_list(res) == [
            [UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://bar.foo', 24],
            [UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://foo-3', 12]
        ], res

        res = session.execute("SELECT time FROM clicks")
        # Result from 'f47ac10b-58cc-4372-a567-0e02b2c3d479' are first
        assert rows_to_list(res) == [[24], [12], [128], [24], [12], [42]], res

        # Check we don't allow empty values for url since this is the full underlying cell name (#6152)
        assert_invalid(session, "INSERT INTO clicks (userid, url, time) VALUES (810e8500-e29b-41d4-a716-446655440000, '', 42)")

    def dense_cf_test(self):
        """
        Test composite 'dense' CF syntax.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE connections (
                userid uuid,
                ip text,
                port int,
                time bigint,
                PRIMARY KEY (userid, ip, port)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        session.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.1', 80, 42)")
        session.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 80, 24)")
        session.execute("INSERT INTO connections (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 90, 42)")
        session.execute("UPDATE connections SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.2' AND port = 80")

        # we don't have to include all of the clustering columns (see CASSANDRA-7990)
        session.execute("INSERT INTO connections (userid, ip, time) VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, '192.168.0.3', 42)")
        session.execute("UPDATE connections SET time = 42 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.4'")

        # Queries
        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert rows_to_list(res) == [['192.168.0.1', 80, 42], ['192.168.0.2', 80, 24], ['192.168.0.2', 90, 42]], res

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip >= '192.168.0.2'")
        assert rows_to_list(res) == [['192.168.0.2', 80, 24], ['192.168.0.2', 90, 42]], res

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip = '192.168.0.2'")
        assert rows_to_list(res) == [['192.168.0.2', 80, 24], ['192.168.0.2', 90, 42]], res

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip > '192.168.0.2'")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'")
        self.assertEqual([['192.168.0.3', None, 42]], rows_to_list(res))

        res = session.execute("SELECT ip, port, time FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.4'")
        self.assertEqual([['192.168.0.4', None, 42]], rows_to_list(res))

        # Deletion
        session.execute("DELETE time FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND ip = '192.168.0.2' AND port = 80")
        res = session.execute("SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert len(res) == 2, res

        session.execute("DELETE FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        res = session.execute("SELECT * FROM connections WHERE userid = 550e8400-e29b-41d4-a716-446655440000")
        assert len(res) == 0, res

        session.execute("DELETE FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'")
        res = session.execute("SELECT * FROM connections WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'")
        self.assertEqual([], res)

    def sparse_cf_test(self):
        """
        Test composite 'sparse' CF syntax.
        """
        session = self.prepare()

        session.execute("""
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
        session.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 12, 'Something else', 'Frodo Baggins')")
        session.execute("INSERT INTO timeline (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 24, 'Something something', 'Frodo Baggins')")
        session.execute("UPDATE timeline SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND posted_month = 1 AND posted_day = 3")
        session.execute("UPDATE timeline SET body = 'Yet one more message' WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 and posted_day = 30")

        # Queries
        res = session.execute("SELECT body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day = 24")
        assert rows_to_list(res) == [['Something something', 'Frodo Baggins']], res

        res = session.execute("SELECT posted_day, body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day > 12")
        assert rows_to_list(res) == [
            [24, 'Something something', 'Frodo Baggins'],
            [30, 'Yet one more message', None]
        ], res

        res = session.execute("SELECT posted_day, body, posted_by FROM timeline WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1")
        assert rows_to_list(res) == [
            [12, 'Something else', 'Frodo Baggins'],
            [24, 'Something something', 'Frodo Baggins'],
            [30, 'Yet one more message', None]
        ], res

    def create_invalid_test(self):
        """
        Check invalid CREATE TABLE requests.
        """

        session = self.prepare()

        assert_invalid(session, "CREATE TABLE test ()", expected=SyntaxException)

        if self.cluster.version() < "1.2":
            assert_invalid(session, "CREATE TABLE test (key text PRIMARY KEY)")

        assert_invalid(session, "CREATE TABLE test (c1 text, c2 text, c3 text)")
        assert_invalid(session, "CREATE TABLE test (key1 text PRIMARY KEY, key2 text PRIMARY KEY)")

        assert_invalid(session, "CREATE TABLE test (key text PRIMARY KEY, key int)")
        assert_invalid(session, "CREATE TABLE test (key text PRIMARY KEY, c int, c text)")

        assert_invalid(session, "CREATE TABLE test (key text, key2 text, c int, d text, PRIMARY KEY (key, key2)) WITH COMPACT STORAGE")

    @freshCluster()
    def limit_ranges_test(self):
        """
        Validate LIMIT option for 'range queries' in SELECT statements.
        """
        session = self.prepare(ordered=True)

        session.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for id in xrange(0, 100):
            for tld in ['com', 'org', 'net']:
                session.execute("INSERT INTO clicks (userid, url, time) VALUES (%i, 'http://foo.%s', 42)" % (id, tld))

        # Queries
        res = session.execute("SELECT * FROM clicks WHERE token(userid) >= token(2) LIMIT 1")
        assert rows_to_list(res) == [[2, 'http://foo.com', 42]], res

        res = session.execute("SELECT * FROM clicks WHERE token(userid) > token(2) LIMIT 1")
        assert rows_to_list(res) == [[3, 'http://foo.com', 42]], res

    def limit_multiget_test(self):
        """
        Validate LIMIT option for 'multiget' in SELECT statements.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for id in xrange(0, 100):
            for tld in ['com', 'org', 'net']:
                session.execute("INSERT INTO clicks (userid, url, time) VALUES (%i, 'http://foo.%s', 42)" % (id, tld))

        # Check that we do limit the output to 1 *and* that we respect query
        # order of keys (even though 48 is after 2)
        res = session.execute("SELECT * FROM clicks WHERE userid IN (48, 2) LIMIT 1")
        if self.cluster.version() >= '2.2':
            assert rows_to_list(res) == [[2, 'http://foo.com', 42]], res
        else:
            assert rows_to_list(res) == [[48, 'http://foo.com', 42]], res

    def tuple_query_mixed_order_columns_prepare(self, session, *col_order):
        session.execute("""
            create table foo (a int, b int, c int, d int , e int, PRIMARY KEY (a, b, c, d, e) )
            WITH CLUSTERING ORDER BY (b {0}, c {1}, d {2}, e {3});
        """.format(col_order[0], col_order[1], col_order[2], col_order[3]))

        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 2, 0, 0, 0);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 1, 0, 0, 0);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 0, 0, 0);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 2, -1);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 1, -1);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 1, 0);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 1, 0, 2);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 2, 1, -3);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, 0, 2, 0, 3);""")
        session.execute("""INSERT INTO foo (a, b, c, d, e) VALUES (0, -1, 2, 2, 2);""")

    @require("7281")
    def tuple_query_mixed_order_columns_test(self):
        """
        @jira_ticket CASSANDRA-7281

        Regression test for broken SELECT statements on tuple relations with
        mixed ASC/DESC clustering order.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, 'DESC', 'ASC', 'DESC', 'ASC')
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        assert rows_to_list(res) == [[0, 2, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 1, 2, -1],
                                     [0, 0, 1, 1, 1], [0, 0, 2, 1, -3], [0, 0, 2, 0, 3]], res

    @require("7281")
    def tuple_query_mixed_order_columns_test2(self):
        """
        @jira_ticket CASSANDRA-7281

        Regression test for broken SELECT statements on tuple relations with
        mixed ASC/DESC clustering order.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, 'DESC', 'DESC', 'DESC', 'ASC')
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        assert rows_to_list(res) == [[0, 2, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 2, 1, -3],
                                     [0, 0, 2, 0, 3], [0, 0, 1, 2, -1], [0, 0, 1, 1, 1]], res

    @require("7281")
    def tuple_query_mixed_order_columns_test3(self):
        """
        @jira_ticket CASSANDRA-7281

        Regression test for broken SELECT statements on tuple relations with
        mixed ASC/DESC clustering order.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, 'ASC', 'DESC', 'DESC', 'ASC')
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        assert rows_to_list(res) == [[0, 0, 2, 1, -3], [0, 0, 2, 0, 3], [0, 0, 1, 2, -1],
                                     [0, 0, 1, 1, 1], [0, 1, 0, 0, 0], [0, 2, 0, 0, 0]], res

    @require("7281")
    def tuple_query_mixed_order_columns_test4(self):
        """
        @jira_ticket CASSANDRA-7281

        Regression test for broken SELECT statements on tuple relations with
        mixed ASC/DESC clustering order.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, 'DESC', 'ASC', 'ASC', 'DESC')
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        assert rows_to_list(res) == [[0, 2, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 1, 1, 1],
                                     [0, 0, 1, 2, -1], [0, 0, 2, 0, 3], [0, 0, 2, 1, -3]], res

    @require("7281")
    def tuple_query_mixed_order_columns_test5(self):
        """
        @jira_ticket CASSANDRA-7281

        Test that tuple relations with non-mixed ASC/DESC order still works.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, 'DESC', 'DESC', 'DESC', 'DESC')
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        assert rows_to_list(res) == [[0, 2, 0, 0, 0], [0, 1, 0, 0, 0], [0, 0, 2, 1, -3],
                                     [0, 0, 2, 0, 3], [0, 0, 1, 2, -1], [0, 0, 1, 1, 1]], res

    @require("7281")
    def tuple_query_mixed_order_columns_test6(self):
        """CASSANDRA-7281: SELECT on tuple relations are broken for mixed ASC/DESC clustering order
            Test that non mixed columns are still working.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, 'ASC', 'ASC', 'ASC', 'ASC')
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) > (0, 1, 1, 0);")
        assert rows_to_list(res) == [[0, 0, 1, 1, 1], [0, 0, 1, 2, -1], [0, 0, 2, 0, 3],
                                     [0, 0, 2, 1, -3], [0, 1, 0, 0, 0], [0, 2, 0, 0, 0]], res

    @require("7281")
    def tuple_query_mixed_order_columns_test7(self):
        """
        @jira_ticket CASSANDRA-7281

        Test that tuple relations with non-mixed ASC/DESC order still works.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, 'DESC', 'ASC', 'DESC', 'ASC')
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) <= (0, 1, 1, 0);")
        assert rows_to_list(res) == [[0, 0, 0, 0, 0], [0, 0, 1, 1, -1], [0, 0, 1, 1, 0],
                                     [0, 0, 1, 0, 2], [0, -1, 2, 2, 2]], res

    @require("7281")
    def tuple_query_mixed_order_columns_test8(self):
        """
        @jira_ticket CASSANDRA-7281

        Test that tuple relations with non-mixed ASC/DESC order still works.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, 'ASC', 'DESC', 'DESC', 'ASC')
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) <= (0, 1, 1, 0);")
        assert rows_to_list(res) == [[0, -1, 2, 2, 2], [0, 0, 1, 1, -1], [0, 0, 1, 1, 0],
                                     [0, 0, 1, 0, 2], [0, 0, 0, 0, 0]], res

    @require("7281")
    def tuple_query_mixed_order_columns_test9(self):
        """
        @jira_ticket CASSANDRA-7281

        Test that tuple relations with non-mixed ASC/DESC order still works.
        """
        session = self.prepare()

        self.tuple_query_mixed_order_columns_prepare(session, 'DESC', 'ASC', 'DESC', 'DESC')
        res = session.execute("SELECT * FROM foo WHERE a=0 AND (b, c, d, e) <= (0, 1, 1, 0);")
        assert rows_to_list(res) == [[0, 0, 0, 0, 0], [0, 0, 1, 1, 0], [0, 0, 1, 1, -1],
                                     [0, 0, 1, 0, 2], [0, -1, 2, 2, 2]], res

    def simple_tuple_query_test(self):
        """
        @jira_ticket CASSANDRA-8613
        """
        session = self.prepare()

        session.execute("create table bard (a int, b int, c int, d int , e int, PRIMARY KEY (a, b, c, d, e))")

        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 2, 0, 0, 0);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 1, 0, 0, 0);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 0, 0, 0);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 2, 2, 2);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 3, 3, 3);""")
        session.execute("""INSERT INTO bard (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);""")

        res = session.execute("SELECT * FROM bard WHERE b=0 AND (c, d, e) > (1, 1, 1) ALLOW FILTERING;")
        assert rows_to_list(res) == [[0, 0, 2, 2, 2], [0, 0, 3, 3, 3]]

    def limit_sparse_test(self):
        """
        Validate LIMIT option for sparse table in SELECT statements.
        """
        session = self.prepare()

        session.execute("""
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
            for tld in ['com', 'org', 'net']:
                session.execute("INSERT INTO clicks (userid, url, day, month, year) VALUES (%i, 'http://foo.%s', 1, 'jan', 2012)" % (id, tld))

        # Queries
        # Check we do get as many rows as requested
        res = session.execute("SELECT * FROM clicks LIMIT 4")
        assert len(res) == 4, res

    def counters_test(self):
        """
        Validate counter support.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE clicks (
                userid int,
                url text,
                total counter,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;
        """)

        session.execute("UPDATE clicks SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'")
        res = session.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        assert rows_to_list(res) == [[1]], res

        session.execute("UPDATE clicks SET total = total - 4 WHERE userid = 1 AND url = 'http://foo.com'")
        res = session.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        assert rows_to_list(res) == [[-3]], res

        session.execute("UPDATE clicks SET total = total+1 WHERE userid = 1 AND url = 'http://foo.com'")
        res = session.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        assert rows_to_list(res) == [[-2]], res

        session.execute("UPDATE clicks SET total = total -2 WHERE userid = 1 AND url = 'http://foo.com'")
        res = session.execute("SELECT total FROM clicks WHERE userid = 1 AND url = 'http://foo.com'")
        assert rows_to_list(res) == [[-4]], res

    def indexed_with_eq_test(self):
        """ Check that you can query for an indexed column even with a key EQ clause """
        session = self.prepare()

        # Create
        session.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        session.execute("CREATE INDEX byAge ON users(age)")

        # Inserts
        session.execute("INSERT INTO users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")
        session.execute("UPDATE users SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479")

        # Queries
        res = session.execute("SELECT firstname FROM users WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND age = 33")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT firstname FROM users WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND age = 33")
        assert rows_to_list(res) == [['Samwise']], res

    def select_key_in_test(self):
        """
        Query for KEY IN (...).
        """
        session = self.prepare()

        # Create
        session.execute("""
            CREATE TABLE users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        # Inserts
        session.execute("""
                INSERT INTO users (userid, firstname, lastname, age)
                VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)
        """)
        session.execute("""
                INSERT INTO users (userid, firstname, lastname, age)
                VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, 'Samwise', 'Gamgee', 33)
        """)

        # Select
        res = session.execute("""
                SELECT firstname, lastname FROM users
                WHERE userid IN (550e8400-e29b-41d4-a716-446655440000, f47ac10b-58cc-4372-a567-0e02b2c3d479)
        """)

        assert len(res) == 2, res

    def exclusive_slice_test(self):
        """
        Test SELECT respects inclusive and exclusive bounds.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            session.execute("INSERT INTO test (k, c, v) VALUES (0, %i, %i)" % (x, x))

        # Queries
        res = session.execute("SELECT v FROM test WHERE k = 0")
        assert len(res) == 10, res

        res = session.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c <= 6")
        assert len(res) == 5 and res[0][0] == 2 and res[len(res) - 1][0] == 6, res

        res = session.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c <= 6")
        assert len(res) == 4 and res[0][0] == 3 and res[len(res) - 1][0] == 6, res

        res = session.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c < 6")
        assert len(res) == 4 and res[0][0] == 2 and res[len(res) - 1][0] == 5, res

        res = session.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c < 6")
        assert len(res) == 3 and res[0][0] == 3 and res[len(res) - 1][0] == 5, res

        # With LIMIT
        res = session.execute("SELECT v FROM test WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2")
        assert len(res) == 2 and res[0][0] == 3 and res[len(res) - 1][0] == 4, res

        res = session.execute("SELECT v FROM test WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2")
        assert len(res) == 2 and res[0][0] == 5 and res[len(res) - 1][0] == 4, res

    def in_clause_wide_rows_test(self):
        """ Check IN support for 'wide rows' in SELECT statement """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            session.execute("INSERT INTO test1 (k, c, v) VALUES (0, %i, %i)" % (x, x))

        res = session.execute("SELECT v FROM test1 WHERE k = 0 AND c IN (5, 2, 8)")
        if self.cluster.version() <= "1.2":
            assert rows_to_list(res) == [[5], [2], [8]], res
        else:
            assert rows_to_list(res) == [[2], [5], [8]], res

        # composites
        session.execute("""
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
            session.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, 0, %i, %i)" % (x, x))

        # Check first we don't allow IN everywhere
        if self.cluster.version() >= '2.2':
            assert_none(session, "SELECT v FROM test2 WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3")
        else:
            assert_invalid(session, "SELECT v FROM test2 WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3")

        res = session.execute("SELECT v FROM test2 WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)")
        assert rows_to_list(res) == [[2], [5], [8]], res

    def order_by_test(self):
        """ Check ORDER BY support in SELECT statement """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        # Inserts
        for x in range(0, 10):
            session.execute("INSERT INTO test1 (k, c, v) VALUES (0, %i, %i)" % (x, x))

        res = session.execute("SELECT v FROM test1 WHERE k = 0 ORDER BY c DESC")
        assert rows_to_list(res) == [[x] for x in range(9, -1, -1)], res

        # composites
        session.execute("""
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
                session.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, %i, %i, %i)" % (x, y, x * 2 + y))

        # Check first we don't always ORDER BY
        assert_invalid(session, "SELECT v FROM test2 WHERE k = 0 ORDER BY c DESC")
        assert_invalid(session, "SELECT v FROM test2 WHERE k = 0 ORDER BY c2 DESC")
        assert_invalid(session, "SELECT v FROM test2 WHERE k = 0 ORDER BY k DESC")

        res = session.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1 DESC")
        assert rows_to_list(res) == [[x] for x in range(7, -1, -1)], res

        res = session.execute("SELECT v FROM test2 WHERE k = 0 ORDER BY c1")
        assert rows_to_list(res) == [[x] for x in range(0, 8)], res

    def more_order_by_test(self):
        """ More ORDER BY checks (#4160) """
        session = self.prepare()

        session.execute("""
            CREATE COLUMNFAMILY Test (
                row text,
                number int,
                string text,
                PRIMARY KEY (row, number)
            ) WITH COMPACT STORAGE
        """)

        session.execute("INSERT INTO Test (row, number, string) VALUES ('row', 1, 'one');")
        session.execute("INSERT INTO Test (row, number, string) VALUES ('row', 2, 'two');")
        session.execute("INSERT INTO Test (row, number, string) VALUES ('row', 3, 'three');")
        session.execute("INSERT INTO Test (row, number, string) VALUES ('row', 4, 'four');")

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number ASC;")
        assert rows_to_list(res) == [[1], [2]], res

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number ASC;")
        assert rows_to_list(res) == [[3], [4]], res

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number < 3 ORDER BY number DESC;")
        assert rows_to_list(res) == [[2], [1]], res

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number >= 3 ORDER BY number DESC;")
        assert rows_to_list(res) == [[4], [3]], res

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number > 3 ORDER BY number DESC;")
        assert rows_to_list(res) == [[4]], res

        res = session.execute("SELECT number FROM Test WHERE row='row' AND number <= 3 ORDER BY number DESC;")
        assert rows_to_list(res) == [[3], [2], [1]], res

    def order_by_validation_test(self):
        """ Check we don't allow order by on row key (#4246) """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY (k1, k2)
            )
        """)

        q = "INSERT INTO test (k1, k2, v) VALUES (%d, %d, %d)"
        session.execute(q % (0, 0, 0))
        session.execute(q % (1, 1, 1))
        session.execute(q % (2, 2, 2))

        assert_invalid(session, "SELECT * FROM test ORDER BY k2")

    def order_by_with_in_test(self):
        """ Check that order-by works with IN (#4327) """
        session = self.prepare()
        session.default_fetch_size = None
        session.execute("""
            CREATE TABLE test(
                my_id varchar,
                col1 int,
                value varchar,
                PRIMARY KEY (my_id, col1)
            )
        """)
        session.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key1', 1, 'a')")
        session.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key2', 3, 'c')")
        session.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key3', 2, 'b')")
        session.execute("INSERT INTO test(my_id, col1, value) VALUES ( 'key4', 4, 'd')")

        query = SimpleStatement("SELECT col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1")
        res = session.execute(query)
        assert rows_to_list(res) == [[1], [2], [3]], res

        query = SimpleStatement("SELECT col1, my_id FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1")
        res = session.execute(query)
        assert rows_to_list(res) == [[1, 'key1'], [2, 'key3'], [3, 'key2']], res

        query = SimpleStatement("SELECT my_id, col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1")
        res = session.execute(query)
        assert rows_to_list(res) == [['key1', 1], ['key3', 2], ['key2', 3]], res

    def reversed_comparator_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH CLUSTERING ORDER BY (c DESC);
        """)

        # Inserts
        for x in range(0, 10):
            session.execute("INSERT INTO test (k, c, v) VALUES (0, %i, %i)" % (x, x))

        res = session.execute("SELECT c, v FROM test WHERE k = 0 ORDER BY c ASC")
        assert rows_to_list(res) == [[x, x] for x in range(0, 10)], res

        res = session.execute("SELECT c, v FROM test WHERE k = 0 ORDER BY c DESC")
        assert rows_to_list(res) == [[x, x] for x in range(9, -1, -1)], res

        session.execute("""
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
                session.execute("INSERT INTO test2 (k, c1, c2, v) VALUES (0, %i, %i, '%i%i')" % (x, y, x, y))

        assert_invalid(session, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 ASC")
        assert_invalid(session, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 DESC")

        res = session.execute("SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC")
        assert rows_to_list(res) == [[x, y, '%i%i' % (x, y)] for x in range(0, 10) for y in range(9, -1, -1)], res

        res = session.execute("SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 ASC, c2 DESC")
        assert rows_to_list(res) == [[x, y, '%i%i' % (x, y)] for x in range(0, 10) for y in range(9, -1, -1)], res

        res = session.execute("SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c1 DESC, c2 ASC")
        assert rows_to_list(res) == [[x, y, '%i%i' % (x, y)] for x in range(9, -1, -1) for y in range(0, 10)], res

        assert_invalid(session, "SELECT c1, c2, v FROM test2 WHERE k = 0 ORDER BY c2 DESC, c1 ASC")

    def invalid_old_property_test(self):
        """ Check obsolete properties from CQL2 are rejected """
        session = self.prepare()

        assert_invalid(session, "CREATE TABLE test (foo text PRIMARY KEY, c int) WITH default_validation=timestamp", expected=SyntaxException)

        session.execute("CREATE TABLE test (foo text PRIMARY KEY, c int)")
        assert_invalid(session, "ALTER TABLE test WITH default_validation=int;", expected=SyntaxException)

    def null_support_test(self):
        """ Test support for nulls """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v1 int,
                v2 set<text>,
                PRIMARY KEY (k, c)
            );
        """)

        # Inserts
        session.execute("INSERT INTO test (k, c, v1, v2) VALUES (0, 0, null, {'1', '2'})")
        session.execute("INSERT INTO test (k, c, v1) VALUES (0, 1, 1)")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, 0, None, set(['1', '2'])], [0, 1, 1, None]], res

        session.execute("INSERT INTO test (k, c, v1) VALUES (0, 1, null)")
        session.execute("INSERT INTO test (k, c, v2) VALUES (0, 0, null)")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, 0, None, None], [0, 1, None, None]], res

        assert_invalid(session, "INSERT INTO test (k, c, v2) VALUES (0, 2, {1, null})")
        assert_invalid(session, "SELECT * FROM test WHERE k = null")
        assert_invalid(session, "INSERT INTO test (k, c, v2) VALUES (0, 0, { 'foo', 'bar', null })")

    def nameless_index_test(self):
        """ Test CREATE INDEX without name and validate the index can be dropped """
        session = self.prepare()

        session.execute("""
            CREATE TABLE users (
                id text PRIMARY KEY,
                birth_year int,
            )
        """)

        session.execute("CREATE INDEX on users(birth_year)")

        session.execute("INSERT INTO users (id, birth_year) VALUES ('Tom', 42)")
        session.execute("INSERT INTO users (id, birth_year) VALUES ('Paul', 24)")
        session.execute("INSERT INTO users (id, birth_year) VALUES ('Bob', 42)")

        res = session.execute("SELECT id FROM users WHERE birth_year = 42")
        assert rows_to_list(res) == [['Tom'], ['Bob']]

        session.execute("DROP INDEX users_birth_year_idx")

        assert_invalid(session, "SELECT id FROM users WHERE birth_year = 42")

    def deletion_test(self):
        """ Test simple deletion and in particular check for #4193 bug """

        session = self.prepare()

        session.execute("""
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
        session.execute(q % row1)
        session.execute(q % row2)

        res = session.execute("SELECT * FROM testcf")
        assert rows_to_list(res) == [list(row1), list(row2)], res

        session.execute("DELETE FROM testcf WHERE username='abc' AND id=2")

        res = session.execute("SELECT * FROM testcf")
        assert rows_to_list(res) == [list(row2)], res

        # Compact case
        session.execute("""
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
        session.execute(q % row1)
        session.execute(q % row2)

        res = session.execute("SELECT * FROM testcf2")
        assert rows_to_list(res) == [list(row1), list(row2)], res

        # Won't be allowed until #3708 is in
        if self.cluster.version() < "1.2":
            assert_invalid(session, "DELETE FROM testcf2 WHERE username='abc' AND id=2")

    def count_test(self):
        session = self.prepare()

        session.execute("""
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

        session.execute(full % (0, 0, 0))
        session.execute(full % (1, 1, 1))
        session.execute(no_v2 % (2, 2))
        session.execute(full % (3, 3, 3))
        session.execute(no_v2 % (4, 4))
        session.execute("INSERT INTO events (kind, time, value1, value2) VALUES ('ev2', 0, 0, 0)")

        res = session.execute("SELECT COUNT(*) FROM events WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[5]], res

        res = session.execute("SELECT COUNT(1) FROM events WHERE kind IN ('ev1', 'ev2') AND time=0")
        assert rows_to_list(res) == [[2]], res

    def reserved_keyword_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test1 (
                key text PRIMARY KEY,
                count counter,
            )
        """)

        assert_invalid(session, "CREATE TABLE test2 ( select text PRIMARY KEY, x int)", expected=SyntaxException)

    def identifier_test(self):
        session = self.prepare()

        # Test case insensitivity
        session.execute("CREATE TABLE test1 (key_23 int PRIMARY KEY, CoLuMn int)")

        # Should work
        session.execute("INSERT INTO test1 (Key_23, Column) VALUES (0, 0)")
        session.execute("INSERT INTO test1 (KEY_23, COLUMN) VALUES (0, 0)")

        # invalid due to repeated identifiers
        assert_invalid(session, "INSERT INTO test1 (key_23, column, column) VALUES (0, 0, 0)")
        assert_invalid(session, "INSERT INTO test1 (key_23, column, COLUMN) VALUES (0, 0, 0)")
        assert_invalid(session, "INSERT INTO test1 (key_23, key_23, column) VALUES (0, 0, 0)")
        assert_invalid(session, "INSERT INTO test1 (key_23, KEY_23, column) VALUES (0, 0, 0)")

        # Reserved keywords
        assert_invalid(session, "CREATE TABLE test1 (select int PRIMARY KEY, column int)", expected=SyntaxException)

    def keyspace_test(self):
        session = self.prepare()

        assert_invalid(session, "CREATE KEYSPACE test1",
                       expected=SyntaxException,
                       matching="code=2000")
        session.execute("CREATE KEYSPACE test2 WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
        assert_invalid(session, "CREATE KEYSPACE My_much_much_too_long_identifier_that_should_not_work WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

        session.execute("DROP KEYSPACE test2")
        assert_invalid(session, "DROP KEYSPACE non_existing",
                       expected=ConfigurationException,
                       matching="code=2300")
        session.execute("CREATE KEYSPACE test2 WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

    def table_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c int
            )
        """)

        session.execute("""
            CREATE TABLE test2 (
                k int,
                name int,
                value int,
                PRIMARY KEY(k, name)
            ) WITH COMPACT STORAGE
        """)

        session.execute("""
            CREATE TABLE test3 (
                k int,
                c int,
                PRIMARY KEY (k),
            )
        """)

        # existing table
        assert_invalid(session, "CREATE TABLE test3 (k int PRIMARY KEY, c int)",
                       expected=AlreadyExists,
                       matching="ks.test3")
        # repeated column
        assert_invalid(session, "CREATE TABLE test4 (k int PRIMARY KEY, c int, k text)",
                       matching="code=2200")

        # compact storage limitations
        assert_invalid(session, "CREATE TABLE test4 (k int, name, int, c1 int, c2 int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE",
                       expected=SyntaxException)

        session.execute("DROP TABLE test1")
        session.execute("TRUNCATE test2")

        session.execute("""
            CREATE TABLE test1 (
                k int PRIMARY KEY,
                c1 int,
                c2 int,
            )
        """)

    def batch_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE users (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """)

        query = SimpleStatement("""
            BEGIN BATCH
                INSERT INTO users (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE users SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO users (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM users WHERE userid = 'user1';
            APPLY BATCH;
        """, consistency_level=ConsistencyLevel.QUORUM)
        session.execute(query)

    def token_range_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c int,
                v int
            )
        """)

        c = 100
        for i in range(0, c):
            session.execute("INSERT INTO test (k, c, v) VALUES (%d, %d, %d)" % (i, i, i))

        rows = session.execute("SELECT k FROM test")
        inOrder = [x[0] for x in rows]
        assert len(inOrder) == c, 'Expecting %d elements, got %d' % (c, len(inOrder))

        if self.cluster.version() < '1.2':
            session.execute("SELECT k FROM test WHERE token(k) >= 0")
        else:
            min_token = -2 ** 63
        res = session.execute("SELECT k FROM test WHERE token(k) >= %d" % min_token)
        assert len(res) == c, "%s [all: %s]" % (str(res), str(inOrder))

        # make sure comparing tokens to int literals doesn't fall down
        session.execute("SELECT k FROM test WHERE token(k) >= 0")

        res = session.execute("SELECT k FROM test WHERE token(k) >= token(%d) AND token(k) < token(%d)" % (inOrder[32], inOrder[65]))
        assert rows_to_list(res) == [[inOrder[x]] for x in range(32, 65)], "%s [all: %s]" % (str(res), str(inOrder))

    def table_options_test(self):
        session = self.prepare()

        session.execute("""
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

        session.execute("""
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

    def timestamp_and_ttl_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                c text,
                d text
            )
        """)

        session.execute("INSERT INTO test (k, c) VALUES (1, 'test')")
        session.execute("INSERT INTO test (k, c) VALUES (2, 'test') USING TTL 400")

        res = session.execute("SELECT k, c, writetime(c), ttl(c) FROM test")
        assert len(res) == 2, res
        for r in res:
            assert isinstance(r[2], (int, long))
            if r[0] == 1:
                assert r[3] is None, res
            else:
                assert isinstance(r[3], (int, long)), res

        # wrap writetime(), ttl() in other functions (test for CASSANDRA-8451)
        res = session.execute("SELECT k, c, blobAsBigint(bigintAsBlob(writetime(c))), ttl(c) FROM test")
        assert len(res) == 2, res
        for r in res:
            assert isinstance(r[2], (int, long))
            if r[0] == 1:
                assert r[3] is None, res
            else:
                assert isinstance(r[3], (int, long)), res

        res = session.execute("SELECT k, c, writetime(c), blobAsInt(intAsBlob(ttl(c))) FROM test")
        assert len(res) == 2, res
        for r in res:
            assert isinstance(r[2], (int, long))
            if r[0] == 1:
                assert r[3] is None, res
            else:
                assert isinstance(r[3], (int, long)), res

        assert_invalid(session, "SELECT k, c, writetime(k) FROM test")

        res = session.execute("SELECT k, d, writetime(d) FROM test WHERE k = 1")
        assert rows_to_list(res) == [[1, None, None]]

    def no_range_ghost_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """)

        for k in range(0, 5):
            session.execute("INSERT INTO test (k, v) VALUES (%d, 0)" % k)

        unsorted_res = session.execute("SELECT k FROM test")
        res = sorted(unsorted_res)
        assert rows_to_list(res) == [[k] for k in range(0, 5)], res

        session.execute("DELETE FROM test WHERE k=2")

        unsorted_res = session.execute("SELECT k FROM test")
        res = sorted(unsorted_res)
        assert rows_to_list(res) == [[k] for k in range(0, 5) if k is not 2], res

        # Example from #3505
        session.execute("CREATE KEYSPACE ks1 with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        session.execute("USE ks1")
        session.execute("""
            CREATE COLUMNFAMILY users (
                KEY varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                birth_year bigint)
        """)

        session.execute("INSERT INTO users (KEY, password) VALUES ('user1', 'ch@ngem3a')")
        session.execute("UPDATE users SET gender = 'm', birth_year = 1980 WHERE KEY = 'user1'")
        res = session.execute("SELECT * FROM users WHERE KEY='user1'")
        assert rows_to_list(res) == [['user1', 1980, 'm', 'ch@ngem3a']], res

        session.execute("TRUNCATE users")

        res = session.execute("SELECT * FROM users")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT * FROM users WHERE KEY='user1'")
        assert rows_to_list(res) == [], res

    @freshCluster()
    def undefined_column_handling_test(self):
        session = self.prepare(ordered=True)

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 int,
            )
        """)

        session.execute("INSERT INTO test (k, v1, v2) VALUES (0, 0, 0)")
        session.execute("INSERT INTO test (k, v1) VALUES (1, 1)")
        session.execute("INSERT INTO test (k, v1, v2) VALUES (2, 2, 2)")

        res = session.execute("SELECT v2 FROM test")
        assert rows_to_list(res) == [[0], [None], [2]], res

        res = session.execute("SELECT v2 FROM test WHERE k = 1")
        assert rows_to_list(res) == [[None]], res

    @freshCluster()
    def range_tombstones_test(self):
        """ Test deletion by 'composite prefix' (range tombstones) """
        cluster = self.cluster

        # Uses 3 nodes just to make sure RowMutation are correctly serialized
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("""
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
                    session.execute("INSERT INTO test1 (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)" % (i, j, k, n, n))

        for i in xrange(0, rows):
            res = session.execute("SELECT v1, v2 FROM test1 where k = %d" % i)
            assert rows_to_list(res) == [[x, x] for x in xrange(i * cpr, (i + 1) * cpr)], res

        for i in xrange(0, rows):
            session.execute("DELETE FROM test1 WHERE k = %d AND c1 = 0" % i)

        for i in xrange(0, rows):
            res = session.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            assert rows_to_list(res) == [[x, x] for x in xrange(i * cpr + col1, (i + 1) * cpr)], res

        cluster.flush()
        time.sleep(0.2)

        for i in xrange(0, rows):
            res = session.execute("SELECT v1, v2 FROM test1 WHERE k = %d" % i)
            assert rows_to_list(res) == [[x, x] for x in xrange(i * cpr + col1, (i + 1) * cpr)], res

    def range_tombstones_compaction_test(self):
        """ Test deletion by 'composite prefix' (range tombstones) with compaction """
        session = self.prepare()

        session.execute("""
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
                session.execute("INSERT INTO test1 (k, c1, c2, v1) VALUES (0, %d, %d, '%s')" % (c1, c2, '%i%i' % (c1, c2)))

        self.cluster.flush()

        session.execute("DELETE FROM test1 WHERE k = 0 AND c1 = 1")

        self.cluster.flush()
        self.cluster.compact()

        res = session.execute("SELECT v1 FROM test1 WHERE k = 0")
        assert rows_to_list(res) == [['%i%i' % (c1, c2)] for c1 in xrange(0, 4) for c2 in xrange(0, 2) if c1 != 1], res

    def delete_row_test(self):
        """ Test deletion of rows """
        session = self.prepare()

        session.execute("""
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
        session.execute(q % (0, 0, 0, 0, 0))
        session.execute(q % (0, 0, 1, 1, 1))
        session.execute(q % (0, 0, 2, 2, 2))
        session.execute(q % (0, 1, 0, 3, 3))

        session.execute("DELETE FROM test WHERE k = 0 AND c1 = 0 AND c2 = 0")
        res = session.execute("SELECT * FROM test")
        assert len(res) == 3, res

    def range_query_2ndary_test(self):
        """ Test range queries with 2ndary indexes (#4257) """
        session = self.prepare()

        session.execute("CREATE TABLE indextest (id int primary key, row int, setid int);")
        session.execute("CREATE INDEX indextest_setid_idx ON indextest (setid)")

        q = "INSERT INTO indextest (id, row, setid) VALUES (%d, %d, %d);"
        session.execute(q % (0, 0, 0))
        session.execute(q % (1, 1, 0))
        session.execute(q % (2, 2, 0))
        session.execute(q % (3, 3, 0))

        assert_invalid(session, "SELECT * FROM indextest WHERE setid = 0 AND row < 1;")
        res = session.execute("SELECT * FROM indextest WHERE setid = 0 AND row < 1 ALLOW FILTERING;")
        assert rows_to_list(res) == [[0, 0, 0]], res

    def compression_option_validation_test(self):
        """ Check for unknown compression parameters options (#4266) """
        session = self.prepare()

        assert_invalid(session, """
          CREATE TABLE users (key varchar PRIMARY KEY, password varchar, gender varchar)
          WITH compression_parameters:sstable_compressor = 'DeflateCompressor';
        """, expected=SyntaxException)

        if self.cluster.version() >= '1.2':
            assert_invalid(session, """
              CREATE TABLE users (key varchar PRIMARY KEY, password varchar, gender varchar)
              WITH compression = { 'sstable_compressor' : 'DeflateCompressor' };
            """, expected=ConfigurationException)

    def keyspace_creation_options_test(self):
        """ Check one can use arbitrary name for datacenter when creating keyspace (#4278) """
        session = self.prepare()

        # we just want to make sure the following is valid
        if self.cluster.version() >= '1.2':
            session.execute("""
                CREATE KEYSPACE Foo
                    WITH replication = { 'class' : 'NetworkTopologyStrategy',
                                         'us-east' : 1,
                                         'us-west' : 1 };
            """)
        else:
            session.execute("""
                CREATE KEYSPACE Foo
                    WITH strategy_class='NetworkTopologyStrategy'
                     AND strategy_options:"us-east"=1
                     AND strategy_options:"us-west"=1;
            """)

    def set_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                tags set<text>,
                PRIMARY KEY (fn, ln)
            )
        """)

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        session.execute(q % "tags = tags + { 'foo' }")
        session.execute(q % "tags = tags + { 'bar' }")
        session.execute(q % "tags = tags + { 'foo' }")
        session.execute(q % "tags = tags + { 'foobar' }")
        session.execute(q % "tags = tags - { 'bar' }")

        res = session.execute("SELECT tags FROM user")
        assert rows_to_list(res) == [[set(['foo', 'foobar'])]], res

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        session.execute(q % "tags = { 'a', 'c', 'b' }")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[set(['a', 'b', 'c'])]], res

        time.sleep(.01)

        session.execute(q % "tags = { 'm', 'n' }")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[set(['m', 'n'])]], res

        session.execute("DELETE tags['m'] FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[set(['n'])]], res

        session.execute("DELETE tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        if self.cluster.version() <= "1.2":
            assert rows_to_list(res) == [None], res
        else:
            assert rows_to_list(res) == [], res

    def map_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                m map<text, int>,
                PRIMARY KEY (fn, ln)
            )
        """)

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        session.execute(q % "m['foo'] = 3")
        session.execute(q % "m['bar'] = 4")
        session.execute(q % "m['woot'] = 5")
        session.execute(q % "m['bar'] = 6")
        session.execute("DELETE m['foo'] FROM user WHERE fn='Tom' AND ln='Bombadil'")

        res = session.execute("SELECT m FROM user")
        assert rows_to_list(res) == [[{'woot': 5, 'bar': 6}]], res

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        session.execute(q % "m = { 'a' : 4 , 'c' : 3, 'b' : 2 }")
        res = session.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[{'a': 4, 'b': 2, 'c': 3}]], res

        time.sleep(.01)

        # Check we correctly overwrite
        session.execute(q % "m = { 'm' : 4 , 'n' : 1, 'o' : 2 }")
        res = session.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        assert rows_to_list(res) == [[{'m': 4, 'n': 1, 'o': 2}]], res

        session.execute(q % "m = {}")
        res = session.execute("SELECT m FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        if self.cluster.version() <= "1.2":
            assert rows_to_list(res) == [None], res
        else:
            assert rows_to_list(res) == [], res

    def list_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE user (
                fn text,
                ln text,
                tags list<text>,
                PRIMARY KEY (fn, ln)
            )
        """)

        q = "UPDATE user SET %s WHERE fn='Tom' AND ln='Bombadil'"
        session.execute(q % "tags = tags + [ 'foo' ]")
        session.execute(q % "tags = tags + [ 'bar' ]")
        session.execute(q % "tags = tags + [ 'foo' ]")
        session.execute(q % "tags = tags + [ 'foobar' ]")

        res = session.execute("SELECT tags FROM user")
        self.assertItemsEqual(rows_to_list(res), [[['foo', 'bar', 'foo', 'foobar']]])

        q = "UPDATE user SET %s WHERE fn='Bilbo' AND ln='Baggins'"
        session.execute(q % "tags = [ 'a', 'c', 'b', 'c' ]")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        self.assertItemsEqual(rows_to_list(res), [[['a', 'c', 'b', 'c']]])

        session.execute(q % "tags = [ 'm', 'n' ] + tags")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        self.assertItemsEqual(rows_to_list(res), [[['m', 'n', 'a', 'c', 'b', 'c']]])

        session.execute(q % "tags[2] = 'foo', tags[4] = 'bar'")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        self.assertItemsEqual(rows_to_list(res), [[['m', 'n', 'foo', 'c', 'bar', 'c']]])

        session.execute("DELETE tags[2] FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        self.assertItemsEqual(rows_to_list(res), [[['m', 'n', 'c', 'bar', 'c']]])

        session.execute(q % "tags = tags - [ 'bar' ]")
        res = session.execute("SELECT tags FROM user WHERE fn='Bilbo' AND ln='Baggins'")
        self.assertItemsEqual(rows_to_list(res), [[['m', 'n', 'c', 'c']]])

    def multi_collection_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE foo(
                k uuid PRIMARY KEY,
                L list<int>,
                M map<text, int>,
                S set<int>
            );
        """)

        session.execute("UPDATE ks.foo SET L = [1, 3, 5] WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        session.execute("UPDATE ks.foo SET L = L + [7, 11, 13] WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        session.execute("UPDATE ks.foo SET S = {1, 3, 5} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        session.execute("UPDATE ks.foo SET S = S + {7, 11, 13} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        session.execute("UPDATE ks.foo SET M = {'foo': 1, 'bar' : 3} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")
        session.execute("UPDATE ks.foo SET M = M + {'foobar' : 4} WHERE k = b017f48f-ae67-11e1-9096-005056c00008;")

        res = session.execute("SELECT L, M, S FROM foo WHERE k = b017f48f-ae67-11e1-9096-005056c00008")
        self.assertItemsEqual(rows_to_list(res), [[
            [1, 3, 5, 7, 11, 13],
            OrderedDict([('bar', 3), ('foo', 1), ('foobar', 4)]),
            sortedset([1, 3, 5, 7, 11, 13])
        ]])

    def range_query_test(self):
        """ Range test query from #4372 """
        session = self.prepare()

        session.execute("CREATE TABLE test (a int, b int, c int, d int, e int, f text, PRIMARY KEY (a, b, c, d, e) )")

        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 2, '2');")
        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, '1');")
        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 2, 1, '1');")
        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 3, '3');")
        session.execute("INSERT INTO test (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 5, '5');")

        res = session.execute("SELECT a, b, c, d, e, f FROM test WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 AND e >= 2;")
        assert rows_to_list(res) == [[1, 1, 1, 1, 2, u'2'], [1, 1, 1, 1, 3, u'3'], [1, 1, 1, 1, 5, u'5']], res

    def update_type_test(self):
        """ Test altering the type of a column, including the one in the primary key (#4041) """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k text,
                c text,
                s set<text>,
                v text,
                PRIMARY KEY (k, c)
            )
        """)

        req = "INSERT INTO test (k, c, v, s) VALUES ('%s', '%s', '%s', {'%s'})"
        # using utf8 character so that we can see the transition to BytesType
        session.execute(req % ('ɸ', 'ɸ', 'ɸ', 'ɸ'))

        session.execute("SELECT * FROM test")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[u'ɸ', u'ɸ', set([u'ɸ']), u'ɸ']], res

        session.execute("ALTER TABLE test ALTER v TYPE blob")
        res = session.execute("SELECT * FROM test")
        # the last should not be utf8 but a raw string
        assert rows_to_list(res) == [[u'ɸ', u'ɸ', set([u'ɸ']), 'ɸ']], res

        session.execute("ALTER TABLE test ALTER k TYPE blob")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [['ɸ', u'ɸ', set([u'ɸ']), 'ɸ']], res

        session.execute("ALTER TABLE test ALTER c TYPE blob")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [['ɸ', 'ɸ', set([u'ɸ']), 'ɸ']], res

        if self.cluster.version() < "2.1":
            assert_invalid(session, "ALTER TABLE test ALTER s TYPE set<blob>", expected=ConfigurationException)
        else:
            session.execute("ALTER TABLE test ALTER s TYPE set<blob>")
            res = session.execute("SELECT * FROM test")
            assert rows_to_list(res) == [['ɸ', 'ɸ', set(['ɸ']), 'ɸ']], res

    def composite_row_key_test(self):
        session = self.prepare()

        session.execute("""
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
            session.execute(req % (0, i, i, i))

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]], res

        res = session.execute("SELECT * FROM test WHERE k1 = 0 and k2 IN (1, 3)")
        assert rows_to_list(res) == [[0, 1, 1, 1], [0, 3, 3, 3]], res

        assert_invalid(session, "SELECT * FROM test WHERE k2 = 3")

        v = self.cluster.version()
        if v < "2.2.0":
            assert_invalid(session, "SELECT * FROM test WHERE k1 IN (0, 1) and k2 = 3")

        res = session.execute("SELECT * FROM test WHERE token(k1, k2) = token(0, 1)")
        assert rows_to_list(res) == [[0, 1, 1, 1]], res

        res = session.execute("SELECT * FROM test WHERE token(k1, k2) > " + str(-((2 ** 63) - 1)))
        assert rows_to_list(res) == [[0, 2, 2, 2], [0, 3, 3, 3], [0, 0, 0, 0], [0, 1, 1, 1]], res

    def cql3_insert_thrift_test(self):
        """ Check that we can insert from thrift into a CQL3 table (#4377) """
        session = self.prepare(start_rpc=True)

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('ks')
        key = struct.pack('>i', 2)
        column_name_component = struct.pack('>i', 4)
        # component length + component + EOC + component length + component + EOC
        column_name = '\x00\x04' + column_name_component + '\x00' + '\x00\x01' + 'v' + '\x00'
        value = struct.pack('>i', 8)
        client.batch_mutate(
            {key: {'test': [Mutation(ColumnOrSuperColumn(column=Column(name=column_name, value=value, timestamp=100)))]}},
            ThriftConsistencyLevel.ONE)

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[2, 4, 8]], res

    def row_existence_test(self):
        """ Check the semantic of CQL row existence (part of #4361) """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v1 int,
                v2 int,
                PRIMARY KEY (k, c)
            )
        """)

        session.execute("INSERT INTO test (k, c, v1, v2) VALUES (1, 1, 1, 1)")

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[1, 1, 1, 1]], res

        assert_invalid(session, "DELETE c FROM test WHERE k = 1 AND c = 1")

        session.execute("DELETE v2 FROM test WHERE k = 1 AND c = 1")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[1, 1, 1, None]], res

        session.execute("DELETE v1 FROM test WHERE k = 1 AND c = 1")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[1, 1, None, None]], res

        session.execute("DELETE FROM test WHERE k = 1 AND c = 1")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [], res

        session.execute("INSERT INTO test (k, c) VALUES (2, 2)")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[2, 2, None, None]], res

    @freshCluster()
    def only_pk_test(self):
        """ Check table with only a PK (#4361) """
        session = self.prepare(ordered=True)

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                PRIMARY KEY (k, c)
            )
        """)

        q = "INSERT INTO test (k, c) VALUES (%d, %d)"
        for k in range(0, 2):
            for c in range(0, 2):
                session.execute(q % (k, c))

        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[x, y] for x in range(0, 2) for y in range(0, 2)], res

        # Check for dense tables too
        session.execute("""
            CREATE TABLE test2 (
                k int,
                c int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE
        """)

        q = "INSERT INTO test2 (k, c) VALUES (%d, %d)"
        for k in range(0, 2):
            for c in range(0, 2):
                session.execute(q % (k, c))

        res = session.execute("SELECT * FROM test2")
        assert rows_to_list(res) == [[x, y] for x in range(0, 2) for y in range(0, 2)], res

    def date_test(self):
        """ Check dates are correctly recognized and validated """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                t timestamp
            )
        """)

        session.execute("INSERT INTO test (k, t) VALUES (0, '2011-02-03')")
        assert_invalid(session, "INSERT INTO test (k, t) VALUES (0, '2011-42-42')")

    @freshCluster()
    def range_slice_test(self):
        """ Test a regression from #1337 """

        cluster = self.cluster

        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("""
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int
            );
        """)
        time.sleep(1)

        session.execute("INSERT INTO test (k, v) VALUES ('foo', 0)")
        session.execute("INSERT INTO test (k, v) VALUES ('bar', 1)")

        res = session.execute("SELECT * FROM test")
        assert len(res) == 2, res

    @freshCluster()
    def composite_index_with_pk_test(self):

        session = self.prepare(ordered=True)
        session.execute("""
            CREATE TABLE blogs (
                blog_id int,
                time1 int,
                time2 int,
                author text,
                content text,
                PRIMARY KEY (blog_id, time1, time2)
            )
        """)

        session.execute("CREATE INDEX ON blogs(author)")

        req = "INSERT INTO blogs (blog_id, time1, time2, author, content) VALUES (%d, %d, %d, '%s', '%s')"
        session.execute(req % (1, 0, 0, 'foo', 'bar1'))
        session.execute(req % (1, 0, 1, 'foo', 'bar2'))
        session.execute(req % (2, 1, 0, 'foo', 'baz'))
        session.execute(req % (3, 0, 1, 'gux', 'qux'))

        res = session.execute("SELECT blog_id, content FROM blogs WHERE author='foo'")
        assert rows_to_list(res) == [[1, 'bar1'], [1, 'bar2'], [2, 'baz']], res

        res = session.execute("SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [[2, 'baz']], res

        res = session.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [[2, 'baz']], res

        res = session.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [[2, 'baz']], res

        res = session.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo' ALLOW FILTERING")
        assert rows_to_list(res) == [], res

        assert_invalid(session, "SELECT content FROM blogs WHERE time2 >= 0 AND author='foo'")

        # as discussed in CASSANDRA-8148, some queries that should have required ALLOW FILTERING
        # in 2.0 have been fixed for 2.2
        v = self.cluster.version()
        if v < "2.2.0":
            session.execute("SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo'")
            session.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo'")
            session.execute("SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo'")
            session.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo'")
            session.execute("SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo'")
        else:
            assert_invalid(session, "SELECT blog_id, content FROM blogs WHERE time1 > 0 AND author='foo'")
            assert_invalid(session, "SELECT blog_id, content FROM blogs WHERE time1 = 1 AND author='foo'")
            assert_invalid(session, "SELECT blog_id, content FROM blogs WHERE time1 = 1 AND time2 = 0 AND author='foo'")
            assert_invalid(session, "SELECT content FROM blogs WHERE time1 = 1 AND time2 = 1 AND author='foo'")
            assert_invalid(session, "SELECT content FROM blogs WHERE time1 = 1 AND time2 > 0 AND author='foo'")

    @freshCluster()
    def limit_bugs_test(self):
        """ Test for LIMIT bugs from 4579 """

        session = self.prepare(ordered=True)
        session.execute("""
            CREATE TABLE testcf (
                a int,
                b int,
                c int,
                d int,
                e int,
                PRIMARY KEY (a, b)
            );
        """)

        session.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (1, 1, 1, 1, 1);")
        session.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (2, 2, 2, 2, 2);")
        session.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (3, 3, 3, 3, 3);")
        session.execute("INSERT INTO testcf (a, b, c, d, e) VALUES (4, 4, 4, 4, 4);")

        res = session.execute("SELECT * FROM testcf;")
        assert rows_to_list(res) == [[1, 1, 1, 1, 1], [2, 2, 2, 2, 2], [3, 3, 3, 3, 3], [4, 4, 4, 4, 4]], res

        res = session.execute("SELECT * FROM testcf LIMIT 1;")  # columns d and e in result row are null
        assert rows_to_list(res) == [[1, 1, 1, 1, 1]], res

        res = session.execute("SELECT * FROM testcf LIMIT 2;")  # columns d and e in last result row are null
        assert rows_to_list(res) == [[1, 1, 1, 1, 1], [2, 2, 2, 2, 2]], res

        session.execute("""
            CREATE TABLE testcf2 (
                a int primary key,
                b int,
                c int,
            );
        """)

        session.execute("INSERT INTO testcf2 (a, b, c) VALUES (1, 1, 1);")
        session.execute("INSERT INTO testcf2 (a, b, c) VALUES (2, 2, 2);")
        session.execute("INSERT INTO testcf2 (a, b, c) VALUES (3, 3, 3);")
        session.execute("INSERT INTO testcf2 (a, b, c) VALUES (4, 4, 4);")

        res = session.execute("SELECT * FROM testcf2;")
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]], res

        res = session.execute("SELECT * FROM testcf2 LIMIT 1;")  # gives 1 row
        assert rows_to_list(res) == [[1, 1, 1]], res

        res = session.execute("SELECT * FROM testcf2 LIMIT 2;")  # gives 1 row
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2]], res

        res = session.execute("SELECT * FROM testcf2 LIMIT 3;")  # gives 2 rows
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2], [3, 3, 3]], res

        res = session.execute("SELECT * FROM testcf2 LIMIT 4;")  # gives 2 rows
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]], res

        res = session.execute("SELECT * FROM testcf2 LIMIT 5;")  # gives 3 rows
        assert rows_to_list(res) == [[1, 1, 1], [2, 2, 2], [3, 3, 3], [4, 4, 4]], res

    def bug_4532_test(self):

        session = self.prepare()
        session.execute("""
            CREATE TABLE compositetest(
                status ascii,
                ctime bigint,
                key ascii,
                nil ascii,
                PRIMARY KEY (status, ctime, key)
            )
        """)

        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345678,'key1','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345678,'key2','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key3','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key4','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345679,'key5','')")
        session.execute("INSERT INTO compositetest(status,ctime,key,nil) VALUES ('C',12345680,'key6','')")

        assert_invalid(session, "SELECT * FROM compositetest WHERE ctime>=12345679 AND key='key3' AND ctime<=12345680 LIMIT 3;")
        assert_invalid(session, "SELECT * FROM compositetest WHERE ctime=12345679  AND key='key3' AND ctime<=12345680 LIMIT 3")

    @freshCluster()
    def order_by_multikey_test(self):
        """ Test for #4612 bug and more generaly order by when multiple C* rows are queried """

        session = self.prepare(ordered=True)
        session.default_fetch_size = None
        session.execute("""
            CREATE TABLE test(
                my_id varchar,
                col1 int,
                col2 int,
                value varchar,
                PRIMARY KEY (my_id, col1, col2)
            );
        """)

        session.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key1', 1, 1, 'a');")
        session.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key2', 3, 3, 'a');")
        session.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key3', 2, 2, 'b');")
        session.execute("INSERT INTO test(my_id, col1, col2, value) VALUES ( 'key4', 2, 1, 'b');")

        res = session.execute("SELECT col1 FROM test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1;")
        assert rows_to_list(res) == [[1], [2], [3]], res

        res = session.execute("SELECT col1, value, my_id, col2 FROM test WHERE my_id in('key3', 'key4') ORDER BY col1, col2;")
        assert rows_to_list(res) == [[2, 'b', 'key4', 1], [2, 'b', 'key3', 2]], res

        assert_invalid(session, "SELECT col1 FROM test ORDER BY col1;")
        assert_invalid(session, "SELECT col1 FROM test WHERE my_id > 'key1' ORDER BY col1;")

    @freshCluster()
    def create_alter_options_test(self):
        session = self.prepare(create_keyspace=False)

        assert_invalid(session, "CREATE KEYSPACE ks1", expected=SyntaxException)
        assert_invalid(session, "CREATE KEYSPACE ks1 WITH replication= { 'replication_factor' : 1 }", expected=ConfigurationException)

        session.execute("CREATE KEYSPACE ks1 WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
        session.execute("CREATE KEYSPACE ks2 WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes=false")

        if self.cluster.version() >= '2.2':
            assert_all(session, "SELECT keyspace_name, durable_writes FROM system.schema_keyspaces",
                       [['system_auth', True], ['ks1', True], ['system_distributed', True], ['system', True], ['system_traces', True], ['ks2', False]])
        else:
            assert_all(session, "SELECT keyspace_name, durable_writes FROM system.schema_keyspaces",
                       [['ks1', True], ['system', True], ['system_traces', True], ['ks2', False]])

        session.execute("ALTER KEYSPACE ks1 WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND durable_writes=False")
        session.execute("ALTER KEYSPACE ks2 WITH durable_writes=true")

        if self.cluster.version() >= '2.2':
            assert_all(session, "SELECT keyspace_name, durable_writes, strategy_class FROM system.schema_keyspaces",
                       [[u'system_auth', True, u'org.apache.cassandra.locator.SimpleStrategy'],
                        [u'ks1', False, u'org.apache.cassandra.locator.NetworkTopologyStrategy'],
                        [u'system_distributed', True, u'org.apache.cassandra.locator.SimpleStrategy'],
                        [u'system', True, u'org.apache.cassandra.locator.LocalStrategy'],
                        [u'system_traces', True, u'org.apache.cassandra.locator.SimpleStrategy'],
                        [u'ks2', True, u'org.apache.cassandra.locator.SimpleStrategy']])
        else:
            assert_all(session, "SELECT keyspace_name, durable_writes, strategy_class FROM system.schema_keyspaces",
                       [[u'ks1', False, u'org.apache.cassandra.locator.NetworkTopologyStrategy'],
                        [u'system', True, u'org.apache.cassandra.locator.LocalStrategy'],
                        [u'system_traces', True, u'org.apache.cassandra.locator.SimpleStrategy'],
                        [u'ks2', True, u'org.apache.cassandra.locator.SimpleStrategy']])

        session.execute("USE ks1")

        assert_invalid(session, "CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'min_threshold' : 4 }", expected=ConfigurationException)
        session.execute("CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }")
        assert_one(session, "SELECT columnfamily_name, min_compaction_threshold FROM system.schema_columnfamilies WHERE keyspace_name='ks1'", ['cf1', 7])

    def remove_range_slice_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            )
        """)

        for i in range(0, 3):
            session.execute("INSERT INTO test (k, v) VALUES (%d, %d)" % (i, i))

        session.execute("DELETE FROM test WHERE k = 1")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, 0], [2, 2]], res

    def indexes_composite_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                blog_id int,
                timestamp int,
                author text,
                content text,
                PRIMARY KEY (blog_id, timestamp)
            )
        """)

        req = "INSERT INTO test (blog_id, timestamp, author, content) VALUES (%d, %d, '%s', '%s')"
        session.execute(req % (0, 0, "bob", "1st post"))
        session.execute(req % (0, 1, "tom", "2nd post"))
        session.execute(req % (0, 2, "bob", "3rd post"))
        session.execute(req % (0, 3, "tom", "4nd post"))
        session.execute(req % (1, 0, "bob", "5th post"))

        session.execute("CREATE INDEX ON test(author)")
        time.sleep(1)

        res = session.execute("SELECT blog_id, timestamp FROM test WHERE author = 'bob'")
        assert rows_to_list(res) == [[1, 0], [0, 0], [0, 2]], res

        session.execute(req % (1, 1, "tom", "6th post"))
        session.execute(req % (1, 2, "tom", "7th post"))
        session.execute(req % (1, 3, "bob", "8th post"))

        res = session.execute("SELECT blog_id, timestamp FROM test WHERE author = 'bob'")
        assert rows_to_list(res) == [[1, 0], [1, 3], [0, 0], [0, 2]], res

        session.execute("DELETE FROM test WHERE blog_id = 0 AND timestamp = 2")

        res = session.execute("SELECT blog_id, timestamp FROM test WHERE author = 'bob'")
        assert rows_to_list(res) == [[1, 0], [1, 3], [0, 0]], res

    def refuse_in_with_indexes_test(self):
        """ Test for the validation bug of #4709 """

        session = self.prepare()
        session.execute("create table t1 (pk varchar primary key, col1 varchar, col2 varchar);")
        session.execute("create index t1_c1 on t1(col1);")
        session.execute("create index t1_c2 on t1(col2);")
        session.execute("insert into t1  (pk, col1, col2) values ('pk1','foo1','bar1');")
        session.execute("insert into t1  (pk, col1, col2) values ('pk1a','foo1','bar1');")
        session.execute("insert into t1  (pk, col1, col2) values ('pk1b','foo1','bar1');")
        session.execute("insert into t1  (pk, col1, col2) values ('pk1c','foo1','bar1');")
        session.execute("insert into t1  (pk, col1, col2) values ('pk2','foo2','bar2');")
        session.execute("insert into t1  (pk, col1, col2) values ('pk3','foo3','bar3');")
        assert_invalid(session, "select * from t1 where col2 in ('bar1', 'bar2');")

    def validate_counter_regular_test(self):
        """
        @jira_ticket CASSANDRA-4706

        Regression test for a validation bug.
        """

        session = self.prepare()
        assert_invalid(session, "CREATE TABLE test (id bigint PRIMARY KEY, count counter, things set<text>)",
                       matching=r"Cannot add a( non)? counter column", expected=ConfigurationException)

    def reversed_compact_test(self):
        """
        @jira_ticket CASSANDRA-4716

        Regression test for #4716 bug and more generally for good behavior of ordering.
        """

        session = self.prepare()
        session.execute("""
            CREATE TABLE test1 (
                k text,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE
              AND CLUSTERING ORDER BY (c DESC);
        """)

        for i in range(0, 10):
            session.execute("INSERT INTO test1(k, c, v) VALUES ('foo', %i, %i)" % (i, i))

        res = session.execute("SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo'")
        assert rows_to_list(res) == [[5], [4], [3]], res

        res = session.execute("SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo'")
        assert rows_to_list(res) == [[6], [5], [4], [3], [2]], res

        res = session.execute("SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC")
        assert rows_to_list(res) == [[3], [4], [5]], res

        res = session.execute("SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC")
        assert rows_to_list(res) == [[2], [3], [4], [5], [6]], res

        res = session.execute("SELECT c FROM test1 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC")
        assert rows_to_list(res) == [[5], [4], [3]], res

        res = session.execute("SELECT c FROM test1 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC")
        assert rows_to_list(res) == [[6], [5], [4], [3], [2]], res

        session.execute("""
            CREATE TABLE test2 (
                k text,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;
        """)

        for i in range(0, 10):
            session.execute("INSERT INTO test2(k, c, v) VALUES ('foo', %i, %i)" % (i, i))

        res = session.execute("SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo'")
        assert rows_to_list(res) == [[3], [4], [5]], res

        res = session.execute("SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo'")
        assert rows_to_list(res) == [[2], [3], [4], [5], [6]], res

        res = session.execute("SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC")
        assert rows_to_list(res) == [[3], [4], [5]], res

        res = session.execute("SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC")
        assert rows_to_list(res) == [[2], [3], [4], [5], [6]], res

        res = session.execute("SELECT c FROM test2 WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC")
        assert rows_to_list(res) == [[5], [4], [3]], res

        res = session.execute("SELECT c FROM test2 WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC")
        assert rows_to_list(res) == [[6], [5], [4], [3], [2]], res

    def unescaped_string_test(self):
        """
        Test that unescaped strings in CQL statements raise syntax exceptions.
        """

        session = self.prepare()
        session.execute("""
            CREATE TABLE test (
                k text PRIMARY KEY,
                c text,
            )
        """)

        # The \ in this query string is not forwarded to cassandra.
        # The ' is being escaped in python, but only ' is forwarded
        # over the wire instead of \'.
        assert_invalid(session, "INSERT INTO test (k, c) VALUES ('foo', 'CQL is cassandra\'s best friend')", expected=SyntaxException)

    def reversed_compact_multikey_test(self):
        """
        @jira_ticket CASSANDRA-4760
        @jira_ticket CASSANDRA-4759

        Regression test for two related tickets.
        """

        session = self.prepare()
        session.execute("""
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
                session.execute("INSERT INTO test(key, c1, c2, value) VALUES ('foo', %i, %i, 'bar');" % (i, j))

        # Equalities

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1")
        assert rows_to_list(res) == [[1, 2], [1, 1], [1, 0]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[1, 0], [1, 1], [1, 2]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 = 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[1, 2], [1, 1], [1, 0]], res

        # GT

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1")
        assert rows_to_list(res) == [[2, 2], [2, 1], [2, 0]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[2, 0], [2, 1], [2, 2]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 > 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[2, 2], [2, 1], [2, 0]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1")
        assert rows_to_list(res) == [[2, 2], [2, 1], [2, 0], [1, 2], [1, 1], [1, 0]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[1, 0], [1, 1], [1, 2], [2, 0], [2, 1], [2, 2]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC")
        assert rows_to_list(res) == [[1, 0], [1, 1], [1, 2], [2, 0], [2, 1], [2, 2]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 >= 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[2, 2], [2, 1], [2, 0], [1, 2], [1, 1], [1, 0]], res

        # LT

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1")
        assert rows_to_list(res) == [[0, 2], [0, 1], [0, 0]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[0, 0], [0, 1], [0, 2]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 < 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[0, 2], [0, 1], [0, 0]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1")
        assert rows_to_list(res) == [[1, 2], [1, 1], [1, 0], [0, 2], [0, 1], [0, 0]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC, c2 ASC")
        assert rows_to_list(res) == [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC")
        assert rows_to_list(res) == [[0, 0], [0, 1], [0, 2], [1, 0], [1, 1], [1, 2]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE key='foo' AND c1 <= 1 ORDER BY c1 DESC, c2 DESC")
        assert rows_to_list(res) == [[1, 2], [1, 1], [1, 0], [0, 2], [0, 1], [0, 0]], res

    def collection_and_regular_test(self):

        session = self.prepare()

        session.execute("""
          CREATE TABLE test (
            k int PRIMARY KEY,
            l list<int>,
            c int
          )
        """)

        session.execute("INSERT INTO test(k, l, c) VALUES(3, [0, 1, 2], 4)")
        session.execute("UPDATE test SET l[0] = 1, c = 42 WHERE k = 3")
        res = session.execute("SELECT l, c FROM test WHERE k = 3")
        self.assertItemsEqual(rows_to_list(res), [[[1, 1, 2], 42]])

    def batch_and_list_test(self):
        session = self.prepare()

        session.execute("""
          CREATE TABLE test (
            k int PRIMARY KEY,
            l list<int>
          )
        """)

        session.execute("""
          BEGIN BATCH
            UPDATE test SET l = l + [ 1 ] WHERE k = 0;
            UPDATE test SET l = l + [ 2 ] WHERE k = 0;
            UPDATE test SET l = l + [ 3 ] WHERE k = 0;
          APPLY BATCH
        """)

        res = session.execute("SELECT l FROM test WHERE k = 0")
        self.assertItemsEqual(rows_to_list(res[0]), [[1, 2, 3]])

        session.execute("""
          BEGIN BATCH
            UPDATE test SET l = [ 1 ] + l WHERE k = 1;
            UPDATE test SET l = [ 2 ] + l WHERE k = 1;
            UPDATE test SET l = [ 3 ] + l WHERE k = 1;
          APPLY BATCH
        """)

        res = session.execute("SELECT l FROM test WHERE k = 1")
        self.assertItemsEqual(rows_to_list(res[0]), [[3, 2, 1]])

    def boolean_test(self):
        session = self.prepare()

        session.execute("""
          CREATE TABLE test (
            k boolean PRIMARY KEY,
            b boolean
          )
        """)

        session.execute("INSERT INTO test (k, b) VALUES (true, false)")
        res = session.execute("SELECT * FROM test WHERE k = true")
        assert rows_to_list(res) == [[True, False]], res

    def multiordering_test(self):
        session = self.prepare()
        session.execute("""
            CREATE TABLE test (
                k text,
                c1 int,
                c2 int,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """)

        for i in range(0, 2):
            for j in range(0, 2):
                session.execute("INSERT INTO test(k, c1, c2) VALUES ('foo', %i, %i)" % (i, j))

        res = session.execute("SELECT c1, c2 FROM test WHERE k = 'foo'")
        assert rows_to_list(res) == [[0, 1], [0, 0], [1, 1], [1, 0]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 ASC, c2 DESC")
        assert rows_to_list(res) == [[0, 1], [0, 0], [1, 1], [1, 0]], res

        res = session.execute("SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 DESC, c2 ASC")
        assert rows_to_list(res) == [[1, 0], [1, 1], [0, 0], [0, 1]], res

        assert_invalid(session, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c2 DESC")
        assert_invalid(session, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c2 ASC")
        assert_invalid(session, "SELECT c1, c2 FROM test WHERE k = 'foo' ORDER BY c1 ASC, c2 ASC")

    def multiordering_validation_test(self):
        session = self.prepare()

        assert_invalid(session, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 DESC)")
        assert_invalid(session, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC)")
        assert_invalid(session, "CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC, c3 DESC)")

        session.execute("CREATE TABLE test1 (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)")
        session.execute("CREATE TABLE test2 (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)")

    def bug_4882_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);
        """)

        session.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 0, 0, 0);")
        session.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 1, 1, 1);")
        session.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 0, 2, 2);")
        session.execute("INSERT INTO test (k, c1, c2, v) VALUES (0, 1, 3, 3);")

        res = session.execute("select * from test where k = 0 limit 1;")
        assert rows_to_list(res) == [[0, 0, 2, 2]], res

    def multi_list_set_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                l1 list<int>,
                l2 list<int>
            )
        """)

        session.execute("INSERT INTO test (k, l1, l2) VALUES (0, [1, 2, 3], [4, 5, 6])")
        session.execute("UPDATE test SET l2[1] = 42, l1[1] = 24  WHERE k = 0")

        res = session.execute("SELECT l1, l2 FROM test WHERE k = 0")
        self.assertItemsEqual(rows_to_list(res), [[[1, 24, 3], [4, 42, 6]]])

    @freshCluster()
    def composite_index_collections_test(self):
        session = self.prepare(ordered=True)
        session.execute("""
            CREATE TABLE blogs (
                blog_id int,
                time1 int,
                time2 int,
                author text,
                content set<text>,
                PRIMARY KEY (blog_id, time1, time2)
            )
        """)

        session.execute("CREATE INDEX ON blogs(author)")

        req = "INSERT INTO blogs (blog_id, time1, time2, author, content) VALUES (%d, %d, %d, '%s', %s)"
        session.execute(req % (1, 0, 0, 'foo', "{ 'bar1', 'bar2' }"))
        session.execute(req % (1, 0, 1, 'foo', "{ 'bar2', 'bar3' }"))
        session.execute(req % (2, 1, 0, 'foo', "{ 'baz' }"))
        session.execute(req % (3, 0, 1, 'gux', "{ 'qux' }"))

        res = session.execute("SELECT blog_id, content FROM blogs WHERE author='foo'")
        assert rows_to_list(res) == [[1, set(['bar1', 'bar2'])], [1, set(['bar2', 'bar3'])], [2, set(['baz'])]], res

    @freshCluster()
    def truncate_clean_cache_test(self):
        session = self.prepare(ordered=True, use_cache=True)

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 int,
            ) WITH CACHING = ALL;
        """)

        for i in range(0, 3):
            session.execute("INSERT INTO test(k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i * 2))

        res = session.execute("SELECT v1, v2 FROM test WHERE k IN (0, 1, 2)")
        assert rows_to_list(res) == [[0, 0], [1, 2], [2, 4]], res

        session.execute("TRUNCATE test")

        res = session.execute("SELECT v1, v2 FROM test WHERE k IN (0, 1, 2)")
        assert rows_to_list(res) == [], res

    def allow_filtering_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        for i in range(0, 3):
            for j in range(0, 3):
                session.execute("INSERT INTO test(k, c, v) VALUES(%d, %d, %d)" % (i, j, j))

        # Don't require filtering, always allowed
        queries = ["SELECT * FROM test WHERE k = 1",
                   "SELECT * FROM test WHERE k = 1 AND c > 2",
                   "SELECT * FROM test WHERE k = 1 AND c = 2"]
        for q in queries:
            session.execute(q)
            session.execute(q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = ["SELECT * FROM test WHERE c = 2",
                   "SELECT * FROM test WHERE c > 2 AND c <= 4"]
        for q in queries:
            assert_invalid(session, q)
            session.execute(q + " ALLOW FILTERING")

        session.execute("""
            CREATE TABLE indexed (
                k int PRIMARY KEY,
                a int,
                b int,
            )
        """)

        session.execute("CREATE INDEX ON indexed(a)")

        for i in range(0, 5):
            session.execute("INSERT INTO indexed(k, a, b) VALUES(%d, %d, %d)" % (i, i * 10, i * 100))

        # Don't require filtering, always allowed
        queries = ["SELECT * FROM indexed WHERE k = 1",
                   "SELECT * FROM indexed WHERE a = 20"]
        for q in queries:
            session.execute(q)
            session.execute(q + " ALLOW FILTERING")

        # Require filtering, allowed only with ALLOW FILTERING
        queries = ["SELECT * FROM indexed WHERE a = 20 AND b = 200"]
        for q in queries:
            assert_invalid(session, q)
            session.execute(q + " ALLOW FILTERING")

    def range_with_deletes_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int,
            )
        """)

        nb_keys = 30
        nb_deletes = 5

        for i in range(0, nb_keys):
            session.execute("INSERT INTO test(k, v) VALUES (%d, %d)" % (i, i))

        for i in random.sample(xrange(nb_keys), nb_deletes):
            session.execute("DELETE FROM test WHERE k = %d" % i)

        res = session.execute("SELECT * FROM test LIMIT %d" % (nb_keys / 2))
        assert len(res) == nb_keys / 2, "Expected %d but got %d" % (nb_keys / 2, len(res))

    def alter_with_collections_test(self):
        """
        @jira_ticket CASSANDRA-4982

        Test you can add columns in a table with collections. Regression test
        for CASSANDRA-4982.
        """
        session = self.prepare()

        session.execute("CREATE TABLE collections (key int PRIMARY KEY, aset set<text>)")
        session.execute("ALTER TABLE collections ADD c text")
        session.execute("ALTER TABLE collections ADD alist list<text>")

    def collection_compact_test(self):
        session = self.prepare()

        assert_invalid(session, """
            CREATE TABLE test (
                user ascii PRIMARY KEY,
                mails list<text>
            ) WITH COMPACT STORAGE;
        """)

    def collection_function_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                l set<int>
            )
        """)

        assert_invalid(session, "SELECT ttl(l) FROM test WHERE k = 0")
        assert_invalid(session, "SELECT writetime(l) FROM test WHERE k = 0")

    def collection_counter_test(self):
        session = self.prepare()

        assert_invalid(session, """
            CREATE TABLE test (
                k int PRIMARY KEY,
                l list<counter>
            )
        """, expected=(InvalidRequest, SyntaxException))

        assert_invalid(session, """
            CREATE TABLE test (
                k int PRIMARY KEY,
                s set<counter>
            )
        """, expected=(InvalidRequest, SyntaxException))

        assert_invalid(session, """
            CREATE TABLE test (
                k int PRIMARY KEY,
                m map<text, counter>
            )
        """, expected=(InvalidRequest, SyntaxException))

    def composite_partition_key_validation_test(self):
        """
        @jira_ticket CASSANDRA-5122

        Regression test for CASSANDRA-5122.
        """
        session = self.prepare()

        session.execute("CREATE TABLE foo (a int, b text, c uuid, PRIMARY KEY ((a, b)));")

        session.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'aze', 4d481800-4c5f-11e1-82e0-3f484de45426)")
        session.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'ert', 693f5800-8acb-11e3-82e0-3f484de45426)")
        session.execute("INSERT INTO foo (a, b , c ) VALUES (  1 , 'opl', d4815800-2d8d-11e0-82e0-3f484de45426)")

        res = session.execute("SELECT * FROM foo")
        assert len(res) == 3, res

        assert_invalid(session, "SELECT * FROM foo WHERE a=1")

    def large_clustering_in_test(self):
        """
        @jira_ticket CASSANDRA-8410
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            )
        """)

        insert_statement = session.prepare("INSERT INTO test (k, c, v) VALUES (?, ?, ?)")
        session.execute(insert_statement, (0, 0, 0))

        select_statement = session.prepare("SELECT * FROM test WHERE k=? AND c IN ?")
        in_values = list(range(10000))

        # try to fetch one existing row and 9999 non-existing rows
        rows = session.execute(select_statement, [0, in_values])
        self.assertEqual(1, len(rows))
        self.assertEqual((0, 0, 0), rows[0])

        # insert approximately 1000 random rows between 0 and 10k
        clustering_values = set([random.randint(0, 9999) for _ in range(1000)])
        clustering_values.add(0)
        args = [(0, i, i) for i in clustering_values]
        execute_concurrent_with_args(session, insert_statement, args)

        rows = session.execute(select_statement, [0, in_values])
        self.assertEqual(len(clustering_values), len(rows))

    @since('1.2.1')
    def timeuuid_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                t timeuuid,
                PRIMARY KEY (k, t)
            )
        """)

        assert_invalid(session, "INSERT INTO test (k, t) VALUES (0, 2012-11-07 18:18:22-0800)", expected=SyntaxException)

        for i in range(4):
            session.execute("INSERT INTO test (k, t) VALUES (0, now())")
            time.sleep(1)

        res = session.execute("SELECT * FROM test")
        assert len(res) == 4, res
        dates = [d[1] for d in res]

        res = session.execute("SELECT * FROM test WHERE k = 0 AND t >= %s" % dates[0])
        assert len(res) == 4, res

        res = session.execute("SELECT * FROM test WHERE k = 0 AND t < %s" % dates[0])
        assert len(res) == 0, res

        res = session.execute("SELECT * FROM test WHERE k = 0 AND t > %s AND t <= %s" % (dates[0], dates[2]))
        assert len(res) == 2, res

        res = session.execute("SELECT * FROM test WHERE k = 0 AND t = %s" % dates[0])
        assert len(res) == 1, res

        assert_invalid(session, "SELECT dateOf(k) FROM test WHERE k = 0 AND t = %s" % dates[0])

        session.execute("SELECT dateOf(t), unixTimestampOf(t) FROM test WHERE k = 0 AND t = %s" % dates[0])
        session.execute("SELECT t FROM test WHERE k = 0 AND t > maxTimeuuid(1234567) AND t < minTimeuuid('2012-11-07 18:18:22-0800')")
        # not sure what to check exactly so just checking the query returns

    def float_with_exponent_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                d double,
                f float
            )
        """)

        session.execute("INSERT INTO test(k, d, f) VALUES (0, 3E+10, 3.4E3)")
        session.execute("INSERT INTO test(k, d, f) VALUES (1, 3.E10, -23.44E-3)")
        session.execute("INSERT INTO test(k, d, f) VALUES (2, 3, -2)")

    def compact_metadata_test(self):
        """
        @jira_ticket CASSANDRA-5189

        Regression test for CASSANDRA-5189.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE bar (
                id int primary key,
                i int
            ) WITH COMPACT STORAGE;
        """)

        session.execute("INSERT INTO bar (id, i) VALUES (1, 2);")
        res = session.execute("SELECT * FROM bar")
        assert rows_to_list(res) == [[1, 2]], res

    @since('2.0')
    def clustering_indexing_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE posts (
                id1 int,
                id2 int,
                author text,
                time bigint,
                v1 text,
                v2 text,
                PRIMARY KEY ((id1, id2), author, time)
            )
        """)

        session.execute("CREATE INDEX ON posts(time)")
        session.execute("CREATE INDEX ON posts(id2)")

        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 0, 'A', 'A')")
        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 1, 'B', 'B')")
        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 1, 'bob', 2, 'C', 'C')")
        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 0, 'tom', 0, 'D', 'D')")
        session.execute("INSERT INTO posts(id1, id2, author, time, v1, v2) VALUES(0, 1, 'tom', 1, 'E', 'E')")

        res = session.execute("SELECT v1 FROM posts WHERE time = 1")
        assert rows_to_list(res) == [['B'], ['E']], res

        res = session.execute("SELECT v1 FROM posts WHERE id2 = 1")
        assert rows_to_list(res) == [['C'], ['E']], res

        res = session.execute("SELECT v1 FROM posts WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0")
        assert rows_to_list(res) == [['A']], res

        # Test for CASSANDRA-8206
        session.execute("UPDATE posts SET v2 = null WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 1")

        res = session.execute("SELECT v1 FROM posts WHERE id2 = 0")
        assert rows_to_list(res) == [['A'], ['B'], ['D']], res

        res = session.execute("SELECT v1 FROM posts WHERE time = 1")
        assert rows_to_list(res) == [['B'], ['E']], res

    @since('2.0')
    def invalid_clustering_indexing_test(self):
        session = self.prepare()

        session.execute("CREATE TABLE test1 (a int, b int, c int, d int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE")
        assert_invalid(session, "CREATE INDEX ON test1(a)")
        assert_invalid(session, "CREATE INDEX ON test1(b)")

        session.execute("CREATE TABLE test2 (a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE")
        assert_invalid(session, "CREATE INDEX ON test2(a)")
        assert_invalid(session, "CREATE INDEX ON test2(b)")
        assert_invalid(session, "CREATE INDEX ON test2(c)")

        session.execute("CREATE TABLE test3 (a int, b int, c int static , PRIMARY KEY (a, b))")
        assert_invalid(session, "CREATE INDEX ON test3(c)")

    @since('2.0')
    def edge_2i_on_complex_pk_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE indexed (
                pk0 int,
                pk1 int,
                ck0 int,
                ck1 int,
                ck2 int,
                value int,
                PRIMARY KEY ((pk0, pk1), ck0, ck1, ck2)
            )
        """)

        session.execute("CREATE INDEX ON indexed(pk0)")
        session.execute("CREATE INDEX ON indexed(ck0)")
        session.execute("CREATE INDEX ON indexed(ck1)")
        session.execute("CREATE INDEX ON indexed(ck2)")

        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (0, 1, 2, 3, 4, 5)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (1, 2, 3, 4, 5, 0)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (2, 3, 4, 5, 0, 1)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (3, 4, 5, 0, 1, 2)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (4, 5, 0, 1, 2, 3)")
        session.execute("INSERT INTO indexed (pk0, pk1, ck0, ck1, ck2, value) VALUES (5, 0, 1, 2, 3, 4)")

        res = session.execute("SELECT value FROM indexed WHERE pk0 = 2")
        self.assertEqual([[1]], rows_to_list(res))

        res = session.execute("SELECT value FROM indexed WHERE ck0 = 0")
        self.assertEqual([[3]], rows_to_list(res))

        res = session.execute("SELECT value FROM indexed WHERE pk0 = 3 AND pk1 = 4 AND ck1 = 0")
        self.assertEqual([[2]], rows_to_list(res))

        res = session.execute("SELECT value FROM indexed WHERE pk0 = 5 AND pk1 = 0 AND ck0 = 1 AND ck2 = 3 ALLOW FILTERING")
        self.assertEqual([[4]], rows_to_list(res))

    def bug_5240_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test(
                interval text,
                seq int,
                id int,
                severity int,
                PRIMARY KEY ((interval, seq), id)
            ) WITH CLUSTERING ORDER BY (id DESC);
        """)

        session.execute("CREATE INDEX ON test(severity);")

        session.execute("insert into test(interval, seq, id , severity) values('t',1, 1, 1);")
        session.execute("insert into test(interval, seq, id , severity) values('t',1, 2, 1);")
        session.execute("insert into test(interval, seq, id , severity) values('t',1, 3, 2);")
        session.execute("insert into test(interval, seq, id , severity) values('t',1, 4, 3);")
        session.execute("insert into test(interval, seq, id , severity) values('t',2, 1, 3);")
        session.execute("insert into test(interval, seq, id , severity) values('t',2, 2, 3);")
        session.execute("insert into test(interval, seq, id , severity) values('t',2, 3, 1);")
        session.execute("insert into test(interval, seq, id , severity) values('t',2, 4, 2);")

        res = session.execute("select * from test where severity = 3 and interval = 't' and seq =1;")
        assert rows_to_list(res) == [['t', 1, 4, 3]], res

    def ticket_5230_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE foo (
                key text,
                c text,
                v text,
                PRIMARY KEY (key, c)
            )
        """)

        session.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '1', '1')")
        session.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '2', '2')")
        session.execute("INSERT INTO foo(key, c, v) VALUES ('foo', '3', '3')")

        res = session.execute("SELECT c FROM foo WHERE key = 'foo' AND c IN ('1', '2');")
        assert rows_to_list(res) == [['1'], ['2']], res

    def conversion_functions_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                i varint,
                b blob
            )
        """)

        session.execute("INSERT INTO test(k, i, b) VALUES (0, blobAsVarint(bigintAsBlob(3)), textAsBlob('foobar'))")
        res = session.execute("SELECT i, blobAsText(b) FROM test WHERE k = 0")
        assert rows_to_list(res) == [[3, 'foobar']], res

    def alter_bug_test(self):
        """
        @jira_ticket CASSANDRA-5232
        """
        session = self.prepare()

        session.execute("CREATE TABLE t1 (id int PRIMARY KEY, t text);")

        session.execute("UPDATE t1 SET t = '111' WHERE id = 1;")
        session.execute("ALTER TABLE t1 ADD l list<text>;")

        time.sleep(.5)

        res = session.execute("SELECT * FROM t1;")
        assert rows_to_list(res) == [[1, None, '111']], res

        session.execute("ALTER TABLE t1 ADD m map<int, text>;")
        time.sleep(.5)
        res = session.execute("SELECT * FROM t1;")
        assert rows_to_list(res) == [[1, None, None, '111']], res

    def bug_5376(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                key text,
                c bigint,
                v text,
                x set<text>,
                PRIMARY KEY (key, c)
            );
        """)

        assert_invalid(session, "select * from test where key = 'foo' and c in (1,3,4);")

    def function_and_reverse_type_test(self):
        """
        @jira_ticket CASSANDRA-5386
        """

        session = self.prepare()
        session.execute("""
            CREATE TABLE test (
                k int,
                c timeuuid,
                v int,
                PRIMARY KEY (k, c)
            ) WITH CLUSTERING ORDER BY (c DESC)
        """)

        session.execute("INSERT INTO test (k, c, v) VALUES (0, now(), 0);")

    def bug_5404(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (key text PRIMARY KEY)")
        # We just want to make sure this doesn't NPE server side
        assert_invalid(session, "select * from test where token(key) > token(int(3030343330393233)) limit 1;")

    def empty_blob_test(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, b blob)")
        session.execute("INSERT INTO test (k, b) VALUES (0, 0x)")
        res = session.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[0, '']], res

    @since('2.0')
    def rename_test(self):
        session = self.prepare(start_rpc=True)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()

        cfdef = CfDef()
        cfdef.keyspace = 'ks'
        cfdef.name = 'test'
        cfdef.column_type = 'Standard'
        cfdef.comparator_type = 'CompositeType(Int32Type, Int32Type, Int32Type)'
        cfdef.key_validation_class = 'UTF8Type'
        cfdef.default_validation_class = 'UTF8Type'

        client.set_keyspace('ks')
        client.system_add_column_family(cfdef)

        session.execute("INSERT INTO ks.test (key, column1, column2, column3, value) VALUES ('foo', 4, 3, 2, 'bar')")

        time.sleep(1)

        session.execute("ALTER TABLE test RENAME column1 TO foo1 AND column2 TO foo2 AND column3 TO foo3")
        assert_one(session, "SELECT foo1, foo2, foo3 FROM test", [4, 3, 2])

    def clustering_order_and_functions_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                t timeuuid,
                PRIMARY KEY (k, t)
            ) WITH CLUSTERING ORDER BY (t DESC)
        """)

        for i in range(0, 5):
            session.execute("INSERT INTO test (k, t) VALUES (%d, now())" % i)

        session.execute("SELECT dateOf(t) FROM test")

    @since('2.0')
    def conditional_update_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
                v2 text,
                v3 int
            )
        """)

        # Shouldn't apply
        assert_one(session, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4", [False])
        assert_one(session, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS", [False])

        # Should apply
        assert_one(session, "INSERT INTO test (k, v1, v2) VALUES (0, 2, 'foo') IF NOT EXISTS", [True])

        # Shouldn't apply
        assert_one(session, "INSERT INTO test (k, v1, v2) VALUES (0, 5, 'bar') IF NOT EXISTS", [False, 0, 2, 'foo', None])
        assert_one(session, "SELECT * FROM test", [0, 2, 'foo', None])

        # Should not apply
        assert_one(session, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF v1 = 4", [False, 2])
        assert_one(session, "SELECT * FROM test", [0, 2, 'foo', None])

        # Should apply (note: we want v2 before v1 in the statement order to exercise #5786)
        assert_one(session, "UPDATE test SET v2 = 'bar', v1 = 3 WHERE k = 0 IF v1 = 2", [True])
        assert_one(session, "UPDATE test SET v2 = 'bar', v1 = 3 WHERE k = 0 IF EXISTS", [True])
        assert_one(session, "SELECT * FROM test", [0, 3, 'bar', None])

        # Shouldn't apply, only one condition is ok
        assert_one(session, "UPDATE test SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'foo'", [False, 3, 'bar'])
        assert_one(session, "SELECT * FROM test", [0, 3, 'bar', None])

        # Should apply
        assert_one(session, "UPDATE test SET v1 = 5, v2 = 'foobar' WHERE k = 0 IF v1 = 3 AND v2 = 'bar'", [True])
        assert_one(session, "SELECT * FROM test", [0, 5, 'foobar', None])

        # Shouldn't apply
        assert_one(session, "DELETE v2 FROM test WHERE k = 0 IF v1 = 3", [False, 5])
        assert_one(session, "SELECT * FROM test", [0, 5, 'foobar', None])

        # Shouldn't apply
        assert_one(session, "DELETE v2 FROM test WHERE k = 0 IF v1 = null", [False, 5])
        assert_one(session, "SELECT * FROM test", [0, 5, 'foobar', None])

        # Should apply
        assert_one(session, "DELETE v2 FROM test WHERE k = 0 IF v1 = 5", [True])
        assert_one(session, "SELECT * FROM test", [0, 5, None, None])

        # Shouln't apply
        assert_one(session, "DELETE v1 FROM test WHERE k = 0 IF v3 = 4", [False, None])

        # Should apply
        assert_one(session, "DELETE v1 FROM test WHERE k = 0 IF v3 = null", [True])
        assert_one(session, "SELECT * FROM test", [0, None, None, None])

        # Should apply
        assert_one(session, "DELETE FROM test WHERE k = 0 IF v1 = null", [True])
        assert_none(session, "SELECT * FROM test")

        # Shouldn't apply
        assert_one(session, "UPDATE test SET v1 = 3, v2 = 'bar' WHERE k = 0 IF EXISTS", [False])

        if self.cluster.version() > "2.1.1":
            # Should apply
            assert_one(session, "DELETE FROM test WHERE k = 0 IF v1 IN (null)", [True])

    @since('2.0.7')
    def conditional_delete_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v1 int,
            )
        """)

        assert_one(session, "DELETE FROM test WHERE k=1 IF EXISTS", [False])

        session.execute("INSERT INTO test (k, v1) VALUES (1, 2)")
        assert_one(session, "DELETE FROM test WHERE k=1 IF EXISTS", [True])
        assert_none(session, "SELECT * FROM test WHERE k=1")
        assert_one(session, "DELETE FROM test WHERE k=1 IF EXISTS", [False])

        session.execute("UPDATE test USING TTL 1 SET v1=2 WHERE k=1")
        time.sleep(1.5)
        assert_one(session, "DELETE FROM test WHERE k=1 IF EXISTS", [False])
        assert_none(session, "SELECT * FROM test WHERE k=1")

        session.execute("INSERT INTO test (k, v1) VALUES (2, 2) USING TTL 1")
        time.sleep(1.5)
        assert_one(session, "DELETE FROM test WHERE k=2 IF EXISTS", [False])
        assert_none(session, "SELECT * FROM test WHERE k=2")

        session.execute("INSERT INTO test (k, v1) VALUES (3, 2)")
        assert_one(session, "DELETE v1 FROM test WHERE k=3 IF EXISTS", [True])
        assert_one(session, "SELECT * FROM test WHERE k=3", [3, None])
        assert_one(session, "DELETE v1 FROM test WHERE k=3 IF EXISTS", [True])
        assert_one(session, "DELETE FROM test WHERE k=3 IF EXISTS", [True])

        # static columns
        session.execute("""
            CREATE TABLE test2 (
                k text,
                s text static,
                i int,
                v text,
                PRIMARY KEY (k, i)
            )""")

        session.execute("INSERT INTO test2 (k, s, i, v) VALUES ('k', 's', 0, 'v')")
        assert_one(session, "DELETE v FROM test2 WHERE k='k' AND i=0 IF EXISTS", [True])
        assert_one(session, "DELETE FROM test2 WHERE k='k' AND i=0 IF EXISTS", [True])
        assert_one(session, "DELETE v FROM test2 WHERE k='k' AND i=0 IF EXISTS", [False])
        assert_one(session, "DELETE FROM test2 WHERE k='k' AND i=0 IF EXISTS", [False])

        # CASSANDRA-6430
        v = self.cluster.version()
        if v >= "2.1.1" or v < "2.1" and v >= "2.0.11":
            assert_invalid(session, "DELETE FROM test2 WHERE k = 'k' IF EXISTS")
            assert_invalid(session, "DELETE FROM test2 WHERE k = 'k' IF v = 'foo'")
            assert_invalid(session, "DELETE FROM test2 WHERE i = 0 IF EXISTS")
            assert_invalid(session, "DELETE FROM test2 WHERE k = 0 AND i > 0 IF EXISTS")
            assert_invalid(session, "DELETE FROM test2 WHERE k = 0 AND i > 0 IF v = 'foo'")

    @freshCluster()
    def range_key_ordered_test(self):
        session = self.prepare(ordered=True)

        session.execute("CREATE TABLE test ( k int PRIMARY KEY)")

        session.execute("INSERT INTO test(k) VALUES (-1)")
        session.execute("INSERT INTO test(k) VALUES ( 0)")
        session.execute("INSERT INTO test(k) VALUES ( 1)")

        assert_all(session, "SELECT * FROM test", [[0], [1], [-1]])
        assert_invalid(session, "SELECT * FROM test WHERE k >= -1 AND k < 1;")

    @since('2.0')
    def select_with_alias_test(self):
        session = self.prepare()
        session.execute('CREATE TABLE users (id int PRIMARY KEY, name text)')

        for id in range(0, 5):
            session.execute("INSERT INTO users (id, name) VALUES (%d, 'name%d') USING TTL 10 AND TIMESTAMP 0" % (id, id))

        # test aliasing count(*)
        res = session.execute('SELECT count(*) AS user_count FROM users')
        self.assertEqual('user_count', res[0]._fields[0])
        self.assertEqual(5, res[0].user_count)

        # test aliasing regular value
        res = session.execute('SELECT name AS user_name FROM users WHERE id = 0')
        self.assertEqual('user_name', res[0]._fields[0])
        self.assertEqual('name0', res[0].user_name)

        # test aliasing writetime
        res = session.execute('SELECT writeTime(name) AS name_writetime FROM users WHERE id = 0')
        self.assertEqual('name_writetime', res[0]._fields[0])
        self.assertEqual(0, res[0].name_writetime)

        # test aliasing ttl
        res = session.execute('SELECT ttl(name) AS name_ttl FROM users WHERE id = 0')
        self.assertEqual('name_ttl', res[0]._fields[0])
        assert res[0].name_ttl in (9, 10)

        # test aliasing a regular function
        res = session.execute('SELECT intAsBlob(id) AS id_blob FROM users WHERE id = 0')
        self.assertEqual('id_blob', res[0]._fields[0])
        self.assertEqual('\x00\x00\x00\x00', res[0].id_blob)

        # test that select throws a meaningful exception for aliases in where clause
        assert_invalid(session, 'SELECT id AS user_id, name AS user_name FROM users WHERE user_id = 0', matching="Aliases aren't allowed in the where clause")

        # test that select throws a meaningful exception for aliases in order by clause
        assert_invalid(session, 'SELECT id AS user_id, name AS user_name FROM users WHERE id IN (0) ORDER BY user_name', matching="Aliases are not allowed in order by clause")

    def nonpure_function_collection_test(self):
        """
        @jira_ticket CASSANDRA-5795
        """

        session = self.prepare()
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v list<timeuuid>)")

        # we just want to make sure this doesn't throw
        session.execute("INSERT INTO test(k, v) VALUES (0, [now()])")

    def empty_in_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE test (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))")

        def fill(table):
            for i in range(0, 2):
                for j in range(0, 2):
                    session.execute("INSERT INTO %s (k1, k2, v) VALUES (%d, %d, %d)" % (table, i, j, i + j))

        def assert_nothing_changed(table):
            res = session.execute("SELECT * FROM %s" % table)  # make sure nothing got removed
            self.assertEqual([[0, 0, 0], [0, 1, 1], [1, 0, 1], [1, 1, 2]], rows_to_list(sorted(res)))

        # Inserts a few rows to make sure we don't actually query something
        fill("test")

        # Test empty IN () in SELECT
        assert_none(session, "SELECT v FROM test WHERE k1 IN ()")
        assert_none(session, "SELECT v FROM test WHERE k1 = 0 AND k2 IN ()")

        # Test empty IN () in DELETE
        session.execute("DELETE FROM test WHERE k1 IN ()")
        assert_nothing_changed("test")

        # Test empty IN () in UPDATE
        session.execute("UPDATE test SET v = 3 WHERE k1 IN () AND k2 = 2")
        assert_nothing_changed("test")

        # Same test, but for compact
        session.execute("CREATE TABLE test_compact (k1 int, k2 int, v int, PRIMARY KEY (k1, k2)) WITH COMPACT STORAGE")

        fill("test_compact")

        assert_none(session, "SELECT v FROM test_compact WHERE k1 IN ()")
        assert_none(session, "SELECT v FROM test_compact WHERE k1 = 0 AND k2 IN ()")

        # Test empty IN () in DELETE
        session.execute("DELETE FROM test_compact WHERE k1 IN ()")
        assert_nothing_changed("test_compact")

        # Test empty IN () in UPDATE
        session.execute("UPDATE test_compact SET v = 3 WHERE k1 IN () AND k2 = 2")
        assert_nothing_changed("test_compact")

    def collection_flush_test(self):
        """
        @jira_ticket CASSANDRA-5805
        """
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, s set<int>)")

        session.execute("INSERT INTO test(k, s) VALUES (1, {1})")
        self.cluster.flush()
        session.execute("INSERT INTO test(k, s) VALUES (1, {2})")
        self.cluster.flush()

        assert_one(session, "SELECT * FROM test", [1, set([2])])

    @since('2.0.1')
    def select_distinct_test(self):
        session = self.prepare()

        # Test a regular (CQL3) table.
        session.execute('CREATE TABLE regular (pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY((pk0, pk1), ck0))')

        for i in xrange(0, 3):
            session.execute('INSERT INTO regular (pk0, pk1, ck0, val) VALUES (%d, %d, 0, 0)' % (i, i))
            session.execute('INSERT INTO regular (pk0, pk1, ck0, val) VALUES (%d, %d, 1, 1)' % (i, i))

        res = session.execute('SELECT DISTINCT pk0, pk1 FROM regular LIMIT 1')
        self.assertEqual([[0, 0]], rows_to_list(res))

        res = session.execute('SELECT DISTINCT pk0, pk1 FROM regular LIMIT 3')
        self.assertEqual([[0, 0], [1, 1], [2, 2]], rows_to_list(sorted(res)))

        # Test a 'compact storage' table.
        session.execute('CREATE TABLE compact (pk0 int, pk1 int, val int, PRIMARY KEY((pk0, pk1))) WITH COMPACT STORAGE')

        for i in xrange(0, 3):
            session.execute('INSERT INTO compact (pk0, pk1, val) VALUES (%d, %d, %d)' % (i, i, i))

        res = session.execute('SELECT DISTINCT pk0, pk1 FROM compact LIMIT 1')
        self.assertEqual([[0, 0]], rows_to_list(res))

        res = session.execute('SELECT DISTINCT pk0, pk1 FROM compact LIMIT 3')
        self.assertEqual([[0, 0], [1, 1], [2, 2]], rows_to_list(sorted(res)))

        # Test a 'wide row' thrift table.
        session.execute('CREATE TABLE wide (pk int, name text, val int, PRIMARY KEY(pk, name)) WITH COMPACT STORAGE')

        for i in xrange(0, 3):
            session.execute("INSERT INTO wide (pk, name, val) VALUES (%d, 'name0', 0)" % i)
            session.execute("INSERT INTO wide (pk, name, val) VALUES (%d, 'name1', 1)" % i)

        res = session.execute('SELECT DISTINCT pk FROM wide LIMIT 1')
        self.assertEqual([[1]], rows_to_list(res))

        res = session.execute('SELECT DISTINCT pk FROM wide LIMIT 3')
        self.assertEqual([[0], [1], [2]], rows_to_list(sorted(res)))

        # Test selection validation.
        assert_invalid(session, 'SELECT DISTINCT pk0 FROM regular', matching="queries must request all the partition key columns")
        assert_invalid(session, 'SELECT DISTINCT pk0, pk1, ck0 FROM regular', matching="queries must only request partition key columns")

    def select_distinct_with_deletions_test(self):
        session = self.prepare()
        session.execute('CREATE TABLE t1 (k int PRIMARY KEY, c int, v int)')
        for i in range(10):
            session.execute('INSERT INTO t1 (k, c, v) VALUES (%d, %d, %d)' % (i, i, i))

        rows = session.execute('SELECT DISTINCT k FROM t1')
        self.assertEqual(10, len(rows))
        key_to_delete = rows[3].k

        session.execute('DELETE FROM t1 WHERE k=%d' % (key_to_delete,))
        rows = list(session.execute('SELECT DISTINCT k FROM t1'))
        self.assertEqual(9, len(rows))

        rows = list(session.execute('SELECT DISTINCT k FROM t1 LIMIT 5'))
        self.assertEqual(5, len(rows))

        session.default_fetch_size = 5
        rows = list(session.execute('SELECT DISTINCT k FROM t1'))
        self.assertEqual(9, len(rows))

    def function_with_null_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                t timeuuid,
            )
        """)

        session.execute("INSERT INTO test(k) VALUES (0)")
        assert_one(session, "SELECT dateOf(t) FROM test WHERE k=0", [None])

    @freshCluster()
    def cas_simple_test(self):
        session = self.prepare(nodes=3, rf=3)

        session.execute("CREATE TABLE tkns (tkn int, consumed boolean, PRIMARY KEY (tkn));")

        for i in range(1, 10):
            query = SimpleStatement("INSERT INTO tkns (tkn, consumed) VALUES (%i,FALSE);" % i, consistency_level=ConsistencyLevel.QUORUM)
            session.execute(query)
            assert_one(session, "UPDATE tkns SET consumed = TRUE WHERE tkn = %i IF consumed = FALSE;" % i, [True], cl=ConsistencyLevel.QUORUM)
            assert_one(session, "UPDATE tkns SET consumed = TRUE WHERE tkn = %i IF consumed = FALSE;" % i, [False, True], cl=ConsistencyLevel.QUORUM)

    def bug_6050_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                a int,
                b int
            )
        """)

        session.execute("CREATE INDEX ON test(a)")
        assert_invalid(session, "SELECT * FROM test WHERE a = 3 AND b IN (1, 3)")

    @since('2.0')
    def bug_6069_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                s set<int>
            )
        """)

        assert_one(session, "INSERT INTO test(k, s) VALUES (0, {1, 2, 3}) IF NOT EXISTS", [True])
        assert_one(session, "SELECT * FROM test", [0, {1, 2, 3}])

    def bug_6115_test(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (k int, v int, PRIMARY KEY (k, v))")

        session.execute("INSERT INTO test (k, v) VALUES (0, 1)")
        session.execute("BEGIN BATCH DELETE FROM test WHERE k=0 AND v=1; INSERT INTO test (k, v) VALUES (0, 2); APPLY BATCH")

        assert_one(session, "SELECT * FROM test", [0, 2])

    def secondary_index_counters(self):
        session = self.prepare()
        session.execute("CREATE TABLE test (k int PRIMARY KEY, c counter)")
        assert_invalid(session, "CREATE INDEX ON test(c)")

    def column_name_validation_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k text,
                c int,
                v timeuuid,
                PRIMARY KEY (k, c)
            )
        """)

        assert_invalid(session, "INSERT INTO test(k, c) VALUES ('', 0)")

        # Insert a value that don't fit 'int'
        assert_invalid(session, "INSERT INTO test(k, c) VALUES (0, 10000000000)")

        # Insert a non-version 1 uuid
        assert_invalid(session, "INSERT INTO test(k, c, v) VALUES (0, 0, 550e8400-e29b-41d4-a716-446655440000)")

    @since('1.2')
    def bug_6327_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                v int,
                PRIMARY KEY (k, v)
            )
        """)

        session.execute("INSERT INTO test (k, v) VALUES (0, 0)")
        self.cluster.flush()
        assert_one(session, "SELECT v FROM test WHERE k=0 AND v IN (1, 0)", [0])

    @since('1.2')
    def large_count_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                v int,
                PRIMARY KEY (k)
            )
        """)

        session.default_fetch_size = 10000
        # We know we page at 10K, so test counting just before, at 10K, just after and
        # a bit after that.
        for k in range(1, 10000):
            session.execute("INSERT INTO test(k) VALUES (%d)" % k)

        assert_one(session, "SELECT COUNT(*) FROM test", [9999])

        session.execute("INSERT INTO test(k) VALUES (%d)" % 10000)

        assert_one(session, "SELECT COUNT(*) FROM test", [10000])

        session.execute("INSERT INTO test(k) VALUES (%d)" % 10001)

        assert_one(session, "SELECT COUNT(*) FROM test", [10001])

        for k in range(10002, 15001):
            session.execute("INSERT INTO test(k) VALUES (%d)" % k)

        assert_one(session, "SELECT COUNT(*) FROM test", [15000])

    def nan_infinity_test(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (f float PRIMARY KEY)")

        session.execute("INSERT INTO test(f) VALUES (NaN)")
        session.execute("INSERT INTO test(f) VALUES (-NaN)")
        session.execute("INSERT INTO test(f) VALUES (Infinity)")
        session.execute("INSERT INTO test(f) VALUES (-Infinity)")

        selected = rows_to_list(session.execute("SELECT * FROM test"))

        # selected should be [[nan], [inf], [-inf]],
        # but assert element-wise because NaN != NaN
        assert len(selected) == 3
        assert len(selected[0]) == 1
        assert math.isnan(selected[0][0])
        assert selected[1] == [float("inf")]
        assert selected[2] == [float("-inf")]

    @since('2.0')
    def static_columns_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                p int,
                s int static,
                v int,
                PRIMARY KEY (k, p)
            )
        """)

        session.execute("INSERT INTO test(k, s) VALUES (0, 42)")

        assert_one(session, "SELECT * FROM test", [0, None, 42, None])

        # Check that writetime works (#7081) -- we can't predict the exact value easily so
        # we just check that it's non zero
        row = session.execute("SELECT s, writetime(s) FROM test WHERE k=0")
        assert list(row[0])[0] == 42 and list(row[0])[1] > 0, row

        session.execute("INSERT INTO test(k, p, s, v) VALUES (0, 0, 12, 0)")
        session.execute("INSERT INTO test(k, p, s, v) VALUES (0, 1, 24, 1)")

        # Check the static columns in indeed "static"
        assert_all(session, "SELECT * FROM test", [[0, 0, 24, 0], [0, 1, 24, 1]])

        # Check we do correctly get the static column value with a SELECT *, even
        # if we're only slicing part of the partition
        assert_one(session, "SELECT * FROM test WHERE k=0 AND p=0", [0, 0, 24, 0])
        assert_one(session, "SELECT * FROM test WHERE k=0 AND p=1", [0, 1, 24, 1])

        # Test for IN on the clustering key (#6769)
        assert_all(session, "SELECT * FROM test WHERE k=0 AND p IN (0, 1)", [[0, 0, 24, 0], [0, 1, 24, 1]])

        # Check things still work if we don't select the static column. We also want
        # this to not request the static columns internally at all, though that part
        # require debugging to assert
        assert_one(session, "SELECT p, v FROM test WHERE k=0 AND p=1", [1, 1])

        # Check selecting only a static column with distinct only yield one value
        # (as we only query the static columns)
        assert_one(session, "SELECT DISTINCT s FROM test WHERE k=0", [24])
        # But without DISTINCT, we still get one result per row
        assert_all(session, "SELECT s FROM test WHERE k=0", [[24], [24]])
        # but that querying other columns does correctly yield the full partition
        assert_all(session, "SELECT s, v FROM test WHERE k=0", [[24, 0], [24, 1]])
        assert_one(session, "SELECT s, v FROM test WHERE k=0 AND p=1", [24, 1])
        assert_one(session, "SELECT p, s FROM test WHERE k=0 AND p=1", [1, 24])
        assert_one(session, "SELECT k, p, s FROM test WHERE k=0 AND p=1", [0, 1, 24])

        # Check that deleting a row don't implicitely deletes statics
        session.execute("DELETE FROM test WHERE k=0 AND p=0")
        assert_all(session, "SELECT * FROM test", [[0, 1, 24, 1]])

        # But that explicitely deleting the static column does remove it
        session.execute("DELETE s FROM test WHERE k=0")
        assert_all(session, "SELECT * FROM test", [[0, 1, None, 1]])

        # Check we can add a static column ...
        session.execute("ALTER TABLE test ADD s2 int static")
        assert_all(session, "SELECT * FROM test", [[0, 1, None, None, 1]])
        session.execute("INSERT INTO TEST (k, p, s2, v) VALUES(0, 2, 42, 2)")
        assert_all(session, "SELECT * FROM test", [[0, 1, None, 42, 1], [0, 2, None, 42, 2]])
        # ... and that we can drop it
        session.execute("ALTER TABLE test DROP s2")
        assert_all(session, "SELECT * FROM test", [[0, 1, None, 1], [0, 2, None, 2]])

    @since('2.0')
    def static_columns_cas_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                id int,
                k text,
                version int static,
                v text,
                PRIMARY KEY (id, k)
            )
        """)

        # Test that INSERT IF NOT EXISTS concerns only the static column if no clustering nor regular columns
        # is provided, but concerns the CQL3 row targetted by the clustering columns otherwise
        session.execute("INSERT INTO test(id, k, v) VALUES (1, 'foo', 'foo')")
        assert_one(session, "INSERT INTO test(id, k, version) VALUES (1, 'foo', 1) IF NOT EXISTS", [False, 1, 'foo', None, 'foo'])
        assert_one(session, "INSERT INTO test(id, version) VALUES (1, 1) IF NOT EXISTS", [True])
        assert_one(session, "SELECT * FROM test", [1, 'foo', 1, 'foo'])
        session.execute("DELETE FROM test WHERE id = 1")

        session.execute("INSERT INTO test(id, version) VALUES (0, 0)")

        assert_one(session, "UPDATE test SET v='foo', version=1 WHERE id=0 AND k='k1' IF version = 0", [True])
        assert_all(session, "SELECT * FROM test", [[0, 'k1', 1, 'foo']])

        assert_one(session, "UPDATE test SET v='bar', version=1 WHERE id=0 AND k='k2' IF version = 0", [False, 1])
        assert_all(session, "SELECT * FROM test", [[0, 'k1', 1, 'foo']])

        assert_one(session, "UPDATE test SET v='bar', version=2 WHERE id=0 AND k='k2' IF version = 1", [True])
        assert_all(session, "SELECT * FROM test", [[0, 'k1', 2, 'foo'], [0, 'k2', 2, 'bar']])

        # Testing batches
        assert_one(session,
                   """
                     BEGIN BATCH
                       UPDATE test SET v='foobar' WHERE id=0 AND k='k1';
                       UPDATE test SET v='barfoo' WHERE id=0 AND k='k2';
                       UPDATE test SET version=3 WHERE id=0 IF version=1;
                     APPLY BATCH
                   """, [False, 0, None, 2])

        assert_one(session,
                   """
                     BEGIN BATCH
                       UPDATE test SET v='foobar' WHERE id=0 AND k='k1';
                       UPDATE test SET v='barfoo' WHERE id=0 AND k='k2';
                       UPDATE test SET version=3 WHERE id=0 IF version=2;
                     APPLY BATCH
                   """, [True])
        assert_all(session, "SELECT * FROM test", [[0, 'k1', 3, 'foobar'], [0, 'k2', 3, 'barfoo']])

        assert_all(session,
                   """
                   BEGIN BATCH
                       UPDATE test SET version=4 WHERE id=0 IF version=3;
                       UPDATE test SET v='row1' WHERE id=0 AND k='k1' IF v='foo';
                       UPDATE test SET v='row2' WHERE id=0 AND k='k2' IF v='bar';
                   APPLY BATCH
                   """, [[False, 0, 'k1', 3, 'foobar'], [False, 0, 'k2', 3, 'barfoo']])

        assert_one(session,
                   """
                     BEGIN BATCH
                       UPDATE test SET version=4 WHERE id=0 IF version=3;
                       UPDATE test SET v='row1' WHERE id=0 AND k='k1' IF v='foobar';
                       UPDATE test SET v='row2' WHERE id=0 AND k='k2' IF v='barfoo';
                     APPLY BATCH
                   """, [True])
        assert_all(session, "SELECT * FROM test", [[0, 'k1', 4, 'row1'], [0, 'k2', 4, 'row2']])

        assert_invalid(session,
                       """
                         BEGIN BATCH
                           UPDATE test SET version=5 WHERE id=0 IF version=4;
                           UPDATE test SET v='row1' WHERE id=0 AND k='k1';
                           UPDATE test SET v='row2' WHERE id=1 AND k='k2';
                         APPLY BATCH
                       """)

        assert_one(session,
                   """
                     BEGIN BATCH
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k1', 'val1') IF NOT EXISTS;
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k2', 'val2') IF NOT EXISTS;
                     APPLY BATCH
                   """, [True])
        assert_all(session, "SELECT * FROM test WHERE id=1", [[1, 'k1', None, 'val1'], [1, 'k2', None, 'val2']])

        assert_one(session,
                   """
                     BEGIN BATCH
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k2', 'val2') IF NOT EXISTS;
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k3', 'val3') IF NOT EXISTS;
                     APPLY BATCH
                   """, [False, 1, 'k2', None, 'val2'])

        assert_one(session,
                   """
                     BEGIN BATCH
                       UPDATE test SET v='newVal' WHERE id=1 AND k='k2' IF v='val0';
                       INSERT INTO TEST (id, k, v) VALUES(1, 'k3', 'val3') IF NOT EXISTS;
                     APPLY BATCH
                   """, [False, 1, 'k2', None, 'val2'])
        assert_all(session, "SELECT * FROM test WHERE id=1", [[1, 'k1', None, 'val1'], [1, 'k2', None, 'val2']])

        assert_one(session,
                   """
                     BEGIN BATCH
                       UPDATE test SET v='newVal' WHERE id=1 AND k='k2' IF v='val2';
                       INSERT INTO TEST (id, k, v, version) VALUES(1, 'k3', 'val3', 1) IF NOT EXISTS;
                     APPLY BATCH
                   """, [True])
        assert_all(session, "SELECT * FROM test WHERE id=1", [[1, 'k1', 1, 'val1'], [1, 'k2', 1, 'newVal'], [1, 'k3', 1, 'val3']])

        if self.cluster.version() >= '2.1':
            assert_one(session,
                       """
                         BEGIN BATCH
                           UPDATE test SET v='newVal1' WHERE id=1 AND k='k2' IF v='val2';
                           UPDATE test SET v='newVal2' WHERE id=1 AND k='k2' IF v='val3';
                         APPLY BATCH
                       """, [False, 1, 'k2', 'newVal'])
        else:
            assert_invalid(session,
                           """
                             BEGIN BATCH
                               UPDATE test SET v='newVal1' WHERE id=1 AND k='k2' IF v='val2';
                               UPDATE test SET v='newVal2' WHERE id=1 AND k='k2' IF v='val3';
                             APPLY BATCH
                           """)

    @since('2.0')
    def static_columns_with_2i_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                p int,
                s int static,
                v int,
                PRIMARY KEY (k, p)
            )
        """)

        session.execute("CREATE INDEX ON test(v)")

        session.execute("INSERT INTO test(k, p, s, v) VALUES (0, 0, 42, 1)")
        session.execute("INSERT INTO test(k, p, v) VALUES (0, 1, 1)")
        session.execute("INSERT INTO test(k, p, v) VALUES (0, 2, 2)")

        assert_all(session, "SELECT * FROM test WHERE v = 1", [[0, 0, 42, 1], [0, 1, 42, 1]])
        assert_all(session, "SELECT p, s FROM test WHERE v = 1", [[0, 42], [1, 42]])
        assert_all(session, "SELECT p FROM test WHERE v = 1", [[0], [1]])
        # We don't support that
        assert_invalid(session, "SELECT s FROM test WHERE v = 1")

    @since('2.0')
    def static_columns_with_distinct_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                p int,
                s int static,
                PRIMARY KEY (k, p)
            )
        """)

        session.execute("INSERT INTO test (k, p) VALUES (1, 1)")
        session.execute("INSERT INTO test (k, p) VALUES (1, 2)")

        assert_all(session, "SELECT k, s FROM test", [[1, None], [1, None]])
        assert_one(session, "SELECT DISTINCT k, s FROM test", [1, None])
        assert_one(session, "SELECT DISTINCT s FROM test WHERE k=1", [None])
        assert_none(session, "SELECT DISTINCT s FROM test WHERE k=2")

        session.execute("INSERT INTO test (k, p, s) VALUES (2, 1, 3)")
        session.execute("INSERT INTO test (k, p) VALUES (2, 2)")

        assert_all(session, "SELECT k, s FROM test", [[1, None], [1, None], [2, 3], [2, 3]])
        assert_all(session, "SELECT DISTINCT k, s FROM test", [[1, None], [2, 3]])
        assert_one(session, "SELECT DISTINCT s FROM test WHERE k=1", [None])
        assert_one(session, "SELECT DISTINCT s FROM test WHERE k=2", [3])

        assert_invalid(session, "SELECT DISTINCT s FROM test")

        # paging to test for CASSANDRA-8108
        session.execute("TRUNCATE test")
        for i in range(10):
            for j in range(10):
                session.execute("INSERT INTO test (k, p, s) VALUES (%s, %s, %s)", (i, j, i))

        session.default_fetch_size = 7
        rows = list(session.execute("SELECT DISTINCT k, s FROM test"))
        self.assertEqual(range(10), sorted([r[0] for r in rows]))
        self.assertEqual(range(10), sorted([r[1] for r in rows]))

        keys = ",".join(map(str, range(10)))
        rows = list(session.execute("SELECT DISTINCT k, s FROM test WHERE k IN (%s)" % (keys,)))
        self.assertEqual(range(10), [r[0] for r in rows])
        self.assertEqual(range(10), [r[1] for r in rows])

        # additional testing for CASSANRA-8087
        session.execute("""
            CREATE TABLE test2 (
                k int,
                c1 int,
                c2 int,
                s1 int static,
                s2 int static,
                PRIMARY KEY (k, c1, c2)
            )
        """)

        for i in range(10):
            for j in range(5):
                for k in range(5):
                    session.execute("INSERT INTO test2 (k, c1, c2, s1, s2) VALUES (%s, %s, %s, %s, %s)", (i, j, k, i, i + 1))

        for fetch_size in (None, 2, 5, 7, 10, 24, 25, 26, 1000):
            session.default_fetch_size = fetch_size
            rows = list(session.execute("SELECT DISTINCT k, s1 FROM test2"))
            self.assertEqual(range(10), sorted([r[0] for r in rows]))
            self.assertEqual(range(10), sorted([r[1] for r in rows]))

            rows = list(session.execute("SELECT DISTINCT k, s2 FROM test2"))
            self.assertEqual(range(10), sorted([r[0] for r in rows]))
            self.assertEqual(range(1, 11), sorted([r[1] for r in rows]))

            print "page size: ", fetch_size
            rows = list(session.execute("SELECT DISTINCT k, s1 FROM test2 LIMIT 10"))
            self.assertEqual(range(10), sorted([r[0] for r in rows]))
            self.assertEqual(range(10), sorted([r[1] for r in rows]))

            keys = ",".join(map(str, range(10)))
            rows = list(session.execute("SELECT DISTINCT k, s1 FROM test2 WHERE k IN (%s)" % (keys,)))
            self.assertEqual(range(10), [r[0] for r in rows])
            self.assertEqual(range(10), [r[1] for r in rows])

            keys = ",".join(map(str, range(10)))
            rows = list(session.execute("SELECT DISTINCT k, s2 FROM test2 WHERE k IN (%s)" % (keys,)))
            self.assertEqual(range(10), [r[0] for r in rows])
            self.assertEqual(range(1, 11), [r[1] for r in rows])

            keys = ",".join(map(str, range(10)))
            rows = list(session.execute("SELECT DISTINCT k, s1 FROM test2 WHERE k IN (%s) LIMIT 10" % (keys,)))
            self.assertEqual(range(10), sorted([r[0] for r in rows]))
            self.assertEqual(range(10), sorted([r[1] for r in rows]))

    def select_count_paging_test(self):
        """
        @jira_ticket CASSANDRA-6579
        Regression test for 'select count' paging bug.
        """

        session = self.prepare()
        session.execute("create table test(field1 text, field2 timeuuid, field3 boolean, primary key(field1, field2));")
        session.execute("create index test_index on test(field3);")

        session.execute("insert into test(field1, field2, field3) values ('hola', now(), false);")
        session.execute("insert into test(field1, field2, field3) values ('hola', now(), false);")

        if self.cluster.version() > '2.2':
            assert_one(session, "select count(*) from test where field3 = false limit 1;", [2])
        else:
            assert_one(session, "select count(*) from test where field3 = false limit 1;", [1])

    @since('2.0')
    def cas_and_ttl_test(self):
        session = self.prepare()
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int, lock boolean)")

        session.execute("INSERT INTO test (k, v, lock) VALUES (0, 0, false)")
        session.execute("UPDATE test USING TTL 1 SET lock=true WHERE k=0")
        time.sleep(2)
        assert_one(session, "UPDATE test SET v = 1 WHERE k = 0 IF lock = null", [True])

    @since('2.0')
    def tuple_notation_test(self):
        """
        @jira_ticket CASSANDRA-4851

        Test for new tuple syntax introduced in CASSANDRA-4851.
        """
        session = self.prepare()

        session.execute("CREATE TABLE test (k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2, v3))")
        for i in range(0, 2):
            for j in range(0, 2):
                for k in range(0, 2):
                    session.execute("INSERT INTO test(k, v1, v2, v3) VALUES (0, %d, %d, %d)" % (i, j, k))

        assert_all(session, "SELECT v1, v2, v3 FROM test WHERE k = 0", [[0, 0, 0],
                                                                       [0, 0, 1],
                                                                       [0, 1, 0],
                                                                       [0, 1, 1],
                                                                       [1, 0, 0],
                                                                       [1, 0, 1],
                                                                       [1, 1, 0],
                                                                       [1, 1, 1]])

        assert_all(session, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v2, v3) >= (1, 0, 1)", [[1, 0, 1], [1, 1, 0], [1, 1, 1]])
        assert_all(session, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v2) >= (1, 1)", [[1, 1, 0], [1, 1, 1]])
        assert_all(session, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v2) > (0, 1) AND (v1, v2, v3) <= (1, 1, 0)", [[1, 0, 0], [1, 0, 1], [1, 1, 0]])

        assert_invalid(session, "SELECT v1, v2, v3 FROM test WHERE k = 0 AND (v1, v3) > (1, 0)")

    def in_with_desc_order_test(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2))")
        session.execute("INSERT INTO test(k, c1, c2) VALUES (0, 0, 0)")
        session.execute("INSERT INTO test(k, c1, c2) VALUES (0, 0, 1)")
        session.execute("INSERT INTO test(k, c1, c2) VALUES (0, 0, 2)")

        assert_all(session, "SELECT * FROM test WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC", [[0, 0, 2], [0, 0, 0]])

    @since('2.0')
    def cas_and_compact_test(self):
        """
        @jira_ticket CASSANDRA-6813

        Test for CAS with compact storage table, and #6813 in particular.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE lock (
                partition text,
                key text,
                owner text,
                PRIMARY KEY (partition, key)
            ) WITH COMPACT STORAGE
        """)

        session.execute("INSERT INTO lock(partition, key, owner) VALUES ('a', 'b', null)")
        assert_one(session, "UPDATE lock SET owner='z' WHERE partition='a' AND key='b' IF owner=null", [True])

        assert_one(session, "UPDATE lock SET owner='b' WHERE partition='a' AND key='b' IF owner='a'", [False, 'z'])
        assert_one(session, "UPDATE lock SET owner='b' WHERE partition='a' AND key='b' IF owner='z'", [True])

        assert_one(session, "INSERT INTO lock(partition, key, owner) VALUES ('a', 'c', 'x') IF NOT EXISTS", [True])

    @since('2.0')
    def list_item_conditional_test(self):
        # Lists
        session = self.prepare()

        frozen_values = (False, True) if self.cluster.version() >= "2.1.3" else (False,)
        for frozen in frozen_values:

            session.execute("DROP TABLE IF EXISTS tlist")

            session.execute("""
                CREATE TABLE tlist (
                    k int PRIMARY KEY,
                    l %s,
                )""" % ("frozen<list<text>>" if frozen else "list<text>",))

            session.execute("INSERT INTO tlist(k, l) VALUES (0, ['foo', 'bar', 'foobar'])")

            assert_invalid(session, "DELETE FROM tlist WHERE k=0 IF l[null] = 'foobar'")
            assert_invalid(session, "DELETE FROM tlist WHERE k=0 IF l[-2] = 'foobar'")
            if self.cluster.version() < "2.1":
                # no longer invalid after CASSANDRA-6839
                assert_invalid(session, "DELETE FROM tlist WHERE k=0 IF l[3] = 'foobar'")
            assert_one(session, "DELETE FROM tlist WHERE k=0 IF l[1] = null", [False, ['foo', 'bar', 'foobar']])
            assert_one(session, "DELETE FROM tlist WHERE k=0 IF l[1] = 'foobar'", [False, ['foo', 'bar', 'foobar']])
            assert_one(session, "SELECT * FROM tlist", [0, ['foo', 'bar', 'foobar']])

            assert_one(session, "DELETE FROM tlist WHERE k=0 IF l[1] = 'bar'", [True])
            assert_none(session, "SELECT * FROM tlist")

    @since('2.0')
    def map_item_conditional_test(self):
        session = self.prepare()

        frozen_values = (False, True) if self.cluster.version() >= "2.1.3" else (False,)
        for frozen in frozen_values:

            session.execute("DROP TABLE IF EXISTS tmap")

            session.execute("""
                CREATE TABLE tmap (
                    k int PRIMARY KEY,
                    m %s
                )""" % ("frozen<map<text, text>>" if frozen else "map<text, text>",))

            session.execute("INSERT INTO tmap(k, m) VALUES (0, {'foo' : 'bar'})")
            assert_invalid(session, "DELETE FROM tmap WHERE k=0 IF m[null] = 'foo'")
            assert_one(session, "DELETE FROM tmap WHERE k=0 IF m['foo'] = 'foo'", [False, {'foo': 'bar'}])
            assert_one(session, "DELETE FROM tmap WHERE k=0 IF m['foo'] = null", [False, {'foo': 'bar'}])
            assert_one(session, "SELECT * FROM tmap", [0, {'foo': 'bar'}])

            assert_one(session, "DELETE FROM tmap WHERE k=0 IF m['foo'] = 'bar'", [True])
            assert_none(session, "SELECT * FROM tmap")

            if self.cluster.version() > "2.1.1":
                session.execute("INSERT INTO tmap(k, m) VALUES (1, null)")
                if frozen:
                    assert_invalid(session, "UPDATE tmap set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m['foo'] IN ('blah', null)")
                else:
                    assert_one(session, "UPDATE tmap set m['foo'] = 'bar', m['bar'] = 'foo' WHERE k = 1 IF m['foo'] IN ('blah', null)", [True])

    @since("2.0")
    def static_with_limit_test(self):
        """
        @jira_ticket CASSANDRA-6956

        Test LIMIT when static columns are present.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                s int static,
                v int,
                PRIMARY KEY (k, v)
            )
        """)

        session.execute("INSERT INTO test(k, s) VALUES(0, 42)")
        for i in range(0, 4):
            session.execute("INSERT INTO test(k, v) VALUES(0, %d)" % i)

        assert_one(session, "SELECT * FROM test WHERE k = 0 LIMIT 1", [0, 0, 42])
        assert_all(session, "SELECT * FROM test WHERE k = 0 LIMIT 2", [[0, 0, 42], [0, 1, 42]])
        assert_all(session, "SELECT * FROM test WHERE k = 0 LIMIT 3", [[0, 0, 42], [0, 1, 42], [0, 2, 42]])

    @since("2.0")
    def static_with_empty_clustering_test(self):
        """
        @jira_ticket CASSANDRA-7455

        Regression test for CASSANDRA-7455.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test(
                pkey text,
                ckey text,
                value text,
                static_value text static,
                PRIMARY KEY(pkey, ckey)
            )
        """)

        session.execute("INSERT INTO test(pkey, static_value) VALUES ('partition1', 'static value')")
        session.execute("INSERT INTO test(pkey, ckey, value) VALUES('partition1', '', 'value')")

        assert_one(session, "SELECT * FROM test", ['partition1', '', 'static value', 'value'])

    @since("1.2")
    def limit_compact_table(self):
        """
        @jira_ticket CASSANDRA-7052
        @jira_ticket CASSANDRA-7059

        Regression test for CASSANDRA-7052.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int,
                v int,
                PRIMARY KEY (k, v)
            ) WITH COMPACT STORAGE
        """)

        for i in range(0, 4):
            for j in range(0, 4):
                session.execute("INSERT INTO test(k, v) VALUES (%d, %d)" % (i, j))

        assert_all(session, "SELECT v FROM test WHERE k=0 AND v > 0 AND v <= 4 LIMIT 2", [[1], [2]])
        assert_all(session, "SELECT v FROM test WHERE k=0 AND v > -1 AND v <= 4 LIMIT 2", [[0], [1]])

        assert_all(session, "SELECT * FROM test WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 2", [[0, 1], [0, 2]])
        assert_all(session, "SELECT * FROM test WHERE k IN (0, 1, 2) AND v > -1 AND v <= 4 LIMIT 2", [[0, 0], [0, 1]])
        assert_all(session, "SELECT * FROM test WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 6", [[0, 1], [0, 2], [0, 3], [1, 1], [1, 2], [1, 3]])

        # Introduced in CASSANDRA-7059
        assert_invalid(session, "SELECT * FROM test WHERE v > 1 AND v <= 3 LIMIT 6 ALLOW FILTERING")

    def key_index_with_reverse_clustering(self):
        """
        @jira_ticket CASSANDRA-6950

        Regression test for CASSANDRA-6950.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY ((k1, k2), v)
            ) WITH CLUSTERING ORDER BY (v DESC)
        """)

        session.execute("CREATE INDEX ON test(k2)")

        session.execute("INSERT INTO test(k1, k2, v) VALUES (0, 0, 1)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (0, 1, 2)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (0, 0, 3)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (1, 0, 4)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (1, 1, 5)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (2, 0, 7)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (2, 1, 8)")
        session.execute("INSERT INTO test(k1, k2, v) VALUES (3, 0, 1)")

        assert_all(session, "SELECT * FROM test WHERE k2 = 0 AND v >= 2 ALLOW FILTERING", [[2, 0, 7], [0, 0, 3], [1, 0, 4]])

    @since('1.2')
    def clustering_order_in_test(self):
        """
        @jira_ticket CASSANDRA-7105

        Regression test for CASSANDRA-7105.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                a int,
                b int,
                c int,
                PRIMARY KEY ((a, b), c)
            ) with clustering order by (c desc)
        """)

        session.execute("INSERT INTO test (a, b, c) VALUES (1, 2, 3)")
        session.execute("INSERT INTO test (a, b, c) VALUES (4, 5, 6)")

        assert_one(session, "SELECT * FROM test WHERE a=1 AND b=2 AND c IN (3)", [1, 2, 3])
        assert_one(session, "SELECT * FROM test WHERE a=1 AND b=2 AND c IN (3, 4)", [1, 2, 3])

    @since('1.2')
    def bug7105_test(self):
        """
        @jira_ticket CASSANDRA-7105

        Regression test for CASSANDRA-7105.
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                a int,
                b int,
                c int,
                d int,
                PRIMARY KEY (a, b)
            )
        """)

        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 2, 3, 3)")
        session.execute("INSERT INTO test (a, b, c, d) VALUES (1, 4, 6, 5)")

        assert_one(session, "SELECT * FROM test WHERE a=1 AND b=2 ORDER BY b DESC", [1, 2, 3, 3])

    @since('2.0')
    def conditional_ddl_keyspace_test(self):
        session = self.prepare(create_keyspace=False)

        # try dropping when doesn't exist
        session.execute("""
            DROP KEYSPACE IF EXISTS my_test_ks
            """)

        # create and confirm
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS my_test_ks
            WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} and durable_writes = true
            """)
        assert_one(session, "select durable_writes from system.schema_keyspaces where keyspace_name = 'my_test_ks';",
                   [True], cl=ConsistencyLevel.ALL)

        # unsuccessful create since it's already there, confirm settings don't change
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS my_test_ks
            WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} and durable_writes = false
            """)

        assert_one(session, "select durable_writes from system.schema_keyspaces where keyspace_name = 'my_test_ks';",
                   [True], cl=ConsistencyLevel.ALL)

        # drop and confirm
        session.execute("""
            DROP KEYSPACE IF EXISTS my_test_ks
            """)

        assert_none(session, "select * from system.schema_keyspaces where keyspace_name = 'my_test_ks'")

    @since('2.0')
    def conditional_ddl_table_test(self):
        session = self.prepare(create_keyspace=False)

        self.create_ks(session, 'my_test_ks', 1)

        # try dropping when doesn't exist
        session.execute("""
            DROP TABLE IF EXISTS my_test_table;
            """)

        # create and confirm
        session.execute("""
            CREATE TABLE IF NOT EXISTS my_test_table (
            id text PRIMARY KEY,
            value1 blob ) with comment = 'foo';
            """)

        assert_one(session,
                   """select comment from system.schema_columnfamilies
                      where keyspace_name = 'my_test_ks' and columnfamily_name = 'my_test_table'""",
                   ['foo'])

        # unsuccessful create since it's already there, confirm settings don't change
        session.execute("""
            CREATE TABLE IF NOT EXISTS my_test_table (
            id text PRIMARY KEY,
            value2 blob ) with comment = 'bar';
            """)

        assert_one(session,
                   """select comment from system.schema_columnfamilies
                      where keyspace_name = 'my_test_ks' and columnfamily_name = 'my_test_table'""",
                   ['foo'])

        # drop and confirm
        session.execute("""
            DROP TABLE IF EXISTS my_test_table;
            """)

        assert_none(session,
                    """select * from system.schema_columnfamilies
                       where keyspace_name = 'my_test_ks' and columnfamily_name = 'my_test_table'""")

    @since('2.0')
    def conditional_ddl_index_test(self):
        session = self.prepare(create_keyspace=False)

        self.create_ks(session, 'my_test_ks', 1)

        session.execute("""
            CREATE TABLE my_test_table (
            id text PRIMARY KEY,
            value1 blob,
            value2 blob) with comment = 'foo';
            """)

        # try dropping when doesn't exist
        session.execute("DROP INDEX IF EXISTS myindex")

        # create and confirm
        session.execute("CREATE INDEX IF NOT EXISTS myindex ON my_test_table (value1)")

        # index building is asynch, wait for it to finish
        for i in range(10):
            results = session.execute(
                """select index_name from system."IndexInfo" where table_name = 'my_test_ks'""")

            if results:
                self.assertEqual([('my_test_table.myindex',)], results)
                break

            time.sleep(0.5)
        else:
            # this is executed when 'break' is never called
            self.fail("Didn't see my_test_table.myindex after polling for 5 seconds")

        # unsuccessful create since it's already there
        session.execute("CREATE INDEX IF NOT EXISTS myindex ON my_test_table (value1)")

        # drop and confirm
        session.execute("DROP INDEX IF EXISTS myindex")
        assert_none(session, """select index_name from system."IndexInfo" where table_name = 'my_test_ks'""")

    @since('2.0')
    def bug_6612_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE session_data (
                username text,
                session_id text,
                app_name text,
                account text,
                last_access timestamp,
                created_on timestamp,
                PRIMARY KEY (username, session_id, app_name, account)
            );
        """)

        session.execute("create index sessionIndex ON session_data (session_id)")
        session.execute("create index sessionAppName ON session_data (app_name)")
        session.execute("create index lastAccessIndex ON session_data (last_access)")

        assert_one(session, "select count(*) from session_data where app_name='foo' and account='bar' and last_access > 4 allow filtering", [0])

        session.execute("insert into session_data (username, session_id, app_name, account, last_access, created_on) values ('toto', 'foo', 'foo', 'bar', 12, 13)")

        assert_one(session, "select count(*) from session_data where app_name='foo' and account='bar' and last_access > 4 allow filtering", [1])

    @since('2.0')
    def blobAs_functions_test(self):
        session = self.prepare()

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int
            );
        """)

        # A blob that is not 4 bytes should be rejected
        assert_invalid(session, "INSERT INTO test(k, v) VALUES (0, blobAsInt(0x01))")

    def alter_clustering_and_static_test(self):
        session = self.prepare()

        session.execute("CREATE TABLE foo (bar int, PRIMARY KEY (bar))")

        # We shouldn't allow static when there is not clustering columns
        assert_invalid(session, "ALTER TABLE foo ADD bar2 text static")

    def drop_and_readd_collection_test(self):
        """
        @jira_ticket CASSANDRA-6276
        """
        session = self.prepare()

        session.execute("create table test (k int primary key, v set<text>, x int)")
        session.execute("insert into test (k, v) VALUES (0, {'fffffffff'})")
        self.cluster.flush()
        session.execute("alter table test drop v")
        assert_invalid(session, "alter table test add v set<int>")

    def downgrade_to_compact_bug_test(self):
        """
        @jira_ticket CASSANDRA-7744
        """
        session = self.prepare()

        session.execute("create table test (k int primary key, v set<text>)")
        session.execute("insert into test (k, v) VALUES (0, {'f'})")
        self.cluster.flush()
        session.execute("alter table test drop v")
        session.execute("alter table test add v int")

    def invalid_string_literals_test(self):
        """
        @jira_ticket CASSANDRA-8101
        """
        session = self.prepare()
        assert_invalid(session, u"insert into invalid_string_literals (k, a) VALUES (0, '\u038E\u0394\u03B4\u03E0')")

        # since the protocol requires strings to be valid UTF-8, the error response to this is a ProtocolError
        session = self.cql_connection(self.cluster.nodelist()[0], keyspace='ks')
        session.execute("create table invalid_string_literals (k int primary key, a ascii, b text)")
        try:
            session.execute("insert into invalid_string_literals (k, c) VALUES (0, '\xc2\x01')")
            self.fail("Expected error")
        except ProtocolException as e:
            self.assertTrue("Cannot decode string as UTF8" in str(e))

    def negative_timestamp_test(self):
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO test (k, v) VALUES (1, 1) USING TIMESTAMP -42")

        assert_one(session, "SELECT writetime(v) FROM TEST WHERE k = 1", [-42])

    def bug_8558_test(self):
        session = self.prepare()
        node1 = self.cluster.nodelist()[0]

        session.execute("CREATE  KEYSPACE space1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        session.execute("CREATE  TABLE space1.table1(a int, b int, c text,primary key(a,b))")
        session.execute("INSERT INTO space1.table1(a,b,c) VALUES(1,1,'1')")
        node1.nodetool('flush')
        session.execute("DELETE FROM space1.table1 where a=1 and b=1")
        node1.nodetool('flush')

        assert_none(session, "select * from space1.table1 where a=1 and b=1")

    def bug_5732_test(self):
        session = self.prepare(use_cache=True)

        session.execute("""
            CREATE TABLE test (
                k int PRIMARY KEY,
                v int,
            )
        """)

        session.execute("ALTER TABLE test WITH CACHING='ALL'")
        session.execute("INSERT INTO test (k,v) VALUES (0,0)")
        session.execute("INSERT INTO test (k,v) VALUES (1,1)")
        session.execute("CREATE INDEX testindex on test(v)")

        # wait for the index to be fully built
        start = time.time()
        while True:
            results = session.execute("""SELECT * FROM system."IndexInfo" WHERE table_name = 'ks' AND index_name = 'test.testindex'""")
            if results:
                break

            if time.time() - start > 10.0:
                results = list(session.execute('SELECT * FROM system."IndexInfo"'))
                raise Exception("Failed to build secondary index within ten seconds: %s" % (results,))
            time.sleep(0.1)

        assert_all(session, "SELECT k FROM test WHERE v = 0", [[0]])

        self.cluster.stop()
        time.sleep(0.5)
        self.cluster.start()
        time.sleep(0.5)

        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        assert_all(session, "SELECT k FROM ks.test WHERE v = 0", [[0]])

    def double_with_npe_test(self):
        """
        @jira_ticket CASSANDRA-9565

        Regression test for a null pointer exception that occurred in the CQL
        parser when parsing a statement that erroneously used 'WITH WITH'.
        """
        session = self.prepare()
        statements = ['ALTER KEYSPACE WITH WITH DURABLE_WRITES = true',
                      'ALTER KEYSPACE ks WITH WITH DURABLE_WRITES = true',
                      'CREATE KEYSPACE WITH WITH DURABLE_WRITES = true',
                      'CREATE KEYSPACE ks WITH WITH DURABLE_WRITES = true']

        for s in statements:
            session.execute('DROP KEYSPACE IF EXISTS ks')
            try:
                session.execute(s)
            except Exception as e:
                self.assertIsInstance(e, SyntaxException)
                self.assertNotIn('NullPointerException', str(e))
