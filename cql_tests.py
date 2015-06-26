# coding: utf-8

import struct
import time

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.protocol import ProtocolException
from cassandra.query import SimpleStatement

from assertions import assert_invalid, assert_one
from dtest import Tester, canReuseCluster, freshCluster
from thrift_bindings.v22.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.v22.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                        Mutation)
from thrift_tests import get_thrift_client
from tools import rows_to_list, since


class CQLTester(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False, nodes=1, rf=1, protocol_version=None, user=None, password=None, **kwargs):
        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        start_rpc = kwargs.pop('start_rpc', False)
        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        if user:
            config = {'authenticator' : 'org.apache.cassandra.auth.PasswordAuthenticator',
                      'authorizer' : 'org.apache.cassandra.auth.CassandraAuthorizer',
                      'permissions_validity_in_ms' : 0}
            cluster.set_configuration_options(values=config)

        if not cluster.nodelist():
            cluster.populate(nodes).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password)
        if create_keyspace:
            if self._preserve_cluster:
                session.execute("DROP KEYSPACE IF EXISTS ks")
            self.create_ks(session, 'ks', rf)
        return session

@since('2.1')
@canReuseCluster
class StorageProxyCQLTester(CQLTester):
    """
    Each CQL statement is exercised at least once in order to
    ensure we execute the code path in StorageProxy.
    Note that in depth CQL validation is done in Java unit tests,
    see CASSANDRA-9160.
    """

    def keyspace_test(self):
        """
        CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
        """
        cursor = self.prepare(create_keyspace=False)

        cursor.execute("CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")

        cursor.execute("USE ks")

        cursor.execute("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")

        cursor.execute("DROP KEYSPACE ks")
        assert_invalid(cursor, "USE ks", expected=InvalidRequest)

    def table_test(self):
        """
        CREATE TABLE, ALTER TABLE, TRUNCATE TABLE, DROP TABLE statements
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        cursor.execute("CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE")

        cursor.execute("ALTER TABLE test1 ADD v2 int")

        for i in xrange(0, 10):
            cursor.execute("INSERT INTO test1 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))
            cursor.execute("INSERT INTO test2 (k, c1, v1) VALUES (%d, %d, %d)" % (i, i, i))

        res = sorted(cursor.execute("SELECT * FROM test1"))
        assert rows_to_list(res) == [[i, i, i] for i in xrange(0, 10)], res

        res = sorted(cursor.execute("SELECT * FROM test2"))
        assert rows_to_list(res) == [[i, i, i] for i in xrange(0, 10)], res

        cursor.execute("TRUNCATE test1")
        cursor.execute("TRUNCATE test2")

        res = cursor.execute("SELECT * FROM test1")
        assert rows_to_list(res) == [], res

        res = cursor.execute("SELECT * FROM test2")
        assert rows_to_list(res) == [], res

        cursor.execute("DROP TABLE test1")
        cursor.execute("DROP TABLE test2")

        assert_invalid(cursor, "SELECT * FROM test1", expected=InvalidRequest)
        assert_invalid(cursor, "SELECT * FROM test2", expected=InvalidRequest)

    def index_test(self):
        """
        CREATE INDEX, DROP INDEX statements
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test3 (k int PRIMARY KEY, v1 int, v2 int)")
        cursor.execute("CREATE INDEX testidx ON test3 (v1)")

        for i in xrange(0, 10):
            cursor.execute("INSERT INTO test3 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))

        res = cursor.execute("SELECT * FROM test3 WHERE v1 = 0")
        assert rows_to_list(res) == [[0, 0, 0]], res

        cursor.execute("DROP INDEX testidx")

        assert_invalid(cursor, "SELECT * FROM test3 where v1 = 0", expected=InvalidRequest)

    def type_test(self):
        """
        CREATE TYPE, ALTER TYPE, DROP TYPE statements
        """
        cursor = self.prepare()

        cursor.execute("CREATE TYPE address_t (street text, city text, zip_code int)")
        cursor.execute("CREATE TABLE test4 (id int PRIMARY KEY, address frozen<address_t>)")

        cursor.execute("ALTER TYPE address_t ADD phones set<text>")
        cursor.execute("CREATE TABLE test5 (id int PRIMARY KEY, address frozen<address_t>)")

        cursor.execute("DROP TABLE test4")
        cursor.execute("DROP TABLE test5")
        cursor.execute("DROP TYPE address_t")
        assert_invalid(cursor, "CREATE TABLE test6 (id int PRIMARY KEY, address frozen<address_t>)", expected=InvalidRequest)

    def user_test(self):
        """
        CREATE USER, ALTER USER, DROP USER statements
        """
        cursor = self.prepare(user='cassandra', password='cassandra')

        cursor.execute("CREATE USER user1 WITH PASSWORD 'secret'")

        cursor.execute("ALTER USER user1 WITH PASSWORD 'secret^2'")

        cursor.execute("DROP USER user1")

    def statements_test(self):
        """
        INSERT, UPDATE, SELECT, SELECT COUNT, DELETE statements
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test7 (kind text, time int, v1 int, v2 int, PRIMARY KEY(kind, time) )")

        for i in xrange(0, 10):
            cursor.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', %d, %d, %d)" % (i, i, i))
            cursor.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', %d, %d, %d)" % (i, i, i))

        res = cursor.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[10]], res

        res = cursor.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2')")
        assert rows_to_list(res) == [[20]], res

        res = cursor.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2') AND time=0")
        assert rows_to_list(res) == [[2]], res

        res = cursor.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [['ev1', i, i, i] for i in xrange(0, 10)], res

        res = cursor.execute("SELECT * FROM test7 WHERE kind = 'ev2'")
        assert rows_to_list(res) == [['ev2', i, i, i] for i in xrange(0, 10)], res

        for i in xrange(0, 10):
            cursor.execute("UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=%d" % (i,))

        res = cursor.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [['ev1', i, 0, 0] for i in xrange(0, 10)], res

        res = cursor.execute("DELETE FROM test7 WHERE kind = 'ev1'")
        res = cursor.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [], res

        res = cursor.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[0]], res

    def batch_test(self):
        """
        BATCH statement
        """
        cursor = self.prepare()

        cursor.execute("""
            CREATE TABLE test8 (
                userid text PRIMARY KEY,
                name text,
                password text
            )
        """)

        query = SimpleStatement("""
            BEGIN BATCH
                INSERT INTO test8 (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
                UPDATE test8 SET password = 'ps22dhds' WHERE userid = 'user3';
                INSERT INTO test8 (userid, password) VALUES ('user4', 'ch@ngem3c');
                DELETE name FROM test8 WHERE userid = 'user1';
            APPLY BATCH;
        """, consistency_level=ConsistencyLevel.QUORUM)
        cursor.execute(query)

@since('2.1')
@canReuseCluster
class MiscellaneousCQLTester(CQLTester):
    """
    CQL tests that cannot be performed as Java unit tests, see CASSANDRA-9160. Please consider
    writing java unit tests for CQL validation, add a new test here only if there is a reason for it,
    e.g. something related to the client protocol or thrift, or examing the log files, or multiple nodes
    required.
    """

    def large_collection_errors(self):
        """
        For large collections, make sure that we are printing warnings.
        """

        # We only warn with protocol 2
        cursor = self.prepare(protocol_version=2)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]
        self.ignore_log_patterns = ["Detected collection for table"]

        cursor.execute("""
            CREATE TABLE maps (
                userid text PRIMARY KEY,
                properties map<int, text>
            );
        """)

        # Insert more than the max, which is 65535
        for i in range(70000):
            cursor.execute("UPDATE maps SET properties[%i] = 'x' WHERE userid = 'user'" % i)

        # Query for the data and throw exception
        cursor.execute("SELECT properties FROM maps WHERE userid = 'user'")
        node1.watch_log_for("Detected collection for table ks.maps with 70000 elements, more than the 65535 limit. Only the first 65535 elements will be returned to the client. Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.")

    def cql3_insert_thrift_test(self):
        """ Check that we can insert from thrift into a CQL3 table (#4377) """
        cursor = self.prepare(start_rpc=True)

        cursor.execute("""
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

        res = cursor.execute("SELECT * FROM test")
        assert rows_to_list(res) == [[2, 4, 8]], res

    def rename_test(self):
        cursor = self.prepare(start_rpc=True)

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

        cursor.execute("INSERT INTO ks.test (key, column1, column2, column3, value) VALUES ('foo', 4, 3, 2, 'bar')")

        time.sleep(1)

        cursor.execute("ALTER TABLE test RENAME column1 TO foo1 AND column2 TO foo2 AND column3 TO foo3")
        assert_one(cursor, "SELECT foo1, foo2, foo3 FROM test", [4, 3, 2])

    def invalid_string_literals_test(self):
        """
        @jira_ticket CASSANDRA-8101
        """
        cursor = self.prepare()
        assert_invalid(cursor, u"insert into invalid_string_literals (k, a) VALUES (0, '\u038E\u0394\u03B4\u03E0')")

        # since the protocol requires strings to be valid UTF-8, the error response to this is a ProtocolError
        cursor = self.cql_connection(self.cluster.nodelist()[0], keyspace='ks')
        cursor.execute("create table invalid_string_literals (k int primary key, a ascii, b text)")
        try:
            cursor.execute("insert into invalid_string_literals (k, c) VALUES (0, '\xc2\x01')")
            self.fail("Expected error")
        except ProtocolException as e:
            self.assertTrue("Cannot decode string as UTF8" in str(e))

    def prepared_statement_invalidation_test(self):
        """
        @jira_ticket CASSANDRA-7910
        """
        cursor = self.prepare()

        cursor.execute("CREATE TABLE test (k int PRIMARY KEY, a int, b int, c int)")
        cursor.execute("INSERT INTO test (k, a, b, c) VALUES (0, 0, 0, 0)")

        wildcard_prepared = cursor.prepare("SELECT * FROM test")
        explicit_prepared = cursor.prepare("SELECT k, a, b, c FROM test")
        result = cursor.execute(wildcard_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, 0)])

        cursor.execute("ALTER TABLE test DROP c")
        result = cursor.execute(wildcard_prepared.bind(None))
        # wildcard select can be automatically re-prepared by the driver
        self.assertEqual(result, [(0, 0, 0)])
        # but re-preparing the statement with explicit columns should fail
        # (see PYTHON-207 for why we expect InvalidRequestException instead of the normal exc)
        assert_invalid(cursor, explicit_prepared.bind(None), expected=InvalidRequest)

        cursor.execute("ALTER TABLE test ADD d int")
        result = cursor.execute(wildcard_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, None)])

        explicit_prepared = cursor.prepare("SELECT k, a, b, d FROM test")

        # when the type is altered, both statements will need to be re-prepared
        # by the driver, but the re-preparation should succeed
        cursor.execute("ALTER TABLE test ALTER d TYPE blob")
        result = cursor.execute(wildcard_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, None)])

        result = cursor.execute(explicit_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, None)])

    @freshCluster()
    def range_slice_test(self):
        """ Test a regression from #1337 """

        cluster = self.cluster

        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.patient_cql_connection(node1)
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

        res = cursor.execute("SELECT * FROM test")
        assert len(res) == 2, res
