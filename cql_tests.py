# coding: utf-8

import struct
import time

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.protocol import ProtocolException
from cassandra.query import SimpleStatement

from assertions import assert_invalid, assert_one, assert_unavailable
from dtest import Tester, canReuseCluster, freshCluster
from thrift_bindings.v22.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.v22.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                        Mutation)
from thrift_tests import get_thrift_client
from tools import rows_to_list, since, require, debug


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
        session = self.prepare(create_keyspace=False)

        session.execute("CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")

        session.execute("USE ks")

        session.execute("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")

        session.execute("DROP KEYSPACE ks")
        assert_invalid(session, "USE ks", expected=InvalidRequest)

    def table_test(self):
        """
        CREATE TABLE, ALTER TABLE, TRUNCATE TABLE, DROP TABLE statements
        """
        session = self.prepare()

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        session.execute("CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE")

        session.execute("ALTER TABLE test1 ADD v2 int")

        for i in xrange(0, 10):
            session.execute("INSERT INTO test1 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))
            session.execute("INSERT INTO test2 (k, c1, v1) VALUES (%d, %d, %d)" % (i, i, i))

        res = sorted(session.execute("SELECT * FROM test1"))
        assert rows_to_list(res) == [[i, i, i] for i in xrange(0, 10)], res

        res = sorted(session.execute("SELECT * FROM test2"))
        assert rows_to_list(res) == [[i, i, i] for i in xrange(0, 10)], res

        session.execute("TRUNCATE test1")
        session.execute("TRUNCATE test2")

        res = session.execute("SELECT * FROM test1")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT * FROM test2")
        assert rows_to_list(res) == [], res

        session.execute("DROP TABLE test1")
        session.execute("DROP TABLE test2")

        assert_invalid(session, "SELECT * FROM test1", expected=InvalidRequest)
        assert_invalid(session, "SELECT * FROM test2", expected=InvalidRequest)

    def index_test(self):
        """
        CREATE INDEX, DROP INDEX statements
        """
        session = self.prepare()

        session.execute("CREATE TABLE test3 (k int PRIMARY KEY, v1 int, v2 int)")
        session.execute("CREATE INDEX testidx ON test3 (v1)")

        for i in xrange(0, 10):
            session.execute("INSERT INTO test3 (k, v1, v2) VALUES (%d, %d, %d)" % (i, i, i))

        res = session.execute("SELECT * FROM test3 WHERE v1 = 0")
        assert rows_to_list(res) == [[0, 0, 0]], res

        session.execute("DROP INDEX testidx")

        assert_invalid(session, "SELECT * FROM test3 where v1 = 0", expected=InvalidRequest)

    def type_test(self):
        """
        CREATE TYPE, ALTER TYPE, DROP TYPE statements
        """
        session = self.prepare()

        session.execute("CREATE TYPE address_t (street text, city text, zip_code int)")
        session.execute("CREATE TABLE test4 (id int PRIMARY KEY, address frozen<address_t>)")

        session.execute("ALTER TYPE address_t ADD phones set<text>")
        session.execute("CREATE TABLE test5 (id int PRIMARY KEY, address frozen<address_t>)")

        session.execute("DROP TABLE test4")
        session.execute("DROP TABLE test5")
        session.execute("DROP TYPE address_t")
        assert_invalid(session, "CREATE TABLE test6 (id int PRIMARY KEY, address frozen<address_t>)", expected=InvalidRequest)

    def user_test(self):
        """
        CREATE USER, ALTER USER, DROP USER statements
        """
        session = self.prepare(user='cassandra', password='cassandra')

        session.execute("CREATE USER user1 WITH PASSWORD 'secret'")

        session.execute("ALTER USER user1 WITH PASSWORD 'secret^2'")

        session.execute("DROP USER user1")

    def statements_test(self):
        """
        INSERT, UPDATE, SELECT, SELECT COUNT, DELETE statements
        """
        session = self.prepare()

        session.execute("CREATE TABLE test7 (kind text, time int, v1 int, v2 int, PRIMARY KEY(kind, time) )")

        for i in xrange(0, 10):
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', %d, %d, %d)" % (i, i, i))
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', %d, %d, %d)" % (i, i, i))

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[10]], res

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2')")
        assert rows_to_list(res) == [[20]], res

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2') AND time=0")
        assert rows_to_list(res) == [[2]], res

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [['ev1', i, i, i] for i in xrange(0, 10)], res

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev2'")
        assert rows_to_list(res) == [['ev2', i, i, i] for i in xrange(0, 10)], res

        for i in xrange(0, 10):
            session.execute("UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time=%d" % (i,))

        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [['ev1', i, 0, 0] for i in xrange(0, 10)], res

        res = session.execute("DELETE FROM test7 WHERE kind = 'ev1'")
        res = session.execute("SELECT * FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [], res

        res = session.execute("SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'")
        assert rows_to_list(res) == [[0]], res

    def batch_test(self):
        """
        BATCH statement
        """
        session = self.prepare()

        session.execute("""
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
        session.execute(query)

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

    def prepared_statement_invalidation_test(self):
        """
        @jira_ticket CASSANDRA-7910
        """
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, a int, b int, c int)")
        session.execute("INSERT INTO test (k, a, b, c) VALUES (0, 0, 0, 0)")

        wildcard_prepared = session.prepare("SELECT * FROM test")
        explicit_prepared = session.prepare("SELECT k, a, b, c FROM test")
        result = session.execute(wildcard_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, 0)])

        session.execute("ALTER TABLE test DROP c")
        result = session.execute(wildcard_prepared.bind(None))
        # wildcard select can be automatically re-prepared by the driver
        self.assertEqual(result, [(0, 0, 0)])
        # but re-preparing the statement with explicit columns should fail
        # (see PYTHON-207 for why we expect InvalidRequestException instead of the normal exc)
        assert_invalid(session, explicit_prepared.bind(None), expected=InvalidRequest)

        session.execute("ALTER TABLE test ADD d int")
        result = session.execute(wildcard_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, None)])

        explicit_prepared = session.prepare("SELECT k, a, b, d FROM test")

        # when the type is altered, both statements will need to be re-prepared
        # by the driver, but the re-preparation should succeed
        session.execute("ALTER TABLE test ALTER d TYPE blob")
        result = session.execute(wildcard_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, None)])

        result = session.execute(explicit_prepared.bind(None))
        self.assertEqual(result, [(0, 0, 0, None)])

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


@since('3.0')
@require("7392")
class AbortedQueriesTester(CQLTester):
    """
    @jira_ticket CASSANDRA-7392
    Test that read-queries that take longer than read_request_timeout_in_ms time out
    """
    def local_query_test(self):
        """
        Check that a query running on the local coordinator node times out
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        # cassandra.test.read_iteration_delay_ms causes the state tracking read iterators
        # introduced by CASSANDRA-7392 to pause by the specified amount of milliseconds during each
        # iteration of non system queries, so that these queries take much longer to complete,
        # see ReadCommand.withStateTracking()
        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.test.read_iteration_delay_ms=100"])
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test1 (
                id int PRIMARY KEY,
                val text
            );
        """)

        for i in xrange(500):
            session.execute("INSERT INTO test1 (id, val) VALUES ({}, 'foo')".format(i))

        mark = node.mark_log()
        assert_unavailable(lambda c: debug(c.execute("SELECT * from test1")), session)
        node.watch_log_for("<SELECT \* FROM ks.test1 (.*)> timed out", from_mark=mark, timeout=30)

    def remote_query_test(self):
        """
        Check that a query running on a node other than the coordinator times out
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, jvm_args=["-Djoin_ring=false"])  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.test.read_iteration_delay_ms=100"])  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test2 (
                id int PRIMARY KEY,
                val text
            );
        """)

        for i in xrange(500):
            session.execute("INSERT INTO test2 (id, val) VALUES ({}, 'foo')".format(i))

        mark = node2.mark_log()
        assert_unavailable(lambda c: debug(c.execute("SELECT * from test2")), session)
        node2.watch_log_for("<SELECT \* FROM ks.test2 (.*)> timed out", from_mark=mark, timeout=30)

    def index_query_test(self):
        """
        Check that a secondary index query times out
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'read_request_timeout_in_ms': 1000})

        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.test.read_iteration_delay_ms=100"])  # see above for explanation
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test3 (
                id int PRIMARY KEY,
                col int,
                val text
            );
        """)

        session.execute("CREATE INDEX ON test3 (col)")

        for i in xrange(500):
            session.execute("INSERT INTO test3 (id, col, val) VALUES ({}, {}, 'foo')".format(i, i // 10))

        mark = node.mark_log()
        assert_unavailable(lambda c: debug(c.execute("SELECT * from test3 WHERE col < 50 ALLOW FILTERING")), session)
        node.watch_log_for("<SELECT \* FROM ks.test3 WHERE col < 50 (.*)> timed out", from_mark=mark, timeout=30)

    @require("6477")
    def materialized_view_test(self):
        """
        Check that a materialized view query times out
        """
        self.fail("Not yet implemented")
