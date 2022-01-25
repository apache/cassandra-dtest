import itertools
import re
import struct
import time
import pytest
import logging

from flaky import flaky

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.metadata import NetworkTopologyStrategy, SimpleStrategy
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

from dtest import Tester, create_ks, mk_bman_path
from distutils.version import LooseVersion
from thrift_bindings.thrift010.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.thrift010.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                        Mutation)
from thrift_test import get_thrift_client
from tools.assertions import (assert_all, assert_invalid, assert_length_equal,
                              assert_none, assert_one, assert_unavailable)

from tools.data import rows_to_list
from tools.metadata_wrapper import (UpdatingClusterMetadataWrapper,
                                    UpdatingKeyspaceMetadataWrapper,
                                    UpdatingTableMetadataWrapper)

since = pytest.mark.since
logger = logging.getLogger(__name__)


class CQLTester(Tester):

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False,
                nodes=1, rf=1, protocol_version=None, user=None, password=None,
                start_rpc=False, **kwargs):
        cluster = self.cluster

        if ordered:
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if use_cache:
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        if user:
            config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                      'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                      'permissions_validity_in_ms': 0}
            cluster.set_configuration_options(values=config)

        if not cluster.nodelist():
            cluster.populate(nodes).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password)
        if create_keyspace:
            create_ks(session, 'ks', rf)
        return session


class TestCQL(CQLTester):
    """
    Each CQL statement is exercised at least once in order to
    ensure we execute the code path in StorageProxy.
    # TODO This probably isn't true anymore?
    Note that in depth CQL validation is done in Java unit tests,
    see CASSANDRA-9160.

    # TODO I'm not convinced we need these. Seems like all the functionality
    #      is covered in greater detail in other test classes.
    """

    def test_keyspace(self):
        """
        Smoke test that basic keyspace operations work:

        - create a keyspace
        - assert keyspace exists and is configured as expected with the driver metadata API
        - ALTER it
        - assert keyspace was correctly altered with the driver metadata API
        - DROP it
        - assert keyspace is no longer in keyspace metadata
        """
        session = self.prepare(create_keyspace=False)
        meta = UpdatingClusterMetadataWrapper(session.cluster)

        assert 'ks' not in meta.keyspaces
        session.execute("CREATE KEYSPACE ks WITH replication = "
                        "{ 'class':'SimpleStrategy', 'replication_factor':1} "
                        "AND DURABLE_WRITES = true")
        assert 'ks' in meta.keyspaces

        ks_meta = UpdatingKeyspaceMetadataWrapper(session.cluster, ks_name='ks')
        assert ks_meta.durable_writes
        assert isinstance(ks_meta.replication_strategy, SimpleStrategy)

        session.execute("ALTER KEYSPACE ks WITH replication = "
                        "{ 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 } "
                        "AND DURABLE_WRITES = false")
        assert not ks_meta.durable_writes
        assert isinstance(ks_meta.replication_strategy, NetworkTopologyStrategy)

        session.execute("DROP KEYSPACE ks")
        assert 'ks' not in meta.keyspaces

    def test_table(self):
        """
        Smoke test that basic table operations work:

        - create a table
        - ALTER the table adding a column
        - insert 10 values
        - SELECT * and assert the values are there
        - TRUNCATE the table
        - SELECT * and assert there are no values
        - DROP the table
        - SELECT * and assert the statement raises an InvalidRequest
        # TODO run SELECTs to make sure each statement works
        """
        session = self.prepare()

        ks_meta = UpdatingKeyspaceMetadataWrapper(session.cluster, ks_name='ks')

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
        assert 'test1' in ks_meta.tables

        t1_meta = UpdatingTableMetadataWrapper(session.cluster, ks_name='ks', table_name='test1')

        session.execute("ALTER TABLE test1 ADD v2 int")
        assert 'v2' in t1_meta.columns

        for i in range(0, 10):
            session.execute("INSERT INTO test1 (k, v1, v2) VALUES ({i}, {i}, {i})".format(i=i))

        assert_all(session, "SELECT * FROM test1", [[i, i, i] for i in range(0, 10)], ignore_order=True)

        session.execute("TRUNCATE test1")

        assert_none(session, "SELECT * FROM test1")

        session.execute("DROP TABLE test1")
        assert 'test1' not in ks_meta.tables

    @since("2.0", max_version="3.X")
    def test_table_compact_storage(self):
        """
        Smoke test that basic table operations work:

        - create a table with COMPACT STORAGE
        - insert 10 values
        - SELECT * and assert the values are there
        - TRUNCATE the table
        - SELECT * and assert there are no values
        - DROP the table
        - SELECT * and assert the statement raises an InvalidRequest
        # TODO run SELECTs to make sure each statement works
        """
        session = self.prepare()

        ks_meta = UpdatingKeyspaceMetadataWrapper(session.cluster, ks_name='ks')

        session.execute("CREATE TABLE test2 (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE")
        assert 'test2' in ks_meta.tables

        for i in range(0, 10):
            session.execute("INSERT INTO test2 (k, c1, v1) VALUES ({i}, {i}, {i})".format(i=i))

        assert_all(session, "SELECT * FROM test2", [[i, i, i] for i in range(0, 10)], ignore_order=True)

        session.execute("TRUNCATE test2")

        assert_none(session, "SELECT * FROM test2")

        session.execute("DROP TABLE test2")
        assert 'test2' not in ks_meta.tables

    def test_index(self):
        """
        Smoke test CQL statements related to indexes:

        - CREATE a table
        - CREATE an index on that table
        - INSERT 10 values into the table
        - SELECT from the table over the indexed value and assert the expected values come back
        - drop the index
        - assert SELECTing over the indexed value raises an InvalidRequest
        # TODO run SELECTs to make sure each statement works
        """
        session = self.prepare()

        session.execute("CREATE TABLE test3 (k int PRIMARY KEY, v1 int, v2 int)")
        table_meta = UpdatingTableMetadataWrapper(session.cluster, ks_name='ks', table_name='test3')
        session.execute("CREATE INDEX testidx ON test3 (v1)")
        assert 'testidx' in table_meta.indexes

        for i in range(0, 10):
            session.execute("INSERT INTO test3 (k, v1, v2) VALUES ({i}, {i}, {i})".format(i=i))

        assert_one(session, "SELECT * FROM test3 WHERE v1 = 0", [0, 0, 0])

        session.execute("DROP INDEX testidx")
        assert 'testidx' not in table_meta.indexes

    def test_type(self):
        """
        Smoke test basic TYPE operations:

        - CREATE a type
        - CREATE a table using that type
        - ALTER the type and CREATE another table
        - DROP the tables and type
        - CREATE another table using the DROPped type and assert it fails with an InvalidRequest
        # TODO run SELECTs to make sure each statement works
        # TODO is this even necessary given the existence of the auth_tests?
        """
        session = self.prepare()
        # even though we only ever use the user_types attribute of this object,
        # we have to access it each time, because attribute access is how the
        # value is updated
        ks_meta = UpdatingKeyspaceMetadataWrapper(session.cluster, ks_name='ks')

        session.execute("CREATE TYPE address_t (street text, city text, zip_code int)")
        assert 'address_t' in ks_meta.user_types

        session.execute("CREATE TABLE test4 (id int PRIMARY KEY, address frozen<address_t>)")

        session.execute("ALTER TYPE address_t ADD phones set<text>")
        assert 'phones' in ks_meta.user_types['address_t'].field_names

        # drop the table so we can safely drop the type it uses
        session.execute("DROP TABLE test4")

        session.execute("DROP TYPE address_t")
        assert 'address_t' not in ks_meta.user_types

    def test_user(self):
        """
        Smoke test for basic USER queries:

        - get a session as the default superuser
        - CREATE a user
        - ALTER that user by giving it a different password
        - DROP that user
        # TODO list users after each to make sure each statement works
        """
        session = self.prepare(user='cassandra', password='cassandra')
        node1 = self.cluster.nodelist()[0]

        def get_usernames():
            return [user.name for user in session.execute('LIST USERS')]

        assert 'user1' not in get_usernames()

        session.execute("CREATE USER user1 WITH PASSWORD 'secret'")
        # use patient to retry until it works, because it takes some time for
        # the CREATE to take
        self.patient_cql_connection(node1, user='user1', password='secret')

        session.execute("ALTER USER user1 WITH PASSWORD 'secret^2'")
        # use patient for same reason as above
        self.patient_cql_connection(node1, user='user1', password='secret^2')

        session.execute("DROP USER user1")
        assert 'user1' not in get_usernames()

    def test_statements(self):
        """
        Smoke test SELECT and UPDATE statements:

        - create a table
        - insert 20 rows into the table
        - run SELECT COUNT queries and assert they return the correct values
            - bare and with IN and equality conditions
        - run SELECT * queries with = conditions
        - run UPDATE queries
        - SELECT * and assert the UPDATEd values are there
        - DELETE with a = condition
        - SELECT the deleted values and make sure nothing is returned
        # TODO run SELECTs to make sure each statement works
        """
        session = self.prepare()

        session.execute("CREATE TABLE test7 (kind text, time int, v1 int, v2 int, PRIMARY KEY(kind, time) )")

        for i in range(0, 10):
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev1', {i}, {i}, {i})".format(i=i))
            session.execute("INSERT INTO test7 (kind, time, v1, v2) VALUES ('ev2', {i}, {i}, {i})".format(i=i))

        assert_one(session, "SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'", [10])

        assert_one(session, "SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2')", [20])

        assert_one(session, "SELECT COUNT(*) FROM test7 WHERE kind IN ('ev1', 'ev2') AND time=0", [2])

        assert_all(session, "SELECT * FROM test7 WHERE kind = 'ev1'", [['ev1', i, i, i] for i in range(0, 10)])

        assert_all(session, "SELECT * FROM test7 WHERE kind = 'ev2'", [['ev2', i, i, i] for i in range(0, 10)])

        for i in range(0, 10):
            session.execute("UPDATE test7 SET v1 = 0, v2 = 0 where kind = 'ev1' AND time={i}".format(i=i))

        assert_all(session, "SELECT * FROM test7 WHERE kind = 'ev1'", [['ev1', i, 0, 0] for i in range(0, 10)])

        session.execute("DELETE FROM test7 WHERE kind = 'ev1'")
        assert_none(session, "SELECT * FROM test7 WHERE kind = 'ev1'")

        assert_one(session, "SELECT COUNT(*) FROM test7 WHERE kind = 'ev1'", [0])

    @since('3.10')
    def test_partition_key_allow_filtering(self):
        """
        Filtering with unrestricted parts of partition keys

        @jira_ticket CASSANDRA-11031
        """
        session = self.prepare()

        session.execute("""
            CREATE TABLE IF NOT EXISTS test_filter (
                k1 int,
                k2 int,
                ck1 int,
                v int,
                PRIMARY KEY ((k1, k2), ck1)
            )
        """)

        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 0, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 1, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 2, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 0, 3, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 0, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 1, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 2, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (0, 1, 3, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 0, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 1, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 2, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 0, 3, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 0, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 1, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 2, 0)")
        session.execute("INSERT INTO test_filter (k1, k2, ck1, v) VALUES (1, 1, 3, 0)")

        # select test
        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 = 0 ALLOW FILTERING",
                   [[0, 0, 0, 0],
                    [0, 0, 1, 0],
                    [0, 0, 2, 0],
                    [0, 0, 3, 0],
                    [0, 1, 0, 0],
                    [0, 1, 1, 0],
                    [0, 1, 2, 0],
                    [0, 1, 3, 0]],
                   ignore_order=True)

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 <= 1 AND k2 >= 1 ALLOW FILTERING",
                   [[0, 1, 0, 0],
                    [0, 1, 1, 0],
                    [0, 1, 2, 0],
                    [0, 1, 3, 0],
                    [1, 1, 0, 0],
                    [1, 1, 1, 0],
                    [1, 1, 2, 0],
                    [1, 1, 3, 0]],
                   ignore_order=True)

        assert_none(session, "SELECT * FROM test_filter WHERE k1 = 2 ALLOW FILTERING")
        assert_none(session, "SELECT * FROM test_filter WHERE k1 <=0 AND k2 > 1 ALLOW FILTERING")

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k2 <= 0 ALLOW FILTERING",
                   [[0, 0, 0, 0],
                    [0, 0, 1, 0],
                    [0, 0, 2, 0],
                    [0, 0, 3, 0],
                    [1, 0, 0, 0],
                    [1, 0, 1, 0],
                    [1, 0, 2, 0],
                    [1, 0, 3, 0]],
                   ignore_order=True)

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 <= 0 AND k2 = 0 ALLOW FILTERING",
                   [[0, 0, 0, 0],
                    [0, 0, 1, 0],
                    [0, 0, 2, 0],
                    [0, 0, 3, 0]])

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k2 = 1 ALLOW FILTERING",
                   [[0, 1, 0, 0],
                    [0, 1, 1, 0],
                    [0, 1, 2, 0],
                    [0, 1, 3, 0],
                    [1, 1, 0, 0],
                    [1, 1, 1, 0],
                    [1, 1, 2, 0],
                    [1, 1, 3, 0]],
                   ignore_order=True)

        assert_none(session, "SELECT * FROM test_filter WHERE k2 = 2 ALLOW FILTERING")

        # filtering on both Partition Key and Clustering key
        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 = 0 AND ck1=0 ALLOW FILTERING",
                   [[0, 0, 0, 0],
                    [0, 1, 0, 0]],
                   ignore_order=True)

        assert_all(session,
                   "SELECT * FROM test_filter WHERE k1 = 0 AND k2=1 AND ck1=0 ALLOW FILTERING",
                   [[0, 1, 0, 0]])

        # count(*) test
        assert_all(session,
                   "SELECT count(*) FROM test_filter WHERE k2 = 0 ALLOW FILTERING",
                   [[8]])

        assert_all(session,
                   "SELECT count(*) FROM test_filter WHERE k2 = 1 ALLOW FILTERING",
                   [[8]])

        assert_all(session,
                   "SELECT count(*) FROM test_filter WHERE k2 = 2 ALLOW FILTERING",
                   [[0]])

        # test invalid query
        with pytest.raises(InvalidRequest):
            session.execute("SELECT * FROM test_filter WHERE k1 = 0")

        with pytest.raises(InvalidRequest):
            session.execute("SELECT * FROM test_filter WHERE k1 = 0 AND k2 > 0")

        with pytest.raises(InvalidRequest):
            session.execute("SELECT * FROM test_filter WHERE k1 >= 0 AND k2 in (0,1,2)")

        with pytest.raises(InvalidRequest):
            session.execute("SELECT * FROM test_filter WHERE k2 > 0")

    def test_batch(self):
        """
        Smoke test for BATCH statements:

        - CREATE a table
        - create a BATCH statement and execute it at QUORUM
        # TODO run SELECTs to make sure each statement works
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


class TestMiscellaneousCQL(CQLTester):
    """
    CQL tests that cannot be performed as Java unit tests, see CASSANDRA-9160.
    If you're considering adding a test here, consider writing Java unit tests
    for CQL validation instead. Add a new test here only if there is a reason
    for it, e.g. the test is related to the client protocol or thrift, requires
    examining the log files, or must run on multiple nodes.
    """

    @since('2.1', max_version='3.0')
    def test_large_collection_errors(self):
        """
        Assert C* logs warnings when selecting too large a collection over
        protocol v2:

        - prepare the cluster and connect using protocol v2
        - CREATE a table containing a map column
        - insert over 65535 elements into the map
        - select all the elements of the map
        - assert that the correct error was logged
        """

        # We only warn with protocol 2
        session = self.prepare(protocol_version=2)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]
        self.fixture_dtest_setup.ignore_log_patterns = ["Detected collection for table"]

        session.execute("""
            CREATE TABLE maps (
                userid text PRIMARY KEY,
                properties map<int, text>
            );
        """)

        # Insert more than the max, which is 65535
        for i in range(70000):
            session.execute("UPDATE maps SET properties[{}] = 'x' WHERE userid = 'user'".format(i))

        # Query for the data and throw exception
        session.execute("SELECT properties FROM maps WHERE userid = 'user'")
        node1.watch_log_for("Detected collection for table ks.maps with 70000 elements, more than the 65535 limit. "
                            "Only the first 65535 elements will be returned to the client. Please see "
                            "http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.")

    @since('2.0', max_version='4')
    def test_cql3_insert_thrift(self):
        """
        Check that we can insert from thrift into a CQL3 table:

        - CREATE a table via CQL
        - insert values via thrift
        - SELECT the inserted values and assert they are there as expected

        @jira_ticket CASSANDRA-4377
        """
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
        column_name = b'\x00\x04' + column_name_component + b'\x00' + b'\x00\x01' + 'v'.encode("utf-8") + b'\x00'
        value = struct.pack('>i', 8)
        client.batch_mutate(
            {key: {'test': [Mutation(ColumnOrSuperColumn(column=Column(name=column_name, value=value, timestamp=100)))]}},
            ThriftConsistencyLevel.ONE)

        assert_one(session, "SELECT * FROM test", [2, 4, 8])

    @since('2.0', max_version='4')
    def test_rename(self):
        """
        Check that a thrift-created table can be renamed via CQL:

        - create a table via the thrift interface
        - INSERT a row via CQL
        - ALTER the name of the table via CQL
        - SELECT from the table and assert the values inserted are there
        """
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
        session.execute("ALTER TABLE test RENAME column1 TO foo1 AND column2 TO foo2 AND column3 TO foo3")
        assert_one(session, "SELECT foo1, foo2, foo3 FROM test", [4, 3, 2])

    def test_invalid_string_literals(self):
        """
        @jira_ticket CASSANDRA-8101

        - assert INSERTing into a nonexistent table fails normally, with an InvalidRequest exception
        - create a table with ascii and text columns
        - assert that trying to execute an insert statement with non-UTF8 contents raises a ProtocolException
            - tries to insert into a nonexistent column to make sure the ProtocolException is raised over other errors
        """
        session = self.prepare()
        # this should fail as normal, not with a ProtocolException
        assert_invalid(session, "insert into invalid_string_literals (k, a) VALUES (0, '\u038E\u0394\u03B4\u03E0')")

        session = self.patient_cql_connection(self.cluster.nodelist()[0], keyspace='ks')
        session.execute("create table invalid_string_literals (k int primary key, a ascii, b text)")

        # this should still fail with an InvalidRequest
        assert_invalid(session, "insert into invalid_string_literals (k, c) VALUES (0, '\u038E\u0394\u03B4\u03E0')")

        # try to insert utf-8 characters into an ascii column and make sure it fails
        with pytest.raises(InvalidRequest, match='Invalid ASCII character in string literal'):
            session.execute("insert into invalid_string_literals (k, a) VALUES (0, '\xE0\x80\x80')")

    def test_prepared_statement_invalidation(self):
        """
        @jira_ticket CASSANDRA-7910

        - CREATE a table and INSERT a row
        - prepare 2 prepared SELECT statements
        - SELECT the row with a bound prepared statement and assert it returns the expected row
        - ALTER the table, dropping a column
        - assert prepared statement without that column in it still works
        - assert prepared statement containing that column fails
        - ALTER the table, adding a column
        - assert prepared statement without that column in it still works
        - assert prepared statement containing that column also still works
        - ALTER the table, changing the type of a column
        - assert that both prepared statements still work
        """
        session = self.prepare()

        session.execute("CREATE TABLE test (k int PRIMARY KEY, a int, b int, c int)")
        session.execute("INSERT INTO test (k, a, b, c) VALUES (0, 0, 0, 0)")

        wildcard_prepared = session.prepare("SELECT * FROM test")
        explicit_prepared = session.prepare("SELECT k, a, b, c FROM test")
        result = session.execute(wildcard_prepared.bind(None))
        assert result, [(0, 0, 0 == 0)]

        session.execute("ALTER TABLE test DROP c")
        result = session.execute(wildcard_prepared.bind(None))
        # wildcard select can be automatically re-prepared by the driver
        assert result, [(0, 0 == 0)]
        # but re-preparing the statement with explicit columns should fail
        # (see PYTHON-207 for why we expect InvalidRequestException instead of the normal exc)
        assert_invalid(session, explicit_prepared.bind(None), expected=InvalidRequest)

        session.execute("ALTER TABLE test ADD d int")
        result = session.execute(wildcard_prepared.bind(None))
        assert result, [(0, 0, 0 == None)]

        if self.cluster.version() < LooseVersion('3.0'):
            explicit_prepared = session.prepare("SELECT k, a, b, d FROM test")

            # when the type is altered, both statements will need to be re-prepared
            # by the driver, but the re-preparation should succeed
            session.execute("ALTER TABLE test ALTER d TYPE blob")
            result = session.execute(wildcard_prepared.bind(None))
            assert result, [(0, 0, 0 == None)]

            result = session.execute(explicit_prepared.bind(None))
            assert result, [(0, 0, 0 == None)]

    def test_range_slice(self):
        """
        Regression test for CASSANDRA-1337:

        - CREATE a table
        - INSERT 2 rows
        - SELECT * from the table
        - assert 2 rows were returned

        @jira_ticket CASSANDRA-1337
        # TODO I don't see how this is an interesting test or how it tests 1337.
        """

        cluster = self.cluster

        cluster.populate(2).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)

        session.execute("""
            CREATE TABLE test (
                k text PRIMARY KEY,
                v int
            );
        """)
        time.sleep(1)

        session.execute("INSERT INTO test (k, v) VALUES ('foo', 0)")
        session.execute("INSERT INTO test (k, v) VALUES ('bar', 1)")

        res = list(session.execute("SELECT * FROM test"))
        assert len(res) == 2, res

    @pytest.mark.skip(reason="Skipping until PYTHON-893 is fixed")
    def test_many_columns(self):
        """
        Test for tables with thousands of columns.
        For CASSANDRA-11621.
        """
        session = self.prepare()
        width = 5000
        cluster = self.cluster

        session.execute("CREATE TABLE very_wide_table (pk int PRIMARY KEY, " +
                        ",".join(["c_{} int".format(i) for i in range(width)]) +
                        ")")

        session.execute("INSERT INTO very_wide_table (pk, " +
                        ",".join(["c_{}".format(i) for i in range(width)]) +
                        ") VALUES (100," +
                        ",".join([str(i) for i in range(width)]) +
                        ")")

        assert_all(session, "SELECT " +
                   ",".join(["c_{}".format(i) for i in range(width)]) +
                   " FROM very_wide_table", [[i for i in range(width)]])

    @since("3.11", max_version="3.X")
    def test_drop_compact_storage_flag(self):
        """
        Test for CASSANDRA-10857, verifying the schema change
        distribution across the other nodes.

        """

        cluster = self.cluster
        cluster.set_configuration_options({'enable_drop_compact_storage': 'true'})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session1 = self.patient_cql_connection(node1)
        session2 = self.patient_cql_connection(node2)
        session3 = self.patient_cql_connection(node3)
        create_ks(session1, 'ks', 3)
        sessions = [session1, session2, session3]

        for session in sessions:
            session.set_keyspace('ks')

        session1.execute("""
            CREATE TABLE test_drop_compact_storage (k int PRIMARY KEY, s1 int) WITH COMPACT STORAGE;
        """)

        session1.execute("INSERT INTO test_drop_compact_storage (k, s1) VALUES (1,1)")
        session1.execute("INSERT INTO test_drop_compact_storage (k, s1) VALUES (2,2)")
        session1.execute("INSERT INTO test_drop_compact_storage (k, s1) VALUES (3,3)")

        for session in sessions:
            res = session.execute("SELECT * from test_drop_compact_storage")
            assert rows_to_list(res) == [[1, 1], [2, 2], [3, 3]]

        session1.execute("ALTER TABLE test_drop_compact_storage DROP COMPACT STORAGE")

        for session in sessions:
            assert_all(session, "SELECT * from test_drop_compact_storage",
                       [[1, None, 1, None],
                        [2, None, 2, None],
                        [3, None, 3, None]])

    @since("4.0")
    def test_truncate_failure(self):
        """
        @jira_ticket CASSANDRA-16208
        Tests that if a TRUNCATE query fails on some replica, the coordinator will immediately return an error to the
        client instead of waiting to time out because it couldn't get the necessary number of success acks.
        """
        cluster = self.cluster
        cluster.populate(3, install_byteman=True).start()
        node1, _, node3 = cluster.nodelist()
        node3.byteman_submit([mk_bman_path('truncate_fail.btm')])

        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', 3)

        logger.debug("Creating data table")
        session.execute("CREATE TABLE data (id int PRIMARY KEY, data text)")
        session.execute("UPDATE data SET data = 'Awesome' WHERE id = 1")

        self.fixture_dtest_setup.ignore_log_patterns = ['Dummy failure']
        logger.debug("Truncating data table (error expected)")

        thrown = False
        exception = None
        try:
            session.execute("TRUNCATE data")
        except Exception as e:
            exception = e
            thrown = True

        assert thrown, "No exception has been thrown"
        assert re.search("Truncate failed on replica /127.0.0.3", str(exception)) is not None

@since('3.2')
class AbortedQueryTester(CQLTester):
    """
    @jira_ticket CASSANDRA-7392

    Test that read-queries that take longer than read_request_timeout_in_ms
    time out.

    # TODO The important part of these is "set up a combination of
    #      configuration options that will make all reads time out, then
    #      try to read and assert it times out". This can probably be made much
    #      simpler -- most of the logic can be factored out. In many cases it
    #      probably isn't even necessary to define a custom table or to insert
    #      more than one value.
    """

    def test_local_query(self):
        """
        Check that a query running on the local coordinator node times out:

        - set the read request timeouts to 1 second
        - start the cluster with read_iteration_delay set to 5 ms
            - the delay will be applied ot each row iterated and will cause
              read queries to take longer than the read timeout
        - CREATE and INSERT into a table
        - SELECT * from the table using a retry policy that never retries, and assert it times out

        @jira_ticket CASSANDRA-7392
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'request_timeout_in_ms': 1000,
                                                  'read_request_timeout_in_ms': 1000,
                                                  'range_request_timeout_in_ms': 1000})

        # cassandra.test.read_iteration_delay_ms causes the state tracking read iterators
        # introduced by CASSANDRA-7392 to pause by the specified amount of milliseconds every
        # CQL row iterated for non system queries, so that these queries take much longer to complete,
        # see ReadCommand.withStateTracking()
        cluster.populate(1).start(jvm_args=["-Dcassandra.monitoring_report_interval_ms=10",
                                            "-Dcassandra.test.read_iteration_delay_ms=5"])
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test1 (
                id int PRIMARY KEY,
                val text
            );
        """)

        for i in range(500):
            session.execute("INSERT INTO test1 (id, val) VALUES ({}, 'foo')".format(i))

        # use debug logs because at info level no-spam logger has unpredictable results
        mark = node.mark_log(filename='debug.log')
        statement = SimpleStatement("SELECT * from test1",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)
        node.watch_log_for("operations timed out", filename='debug.log', from_mark=mark, timeout=120)

    def test_remote_query(self):
        """
        Check that a query running on a node other than the coordinator times out:

        - populate the cluster with 2 nodes
        - set the read request timeouts to 1 second
        - start one node without having it join the ring
        - start the other node with read_iteration_delay set to 5 ms
            - the delay will be applied ot each row iterated and will cause
              read queries to take longer than the read timeout
        - CREATE a table
        - INSERT 5000 rows on a session on the node that is not a member of the ring
        - run SELECT statements and assert they fail
        # TODO refactor SELECT statements:
        #        - run the statements in a loop to reduce duplication
        #        - watch the log after each query
        #        - assert we raise the right error
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'request_timeout_in_ms': 1000,
                                                  'read_request_timeout_in_ms': 1000,
                                                  'range_request_timeout_in_ms': 1000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, join_ring=False)  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True,
                    jvm_args=["-Dcassandra.monitoring_report_interval_ms=10",
                              "-Dcassandra.test.read_iteration_delay_ms=5"])  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test2 (
                id int,
                col int,
                val text,
                PRIMARY KEY(id, col)
            );
        """)

        for i, j in itertools.product(list(range(10)), list(range(500))):
            session.execute("INSERT INTO test2 (id, col, val) VALUES ({}, {}, 'foo')".format(i, j))

        # use debug logs because at info level no-spam logger has unpredictable results
        mark = node2.mark_log(filename='debug.log')

        statement = SimpleStatement("SELECT * from test2",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)

        statement = SimpleStatement("SELECT * from test2 where id = 1",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)

        statement = SimpleStatement("SELECT * from test2 where id IN (1, 2, 3) AND col > 10",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)

        statement = SimpleStatement("SELECT * from test2 where col > 5 ALLOW FILTERING",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())
        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)

        node2.watch_log_for("operations timed out", filename='debug.log', from_mark=mark, timeout=60)

    def test_index_query(self):
        """
        Check that a secondary index query times out:

        - populate a 1-node cluster
        - set the read request timeouts to 1 second
        - start one node without having it join the ring
        - start the other node with read_iteration_delay set to 5 ms
            - the delay will be applied ot each row iterated and will cause
              read queries to take longer than the read timeout
        - CREATE a table
        - CREATE an index on the table
        - INSERT 500 values into the table
        - SELECT over the table and assert it times out
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'request_timeout_in_ms': 1000,
                                                  'read_request_timeout_in_ms': 1000,
                                                  'range_request_timeout_in_ms': 1000})

        cluster.populate(1).start(jvm_args=["-Dcassandra.monitoring_report_interval_ms=10",
                                            "-Dcassandra.test.read_iteration_delay_ms=5"])  # see above for explanation
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test3 (
                id int PRIMARY KEY,
                col int,
                val text
            );
        """)

        session.execute("CREATE INDEX ON test3 (col)")

        for i in range(500):
            session.execute("INSERT INTO test3 (id, col, val) VALUES ({}, 50, 'foo')".format(i))

        # use debug logs because at info level no-spam logger has unpredictable results
        mark = node.mark_log(filename='debug.log')
        statement = session.prepare("SELECT * from test3 WHERE col = ? ALLOW FILTERING")
        statement.consistency_level = ConsistencyLevel.ONE
        statement.retry_policy = FallthroughRetryPolicy()
        assert_unavailable(lambda c: logger.debug(c.execute(statement, [50])), session)
        node.watch_log_for("operations timed out", filename='debug.log', from_mark=mark, timeout=120)

    def test_materialized_view(self):
        """
        Check that a materialized view query times out:

        - populate a 2-node cluster
        - set the read request timeouts to 1 second
        - start one node without having it join the ring
        - start the other node with read_iteration_delay set to 5 ms
            - the delay will be applied ot each row iterated and will cause
              read queries to take longer than the read timeout
        - CREATE a table
        - INSERT 500 values into that table
        - CREATE a materialized view over that table
        - assert querying that table results in an unavailable exception
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'request_timeout_in_ms': 1000,
                                                  'read_request_timeout_in_ms': 1000,
                                                  'range_request_timeout_in_ms': 1000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, join_ring=False)  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True,
                    jvm_args=["-Dcassandra.monitoring_report_interval_ms=10",
                              "-Dcassandra.test.read_iteration_delay_ms=5"])  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test4 (
                id int PRIMARY KEY,
                col int,
                val text
            );
        """)

        session.execute(("CREATE MATERIALIZED VIEW mv AS SELECT * FROM test4 "
                         "WHERE col IS NOT NULL AND id IS NOT NULL PRIMARY KEY (col, id)"))

        for i in range(500):
            session.execute("INSERT INTO test4 (id, col, val) VALUES ({}, 50, 'foo')".format(i))

        # use debug logs because at info level no-spam logger has unpredictable results
        mark = node2.mark_log(filename='debug.log')
        statement = SimpleStatement("SELECT * FROM mv WHERE col = 50",
                                    consistency_level=ConsistencyLevel.ONE,
                                    retry_policy=FallthroughRetryPolicy())

        assert_unavailable(lambda c: logger.debug(c.execute(statement)), session)
        node2.watch_log_for("operations timed out", filename='debug.log', from_mark=mark, timeout=60)


@since('3.10')
class TestCQLSlowQuery(CQLTester):
    """
    Test slow query logging.

    @jira_ticket CASSANDRA-12403
    """
    def test_local_query(self):
        """
        Check that a query running locally on the coordinator is reported as slow:

        - start a one node cluster with slow_query_log_timeout_in_ms set to a small value
          and the read request timeouts set to a large value (to ensure the query is not aborted) and
          read_iteration_delay set to a value big enough for the query to exceed slow_query_log_timeout_in_ms
          (this will cause read queries to take longer than the slow query timeout)
        - CREATE and INSERT into a table
        - SELECT * from the table using a retry policy that never retries, and check that the slow
          query log messages are present in the debug logs (we cannot check the logs at info level because
          the no spam logger has unpredictable results)

        @jira_ticket CASSANDRA-12403
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'slow_query_log_timeout_in_ms': 10,
                                                  'request_timeout_in_ms': 120000,
                                                  'read_request_timeout_in_ms': 120000,
                                                  'range_request_timeout_in_ms': 120000})

        # cassandra.test.read_iteration_delay_ms causes the state tracking read iterators
        # introduced by CASSANDRA-7392 to pause by the specified amount of milliseconds during each
        # iteration of non system queries, so that these queries take much longer to complete,
        # see ReadCommand.withStateTracking()
        cluster.populate(1).start(jvm_args=["-Dcassandra.monitoring_report_interval_ms=10",
                                            "-Dcassandra.test.read_iteration_delay_ms=1"])
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        self._assert_logs_slow_queries(node, session)

    def test_remote_query(self):
        """
        Check that a query running on a node other than the coordinator is reported as slow:

        - populate the cluster with 2 nodes
        - start one node without having it join the ring
        - start the other one node with slow_query_log_timeout_in_ms set to a small value
          and the read request timeouts set to a large value (to ensure the query is not aborted) and
          read_iteration_delay set to a value big enough for the query to exceed slow_query_log_timeout_in_ms
          (this will cause read queries to take longer than the slow query timeout)
        - CREATE a table
        - INSERT 5000 rows on a session on the node that is not a member of the ring
        - run SELECT statements and check that the slow query messages are present in the debug logs
          (we cannot check the logs at info level because the no spam logger has unpredictable results)

        @jira_ticket CASSANDRA-12403
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'slow_query_log_timeout_in_ms': 1,
                                                  'request_timeout_in_ms': 120000,
                                                  'read_request_timeout_in_ms': 120000,
                                                  'range_request_timeout_in_ms': 120000})

        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True, join_ring=False)  # ensure other node executes queries
        node2.start(wait_for_binary_proto=True,
                    jvm_args=["-Dcassandra.monitoring_report_interval_ms=10",
                              "-Dcassandra.test.read_iteration_delay_ms=2"])  # see above for explanation

        session = self.patient_exclusive_cql_connection(node1)

        self._assert_logs_slow_queries(node2, session)

    def test_disable_slow_query_log(self):
        """
        Check that a query is NOT reported as slow if slow query logging is disabled.

        - start a one node cluster with slow_query_log_timeout_in_ms set to 0 milliseconds
          (this will disable slow query logging), the read request timeouts set to a large value
          (to ensure queries are not aborted) and read_iteration_delay set to 5 milliseconds
          (this will cause read queries to take longer than usual)
        - CREATE and INSERT into a table
        - SELECT * from the table using a retry policy that never retries, and check that the slow
          query log messages are present in the logs

        @jira_ticket CASSANDRA-12403
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'slow_query_log_timeout_in_ms': 0,
                                                  'request_timeout_in_ms': 120000,
                                                  'read_request_timeout_in_ms': 120000,
                                                  'range_request_timeout_in_ms': 120000})

        # cassandra.test.read_iteration_delay_ms causes the state tracking read iterators
        # introduced by CASSANDRA-7392 to pause by the specified amount of milliseconds during each
        # iteration of non system queries, so that these queries take much longer to complete,
        # see ReadCommand.withStateTracking()
        cluster.populate(1).start(jvm_args=["-Dcassandra.monitoring_report_interval_ms=10",
                                            "-Dcassandra.test.read_iteration_delay_ms=1"])
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE test3 (
                id int PRIMARY KEY,
                val text
            );
        """)

        for i in range(100):
            session.execute("INSERT INTO test3 (id, val) VALUES ({}, 'foo')".format(i))

        session.execute(SimpleStatement("SELECT * from test3",
                                        consistency_level=ConsistencyLevel.ONE,
                                        retry_policy=FallthroughRetryPolicy()))

        time.sleep(1)  # do our best to ensure logs had a chance to appear

        self._check_logs(node, "SELECT \* FROM ks.test3", 'debug.log', 0)

    @staticmethod
    def _assert_logs_slow_queries(node, session):
        TestCQLSlowQuery._assert_logs_slow_queries_with_skinny_table(node, session)
        for asc in (True, False):
            TestCQLSlowQuery._assert_logs_slow_queries_with_wide_table(node, session, asc=asc)

    @staticmethod
    def _assert_logs_slow_queries_with_skinny_table(node, session):
        create_ks(session, 'ks', 1)
        table = "t_skinny"
        session.execute("""
            CREATE TABLE {} (
                k int,
                v int,
                l list<int>,
                s set<int>,
                m map<int, int>,
                "Escaped_Name" int,
                PRIMARY KEY(k)
            );
        """.format(table))

        # insert some partitions
        for k in range(100):
            session.execute("INSERT INTO {} (k, v) VALUES ({}, 1)".format(table, k))

        # test logging of slow queries without restricitons
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {}",
                                      logged_query="SELECT \* FROM ks.t")

        # test logging of slow queries with restriciton on token
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE token(k) < 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE token\(k\) < 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE token(k) > 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE token\(k\) > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE token(k) <= 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE token\(k\) <= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE token(k) >= 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE token\(k\) >= 0")

        # test logging of slow queries with restriciton on partition key
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k > 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k < 100 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k < 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k >= 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k >= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k <= 100 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k <= 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k > 0 AND k <= 100 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k > 0 AND k <= 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k >= 0 AND k < 100 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k >= 0 AND k < 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k <= 100 AND k > 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k > 0 AND k <= 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k < 100 AND k >= 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k >= 0 AND k < 100")

        # test logging of slow queries with restriciton on regular column
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v = 1 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v = 1")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v > 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v < 2 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v < 2")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v >= 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v >= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v <= 2 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v <= 2")

        # test logging of slow queries with column selections
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT v FROM {}",
                                      logged_query="SELECT v FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT l FROM {}",
                                      logged_query="SELECT l FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT s FROM {}",
                                      logged_query="SELECT s FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT m FROM {}",
                                      logged_query="SELECT m FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT \"Escaped_Name\" FROM {}",
                                      logged_query="SELECT \"Escaped_Name\" FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT v,s FROM {}",
                                      logged_query="SELECT s, v FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT k,v,s FROM {}",
                                      logged_query="SELECT s, v FROM ks.{}")

        # test logging of slow queries with primary key-only column selections
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT k FROM {}",
                                      logged_query="SELECT \* FROM ks.{}")

        # test logging of slow queries with sub-selections (only supported since 4.0)
        if node.cluster.version() >= '4.0':
            TestCQLSlowQuery._assert_logs(node, session, table,
                                          query="SELECT s[0] FROM {}",
                                          logged_query="SELECT s\[0\] FROM ks.{}")
            TestCQLSlowQuery._assert_logs(node, session, table,
                                          query="SELECT m[0] FROM {}",
                                          logged_query="SELECT m\[0\] FROM ks.{}")
            TestCQLSlowQuery._assert_logs(node, session, table,
                                          query="SELECT k,v,s[0],m[1] FROM {}",
                                          logged_query="SELECT m\[1\], s\[0\], v FROM ks.{}")

    @staticmethod
    def _assert_logs_slow_queries_with_wide_table(node, session, asc=True):
        create_ks(session, 'ks', 1)
        table = "t_wide_asc" if asc else "t_wide_desc"
        session.execute("""
            CREATE TABLE {} (
                k int,
                c1 int,
                c2 int,
                v int,
                s int STATIC,
                lt list<int>,
                st set<int>,
                mp map<int, int>,
                "Escaped_Name" int,
                PRIMARY KEY(k, c1, c2)
            ) WITH CLUSTERING ORDER BY (c1 {}, c2 {});
        """.format(table, "ASC" if asc else "DESC", "ASC" if asc else "DESC"))

        # insert some partitions
        for k in range(100):
            session.execute("INSERT INTO {} (k, c1, c2, s, v) VALUES ({}, 1, 1, 1, 1)".format(table, k))

        # add some rows to one of the partitions
        for c1 in range(100):
            session.execute("INSERT INTO {} (k, c1, c2, s, v) VALUES (0, {}, 1, 1, 1)".format(table, c1))

        # add some rows to one of the pairs partition-first clustering
        for c2 in range(100):
            session.execute("INSERT INTO {} (k, c1, c2, s, v) VALUES (0, 0, {}, 1, 1)".format(table, c2))

        # test logging of slow queries without restricitons
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {}",
                                      logged_query="SELECT \* FROM ks.t")

        # test logging of slow queries with restriciton on token
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE token(k) < 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE token\(k\) < 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE token(k) > 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE token\(k\) > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE token(k) <= 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE token\(k\) <= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE token(k) >= 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE token\(k\) >= 0")

        # test logging of slow queries with restriciton on partition key
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k = 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k IN (0) ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k IN (0, 1) ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k > 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k < 100 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k < 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k >= 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k >= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k <= 100 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE k <= 100")

        # test logging of slow queries with restriciton on first clustering key
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE c1 = 1 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE c1 = 1")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE c1 IN (1) ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE c1 = 1")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT v FROM {} WHERE c1 IN (1, 2) ALLOW FILTERING",
                                      logged_query="SELECT v FROM ks.{} WHERE c1 IN \({}, {}\)"
                                      .format(table, 1 if asc else 2, 2 if asc else 1))
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE c1 > 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE c1 > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE c1 < 100 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE c1 < 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE c1 >= 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE c1 >= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE c1 <= 100 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE c1 <= 100")

        # test logging of slow queries with restriction in both partition and first clustering key prefix
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k = 0 AND c1 > 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0 AND c1 > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k = 0 AND c1 < 100",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0 AND c1 < 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k = 0 AND c1 >= 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0 AND c1 >= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k = 0 AND c1 <= 100",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0 AND c1 <= 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k IN (0) AND c1 > 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0 AND c1 > 0")

        # test logging of slow queries with restriction in both partition and second clustering key prefix
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k = 0 AND c1 = 0 AND c2 > 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0 AND c1 = 0 AND c2 > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k = 0 AND c1 = 0 AND c2 < 100",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0 AND c1 = 0 AND c2 < 100")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k = 0 AND c1 = 0 AND c2 >= 0",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0 AND c1 = 0 AND c2 >= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE k = 0 AND c1 = 0 AND c2 <= 100",
                                      logged_query="SELECT \* FROM ks.{} WHERE k = 0 AND c1 = 0 AND c2 <= 100")

        # test logging of slow queries with restriciton on regular column
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v = 1 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v = 1")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v > 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v < 2 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v < 2")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v >= 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v >= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE v <= 2 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE v <= 2")

        # test logging of slow queries with restriciton on static column
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE s = 1 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE s = 1")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE s > 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE s > 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE s < 2 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE s < 2")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE s >= 0 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE s >= 0")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT * FROM {} WHERE s <= 2 ALLOW FILTERING",
                                      logged_query="SELECT \* FROM ks.{} WHERE s <= 2")

        # test logging of slow queries with column selections
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT s FROM {}",
                                      logged_query="SELECT s FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT v FROM {}",
                                      logged_query="SELECT v FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT lt FROM {}",
                                      logged_query="SELECT lt FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT st FROM {}",
                                      logged_query="SELECT st FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT mp FROM {}",
                                      logged_query="SELECT mp FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT \"Escaped_Name\" FROM {}",
                                      logged_query="SELECT \"Escaped_Name\" FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT v,s,st FROM {}",
                                      logged_query="SELECT s, st, v FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT k,v,s,st FROM {}",
                                      logged_query="SELECT s, st, v FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT k,c1,v,st FROM {}",
                                      logged_query="SELECT st, v FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT k,c2,v,st FROM {}",
                                      logged_query="SELECT st, v FROM ks.{}")
        TestCQLSlowQuery._assert_logs(node, session, table,
                                      query="SELECT k,c1,c2,v,st FROM {}",
                                      logged_query="SELECT st, v FROM ks.{}")

        # test logging of slow queries with primary key-only column selections
        logged_query = "SELECT \* FROM ks.{}"
        TestCQLSlowQuery._assert_logs(node, session, table, query="SELECT k FROM {}", logged_query=logged_query)
        TestCQLSlowQuery._assert_logs(node, session, table, query="SELECT c1 FROM {}", logged_query=logged_query)
        TestCQLSlowQuery._assert_logs(node, session, table, query="SELECT c2 FROM {}", logged_query=logged_query)
        TestCQLSlowQuery._assert_logs(node, session, table, query="SELECT k,c1 FROM {}", logged_query=logged_query)
        TestCQLSlowQuery._assert_logs(node, session, table, query="SELECT k,c1,c2 FROM {}", logged_query=logged_query)

        # test logging of slow queries with column sub-selections (only supported since 4.0)
        if node.cluster.version() >= '4.0':
            TestCQLSlowQuery._assert_logs(node, session, table,
                                          query="SELECT st[0] FROM {}",
                                          logged_query="SELECT st\[0\] FROM ks.{}")
            TestCQLSlowQuery._assert_logs(node, session, table,
                                          query="SELECT mp[0] FROM {}",
                                          logged_query="SELECT mp\[0\] FROM ks.{}")
            TestCQLSlowQuery._assert_logs(node, session, table,
                                          query="SELECT k,c1,v,st[0],mp[1] FROM {}",
                                          logged_query="SELECT mp\[1\], st\[0\], v FROM ks.{}")

    @staticmethod
    def _assert_logs(node, session, table, query, logged_query):

        # only check debug logs because at INFO level the no-spam logger has unpredictable results
        mark = node.mark_log(filename='debug.log')

        session.execute(SimpleStatement(query.format(table),
                                        consistency_level=ConsistencyLevel.ONE,
                                        retry_policy=FallthroughRetryPolicy()))

        node.watch_log_for(["operations were slow", logged_query.format(table)],
                           from_mark=mark, filename='debug.log', timeout=60)

    @staticmethod
    def _check_logs(node, pattern, filename, num_expected):
        ret = node.grep_log(pattern, filename=filename)
        assert_length_equal(ret, num_expected)


class TestLWTWithCQL(Tester):
    """
    Validate CQL queries for LWTs for static columns for null and non-existing rows
    @jira_ticket CASSANDRA-9842
    """

    @pytest.fixture(scope='function', autouse=True)
    def fixture_post_initialize_cluster(self, fixture_dtest_setup):
        cluster = fixture_dtest_setup.cluster
        cluster.populate(3)
        cluster.start()

    def get_lwttester_session(self):
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        session.execute("""CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION={'class':'SimpleStrategy',
            'replication_factor':1}""")
        session.execute("USE ks")
        return session

    def test_lwt_with_static_columns(self):
        session = self.get_lwttester_session()

        session.execute("""
            CREATE TABLE lwt_with_static (a int, b int, s int static, d text, PRIMARY KEY (a, b))
        """)

        assert_one(session, "UPDATE lwt_with_static SET s = 1 WHERE a = 1 IF s = NULL", [True])

        assert_one(session, "SELECT * FROM lwt_with_static", [1, None, 1, None])

        assert_one(session, "UPDATE lwt_with_static SET s = 2 WHERE a = 2 IF EXISTS", [False])

        assert_one(session, "SELECT * FROM lwt_with_static WHERE a = 1", [1, None, 1, None])

        assert_one(session, "INSERT INTO lwt_with_static (a, s) VALUES (2, 2) IF NOT EXISTS", [True])

        assert_one(session, "SELECT * FROM lwt_with_static WHERE a = 2", [2, None, 2, None])

        assert_one(session, "BEGIN BATCH\n" +
                   "INSERT INTO lwt_with_static (a, b, d) values (3, 3, 'a');\n" +
                   "UPDATE lwt_with_static SET s = 3 WHERE a = 3 IF s = null;\n" +
                   "APPLY BATCH;", [True])

        assert_one(session, "SELECT * FROM lwt_with_static WHERE a = 3", [3, 3, 3, "a"])

        # LWT applies before INSERT
        assert_one(session, "BEGIN BATCH\n" +
                   "INSERT INTO lwt_with_static (a, b, d) values (4, 4, 'a');\n" +
                   "UPDATE lwt_with_static SET s = 4 WHERE a = 4 IF s = null;\n" +
                   "APPLY BATCH;", [True])

        assert_one(session, "SELECT * FROM lwt_with_static WHERE a = 4", [4, 4, 4, "a"])

    def _validate_non_existing_or_null_values(self, table_name, session):
        assert_one(session, "UPDATE {} SET s = 1 WHERE a = 1 IF s = NULL".format(table_name), [True])

        assert_one(session, "SELECT a, s, d FROM {} WHERE a = 1".format(table_name), [1, 1, None])

        assert_one(session, "UPDATE {} SET s = 2 WHERE a = 2 IF s IN (10,20,NULL)".format(table_name), [True])

        assert_one(session, "SELECT a, s, d FROM {} WHERE a = 2".format(table_name), [2, 2, None])

        assert_one(session, "UPDATE {} SET s = 4 WHERE a = 4 IF s != 4".format(table_name), [True])

        assert_one(session, "SELECT a, s, d FROM {} WHERE a = 4".format(table_name), [4, 4, None])

    def _is_new_lwt_format_version(self, version):
        return version > LooseVersion('3.9') or (version > LooseVersion('3.0.9') and version < LooseVersion('3.1'))

    @flaky
    def test_conditional_updates_on_static_columns_with_null_values(self):
        session = self.get_lwttester_session()

        table_name = "conditional_updates_on_static_columns_with_null"
        session.execute("""
            CREATE TABLE {} (a int, b int, s int static, d text, PRIMARY KEY (a, b))
        """.format(table_name))

        for i in range(1, 6):
            session.execute("INSERT INTO {} (a, b) VALUES ({}, {})".format(table_name, i, i))

        self._validate_non_existing_or_null_values(table_name, session)

        assert_one(session, "UPDATE {} SET s = 30 WHERE a = 3 IF s IN (10,20,30)".format(table_name),
                   [False, None] if self._is_new_lwt_format_version(self.cluster.version()) else [False])

        assert_one(session, "SELECT * FROM {} WHERE a = 3".format(table_name), [3, 3, None, None])

        for operator in [">", "<", ">=", "<=", "="]:
            assert_one(session, "UPDATE {} SET s = 50 WHERE a = 5 IF s {} 3".format(table_name, operator),
                       [False, None] if self._is_new_lwt_format_version(self.cluster.version()) else [False])

            assert_one(session, "SELECT * FROM {} WHERE a = 5".format(table_name), [5, 5, None, None])

    def test_conditional_updates_on_static_columns_with_non_existing_values(self):
        session = self.get_lwttester_session()

        table_name = "conditional_updates_on_static_columns_with_ne"
        session.execute("""
            CREATE TABLE {} (a int, b int, s int static, d text, PRIMARY KEY (a, b))
        """.format(table_name))

        self._validate_non_existing_or_null_values(table_name, session)

        assert_one(session, "UPDATE {} SET s = 30 WHERE a = 3 IF s IN (10,20,30)".format(table_name), [False])

        assert_none(session, "SELECT * FROM {} WHERE a = 3".format(table_name))

        for operator in [">", "<", ">=", "<=", "="]:
            assert_one(session, "UPDATE {} SET s = 50 WHERE a = 5 IF s {} 3".format(table_name, operator), [False])

            assert_none(session, "SELECT * FROM {} WHERE a = 5".format(table_name))

    def _validate_non_existing_or_null_values_batch(self, table_name, session):
        assert_one(session, """
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, d) values (2, 2, 'a');
                UPDATE {table_name} SET s = 2 WHERE a = 2 IF s = null;
            APPLY BATCH""".format(table_name=table_name), [True])

        assert_one(session, "SELECT * FROM {table_name} WHERE a = 2".format(table_name=table_name), [2, 2, 2, "a"])

        assert_one(session, """
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s, d) values (4, 4, 4, 'a')
                UPDATE {table_name} SET s = 5 WHERE a = 4 IF s = null;
            APPLY BATCH""".format(table_name=table_name), [True])

        assert_one(session, "SELECT * FROM {table_name} WHERE a = 4".format(table_name=table_name), [4, 4, 5, "a"])

        assert_one(session, """
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s, d) values (5, 5, 5, 'a')
                UPDATE {table_name} SET s = 6 WHERE a = 5 IF s IN (1,2,null)
            APPLY BATCH""".format(table_name=table_name), [True])

        assert_one(session, "SELECT * FROM {table_name} WHERE a = 5".format(table_name=table_name), [5, 5, 6, "a"])

        assert_one(session, """
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s, d) values (7, 7, 7, 'a')
                UPDATE {table_name} SET s = 8 WHERE a = 7 IF s != 7;
            APPLY BATCH""".format(table_name=table_name), [True])

        assert_one(session, "SELECT * FROM {table_name} WHERE a = 7".format(table_name=table_name), [7, 7, 8, "a"])

    def test_conditional_updates_on_static_columns_with_null_values_batch(self):
        session = self.get_lwttester_session()

        table_name = "lwt_on_static_columns_with_null_batch"
        session.execute("""
            CREATE TABLE {table_name} (a int, b int, s int static, d text, PRIMARY KEY (a, b))
        """.format(table_name=table_name))

        for i in range(1, 7):
            session.execute("INSERT INTO {table_name} (a, b) VALUES ({i}, {i})".format(table_name=table_name, i=i))

        self._validate_non_existing_or_null_values_batch(table_name, session)

        for operator in [">", "<", ">=", "<=", "="]:
            assert_one(session, """
                BEGIN BATCH
                    INSERT INTO {table_name} (a, b, s, d) values (3, 3, 40, 'a')
                    UPDATE {table_name} SET s = 30 WHERE a = 3 IF s {operator} 5;
                APPLY BATCH""".format(table_name=table_name, operator=operator),
                       [False, 3, 3, None] if self._is_new_lwt_format_version(self.cluster.version()) else [False])

            assert_one(session, "SELECT * FROM {table_name} WHERE a = 3".format(table_name=table_name), [3, 3, None, None])

        assert_one(session, """
                BEGIN BATCH
                    INSERT INTO {table_name} (a, b, s, d) values (6, 6, 70, 'a')
                    UPDATE {table_name} SET s = 60 WHERE a = 6 IF s IN (1,2,3)
                APPLY BATCH""".format(table_name=table_name),
                   [False, 6, 6, None] if self._is_new_lwt_format_version(self.cluster.version()) else [False])

        assert_one(session, "SELECT * FROM {table_name} WHERE a = 6".format(table_name=table_name), [6, 6, None, None])

    def test_conditional_deletes_on_static_columns_with_null_values(self):
        session = self.get_lwttester_session()

        table_name = "conditional_deletes_on_static_with_null"
        session.execute("""
            CREATE TABLE {} (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))
        """.format(table_name))

        for i in range(1, 6):
            session.execute("INSERT INTO {} (a, b, s1, s2, v) VALUES ({}, {}, {}, null, {})".format(table_name, i, i, i, i))

        assert_one(session, "DELETE s1 FROM {} WHERE a = 1 IF s2 = null".format(table_name), [True])

        assert_one(session, "SELECT * FROM {} WHERE a = 1".format(table_name), [1, 1, None, None, 1])

        assert_one(session, "DELETE s1 FROM {} WHERE a = 2 IF s2 IN (10,20,30)".format(table_name), [False, None])

        assert_one(session, "SELECT * FROM {} WHERE a = 2".format(table_name), [2, 2, 2, None, 2])

        assert_one(session, "DELETE s1 FROM {} WHERE a = 3 IF s2 IN (null,20,30)".format(table_name), [True])

        assert_one(session, "SELECT * FROM {} WHERE a = 3".format(table_name), [3, 3, None, None, 3])

        assert_one(session, "DELETE s1 FROM {} WHERE a = 4 IF s2 != 4".format(table_name), [True])

        assert_one(session, "SELECT * FROM {} WHERE a = 4".format(table_name), [4, 4, None, None, 4])

        for operator in [">", "<", ">=", "<=", "="]:
            assert_one(session, "DELETE s1 FROM {} WHERE a = 5 IF s2 {} 3".format(table_name, operator), [False, None])
            assert_one(session, "SELECT * FROM {} WHERE a = 5".format(table_name), [5, 5, 5, None, 5])

    def test_conditional_deletes_on_static_columns_with_null_values_batch(self):
        session = self.get_lwttester_session()

        table_name = "conditional_deletes_on_static_with_null_batch"
        session.execute("""
            CREATE TABLE {} (a int, b int, s1 int static, s2 int static, v int, PRIMARY KEY (a, b))
        """.format(table_name))

        assert_one(session, """
             BEGIN BATCH
                 INSERT INTO {table_name} (a, b, s1, v) values (2, 2, 2, 2);
                 DELETE s1 FROM {table_name} WHERE a = 2 IF s2 = null;
             APPLY BATCH""".format(table_name=table_name), [True])

        assert_one(session, "SELECT * FROM {} WHERE a = 2".format(table_name), [2, 2, None, None, 2])

        for operator in [">", "<", ">=", "<=", "="]:
            assert_one(session, """
                BEGIN BATCH
                    INSERT INTO {table_name} (a, b, s1, v) values (3, 3, 3, 3);
                    DELETE s1 FROM {table_name} WHERE a = 3 IF s2 {operator} 5;
                APPLY BATCH""".format(table_name=table_name, operator=operator), [False])

            assert_none(session, "SELECT * FROM {} WHERE a = 3".format(table_name))

        assert_one(session, """
             BEGIN BATCH
                 INSERT INTO {table_name} (a, b, s1, v) values (6, 6, 6, 6);
                 DELETE s1 FROM {table_name} WHERE a = 6 IF s2 IN (1,2,3);
             APPLY BATCH""".format(table_name=table_name), [False])

        assert_none(session, "SELECT * FROM {} WHERE a = 6".format(table_name))

        assert_one(session, """
             BEGIN BATCH
                 INSERT INTO {table_name} (a, b, s1, v) values (4, 4, 4, 4);
                 DELETE s1 FROM {table_name} WHERE a = 4 IF s2 = null;
             APPLY BATCH""".format(table_name=table_name), [True])

        assert_one(session, "SELECT * FROM {} WHERE a = 4".format(table_name), [4, 4, None, None, 4])

        assert_one(session, """
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s1, v) VALUES (5, 5, 5, 5);
                DELETE s1 FROM {table_name} WHERE a = 5 IF s1 IN (1,2,null);
            APPLY BATCH""".format(table_name=table_name), [True])

        assert_one(session, "SELECT * FROM {} WHERE a = 5".format(table_name), [5, 5, None, None, 5])

        assert_one(session, """
            BEGIN BATCH
                INSERT INTO {table_name} (a, b, s1, v) values (7, 7, 7, 7);
                DELETE s1 FROM {table_name} WHERE a = 7 IF s2 != 7;
            APPLY BATCH""".format(table_name=table_name), [True])

        assert_one(session, "SELECT * FROM {} WHERE a = 7".format(table_name), [7, 7, None, None, 7])

    def lwt_with_empty_resultset(self):
        """
        LWT with unset row.
        @jira_ticket CASSANDRA-12694
        """
        session = self.get_lwttester_session()

        session.execute("""
            CREATE TABLE test (pk text, v1 int, v2 text, PRIMARY KEY (pk));
        """)
        session.execute("update test set v1 = 100 where pk = 'test1';")
        node1 = self.cluster.nodelist()[0]
        self.cluster.flush()
        assert_one(session, "UPDATE test SET v1 = 100 WHERE pk = 'test1' IF v2 = null;", [True])
