import uuid

from cassandra import ConsistencyLevel, WriteFailure, WriteTimeout

from distutils.version import LooseVersion
from dtest import Tester
from thrift_bindings.v22 import ttypes as thrift_types
from thrift_tests import get_thrift_client
from tools.decorators import known_failure, since

KEYSPACE = "foo"


# These tests use the cassandra.test.fail_writes_ks option, which was only
# implemented in 2.2, so we skip it before then.
@since('2.2')
class TestWriteFailures(Tester):
    """
    Tests for write failures in the replicas,
    @jira_ticket CASSANDRA-8592.

    They require CURRENT_VERSION = VERSION_4 in CassandraDaemon.Server
    otherwise these tests will fail.
    """

    def setUp(self):
        super(TestWriteFailures, self).setUp()

        self.ignore_log_patterns = [
            "Testing write failures",  # The error to simulate a write failure
            "ERROR WRITE_FAILURE",     # Logged in DEBUG mode for write failures
            "MigrationStage"           # This occurs sometimes due to node down (because of restart)
        ]

        self.supports_v5_protocol = self.cluster.version() >= LooseVersion('3.10')
        self.expected_expt = WriteFailure
        self.protocol_version = 5 if self.supports_v5_protocol else 4
        self.replication_factor = 3
        self.consistency_level = ConsistencyLevel.ALL
        self.failing_nodes = [1, 2]

    def tearDown(self):
        super(TestWriteFailures, self).tearDown()

    def _prepare_cluster(self, start_rpc=False):
        self.cluster.populate(3)

        if start_rpc:
            self.cluster.set_configuration_options(values={'start_rpc': True})

        self.cluster.start(wait_for_binary_proto=True)
        self.nodes = self.cluster.nodes.values()

        session = self.patient_exclusive_cql_connection(self.nodes[0], protocol_version=self.protocol_version)

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '%s' }
            """ % (KEYSPACE, self.replication_factor))
        session.set_keyspace(KEYSPACE)

        session.execute("CREATE TABLE IF NOT EXISTS mytable (key text PRIMARY KEY, value text) WITH COMPACT STORAGE")
        session.execute("CREATE TABLE IF NOT EXISTS countertable (key uuid PRIMARY KEY, value counter)")

        for idx in self.failing_nodes:
            node = self.nodes[idx]
            node.stop()
            node.start(wait_for_binary_proto=True, jvm_args=["-Dcassandra.test.fail_writes_ks=" + KEYSPACE])

            if idx == 0:
                session = self.patient_exclusive_cql_connection(node, protocol_version=self.protocol_version)
                session.set_keyspace(KEYSPACE)

        return session

    def _perform_cql_statement(self, text):
        session = self._prepare_cluster()

        statement = session.prepare(text)
        statement.consistency_level = self.consistency_level

        if self.expected_expt is None:
            session.execute(statement)
        else:
            with self.assertRaises(self.expected_expt) as cm:
                session.execute(statement)
            return cm.exception

    def _assert_error_code_map_exists_with_code(self, exception, expected_code):
        """
        Asserts that the given exception contains an error code map
        where at least one node responded with some expected code.
        This is meant for testing failure exceptions on protocol v5.
        """
        self.assertIsNotNone(exception)
        self.assertIsNotNone(exception.error_code_map)
        expected_code_found = False
        for error_code in exception.error_code_map.values():
            if error_code == expected_code:
                expected_code_found = True
                break
        self.assertTrue(expected_code_found, "The error code map did not contain " + str(expected_code))

    @since('2.2', max_version='2.2.x')
    def test_mutation_v2(self):
        """
        A failed mutation at v2 receives a WriteTimeout
        """
        self.expected_expt = WriteTimeout
        self.protocol_version = 2
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12213',
                   flaky=True)
    def test_mutation_v3(self):
        """
        A failed mutation at v3 receives a WriteTimeout
        """
        self.expected_expt = WriteTimeout
        self.protocol_version = 3
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    def test_mutation_v4(self):
        """
        A failed mutation at v4 receives a WriteFailure
        """
        self.expected_expt = WriteFailure
        self.protocol_version = 4
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    @since('3.10')
    def test_mutation_v5(self):
        """
        A failed mutation at v5 receives a WriteFailure with an error code map containing error code 0x0000
        """
        self.expected_expt = WriteFailure
        self.protocol_version = 5
        exc = self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")
        self._assert_error_code_map_exists_with_code(exc, 0x0000)

    def test_mutation_any(self):
        """
        A WriteFailure is not received at consistency level ANY
        even if all nodes fail because of hinting
        """
        self.consistency_level = ConsistencyLevel.ANY
        self.expected_expt = None
        self.failing_nodes = [0, 1, 2]
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    def test_mutation_one(self):
        """
            A WriteFailure is received at consistency level ONE
            if all nodes fail
        """
        self.consistency_level = ConsistencyLevel.ONE
        self.failing_nodes = [0, 1, 2]
        exc = self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")
        if self.supports_v5_protocol:
            self._assert_error_code_map_exists_with_code(exc, 0x0000)

    def test_mutation_quorum(self):
        """
        A WriteFailure is not received at consistency level
        QUORUM if quorum succeeds
        """
        self.consistency_level = ConsistencyLevel.QUORUM
        self.expected_expt = None
        self.failing_nodes = [2]
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

    def test_batch(self):
        """
        A failed batch receives a WriteFailure
        """
        exc = self._perform_cql_statement("""
            BEGIN BATCH
            INSERT INTO mytable (key, value) VALUES ('key2', 'Value 2') USING TIMESTAMP 1111111111111111
            INSERT INTO mytable (key, value) VALUES ('key3', 'Value 3') USING TIMESTAMP 1111111111111112
            APPLY BATCH
        """)
        if self.supports_v5_protocol:
            self._assert_error_code_map_exists_with_code(exc, 0x0000)

    def test_counter(self):
        """
        A failed counter mutation receives a WriteFailure
        """
        _id = str(uuid.uuid4())
        exc = self._perform_cql_statement("""
            UPDATE countertable
                SET value = value + 1
                where key = {uuid}
        """.format(uuid=_id))
        if self.supports_v5_protocol:
            self._assert_error_code_map_exists_with_code(exc, 0x0000)

    def test_paxos(self):
        """
        A light transaction receives a WriteFailure
        """
        exc = self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1') IF NOT EXISTS")
        if self.supports_v5_protocol:
            self._assert_error_code_map_exists_with_code(exc, 0x0000)

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12213',
                   flaky=True)
    def test_paxos_any(self):
        """
        A light transaction at consistency level ANY does not receive a WriteFailure
        """
        self.consistency_level = ConsistencyLevel.ANY
        self.expected_expt = None
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1') IF NOT EXISTS")

    def test_thrift(self):
        """
        A thrift client receives a TimedOutException
        """
        self._prepare_cluster(start_rpc=True)
        self.expected_expt = thrift_types.TimedOutException

        client = get_thrift_client()
        client.transport.open()
        client.set_keyspace(KEYSPACE)

        with self.assertRaises(self.expected_expt):
            client.insert('key1',
                          thrift_types.ColumnParent('mytable'),
                          thrift_types.Column('value', 'Value 1', 0),
                          thrift_types.ConsistencyLevel.ALL)

        client.transport.close()
