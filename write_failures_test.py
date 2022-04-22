import uuid
import pytest
import logging

from cassandra import ConsistencyLevel, WriteFailure, WriteTimeout
from cassandra.query import SimpleStatement

from dtest import Tester
from thrift_bindings.thrift010 import ttypes as thrift_types
from thrift_test import get_thrift_client
from tools.jmxutils import (JolokiaAgent, make_mbean)

since = pytest.mark.since
logger = logging.getLogger(__name__)

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
    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            "Testing write failures",  # The error to simulate a write failure
            "ERROR WRITE_FAILURE",     # Logged in DEBUG mode for write failures
            "MigrationStage"           # This occurs sometimes due to node down (because of restart)
        )

    @pytest.fixture(scope="function", autouse=True)
    def fixture_set_test_defauls(self, fixture_dtest_setup):
        self.supports_v5_protocol = fixture_dtest_setup.supports_v5_protocol(fixture_dtest_setup.cluster.version())
        self.expected_expt = WriteFailure
        self.protocol_version = 5 if self.supports_v5_protocol else 4
        self.replication_factor = 3
        self.consistency_level = ConsistencyLevel.ALL
        self.failing_nodes = [1, 2]

    def _prepare_cluster(self, start_rpc=False, compact_storage=False):
        self.cluster.populate(3)

        if start_rpc:
            self.cluster.set_configuration_options(values={'start_rpc': True})

        self.cluster.start()
        self.nodes = list(self.cluster.nodes.values())

        session = self.patient_exclusive_cql_connection(self.nodes[0], protocol_version=self.protocol_version)

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '%s' }
            """ % (KEYSPACE, self.replication_factor))
        session.set_keyspace(KEYSPACE)

        session.execute("CREATE TABLE IF NOT EXISTS mytable (key text PRIMARY KEY, value text) %s"%("WITH COMPACT STORAGE" if compact_storage else ''))
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
            with pytest.raises(self.expected_expt) as cm:
                session.execute(statement)
            return cm._excinfo[1]

    def _assert_error_code_map_exists_with_code(self, exception, expected_code):
        """
        Asserts that the given exception contains an error code map
        where at least one node responded with some expected code.
        This is meant for testing failure exceptions on protocol v5.
        """
        assert exception is not None
        assert exception.error_code_map is not None
        expected_code_found = False
        for error_code in list(exception.error_code_map.values()):
            if error_code == expected_code:
                expected_code_found = True
                break
        assert expected_code_found, "The error code map did not contain " + str(expected_code)

    @since('2.2', max_version='2.2.x')
    def test_mutation_v2(self):
        """
        A failed mutation at v2 receives a WriteTimeout
        """
        self.expected_expt = WriteTimeout
        self.protocol_version = 2
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1')")

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

    @since('4.0')
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

    def test_paxos_any(self):
        """
        A light transaction at consistency level ANY does not receive a WriteFailure
        """
        self.consistency_level = ConsistencyLevel.ANY
        self.expected_expt = None
        self._perform_cql_statement("INSERT INTO mytable (key, value) VALUES ('key1', 'Value 1') IF NOT EXISTS")

    @since('2.0', max_version='4')
    def test_thrift(self):
        """
        A thrift client receives a TimedOutException
        """
        self._prepare_cluster(start_rpc=True, compact_storage=True)
        self.expected_expt = thrift_types.TimedOutException

        client = get_thrift_client()
        client.transport.open()
        client.set_keyspace(KEYSPACE)

        with pytest.raises(self.expected_expt):
            client.insert('key1'.encode(),
                          thrift_types.ColumnParent('mytable'),
                          thrift_types.Column('value'.encode(), 'Value 1'.encode(), 0),
                          thrift_types.ConsistencyLevel.ALL)

        client.transport.close()


def assert_write_failure(session, query, consistency_level):
    statement = SimpleStatement(query, consistency_level=consistency_level)
    with pytest.raises(WriteFailure):
        session.execute(statement)


@since('3.0', max_version='4.0.x')
class TestMultiDCWriteFailures(Tester):
    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            "is too large for the maximum size of",  # 3.0+
            "Encountered an oversized mutation",     # 4.0+
            "ERROR WRITE_FAILURE",     # Logged in DEBUG mode for write failures
            "MigrationStage"           # This occurs sometimes due to node down (because of restart)
        )

    def test_oversized_mutation(self):
        """
        Test that multi-DC write failures return operation failed rather than a timeout.
        @jira_ticket CASSANDRA-16334.
        """

        cluster = self.cluster
        cluster.populate([2, 2])
        cluster.set_configuration_options(values={'max_mutation_size_in_kb': 128})
        cluster.start()

        node1 = cluster.nodelist()[0]
        session = self.patient_exclusive_cql_connection(node1)

        session.execute("CREATE KEYSPACE k WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 2}")
        session.execute("CREATE TABLE k.t (key int PRIMARY KEY, val blob)")

        payload = '1' * 1024 * 256
        query = "INSERT INTO k.t (key, val) VALUES (1, textAsBlob('{}'))".format(payload)

        assert_write_failure(session, query, ConsistencyLevel.LOCAL_ONE)
        assert_write_failure(session, query, ConsistencyLevel.ONE)

        # verify that no hints are created
        with JolokiaAgent(node1) as jmx:
            assert 0 == jmx.read_attribute(make_mbean('metrics', type='Storage', name='TotalHints'), 'Count')
