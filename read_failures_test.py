from cassandra import ConsistencyLevel, ReadFailure, ReadTimeout
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

from dtest import Tester
from tools.decorators import known_failure, since

KEYSPACE = "readfailures"


class TestReadFailures(Tester):
    """
    Tests for read failures in the replicas, introduced as a part of
    @jira_ticket CASSANDRA-12311.
    """

    def setUp(self):
        super(TestReadFailures, self).setUp()

        self.ignore_log_patterns = [
            "Scanned over [1-9][0-9]* tombstones"  # This is expected when testing read failures due to tombstones
        ]

        self.tombstone_failure_threshold = 500
        self.replication_factor = 3
        self.consistency_level = ConsistencyLevel.ALL
        self.expected_expt = ReadFailure

    def tearDown(self):
        super(TestReadFailures, self).tearDown()

    def _prepare_cluster(self):
        self.cluster.set_configuration_options(
            values={'tombstone_failure_threshold': self.tombstone_failure_threshold}
        )
        self.cluster.populate(3)
        self.cluster.start(wait_for_binary_proto=True)
        self.nodes = self.cluster.nodes.values()

        session = self.patient_exclusive_cql_connection(self.nodes[0], protocol_version=self.protocol_version)

        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '%s' }
            """ % (KEYSPACE, self.replication_factor))
        session.set_keyspace(KEYSPACE)
        session.execute("CREATE TABLE IF NOT EXISTS tombstonefailure (id int PRIMARY KEY, value text)")

        return session

    def _insert_tombstones(self, session, number_of_tombstones):
        for num_id in range(number_of_tombstones):
            session.execute(SimpleStatement("DELETE value FROM tombstonefailure WHERE id = {}".format(num_id),
                                            consistency_level=self.consistency_level, retry_policy=FallthroughRetryPolicy()))

    def _perform_cql_statement(self, session, text_statement):
        statement = session.prepare(text_statement)
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

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12531',
                   flaky=False)
    @since('2.1')
    def test_tombstone_failure_v3(self):
        """
        A failed read due to tombstones at v3 should result in a ReadTimeout
        """
        self.protocol_version = 3
        self.expected_expt = ReadTimeout
        session = self._prepare_cluster()
        self._insert_tombstones(session, 600)
        self._perform_cql_statement(session, "SELECT value FROM tombstonefailure")

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12531',
                   flaky=False)
    @since('2.2')
    def test_tombstone_failure_v4(self):
        """
        A failed read due to tombstones at v4 should result in a ReadFailure
        """
        self.protocol_version = 4
        session = self._prepare_cluster()
        self._insert_tombstones(session, 600)
        self._perform_cql_statement(session, "SELECT value FROM tombstonefailure")

    @since('3.10')
    def test_tombstone_failure_v5(self):
        """
        A failed read due to tombstones at v5 should result in a ReadFailure with
        an error code map containing error code 0x0001 (indicating that the replica(s)
        read too many tombstones)
        """
        self.protocol_version = 5
        session = self._prepare_cluster()
        self._insert_tombstones(session, 600)
        exc = self._perform_cql_statement(session, "SELECT value FROM tombstonefailure")
        self._assert_error_code_map_exists_with_code(exc, 0x0001)
