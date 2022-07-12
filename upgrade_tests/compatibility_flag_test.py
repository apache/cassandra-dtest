import pytest
import logging

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from dtest import Tester
from tools.assertions import assert_all

since = pytest.mark.since
logger = logging.getLogger(__name__)


@pytest.mark.upgrade_test
class TestCompatibilityFlag(Tester):
    """
    Test 30 protocol compatibility flag

    @jira CASSANDRA-13004
    """

    def _compatibility_flag_off_with_30_node_test(self, from_version):
        """
        Test compatibility with 30 protocol version: if the flag is unset, schema agreement can not be reached
        """

        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()
        cluster.set_install_dir(version=from_version)
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()
        cluster.start()

        node1.drain()
        node1.watch_log_for("DRAINED")
        node1.stop(wait_other_notice=False)
        logger.debug("Upgrading to current version")
        self.set_node_to_current_version(node1)
        node1.start(wait_for_binary_proto=True)

        node1.watch_log_for("Not pulling schema because versions match or shouldPullSchemaFrom returned false", filename='debug.log')
        node2.watch_log_for("Not pulling schema because versions match or shouldPullSchemaFrom returned false", filename='debug.log')

    def _compatibility_flag_on_with_30_test(self, from_version):
        """
        Test compatibility with 30 protocol version: if the flag is set, schema agreement can be reached
        """

        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()
        cluster.set_install_dir(version=from_version)
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()
        cluster.start()

        node1.drain()
        node1.watch_log_for("DRAINED")
        node1.stop(wait_other_notice=False)
        logger.debug("Upgrading to current version")
        self.set_node_to_current_version(node1)
        node1.start(jvm_args=["-Dcassandra.force_3_0_protocol_version=true"], wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)
        self._run_test(session)

    def test__compatibility_flag_on_3014(self):
        """
        Test compatibility between post-13004 nodes, one of which is in compatibility mode
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True)
        node2.start(jvm_args=["-Dcassandra.force_3_0_protocol_version=true"], wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)
        self._run_test(session)

    def test__compatibility_flag_off_3014(self):
        """
        Test compatibility between post-13004 nodes
        """
        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        node1.start(wait_for_binary_proto=True)
        node2.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node1)
        self._run_test(session)

    def _run_test(self, session):
        # Make sure the system_auth table will get replicated to the node that we're going to replace

        session.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'} ;")
        session.execute("CREATE TABLE test.test (a text PRIMARY KEY, b text, c text);")
        session.cluster.control_connection.wait_for_schema_agreement()

        for i in range(1, 6):
            session.execute(SimpleStatement("INSERT INTO test.test (a, b, c) VALUES ('{}', '{}', '{}');".format(i, i + 1, i + 2),
                                            consistency_level=ConsistencyLevel.ALL))

        assert_all(session,
                   "SELECT * FROM test.test",
                   [[str(i), str(i + 1), str(i + 2)] for i in range(1, 6)], ignore_order=True,
                   cl=ConsistencyLevel.ALL)

        assert_all(session,
                   "SELECT a,c FROM test.test",
                   [[str(i), str(i + 2)] for i in range(1, 6)], ignore_order=True,
                   cl=ConsistencyLevel.ALL)


@since('3.0.14', max_version='3.0.x')
class CompatibilityFlag30XTest(TestCompatibilityFlag):

    def test_compatibility_flag_off_with_30_node(self):
        self._compatibility_flag_off_with_30_node_test('3.0.12')

    def test_compatibility_flag_on_with_3_0(self):
        self._compatibility_flag_on_with_30_test('3.0.12')

    def test_compatibility_flag_on_3014(self):
        self._compatibility_flag_on_3014_test()

    def test_compatibility_flag_off_3014(self):
        self._compatibility_flag_off_3014_test()


@since('3.11', max_version='4')
class CompatibilityFlag3XTest(TestCompatibilityFlag):

    def test_compatibility_flag_off_with_30_node(self):
        self._compatibility_flag_off_with_30_node_test('3.10')

    def test_compatibility_flag_on_with_3_0(self):
        self._compatibility_flag_on_with_30_test('3.10')

    def test_compatibility_flag_on_3014(self):
        self._compatibility_flag_on_3014_test()

    def test_compatibility_flag_off_3014(self):
        self._compatibility_flag_off_3014_test()
