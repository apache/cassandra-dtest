import time
import pytest
import logging

from cassandra.query import dict_factory
from ccmlib.node import NodeError

from dtest import Tester
from cassandra.protocol import SyntaxException, ConfigurationException

since = pytest.mark.since
logger = logging.getLogger(__name__)

VERSION_311 = 'github:apache/cassandra-3.11'
VERSION_TRUNK = 'github:apache/trunk'


@pytest.mark.upgrade_test
@since('4.0')
class TestUpgradeSuperColumnsThrough(Tester):
    def upgrade_to_version(self, tag, start_rpc=True, wait=True, nodes=None):
        logger.debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            logger.debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            logger.debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()

        # Restart nodes on new version
        for node in nodes:
            logger.debug('Starting %s on new version (%s)' % (node.name, tag))
            node.start(wait_other_notice=wait, wait_for_binary_proto=wait)

    def prepare(self, num_nodes=1, cassandra_version="github:apache/cassandra-2.2"):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version=cassandra_version)
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()

        cluster.populate(num_nodes)

        cluster.start()
        return cluster

    def test_mixed_cluster(self):
        cluster = self.prepare(num_nodes=2, cassandra_version=VERSION_311)
        node1, node2 = self.cluster.nodelist()

        node1.drain()
        node1.watch_log_for("DRAINED")
        node1.stop(wait_other_notice=False)
        node1.set_install_dir(version=VERSION_TRUNK)
        node1.start(wait_for_binary_proto=True)

        session = self.patient_cql_connection(node2, row_factory=dict_factory)

        # Schema propagation will time out
        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '2' };")
        thrown = False
        try:
            session.execute("CREATE TABLE ks.compact_table (pk int PRIMARY KEY, col1 int, col2 int) WITH COMPACT STORAGE")
        except (ConfigurationException, SyntaxException):
            thrown = True

        assert thrown

    def test_upgrade_with_dropped_compact_storage(self):
        cluster = self.prepare(cassandra_version=VERSION_311)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node, row_factory=dict_factory)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '1' };")
        session.execute("CREATE TABLE ks.compact_table (pk int PRIMARY KEY, col1 int, col2 int) WITH COMPACT STORAGE")

        for i in range(1, 5):
            session.execute("INSERT INTO ks.compact_table (pk, col1, col2) VALUES ({i}, {i}, {i})".format(i=i))

        session.execute("ALTER TABLE ks.compact_table DROP COMPACT STORAGE")

        self.upgrade_to_version(VERSION_TRUNK, wait=True)

        session = self.patient_cql_connection(node, row_factory=dict_factory)
        assert (list(session.execute("SELECT * FROM ks.compact_table WHERE pk = 1")) ==
                     [{'col2': 1, 'pk': 1, 'column1': None, 'value': None, 'col1': 1}])

    def test_upgrade_with_dropped_compact_storage_index(self):
        cluster = self.prepare(cassandra_version=VERSION_311)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node, row_factory=dict_factory)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '1' };")
        session.execute("CREATE TABLE ks.compact_table (pk ascii PRIMARY KEY, col1 ascii) WITH COMPACT STORAGE")
        session.execute("CREATE INDEX ON ks.compact_table(col1)")

        for i in range(1, 10):
            session.execute("INSERT INTO ks.compact_table (pk, col1) VALUES ('{pk}', '{col1}')".format(pk=i, col1=i * 10))

        assert (list(session.execute("SELECT * FROM ks.compact_table WHERE col1 = '50'")) ==
                     [{'pk': '5', 'col1': '50'}])
        assert (list(session.execute("SELECT * FROM ks.compact_table WHERE pk = '5'")) ==
                     [{'pk': '5', 'col1': '50'}])
        session.execute("ALTER TABLE ks.compact_table DROP COMPACT STORAGE")

        assert (list(session.execute("SELECT * FROM ks.compact_table WHERE col1 = '50'")) ==
                     [{'col1': '50', 'column1': None, 'pk': '5', 'value': None}])
        assert (list(session.execute("SELECT * FROM ks.compact_table WHERE pk = '5'")) ==
                     [{'col1': '50', 'column1': None, 'pk': '5', 'value': None}])

        self.upgrade_to_version(VERSION_TRUNK, wait=True)

        session = self.patient_cql_connection(node, row_factory=dict_factory)

        assert (list(session.execute("SELECT * FROM ks.compact_table WHERE col1 = '50'")) ==
                     [{'col1': '50', 'column1': None, 'pk': '5', 'value': None}])
        assert (list(session.execute("SELECT * FROM ks.compact_table WHERE pk = '5'")) ==
                     [{'col1': '50', 'column1': None, 'pk': '5', 'value': None}])
