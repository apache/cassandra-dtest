import time

import pytest
import logging

from cassandra.protocol import InvalidRequest

from dtest import Tester

since = pytest.mark.since
logger = logging.getLogger(__name__)

VERSION_30 = 'github:apache/cassandra-3.0'
VERSION_311 = 'github:apache/cassandra-3.11'
VERSION_TRUNK = 'github:apache/trunk'


@pytest.mark.upgrade_test
class TestDropCompactStorage(Tester):
    def prepare(self):
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="2.1.14")
        self.install_nodetool_legacy_parsing()
        cluster.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node1)
        session.execute(
            "CREATE KEYSPACE drop_compact_storage_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'};")
        session.cluster.control_connection.wait_for_schema_agreement()
        session.execute(
            "CREATE TABLE drop_compact_storage_test.test (a text PRIMARY KEY, b text, c text) WITH COMPACT STORAGE;")
        session.cluster.control_connection.wait_for_schema_agreement()

        for i in range(1, 100):
            session.execute(
                "INSERT INTO drop_compact_storage_test.test (a, b, c) VALUES ('{}', '{}', '{}');".format(i, i + 1,
                                                                                                         i + 2))
        return cluster

    def drop_compact_storage(self, session, *args):
        try:
            session.execute("ALTER TABLE drop_compact_storage_test.test DROP COMPACT STORAGE")
            pytest.fail("No exception has been thrown")
        except InvalidRequest as e:
            for _ in args:
                assert _ in str(e)

    def upgrade_node(self, node, to_version):
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop(wait_other_notice=False)

        node.set_install_dir(version=to_version)
        node.set_configuration_options(values={'enable_drop_compact_storage': 'true'})
        node.start(wait_for_binary_proto=True)

    @since('3.0', max_version='3.11')
    def test_drop_compact_storage(self):
        """
        @jira_ticket CASSANDRA-15897

        Test to verify that dropping compact storage is not possible prior running `nodetool upgradesstables` when we still
        have old pre-3.0 SSTables in the cluster.
        """
        cluster = self.prepare()
        node1, node2, node3 = cluster.nodelist()

        logging.debug("Upgrading to current version")
        for node in [node1, node2]:
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

            self.set_node_to_current_version(node)
            node.set_configuration_options(values={'enable_drop_compact_storage': 'true'})
            node.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node1)
        assert_msg = "Cannot DROP COMPACT STORAGE as some nodes in the cluster ([/127.0.0.3]) are not on 3.0+ yet. " \
                     "Please upgrade those nodes and run `upgradesstables` before retrying."

        self.drop_compact_storage(session, assert_msg)

        self.upgrade_node(node3, VERSION_30)

        # time provided to gossip to update its info after upgrade
        time.sleep(5)

        for node in [node1, node2, node3]:
            node.nodetool("upgradesstables")

        time.sleep(2)

        session.execute("ALTER TABLE drop_compact_storage_test.test DROP COMPACT STORAGE")
        session.cluster.control_connection.wait_for_schema_agreement()
        session.execute("SELECT * FROM drop_compact_storage_test.test")

    @since('4.0')
    def test_drop_compact_storage_mixed_cluster(self):
        """
        @jira_ticket CASSANDRA-15897

        Test to verify that dropping compact storage is not possible prior running `nodetool upgradesstables` when we still
        have old pre-3.0 sstables in the cluster. Also, all nodes to be on the same major version.
        """
        cluster = self.prepare()
        node1, node2, node3 = cluster.nodelist()

        logging.debug("Upgrading all nodes to version 3.0")
        for node in [node1, node2, node3]:
            self.upgrade_node(node, VERSION_30)

        session = self.patient_exclusive_cql_connection(node3)
        assert_msg_part1 = "Cannot DROP COMPACT STORAGE as some nodes in the cluster"
        assert_msg_part2 = "non-upgraded 2.x sstables. Please run `upgradesstables` on those nodes before retrying"

        self.drop_compact_storage(session, assert_msg_part1, node1.ip_addr, node2.ip_addr, node3.ip_addr, assert_msg_part2)

        node3.nodetool("upgradesstables")
        time.sleep(1)

        self.drop_compact_storage(session, assert_msg_part1, node1.ip_addr, node2.ip_addr, assert_msg_part2)

        self.upgrade_node(node3, VERSION_311)

        time.sleep(5)

        session = self.patient_exclusive_cql_connection(node3)
        self.drop_compact_storage(session, assert_msg_part1, node1.ip_addr, node2.ip_addr, assert_msg_part2)

        node2.nodetool("upgradesstables")

        # time for gossip to update its info after running upgradesstables
        time.sleep(2)

        assert_msg = "Cannot DROP COMPACT STORAGE as some nodes in the cluster ([/127.0.0.1]) has some " \
                     "non-upgraded 2.x sstables. Please run `upgradesstables` on those nodes before retrying"
        self.drop_compact_storage(session, assert_msg)

        self.upgrade_node(node2, VERSION_311)

        self.upgrade_node(node3, VERSION_TRUNK)

        node1.nodetool("upgradesstables")
        time.sleep(2)

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("SELECT * FROM drop_compact_storage_test.test")
        assert_msg_part1 = "Cannot DROP COMPACT STORAGE as some nodes in the cluster"
        assert_msg_part2 = "are not on 4.0+ yet. " \
                           "Please upgrade those nodes and run `upgradesstables` before retrying."
        self.drop_compact_storage(session, assert_msg_part1, node1.ip_addr, node2.ip_addr, assert_msg_part2)

        for node in [node1, node2]:
            self.upgrade_node(node, VERSION_TRUNK)
            time.sleep(5)
            node.nodetool("upgradesstables")

        time.sleep(10)

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("SELECT * FROM drop_compact_storage_test.test")
        session.execute("ALTER TABLE drop_compact_storage_test.test DROP COMPACT STORAGE")
        session.cluster.control_connection.wait_for_schema_agreement()
        session.execute("SELECT * FROM drop_compact_storage_test.test")
