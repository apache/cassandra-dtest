import pytest
import logging

from dtest import Tester
from tools.assertions import assert_crc_check_chance_equal, assert_one

since = pytest.mark.since
logger = logging.getLogger(__name__)

@pytest.mark.upgrade_test
@since('3.0')
class TestCrcCheckChanceUpgrade(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
             # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        )

    @pytest.mark.no_offheap_memtables
    def test_crc_check_chance_upgrade(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="github:apache/cassandra-2.2")
        self.install_nodetool_legacy_parsing()
        cluster.populate(2).start()

        node1, node2 = cluster.nodelist()

        # Create table
        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("""CREATE TABLE ks.cf1 (id int primary key, val int) WITH compression = {
                          'sstable_compression': 'DeflateCompressor',
                          'chunk_length_kb': 256,
                          'crc_check_chance': 0.6 }
                        """)

        # Insert and query data
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (0, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (1, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (2, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (3, 0)")
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=2", [2, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=3", [3, 0])
        session.shutdown()

        self.verify_old_crc_check_chance(node1)
        self.verify_old_crc_check_chance(node2)

        # upgrade node1 to 3.0
        self.upgrade_to_version("cassandra-3.0", node1)

        self.verify_new_crc_check_chance(node1)
        self.verify_old_crc_check_chance(node2)

        # Insert and query data
        session = self.patient_cql_connection(node1)
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (4, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (5, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (6, 0)")
        session.execute("INSERT INTO ks.cf1(id, val) VALUES (7, 0)")
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=2", [2, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=3", [3, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=4", [4, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=5", [5, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=6", [6, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=7", [7, 0])
        session.shutdown()

        # upgrade node2 to 3.0
        self.upgrade_to_version("cassandra-3.0", node2)

        self.verify_new_crc_check_chance(node1)
        self.verify_new_crc_check_chance(node2)

        # read data again
        session = self.patient_cql_connection(node1)
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=0", [0, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=1", [1, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=2", [2, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=3", [3, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=4", [4, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=5", [5, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=6", [6, 0])
        assert_one(session, "SELECT * FROM ks.cf1 WHERE id=7", [7, 0])
        session.shutdown()

        logger.debug('Test completed successfully')

    def verify_old_crc_check_chance(self, node):
        session = self.patient_exclusive_cql_connection(node)
        session.cluster.refresh_schema_metadata(0)
        meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']
        logger.debug(meta.options['compression_parameters'])
        assert '{"crc_check_chance":"0.6","sstable_compression":"org.apache.cassandra.io.compress.DeflateCompressor","chunk_length_kb":"256"}' \
               == meta.options['compression_parameters']
        session.shutdown()

    def verify_new_crc_check_chance(self, node):
        session = self.patient_exclusive_cql_connection(node)
        session.cluster.refresh_schema_metadata(0)
        meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']
        assert 'org.apache.cassandra.io.compress.DeflateCompressor' == meta.options['compression']['class']
        assert '256' == meta.options['compression']['chunk_length_in_kb']
        assert_crc_check_chance_equal(session, "cf1", 0.6)
        session.shutdown()

    def upgrade_to_version(self, tag, node):
        format_args = {'node': node.name, 'tag': tag}
        logger.debug('Upgrading node {node} to {tag}'.format(**format_args))
        self.install_legacy_parsing(node)
        # drain and shutdown
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop(wait_other_notice=False)
        logger.debug('{node} stopped'.format(**format_args))

        # Update Cassandra Directory
        logger.debug('Updating version to tag {tag}'.format(**format_args))

        logger.debug('Set new cassandra dir for {node}: {tag}'.format(**format_args))
        node.set_install_dir(version='git:' + tag, verbose=True)
        self.install_legacy_parsing(node)
        # Restart node on new version
        logger.debug('Starting {node} on new version ({tag})'.format(**format_args))
        # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
        node.set_log_level("INFO")
        node.start(wait_for_binary_proto=True, jvm_args=['-Dcassandra.disable_max_protocol_auto_override=true'])

        logger.debug('Running upgradesstables')
        node.nodetool('upgradesstables -a')
        logger.debug('Upgrade of {node} complete'.format(**format_args))
