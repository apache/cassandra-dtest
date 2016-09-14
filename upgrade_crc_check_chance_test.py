from unittest import skipIf

from dtest import OFFHEAP_MEMTABLES, Tester, debug
from tools.assertions import assert_crc_check_chance_equal, assert_one
from tools.decorators import since


@since('3.0')
class TestCrcCheckChanceUpgrade(Tester):
    ignore_log_patterns = (
        # This one occurs if we do a non-rolling upgrade, the node
        # it's trying to send the migration to hasn't started yet,
        # and when it does, it gets replayed and everything is fine.
        r'Can\'t send migration request: node.*is down',
    )

    @skipIf(OFFHEAP_MEMTABLES, 'offheap_objects are not available in 3.0')
    def crc_check_chance_upgrade_test(self):
        """
        Tests behavior of compression property crc_check_chance after upgrade to 3.0,
        when it was promoted to a top-level property

        @jira_ticket CASSANDRA-9839
        """
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="git:cassandra-2.2")
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

        debug('Test completed successfully')

    def verify_old_crc_check_chance(self, node):
        session = self.patient_exclusive_cql_connection(node)
        session.cluster.refresh_schema_metadata(0)
        meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']
        debug(meta.options['compression_parameters'])
        self.assertEqual('{"crc_check_chance":"0.6","sstable_compression":"org.apache.cassandra.io.compress.DeflateCompressor","chunk_length_kb":"256"}',
                         meta.options['compression_parameters'])
        session.shutdown()

    def verify_new_crc_check_chance(self, node):
        session = self.patient_exclusive_cql_connection(node)
        session.cluster.refresh_schema_metadata(0)
        meta = session.cluster.metadata.keyspaces['ks'].tables['cf1']
        self.assertEqual('org.apache.cassandra.io.compress.DeflateCompressor', meta.options['compression']['class'])
        self.assertEqual('256', meta.options['compression']['chunk_length_in_kb'])
        assert_crc_check_chance_equal(session, "cf1", 0.6)
        session.shutdown()

    def upgrade_to_version(self, tag, node):
        format_args = {'node': node.name, 'tag': tag}
        debug('Upgrading node {node} to {tag}'.format(**format_args))
        # drain and shutdown
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop(wait_other_notice=False)
        debug('{node} stopped'.format(**format_args))

        # Update Cassandra Directory
        debug('Updating version to tag {tag}'.format(**format_args))

        debug('Set new cassandra dir for {node}: {tag}'.format(**format_args))
        node.set_install_dir(version='git:' + tag, verbose=True)
        # Restart node on new version
        debug('Starting {node} on new version ({tag})'.format(**format_args))
        # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
        node.set_log_level("INFO")
        node.start(wait_other_notice=True, wait_for_binary_proto=True)

        debug('Running upgradesstables')
        node.nodetool('upgradesstables -a')
        debug('Upgrade of {node} complete'.format(**format_args))
