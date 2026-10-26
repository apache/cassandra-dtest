import pytest
import logging
import re
import time

from cassandra import Unauthorized
from dtest import Tester
from tools.assertions import assert_all, assert_exception, assert_none

since = pytest.mark.since
logger = logging.getLogger(__name__)



class TestSystemKeyspaces(Tester):

    @since('3.0')
    def test_host_id_is_set(self):
        """
        @jira_ticket CASSANDRA-18153

        Test that the host ID in system.local is set after startup.
        """
        cluster = self.cluster
        cluster.data_dir_count = 1
        cluster.set_configuration_options(values={'commitlog_sync_period_in_ms': 500})
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        node.nodetool("disableautocompaction")

        session = self.patient_cql_connection(node)
        host_id_in_system_local = str(session.execute("SELECT host_id FROM system.local")[0].host_id)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        session.execute("CREATE TABLE ks.cf (k int PRIMARY KEY, v int)")
        node.nodetool("disableautocompaction ks cf")

        session.execute("INSERT INTO ks.cf (k, v) VALUES (0, 0)")
        node.flush()

        self.assert_host_id_in_all_sstables(host_id_in_system_local, node)

        # let's have something in the commitlog which was not flushed to sstable
        session.execute("INSERT INTO ks.cf (k, v) VALUES (1, 1)")

        # wait for the commitlog to be synced and kill the node
        time.sleep(2)
        cluster.stop(gently=False)

        cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        assert host_id_in_system_local == str(session.execute("SELECT host_id FROM system.local")[0].host_id), "Host ID in system.local changed after restart"

        node.flush()
        self.assert_host_id_in_all_sstables(host_id_in_system_local, node)

    @since('3.0')
    def test_consistent_host_id(self):
        """
        @jira_ticket CASSANDRA-18153

        Test that the host ID in system.local is consistent across restarts.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'commitlog_sync_period_in_ms': 500})
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        host_id_in_system_local = str(session.execute("SELECT host_id FROM system.local")[0].host_id)
        logger.info("Host ID in system.local: {}".format(host_id_in_system_local))

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        session.execute("CREATE TABLE ks.cf (k int PRIMARY KEY, v int)")

        session.execute("INSERT INTO ks.cf (k, v) VALUES (0, 0)")
        node.flush()

        # let's do something nasty - system.local is flushed without host ID
        session.execute("UPDATE system.local SET host_id = NULL WHERE key = 'local'")
        node.flush()
        node.compact()

        # let's generate some random host ID and leave it only in the commitlog
        random_host_id = "12345678-1234-1234-1234-123456789012"
        session.execute("UPDATE system.local SET host_id = {} WHERE key = 'local'".format(random_host_id))

        # wait for the commitlog to be synced and kill the node
        time.sleep(2)
        cluster.stop(gently=False)

        cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)

        host_id_in_system_local_after_restart = str(session.execute("SELECT host_id FROM system.local")[0].host_id)
        logger.info("Host ID in system.local after restart: {}".format(host_id_in_system_local_after_restart))
        # now we expect that although system.local has no host ID, it wasn't generated at startup because there was something in the commitlog
        # eventually, we should read that new host ID from the commitlog
        assert host_id_in_system_local_after_restart == random_host_id, "Host ID in system.local changed after restart: {} != {}".format(host_id_in_system_local_after_restart, random_host_id)

    def get_host_id_from_sstable_metadata(self, node, sstable_paths):
        host_ids = {}
        for sstable_path in sstable_paths:
            (out, err, rc) = node.run_sstablemetadata(datafiles=[sstable_path])
            assert rc == 0, "sstablemetadata failed with error: {}".format(err)
            # extract host id from out using "Originating host id: (\\S*)" regex
            host_id = re.search("Originating host id: (\\S*)", out)
            if host_id is None:
                logger.info("Could not find host ID in sstablemetadata output: {}".format(out))
            else:
                if sstable_path in host_ids:
                    logger.info("Duplicate sstable path: {}".format(sstable_path))
                host_ids[sstable_path] = host_id.group(1)

        return host_ids

    def assert_host_id_in_all_sstables(self, expected_host_id, node):
        keyspaces = node.list_keyspaces()
        # we need to explicitly add local system keyspace to the list
        keyspaces.append("system")

        sstable_paths = []
        for keyspace in keyspaces:
            sstable_paths.extend(node.get_sstables(keyspace, ""))

        host_ids = self.get_host_id_from_sstable_metadata(node, sstable_paths)
        assert len(host_ids) == len(sstable_paths), "Expected to find host ID in all sstables: {} != {}".format(
            len(host_ids), len(sstable_paths))

        # assert that host IDs in system.local and sstables are the same
        for sstable_path in sstable_paths:
            assert expected_host_id == host_ids[sstable_path], \
                "Host ID in system.local and sstable {} are different: {} != {}".format(sstable_path,
                                                                                        expected_host_id,
                                                                                        host_ids[sstable_path])

    @since('3.0')
    def test_local_system_keyspaces(self):
        cluster = self.cluster
        cluster.populate(1).start()

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        # ALTER KEYSPACE should fail for system and system_schema
        stmt = """
            ALTER KEYSPACE system
            WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1'};"""
        assert_exception(session, stmt, expected=Unauthorized)

        stmt = """
            ALTER KEYSPACE system_schema
            WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1'};"""
        assert_exception(session, stmt, expected=Unauthorized)

        # DROP KEYSPACE should fail for system and system_schema
        assert_exception(session, 'DROP KEYSPACE system;', expected=Unauthorized)
        assert_exception(session, 'DROP KEYSPACE system_schema;', expected=Unauthorized)

        # CREATE TABLE should fail in system and system_schema
        assert_exception(session,
                         'CREATE TABLE system.new_table (id int PRIMARY KEY);',
                         expected=Unauthorized)

        assert_exception(session,
                         'CREATE TABLE system_schema.new_table (id int PRIMARY KEY);',
                         expected=Unauthorized)

        # ALTER TABLE should fail in system and system_schema
        assert_exception(session,
                         "ALTER TABLE system.local WITH comment = '';",
                         expected=Unauthorized)

        assert_exception(session,
                         "ALTER TABLE system_schema.tables WITH comment = '';",
                         expected=Unauthorized)

        # DROP TABLE should fail in system and system_schema
        assert_exception(session, 'DROP TABLE system.local;', expected=Unauthorized)
        assert_exception(session, 'DROP TABLE system_schema.tables;', expected=Unauthorized)

    @since('3.0')
    def test_replicated_system_keyspaces(self):
        cluster = self.cluster
        cluster.populate(1).start()

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        # ALTER KEYSPACE should work for system_auth, system_distributed, and system_traces
        stmt = """
            ALTER KEYSPACE system_auth
            WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1'};"""
        assert_none(session, stmt)

        stmt = """
            ALTER KEYSPACE system_distributed
            WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1'};"""
        assert_none(session, stmt)

        stmt = """
            ALTER KEYSPACE system_traces
            WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '1'};"""
        assert_none(session, stmt)

        stmt = """
            SELECT replication
            FROM system_schema.keyspaces
            WHERE keyspace_name IN ('system_auth', 'system_distributed', 'system_traces');"""
        replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '1'}
        assert_all(session, stmt, [[replication], [replication], [replication]])

        # DROP KEYSPACE should fail for system_auth, system_distributed, and system_traces
        assert_exception(session, 'DROP KEYSPACE system_auth;', expected=Unauthorized)
        assert_exception(session, 'DROP KEYSPACE system_distributed;', expected=Unauthorized)
        assert_exception(session, 'DROP KEYSPACE system_traces;', expected=Unauthorized)

        # CREATE TABLE should fail in system_auth, system_distributed, and system_traces
        assert_exception(session,
                         'CREATE TABLE system_auth.new_table (id int PRIMARY KEY);',
                         expected=Unauthorized)

        assert_exception(session,
                         'CREATE TABLE system_distributed.new_table (id int PRIMARY KEY);',
                         expected=Unauthorized)

        assert_exception(session,
                         'CREATE TABLE system_traces.new_table (id int PRIMARY KEY);',
                         expected=Unauthorized)

        # ALTER TABLE should fail in system_auth, system_distributed, and system_traces
        assert_exception(session,
                         "ALTER TABLE system_auth.roles WITH comment = '';",
                         expected=Unauthorized)

        assert_exception(session,
                         "ALTER TABLE system_distributed.repair_history WITH comment = '';",
                         expected=Unauthorized)

        assert_exception(session,
                         "ALTER TABLE system_traces.sessions WITH comment = '';",
                         expected=Unauthorized)

        # DROP TABLE should fail in system_auth, system_distributed, and system_traces
        assert_exception(session, 'DROP TABLE system_auth.roles;', expected=Unauthorized)
        assert_exception(session, 'DROP TABLE system_distributed.repair_history;', expected=Unauthorized)
        assert_exception(session, 'DROP TABLE system_traces.sessions;', expected=Unauthorized)
