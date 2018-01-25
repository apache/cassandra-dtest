import pytest
import logging

from cassandra import Unauthorized
from dtest import Tester
from tools.assertions import assert_all, assert_exception, assert_none

since = pytest.mark.since
logger = logging.getLogger(__name__)



class TestSystemKeyspaces(Tester):

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
