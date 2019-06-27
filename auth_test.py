import random
import string
import time
from collections import namedtuple
from datetime import datetime, timedelta
from distutils.version import LooseVersion
import re
import pytest
import logging

from cassandra import AuthenticationFailed, InvalidRequest, Unauthorized, Unavailable
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import ServerError, SyntaxException

from dtest_setup_overrides import DTestSetupOverrides
from dtest import Tester
from tools.assertions import (assert_all, assert_exception, assert_invalid,
                              assert_length_equal, assert_one,
                              assert_unauthorized)
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)
from tools.metadata_wrapper import UpdatingKeyspaceMetadataWrapper
from tools.misc import ImmutableMapping

since = pytest.mark.since
logger = logging.getLogger(__name__)

class TestAuth(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        )

    def test_system_auth_ks_is_alterable(self):
        """
        * Launch a three node cluster
        * Verify the default RF of system_auth is 1
        * Increase the system_auth RF to 3
        * Run repair, see 10655
        * Restart the cluster
        * Check that each node agrees on the system_auth RF

        @jira_ticket CASSANDRA-10655
        """
        self.prepare(nodes=3)
        logger.debug("nodes started")

        session = self.get_session(user='cassandra', password='cassandra')
        auth_metadata = UpdatingKeyspaceMetadataWrapper(
            cluster=session.cluster,
            ks_name='system_auth',
            max_schema_agreement_wait=30  # 3x the default of 10
        )
        assert 1 == auth_metadata.replication_strategy.replication_factor

        session.execute("""
            ALTER KEYSPACE system_auth
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
        """)

        assert 3 == auth_metadata.replication_strategy.replication_factor

        # Run repair to workaround read repair issues caused by CASSANDRA-10655
        logger.debug("Repairing before altering RF")
        self.cluster.repair()

        logger.debug("Shutting down client cluster")
        session.cluster.shutdown()

        # make sure schema change is persistent
        logger.debug("Stopping cluster..")
        self.cluster.stop()
        logger.debug("Restarting cluster..")
        self.cluster.start(wait_other_notice=True)

        # check each node directly
        for i in range(3):
            logger.debug('Checking node: {i}'.format(i=i))
            node = self.cluster.nodelist()[i]
            exclusive_auth_metadata = UpdatingKeyspaceMetadataWrapper(
                cluster=self.patient_exclusive_cql_connection(node, user='cassandra', password='cassandra').cluster,
                ks_name='system_auth'
            )
            assert 3 == exclusive_auth_metadata.replication_strategy.replication_factor

    def test_login(self):
        """
        * Launch a one node cluster
        * Connect as the default user/password
        * Verify that default user w/ bad password gives AuthenticationFailed exception
        * Verify that bad user gives AuthenticationFailed exception
        """
        # also tests default user creation (cassandra/cassandra)
        self.prepare()
        self.get_session(user='cassandra', password='cassandra')
        try:
            self.get_session(user='cassandra', password='badpassword')
        except NoHostAvailable as e:
            assert isinstance(list(e.errors.values())[0], AuthenticationFailed)
        try:
            self.get_session(user='doesntexist', password='doesntmatter')
        except NoHostAvailable as e:
            assert isinstance(list(e.errors.values())[0], AuthenticationFailed)

    # from 2.2 role creation is granted by CREATE_ROLE permissions, not superuser status
    @since('1.2', max_version='2.1.x')
    def test_only_superuser_can_create_users(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify we can create a user, 'jackob', as the default superuser
        * Connect as the new user, 'jackob'
        * Verify we cannot create a second user as 'jackob'
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER jackob WITH PASSWORD '12345' NOSUPERUSER")

        jackob = self.get_session(user='jackob', password='12345')
        assert_unauthorized(jackob, "CREATE USER james WITH PASSWORD '54321' NOSUPERUSER", 'Only superusers are allowed to perform CREATE (\\[ROLE\\|USER\\]|USER) queries', )

    @since('1.2', max_version='2.1.x')
    def test_password_authenticator_create_user_requires_password(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify we cannot create a new user without specifying a password for them
        * Verify we can create the new user if the password is specified
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_invalid(session, "CREATE USER jackob NOSUPERUSER", 'PasswordAuthenticator requires PASSWORD option')
        session.execute("CREATE USER jackob WITH PASSWORD '12345' NOSUPERUSER")

    def test_cant_create_existing_user(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user
        * Verify that attempting to create a duplicate user fails with InvalidRequest
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        session.execute("CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER")
        assert_invalid(session, "CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER", 'james@example.com already exists')

    def test_list_users(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create two new users, and two new superusers.
        * Verify that LIST USERS shows all five users.
        * Verify that the correct users are listed as super users.
        * Connect as one of the new users, and check that the LIST USERS behavior is also correct there.
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        session.execute("CREATE USER alex WITH PASSWORD '12345' NOSUPERUSER")
        session.execute("CREATE USER bob WITH PASSWORD '12345' SUPERUSER")
        session.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        session.execute("CREATE USER dave WITH PASSWORD '12345' SUPERUSER")

        rows = list(session.execute("LIST USERS"))
        assert 5 == len(rows)
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        assert users['cassandra']
        assert not users['alex']
        assert users['bob']
        assert not users['cathy']
        assert users['dave']

        self.get_session(user='dave', password='12345')
        rows = list(session.execute("LIST USERS"))
        assert 5 == len(rows)
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        assert users['cassandra']
        assert not users['alex']
        assert users['bob']
        assert not users['cathy']
        assert users['dave']

    @since('2.2')
    def test_handle_corrupt_role_data(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role
        * Confirm that there exists 2 users
        * Manually corrupt / delete the is_superuser cell of that role
        * Confirm that listing users shows an invalid request
        * Confirm that corrupted user can no longer login
        @jira_ticket CASSANDRA-12700
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        session.execute("CREATE USER bob WITH PASSWORD '12345' SUPERUSER")

        bob = self.get_session(user='bob', password='12345')
        rows = list(bob.execute("LIST USERS"))
        assert_length_equal(rows, 2)

        session.execute("UPDATE system_auth.roles SET is_superuser=null WHERE role='bob'")

        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            r'Invalid metadata has been detected for role bob']
        assert_exception(session, "LIST USERS", "Invalid metadata has been detected for role", expected=(ServerError))
        try:
            self.get_session(user='bob', password='12345')
        except NoHostAvailable as e:
            assert isinstance(list(e.errors.values())[0], AuthenticationFailed)

    def test_user_cant_drop_themselves(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify the superuser can't drop themselves
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        # handle different error messages between versions pre and post 2.2.0
        assert_invalid(session, "DROP USER cassandra", "(Users aren't allowed to DROP themselves|Cannot DROP primary role for current login)")

    # from 2.2 role deletion is granted by DROP_ROLE permissions, not superuser status
    @since('1.2', max_version='2.1.x')
    def test_only_superusers_can_drop_users(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create two new users
        * Verify all users are present with LIST USERS
        * Connect as one of the new users, 'cathy'
        * Verify that 'cathy', not being a superuser, cannot drop other users, and gets an Unauthorized exception
        * Verify the default superuser can drop other users
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE USER dave WITH PASSWORD '12345' NOSUPERUSER")
        rows = list(cassandra.execute("LIST USERS"))
        assert 3 == len(rows)

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, 'DROP USER dave', 'Only superusers are allowed to perform DROP (\\[ROLE\\|USER\\]|USER) queries')

        rows = list(cassandra.execute("LIST USERS"))
        assert 3 == len(rows)

        cassandra.execute('DROP USER dave')
        rows = list(cassandra.execute("LIST USERS"))
        assert 2 == len(rows)

    def test_dropping_nonexistent_user_throws_exception(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify that dropping a nonexistent user throws InvalidRequest
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_invalid(session, 'DROP USER nonexistent', "nonexistent doesn't exist")

    def test_drop_user_case_sensitive(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user, 'Test'
        * Verify that the drop user statement is case sensitive
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER Test WITH PASSWORD '12345'")

        # Should be invalid, as 'test' does not exist
        assert_invalid(cassandra, "DROP USER test")

        cassandra.execute("DROP USER Test")
        rows = [x[0] for x in list(cassandra.execute("LIST USERS"))]
        assert rows == ['cassandra']

        cassandra.execute("CREATE USER test WITH PASSWORD '12345'")

        # Should be invalid, as 'TEST' does not exist
        assert_invalid(cassandra, "DROP USER TEST")

        cassandra.execute("DROP USER test")
        rows = [x[0] for x in list(cassandra.execute("LIST USERS"))]
        assert rows == ['cassandra']

    def test_alter_user_case_sensitive(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user, 'Test'
        * Verify that ALTER statements on the user are case sensitive
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER Test WITH PASSWORD '12345'")
        cassandra.execute("ALTER USER Test WITH PASSWORD '54321'")
        assert_invalid(cassandra, "ALTER USER test WITH PASSWORD '12345'")
        assert_invalid(cassandra, "ALTER USER TEST WITH PASSWORD '12345'")

        cassandra.execute('DROP USER Test')
        cassandra.execute("CREATE USER test WITH PASSWORD '12345'")
        assert_invalid(cassandra, "ALTER USER Test WITH PASSWORD '12345'")
        assert_invalid(cassandra, "ALTER USER TEST WITH PASSWORD '12345'")
        cassandra.execute("ALTER USER test WITH PASSWORD '54321'")

    def test_regular_users_can_alter_their_passwords_only(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create two users, 'cathy' and 'bob'
        * Connect as 'cathy'
        * Verify 'cathy' can alter her own password
        * Verify that if 'cathy' tries to alter bob's password, it throws Unauthorized
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cathy = self.get_session(user='cathy', password='12345')
        cathy.execute("ALTER USER cathy WITH PASSWORD '54321'")
        cathy = self.get_session(user='cathy', password='54321')
        assert_unauthorized(cathy, "ALTER USER bob WITH PASSWORD 'cantchangeit'",
                            "You aren't allowed to alter this user|User cathy does not have sufficient privileges to perform the requested operation")

    def test_users_cant_alter_their_superuser_status(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Attempt to remove our own superuser status. Assert this throws Unauthorized
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_unauthorized(session, "ALTER USER cassandra NOSUPERUSER", "You aren't allowed to alter your own superuser status")

    def test_only_superuser_alters_superuser_status(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy'
        * Connect as 'cathy'
        * Verify that Unauthorized is thrown if cathy attempts to alter another user's superuser status
        * Verify that the default superuser can alter cathy's superuser status
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, "ALTER USER cassandra NOSUPERUSER", "Only superusers are allowed to alter superuser status")

        cassandra.execute("ALTER USER cathy SUPERUSER")

    def test_altering_nonexistent_user_throws_exception(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Assert that altering a nonexistent user throws InvalidRequest
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_invalid(session, "ALTER USER nonexistent WITH PASSWORD 'doesn''tmatter'", "nonexistent doesn't exist")

    def test_conditional_create_drop_user(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Attempt to create a user twice, using IF NOT EXISTS
        * Verify neither query fails, but the user is only created once
        * Attempt to DROP USER IF EXISTS, twice. Ensure both succeed.
        * Verify only the default superuser remains
        """
        self.prepare()
        session = self.get_session(user='cassandra', password='cassandra')

        if self.dtest_config.cassandra_version_from_build >= '4.0':
            assert_one(session, "LIST USERS", ['cassandra', True, 'ALL'])
        else:
            assert_one(session, "LIST USERS", ['cassandra', True])

        session.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'sup'")
        session.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'ignored'")

        self.get_session(user='aleksey', password='sup')

        if self.dtest_config.cassandra_version_from_build >= '4.0':
            assert_all(session, "LIST USERS", [['aleksey', False, 'ALL'], ['cassandra', True, 'ALL']])
        else:
            assert_all(session, "LIST USERS", [['aleksey', False], ['cassandra', True]])

        session.execute("DROP USER IF EXISTS aleksey")
        if self.dtest_config.cassandra_version_from_build >= '4.0':
            assert_one(session, "LIST USERS", ['cassandra', True, 'ALL'])
        else:
            assert_one(session, "LIST USERS", ['cassandra', True])

        session.execute("DROP USER IF EXISTS aleksey")
        if self.dtest_config.cassandra_version_from_build >= '4.0':
            assert_one(session, "LIST USERS", ['cassandra', True, 'ALL'])
        else:
            assert_one(session, "LIST USERS", ['cassandra', True])

    def test_create_ks_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Connect as 'cathy'
        * Assert that trying to create a ks as 'cathy' throws Unauthorized
        * Grant 'cathy' create permissions
        * Assert that 'cathy' can create a ks
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy,
                            "CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}",
                            "User cathy has no CREATE permission on <all keyspaces> or any of its parents")

        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO cathy")
        cathy.execute("""CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}""")

    def test_create_cf_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Connect as 'cathy'
        * Assert that trying to create a table as 'cathy' throws Unauthorized
        * Grant 'cathy' create permissions
        * Assert that 'cathy' can create a table
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, "CREATE TABLE ks.cf (id int primary key)",
                            "User cathy has no CREATE permission on <keyspace ks> or any of its parents")

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute("CREATE TABLE ks.cf (id int primary key)")

    def test_alter_ks_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Connect as 'cathy'
        * Assert that trying to alter a ks as 'cathy' throws Unauthorized
        * Grant 'cathy' alter permissions
        * Assert that 'cathy' can alter a ks
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy,
                            "ALTER KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}",
                            "User cathy has no ALTER permission on <keyspace ks> or any of its parents")

        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO cathy")
        cathy.execute("ALTER KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")

    def test_alter_cf_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Connect as 'cathy'
        * Assert that trying to alter a table as 'cathy' throws Unauthorized
        * Grant 'cathy' alter permissions
        * Assert that 'cathy' can alter a table
        * Revoke cathy's alter permissions
        * Assert that trying to alter a table as 'cathy' throws Unauthorized
        * Repeat the grant/revoke loop two more times
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, "ALTER TABLE ks.cf ADD val int", "User cathy has no ALTER permission on <table ks.cf> or any of its parents")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf ADD val int")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        assert_unauthorized(cathy, "CREATE INDEX ON ks.cf(val)", "User cathy has no ALTER permission on <table ks.cf> or any of its parents")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("CREATE INDEX ON ks.cf(val)")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")

        cathy.execute("USE ks")
        assert_unauthorized(cathy, "DROP INDEX cf_val_idx", "User cathy has no ALTER permission on <table ks.cf> or any of its parents")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("DROP INDEX cf_val_idx")

    @since('3.0')
    def test_materialized_views_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Create a ks, table
        * Connect as cathy
        * Try CREATE MV without ALTER permission on base table, assert throws Unauthorized
        * Grant cathy ALTER permissions, then CREATE MV successfully
        * Try to SELECT from the mv, assert throws Unauthorized
        * Grant cathy SELECT permissions, and read from the MV successfully
        * Revoke cathy's ALTER permissions, assert DROP MV throws Unauthorized
        * Restore cathy's ALTER permissions, DROP MV successfully
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, value text)")

        # Try CREATE MV without ALTER permission on base table
        create_mv = "CREATE MATERIALIZED VIEW ks.mv1 AS SELECT * FROM ks.cf WHERE id IS NOT NULL " \
                    "AND value IS NOT NULL PRIMARY KEY (value, id)"
        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, create_mv, "User cathy has no ALTER permission on <table ks.cf> or any of its parents")

        # Grant ALTER permission and CREATE MV
        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute(create_mv)

        # TRY SELECT MV without SELECT permission on base table
        assert_unauthorized(cathy, "SELECT * FROM ks.mv1", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")

        # Grant SELECT permission and CREATE MV
        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        cathy.execute("SELECT * FROM ks.mv1")

        # Revoke ALTER permission and try DROP MV
        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        cathy.execute("USE ks")
        assert_unauthorized(cathy, "DROP MATERIALIZED VIEW mv1", "User cathy has no ALTER permission on <table ks.cf> or any of its parents")

        # GRANT ALTER permission and DROP MV
        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("DROP MATERIALIZED VIEW mv1")

    def test_drop_ks_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create user 'cathy', with no permissions
        * Create a new keyspace, 'ks'
        * Connect as cathy
        * Try to DROP ks, assert throws Unauthorized
        * Grant DROP permission to cathy, drop ks successfully
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, "DROP KEYSPACE ks", "User cathy has no DROP permission on <keyspace ks> or any of its parents")

        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute("DROP KEYSPACE ks")

    def test_drop_cf_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create user 'cathy', with no permissions
        * Create a new table, 'ks.cf'
        * Connect as cathy
        * Try to DROP ks.cf, assert throws Unauthorized
        * Grant DROP permission to cathy, drop ks.cf successfully
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, "DROP TABLE ks.cf", "User cathy has no DROP permission on <table ks.cf> or any of its parents")

        cassandra.execute("GRANT DROP ON ks.cf TO cathy")
        cathy.execute("DROP TABLE ks.cf")

    def test_modify_and_select_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Create table ks.cf
        * Connect as cathy
        * Asserting selecting from cf throws Unauthorized
        * Grant SELECT to cathy, verify she can read from cf
        * Assert insert, update, delete, and truncate all throw Unauthorized
        * Grant MODIFY to cathy, verify she can now perform all modification queries
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, "SELECT * FROM ks.cf", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")

        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 0 == len(rows)

        assert_unauthorized(cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "UPDATE ks.cf SET val = 1 WHERE id = 1", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "DELETE FROM ks.cf WHERE id = 1", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "TRUNCATE ks.cf", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("UPDATE ks.cf SET val = 1 WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 2 == len(rows)

        cathy.execute("DELETE FROM ks.cf WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 1 == len(rows)

        rows = list(cathy.execute("TRUNCATE ks.cf"))
        assert rows == []

    @since('2.2')
    def test_grant_revoke_without_ks_specified(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create table ks.cf
        * Create a new users, 'cathy' and 'bob', with no permissions
        * Grant ALL on ks.cf to cathy
        * As cathy, try granting SELECT on cf to bob, without specifying the ks; verify it fails
        * As cathy, USE ks, try again, verify it works this time
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')

        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cassandra.execute("GRANT ALL ON ks.cf TO cathy")

        cathy = self.get_session(user='cathy', password='12345')
        bob = self.get_session(user='bob', password='12345')

        assert_invalid(cathy, "GRANT SELECT ON cf TO bob", "No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename")
        assert_unauthorized(bob, "SELECT * FROM ks.cf", "User bob has no SELECT permission on <table ks.cf> or any of its parents")

        cathy.execute("USE ks")
        cathy.execute("GRANT SELECT ON cf TO bob")
        bob.execute("SELECT * FROM ks.cf")

    def test_grant_revoke_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create two new users, 'cathy' and 'bob'
        * Connect as cathy
        * Verify Unauthorized is thrown if cathy tries to grant bob SELECT permissions
        * Grant cathy AUTHORIZE
        * Verify Unauthorized is still thrown if cathy tries to grant bob SELECT permissions
        * Grant cathy SELECT
        * Verify she can grant bob SELECT
        # Verify bob can SELECT
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_session(user='cathy', password='12345')
        # missing both SELECT and AUTHORIZE
        assert_unauthorized(cathy, "GRANT SELECT ON ALL KEYSPACES TO bob", "User cathy has no AUTHORIZE permission on <all keyspaces> or any of its parents")

        cassandra.execute("GRANT AUTHORIZE ON ALL KEYSPACES TO cathy")

        # still missing SELECT
        assert_unauthorized(cathy, "GRANT SELECT ON ALL KEYSPACES TO bob", "User cathy has no SELECT permission on <all keyspaces> or any of its parents")

        cassandra.execute("GRANT SELECT ON ALL KEYSPACES TO cathy")

        # should succeed now with both SELECT and AUTHORIZE
        cathy.execute("GRANT SELECT ON ALL KEYSPACES TO bob")

        bob = self.get_session(user='bob', password='12345')
        bob.execute("SELECT * FROM ks.cf")

    def test_grant_revoke_nonexistent_user_or_ks(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a keyspace, 'ks', and a new user, 'cathy'
        * Grant and Revoke permissions to cathy on a nonexistent keyspace, assert throws InvalidRequest
        * Grant and Revoke permissions to a nonexistent user to ks, assert throws InvalidRequest
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        assert_invalid(cassandra, "GRANT ALL ON KEYSPACE nonexistent TO cathy", "<keyspace nonexistent> doesn't exist")

        assert_invalid(cassandra, "GRANT ALL ON KEYSPACE ks TO nonexistent", "(User|Role) nonexistent doesn't exist")

        assert_invalid(cassandra, "REVOKE ALL ON KEYSPACE nonexistent FROM cathy", "<keyspace nonexistent> doesn't exist")

        assert_invalid(cassandra, "REVOKE ALL ON KEYSPACE ks FROM nonexistent", "(User|Role) nonexistent doesn't exist")

    def test_grant_revoke_cleanup(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a table, ks.cf
        * Create a new user, 'cathy', grant her ALL permissions on ks.cf
        * Verify she can read/write from the table
        * DROP and CREATE cathy
        * Assert her permissions are gone, and operations throw Unauthorized
        * Grant ALL permissions back to cathy, verify she can read/write
        * DROP and CREATE ks.cf
        * Verify cathy's permissions on ks.cf are gone, and operations throw Unauthorized
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")

        cathy = self.get_session(user='cathy', password='12345')
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 1 == len(rows)

        # drop and recreate the user, make sure permissions are gone
        cassandra.execute("DROP USER cathy")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        assert_unauthorized(cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "SELECT * FROM ks.cf", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")

        # grant all the permissions back
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 1 == len(rows)

        # drop and recreate the keyspace, make sure permissions are gone
        cassandra.execute("DROP KEYSPACE ks")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        assert_unauthorized(cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "SELECT * FROM ks.cf", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")

    def test_permissions_caching(self):
        """
        * Launch a one node cluster, with a 2s permission cache
        * Connect as the default superuser
        * Create a new user, 'cathy'
        * Create a table, ks.cf
        * Connect as cathy in two separate sessions
        * Grant SELECT to cathy
        * Verify that reading from ks.cf throws Unauthorized until the cache expires
        * Verify that after the cache expires, we can eventually read with both sessions

        @jira_ticket CASSANDRA-8194
        """
        self.prepare(permissions_validity=2000)

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_session(user='cathy', password='12345')
        # another user to make sure the cache is at user level
        cathy2 = self.get_session(user='cathy', password='12345')
        cathys = [cathy, cathy2]

        assert_unauthorized(cathy, "SELECT * FROM ks.cf", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")

        def check_caching(attempt=0):
            attempt += 1
            if attempt > 3:
                self.fail("Unable to verify cache expiry in 3 attempts, failing")

            logger.debug("Attempting to verify cache expiry, attempt #{i}".format(i=attempt))
            # grant SELECT to cathy
            cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
            grant_time = datetime.now()
            # selects should still fail after 1 second, but if execution was
            # delayed for some reason such that the cache expired, retry
            time.sleep(1.0)
            for c in cathys:
                try:
                    c.execute("SELECT * FROM ks.cf")
                    # this should still fail, but if the cache has expired while we paused, try again
                    delta = datetime.now() - grant_time
                    if delta > timedelta(seconds=2):
                        # try again
                        cassandra.execute("REVOKE SELECT ON ks.cf FROM cathy")
                        time.sleep(2.5)
                        check_caching(attempt)
                    else:
                        # legit failure
                        self.fail("Expecting query to raise an exception, but nothing was raised.")
                except Unauthorized as e:
                    assert re.search("User cathy has no SELECT permission on <table ks.cf> or any of its parents", str(e))

        check_caching()

        # wait until the cache definitely expires and retry - should succeed now
        time.sleep(1.5)
        # refresh of user permissions is done asynchronously, the first request
        # will trigger the refresh, but we'll continue to use the cached set until
        # that completes (CASSANDRA-8194).
        # make a request to trigger the refresh
        try:
            cathy.execute("SELECT * FROM ks.cf")
        except Unauthorized:
            pass

        # once the async refresh completes, both clients should have the granted permissions
        success = False
        cnt = 0
        while not success and cnt < 10:
            try:
                for c in cathys:
                    rows = list(c.execute("SELECT * FROM ks.cf"))
                    assert 0 == len(rows)
                success = True
            except Unauthorized:
                pass
            cnt += 1
            time.sleep(0.1)

        assert success

    def test_list_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create two users, 'cathy' and 'bob'
        * Create two tables, ks.cf and ks.cf2
        * Grant a number of permissions to each user
        * Verify that LIST PERMISSIONS shows correct permissions for each user
        * Verify that only the superuser can LIST PERMISSIONS

        @jira_ticket CASSANDRA-7216
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE TABLE ks.cf2 (id int primary key, val int)")

        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO cathy")
        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO bob")
        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cassandra.execute("GRANT DROP ON ks.cf TO bob")
        cassandra.execute("GRANT MODIFY ON ks.cf2 TO bob")
        cassandra.execute("GRANT SELECT ON ks.cf2 TO cathy")

        all_permissions = [('cathy', '<all keyspaces>', 'CREATE'),
                           ('cathy', '<table ks.cf>', 'MODIFY'),
                           ('cathy', '<table ks.cf2>', 'SELECT'),
                           ('bob', '<keyspace ks>', 'ALTER'),
                           ('bob', '<table ks.cf>', 'DROP'),
                           ('bob', '<table ks.cf2>', 'MODIFY')]

        # CASSANDRA-7216 automatically grants permissions on a role to its creator
        if self.cluster.cassandra_version() >= '2.2.0':
            all_permissions.extend(data_resource_creator_permissions('cassandra', '<keyspace ks>'))
            all_permissions.extend(data_resource_creator_permissions('cassandra', '<table ks.cf>'))
            all_permissions.extend(data_resource_creator_permissions('cassandra', '<table ks.cf2>'))
            all_permissions.extend(role_creator_permissions('cassandra', '<role bob>'))
            all_permissions.extend(role_creator_permissions('cassandra', '<role cathy>'))

        self.assertPermissionsListed(all_permissions, cassandra, "LIST ALL PERMISSIONS")

        self.assertPermissionsListed([('cathy', '<all keyspaces>', 'CREATE'),
                                      ('cathy', '<table ks.cf>', 'MODIFY'),
                                      ('cathy', '<table ks.cf2>', 'SELECT')],
                                     cassandra, "LIST ALL PERMISSIONS OF cathy")

        expected_permissions = [('cathy', '<table ks.cf>', 'MODIFY'), ('bob', '<table ks.cf>', 'DROP')]
        if self.cluster.cassandra_version() >= '2.2.0':
            expected_permissions.extend(data_resource_creator_permissions('cassandra', '<table ks.cf>'))
        self.assertPermissionsListed(expected_permissions, cassandra, "LIST ALL PERMISSIONS ON ks.cf NORECURSIVE")

        expected_permissions = [('cathy', '<table ks.cf2>', 'SELECT')]
        # CASSANDRA-7216 automatically grants permissions on a role to its creator
        if self.cluster.cassandra_version() >= '2.2.0':
            expected_permissions.append(('cassandra', '<table ks.cf2>', 'SELECT'))
            expected_permissions.append(('cassandra', '<keyspace ks>', 'SELECT'))
        self.assertPermissionsListed(expected_permissions, cassandra, "LIST SELECT ON ks.cf2")

        self.assertPermissionsListed([('cathy', '<all keyspaces>', 'CREATE'),
                                      ('cathy', '<table ks.cf>', 'MODIFY')],
                                     cassandra, "LIST ALL ON ks.cf OF cathy")

        bob = self.get_session(user='bob', password='12345')
        self.assertPermissionsListed([('bob', '<keyspace ks>', 'ALTER'),
                                      ('bob', '<table ks.cf>', 'DROP'),
                                      ('bob', '<table ks.cf2>', 'MODIFY')],
                                     bob, "LIST ALL PERMISSIONS OF bob")

        assert_unauthorized(bob, "LIST ALL PERMISSIONS", "You are not authorized to view everyone's permissions")

        assert_unauthorized(bob, "LIST ALL PERMISSIONS OF cathy", "You are not authorized to view cathy's permissions")

    def test_type_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy'
        * Connect as cathy
        * Try to create, alter, and drop types. Assert throws Unauthorized
        * Grant CREATE, ALTER, and DROP permissions to cathy, verify she can now do so
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, "CREATE TYPE ks.address (street text, city text)", "User cathy has no CREATE permission on <keyspace ks> or any of its parents")
        assert_unauthorized(cathy, "ALTER TYPE ks.address ADD zip_code int", "User cathy has no ALTER permission on <keyspace ks> or any of its parents")
        assert_unauthorized(cathy, "DROP TYPE ks.address", "User cathy has no DROP permission on <keyspace ks> or any of its parents")

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute("CREATE TYPE ks.address (street text, city text)")
        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO cathy")
        cathy.execute("ALTER TYPE ks.address ADD zip_code int")
        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute("DROP TYPE ks.address")

    def test_restart_node_doesnt_lose_auth_data(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create some new users, grant them permissions
        * Stop the cluster, switch to AllowAll auth, restart the cluster
        * Stop the cluster, switch back to auth, restart the cluster
        * Check all user auth data was preserved
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER philip WITH PASSWORD 'strongpass'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int PRIMARY KEY)")
        cassandra.execute("GRANT ALL ON ks.cf to philip")

        self.cluster.stop()
        config = {'authenticator': 'org.apache.cassandra.auth.AllowAllAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.AllowAllAuthorizer'}
        if self.dtest_config.cassandra_version_from_build >= '4.0':
            config['network_authorizer'] = 'org.apache.cassandra.auth.AllowAllNetworkAuthorizer'
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        self.cluster.stop()
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'}
        if self.dtest_config.cassandra_version_from_build >= '4.0':
            config['network_authorizer'] = 'org.apache.cassandra.auth.CassandraNetworkAuthorizer'
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        philip = self.get_session(user='philip', password='strongpass')
        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, "SELECT * FROM ks.cf", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")
        philip.execute("SELECT * FROM ks.cf")

    @since('3.10')
    def test_auth_metrics(self):
        """
        Success and failure metrics were added to the authentication procedure
        so as to estimate the percentage of authentication attempts that failed.
        @jira_ticket CASSANDRA-10635
        """
        cluster = self.cluster
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': 0}
        self.cluster.set_configuration_options(values=config)
        cluster.set_datadir_count(1)
        cluster.populate(1)
        [node] = cluster.nodelist()
        remove_perf_disable_shared_mem(node)
        cluster.start(wait_for_binary_proto=True)

        with JolokiaAgent(node) as jmx:
            success = jmx.read_attribute(
                make_mbean('metrics', type='Client', name='AuthSuccess'), 'Count')
            failure = jmx.read_attribute(
                make_mbean('metrics', type='Client', name='AuthFailure'), 'Count')

            assert 0 == success
            assert 0 == failure

            try:
                self.get_session(user='cassandra', password='wrong_password')
            except NoHostAvailable as e:
                assert isinstance(list(e.errors.values())[0], AuthenticationFailed)

            self.get_session(user='cassandra', password='cassandra')

            success = jmx.read_attribute(
                make_mbean('metrics', type='Client', name='AuthSuccess'), 'Count')
            failure = jmx.read_attribute(
                make_mbean('metrics', type='Client', name='AuthFailure'), 'Count')

            assert success > 0
            assert failure > 0

    def prepare(self, nodes=1, permissions_validity=0):
        """
        Sets up and launches C* cluster.
        @param nodes Number of nodes in the cluster. Default is 1
        @param permissions_validity The timeout for the permissions cache in ms. Default is 0.
        """
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': permissions_validity}
        if self.dtest_config.cassandra_version_from_build >= '3.0':
            config['enable_materialized_views'] = 'true'
        if self.dtest_config.cassandra_version_from_build >= '4.0':
            config['network_authorizer'] = 'org.apache.cassandra.auth.CassandraNetworkAuthorizer'
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start()

        n = self.cluster.wait_for_any_log('Created default superuser', 25)
        logger.debug("Default role created by " + n.name)

    def get_session(self, node_idx=0, user=None, password=None):
        """
        Connect with a set of credentials to a given node. Connection is not exclusive to that node.
        @param node_idx Initial node to connect to
        @param user User to connect as
        @param password Password to use
        @return Session as user, to specified node
        """
        node = self.cluster.nodelist()[node_idx]
        session = self.patient_cql_connection(node, user=user, password=password)
        return session

    def assertPermissionsListed(self, expected, session, query):
        """
        Issues the given query with the session. Asserts the value of expected permissions matches
        the returned permissions from the query.
        @param expected Expected permissions. Should be a list of permissions in the form [username, resource, permission]
        @param session Session to use
        @param query Query to run
        """
        rows = session.execute(query)
        perms = [(str(r.username), str(r.resource), str(r.permission)) for r in rows]
        assert sorted(expected) == sorted(perms)


def data_resource_creator_permissions(creator, resource):
    """
    Assemble a list of all permissions needed to create data on a given resource
    @param creator User who needs permissions
    @param resource The resource to grant permissions on
    @return A list of permissions for creator on resource
    """
    permissions = []
    for perm in 'SELECT', 'MODIFY', 'ALTER', 'DROP', 'AUTHORIZE':
        permissions.append((creator, resource, perm))
    if resource.startswith("<keyspace "):
        permissions.append((creator, resource, 'CREATE'))
        keyspace = resource[10:-1]
        # also grant the creator of a ks perms on functions in that ks
        for perm in 'CREATE', 'ALTER', 'DROP', 'AUTHORIZE', 'EXECUTE':
            permissions.append((creator, '<all functions in %s>' % keyspace, perm))
    return permissions


@since('2.2')
class TestAuthRoles(Tester):

    Role = None
    cassandra_role = None

    @pytest.fixture(autouse=True)
    def fixture_setup_auth(self, fixture_dtest_setup):
        if fixture_dtest_setup.dtest_config.cassandra_version_from_build >= '4.0':
            fixture_dtest_setup.cluster.set_configuration_options(values={
                'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                'network_authorizer': 'org.apache.cassandra.auth.CassandraNetworkAuthorizer',
                'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
                'permissions_validity_in_ms': 0,
                'roles_validity_in_ms': 0,
                'num_tokens': 1
            })
        else:
            fixture_dtest_setup.cluster.set_configuration_options(values={
                'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
                'permissions_validity_in_ms': 0,
                'roles_validity_in_ms': 0,
                'num_tokens': 1
            })
        fixture_dtest_setup.cluster.populate(1, debug=True).start(wait_for_binary_proto=True, jvm_args=['-XX:-PerfDisableSharedMem'])
        nodes = fixture_dtest_setup.cluster.nodelist()
        fixture_dtest_setup.superuser = fixture_dtest_setup.patient_exclusive_cql_connection(nodes[0], user='cassandra', password='cassandra')

    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        """
        @jira_ticket CASSANDRA-7653
        """
        dtest_setup_overrides = DTestSetupOverrides()
        if dtest_config.cassandra_version_from_build >= '3.0':
            dtest_setup_overrides.cluster_options = ImmutableMapping({'enable_user_defined_functions': 'true',
                                                                      'enable_scripted_user_defined_functions': 'true'})
        else:
            dtest_setup_overrides.cluster_options = ImmutableMapping({'enable_user_defined_functions': 'true'})

        if dtest_config.cassandra_version_from_build >= '4.0':
            self.Role = namedtuple('Role', ['name', 'superuser', 'login', 'options', 'dcs'])
            self.cassandra_role = self.Role('cassandra', True, True, {}, 'ALL')
        else:
            self.Role = namedtuple('Role', ['name', 'superuser', 'login', 'options'])
            self.cassandra_role = self.Role('cassandra', True, True, {})

        return dtest_setup_overrides

    def role(self, name, superuser=False, login=True, options=None):
        options = options or {}
        if self.dtest_config.cassandra_version_from_build >= '4.0':
            dcs = 'n/a' if not login else 'ALL'
            return self.Role(name, superuser, login, options, dcs)
        else:
            return self.Role(name, superuser, login, options)

    def test_create_drop_role(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, check it exists
        * Drop the role, check it is gone
        """
        # self.prepare()
        # cassandra = self.get_session(user='cassandra', password='cassandra')
        role1 = self.role('role1', login=False)

        assert_one(self.superuser, 'LIST ROLES', list(self.cassandra_role))

        self.superuser.execute("CREATE ROLE role1")
        assert_all(self.superuser, "LIST ROLES", [list(self.cassandra_role), list(role1)])

        self.superuser.execute("DROP ROLE role1")
        assert_one(self.superuser, "LIST ROLES", list(self.cassandra_role))

    def test_conditional_create_drop_role(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role twice, using IF NOT EXISTS
        * Check neither query failed, but role was only created once
        * Drop the new role twice, using IF EXISTS
        * Check neither query failed, but only superuser remains
        """
        role1 = self.role('role1', login=False)
        assert_one(self.superuser, "LIST ROLES", list(self.cassandra_role))

        self.superuser.execute("CREATE ROLE IF NOT EXISTS role1")
        self.superuser.execute("CREATE ROLE IF NOT EXISTS role1")
        assert_all(self.superuser, "LIST ROLES", [list(self.cassandra_role), list(role1)])

        self.superuser.execute("DROP ROLE IF EXISTS role1")
        self.superuser.execute("DROP ROLE IF EXISTS role1")
        assert_one(self.superuser, "LIST ROLES", list(self.cassandra_role))

    def test_create_drop_role_validation(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new, nonsuperuser role, 'mike'
        * Connect as mike
        * Try to create a new ROLE as mike, assert throws Unauthorized
        * Create a new role as the superuser
        * Try to DROP the new role as mike, assert throws Unauthorized
        * Try to recreate the new role as superuser, assert throws InvalidRequest
        * Drop the new role.
        * Try to drop the new role again, assert throws InvalidRequest
        """
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        mike = self.get_session(user='mike', password='12345')

        assert_unauthorized(mike,
                            "CREATE ROLE role2",
                            "User mike does not have sufficient privileges to perform the requested operation")
        self.superuser.execute("CREATE ROLE role1")

        assert_unauthorized(mike,
                            "DROP ROLE role1",
                            "User mike does not have sufficient privileges to perform the requested operation")

        assert_invalid(self.superuser, "CREATE ROLE role1", "role1 already exists")
        self.superuser.execute("DROP ROLE role1")
        assert_invalid(self.superuser, "DROP ROLE role1", "role1 doesn't exist")

    def test_role_admin_validation(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a non-login admin role, grant ALL ON ALL ROLES to admin
        * Create a new role, 'mike', GRANT admin to mike
        * Create a new role, 'klaus', with no permissions
        * Connect in two sessions, one as mike, the other as klaus
        * Verify mike can create roles
        * Verify roles without login permission cannot log in
        * Verify ALTER permission is needed to modify a role
        * Verify only superuser roles can set superuser status
        * Verify a role without ALTER can only modify itself
        * Verify mike can drop roles
        * Verify roles without admin cannot drop roles
        """
        self.superuser.execute("CREATE ROLE administrator WITH SUPERUSER = false AND LOGIN = false")
        self.superuser.execute("GRANT ALL ON ALL ROLES TO administrator")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("GRANT administrator TO mike")
        self.superuser.execute("CREATE ROLE klaus WITH PASSWORD = '54321' AND SUPERUSER = false AND LOGIN = true")
        administrator = self.role('administrator', login=False)
        mike = self.role('mike')
        as_mike = self.get_session(user='mike', password='12345')
        klaus = self.role('klaus')
        as_klaus = self.get_session(user='klaus', password='54321')

        # roles with CREATE on ALL ROLES can create roles
        as_mike.execute("CREATE ROLE role1 WITH PASSWORD = '11111' AND LOGIN = false")

        # require ALTER on ALL ROLES or a SPECIFIC ROLE to modify
        self.assert_login_not_allowed('role1', '11111')
        self.superuser.execute("GRANT ALTER on ROLE role1 TO klaus")
        as_klaus.execute("ALTER ROLE role1 WITH LOGIN = true")
        as_mike.execute("ALTER ROLE role1 WITH PASSWORD = '22222'")
        as_role1 = self.get_session(user='role1', password='22222')

        # only superusers can set superuser status
        assert_unauthorized(as_mike, "ALTER ROLE role1 WITH SUPERUSER = true",
                            "Only superusers are allowed to alter superuser status")
        assert_unauthorized(as_mike, "ALTER ROLE mike WITH SUPERUSER = true",
                            "You aren't allowed to alter your own superuser status or that of a role granted to you")

        # roles without necessary permissions cannot create, drop or alter roles except themselves
        assert_unauthorized(as_role1, "CREATE ROLE role2 WITH LOGIN = false",
                            "User role1 does not have sufficient privileges to perform the requested operation")
        assert_unauthorized(as_role1, "ALTER ROLE mike WITH LOGIN = false",
                            "User role1 does not have sufficient privileges to perform the requested operation")
        assert_unauthorized(as_role1, "DROP ROLE mike",
                            "User role1 does not have sufficient privileges to perform the requested operation")
        as_role1.execute("ALTER ROLE role1 WITH PASSWORD = '33333'")

        # roles with roleadmin can drop roles
        as_mike.execute("DROP ROLE role1")
        assert_all(self.superuser, "LIST ROLES", [list(administrator),
                                                  list(self.cassandra_role),
                                                  list(klaus),
                                                  list(mike)])

        # revoking role admin removes its privileges
        self.superuser.execute("REVOKE administrator FROM mike")
        assert_unauthorized(as_mike, "CREATE ROLE role3 WITH LOGIN = false",
                            "User mike does not have sufficient privileges to perform the requested operation")

    def test_creator_of_db_resource_granted_all_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike', grant it CREATE on ALL KS and ALL ROLES
        * Connect as mike
        * Verify that mike is automatically granted permissions on any resource he creates
        """
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("GRANT CREATE ON ALL KEYSPACES TO mike")
        self.superuser.execute("GRANT CREATE ON ALL ROLES TO mike")

        as_mike = self.get_session(user='mike', password='12345')
        # mike should automatically be granted permissions on any resource he creates, i.e. tables or roles
        as_mike.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        as_mike.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        as_mike.execute("CREATE ROLE role1 WITH PASSWORD = '11111' AND SUPERUSER = false AND LOGIN = true")
        as_mike.execute("""CREATE FUNCTION ks.state_function_1(a int, b int)
                        CALLED ON NULL INPUT
                        RETURNS int
                        LANGUAGE javascript
                        AS ' a + b'""")
        as_mike.execute("""CREATE AGGREGATE ks.simple_aggregate_1(int)
                        SFUNC state_function_1
                        STYPE int
                        INITCOND 0""")

        cassandra_permissions = role_creator_permissions('cassandra', '<role mike>')
        mike_permissions = [('mike', '<all roles>', 'CREATE'),
                            ('mike', '<all keyspaces>', 'CREATE')]
        mike_permissions.extend(role_creator_permissions('mike', '<role role1>'))
        mike_permissions.extend(data_resource_creator_permissions('mike', '<keyspace ks>'))
        mike_permissions.extend(data_resource_creator_permissions('mike', '<table ks.cf>'))
        mike_permissions.extend(function_resource_creator_permissions('mike', '<function ks.state_function_1(int, int)>'))
        mike_permissions.extend(function_resource_creator_permissions('mike', '<function ks.simple_aggregate_1(int)>'))

        self.assert_permissions_listed(cassandra_permissions + mike_permissions,
                                       self.superuser,
                                       "LIST ALL PERMISSIONS")

    def test_create_and_grant_roles_with_superuser_status(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a superuser role, a nonsuperuser role, and 'mike'
        * Grant CREATE and AUTHORIZE to mike
        * Connect as mike
        * Verify mike can create new roles, but not grant them superuser
        """
        self.superuser.execute("CREATE ROLE another_superuser WITH SUPERUSER = true AND LOGIN = false")
        self.superuser.execute("CREATE ROLE non_superuser WITH SUPERUSER = false AND LOGIN = false")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        # mike can create and grant any role, except superusers
        self.superuser.execute("GRANT CREATE ON ALL ROLES TO mike")
        self.superuser.execute("GRANT AUTHORIZE ON ALL ROLES TO mike")

        # mike can create roles, but not with superuser status
        # and can grant any role, including those with superuser status
        as_mike = self.get_session(user='mike', password='12345')
        as_mike.execute("CREATE ROLE role1 WITH SUPERUSER = false")
        as_mike.execute("GRANT non_superuser TO role1")
        as_mike.execute("GRANT another_superuser TO role1")
        assert_unauthorized(as_mike, "CREATE ROLE role2 WITH SUPERUSER = true",
                            "Only superusers can create a role with superuser status")
        assert_all(self.superuser, "LIST ROLES OF role1", [list(self.role('another_superuser', superuser=True, login=False)),
                                                           list(self.role('non_superuser', login=False)),
                                                           list(self.role('role1', login=False))])

    def test_drop_and_revoke_roles_with_superuser_status(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create multiple roles, including 'mike'
        * Grant mike DROP and AUTHORIZE
        * Connect as mike
        * Verify mike can drop or revoke any roles, regardless of its superuser status
        """
        self.superuser.execute("CREATE ROLE another_superuser WITH SUPERUSER = true AND LOGIN = false")
        self.superuser.execute("CREATE ROLE non_superuser WITH SUPERUSER = false AND LOGIN = false")
        self.superuser.execute("CREATE ROLE role1 WITH SUPERUSER = false")
        self.superuser.execute("GRANT another_superuser TO role1")
        self.superuser.execute("GRANT non_superuser TO role1")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("GRANT DROP ON ALL ROLES TO mike")
        self.superuser.execute("GRANT AUTHORIZE ON ALL ROLES TO mike")

        # mike can drop and revoke any role, including superusers
        as_mike = self.get_session(user='mike', password='12345')
        as_mike.execute("REVOKE another_superuser FROM role1")
        as_mike.execute("REVOKE non_superuser FROM role1")
        as_mike.execute("DROP ROLE non_superuser")
        as_mike.execute("DROP ROLE role1")

    def test_drop_role_removes_memberships(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create three roles, 'role1', 'role2', and 'mike'
        * Grant role2 to role1, and role1 to mike
        * Assert LIST ROLES of mike returns correct all three roles
        * Drop role2, assert not in LIST ROLES of mike
        * Recreate role2. Grant to role1 again. Verify is in LIST ROLES of mike
        * DROP role1. Verify mike's roles only include mike, but LIST ROLES still includes role2
        """
        self.superuser.execute("CREATE ROLE role1")
        self.superuser.execute("CREATE ROLE role2")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("GRANT role2 TO role1")
        self.superuser.execute("GRANT role1 TO mike")
        mike = self.role('mike')
        role1 = self.role('role1', login=False)
        role2 = self.role('role2', login=False)
        assert_all(self.superuser, "LIST ROLES OF mike", [list(mike), list(role1), list(role2)])

        # drop the role indirectly granted
        self.superuser.execute("DROP ROLE role2")
        assert_all(self.superuser, "LIST ROLES OF mike", [list(mike), list(role1)])

        self.superuser.execute("CREATE ROLE role2")
        self.superuser.execute("GRANT role2 to role1")
        assert_all(self.superuser, "LIST ROLES OF mike", [list(mike), list(role1), list(role2)])
        # drop the directly granted role
        self.superuser.execute("DROP ROLE role1")
        assert_one(self.superuser, "LIST ROLES OF mike", list(mike))
        assert_all(self.superuser, "LIST ROLES", [list(self.cassandra_role), list(mike), list(role2)])

    def test_drop_role_revokes_permissions_granted_on_it(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create ROLES role1, role2, and mike
        * Grant permissions to role1 and role2. Grant role1 and role2 to mike
        * List mike's permissions, verify permissions from role1 and role2 in list
        * Drop role1 and role2
        * Assert mike has no permissions remaining
        """
        self.superuser.execute("CREATE ROLE role1")
        self.superuser.execute("CREATE ROLE role2")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("GRANT ALTER ON ROLE role1 TO mike")
        self.superuser.execute("GRANT AUTHORIZE ON ROLE role2 TO mike")

        self.assert_permissions_listed([("mike", "<role role1>", "ALTER"),
                                        ("mike", "<role role2>", "AUTHORIZE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")

        self.superuser.execute("DROP ROLE role1")
        self.superuser.execute("DROP ROLE role2")
        assert list(self.superuser.execute("LIST ALL PERMISSIONS OF mike")) == []

    def test_grant_revoke_roles(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create ROLES role1, role2, and mike
        * Grant role1 to role2, and role2 to mike
        * Verify the output of LIST ROLES
        * REVOKE various roles, verify the output of LIST ROLES
        """
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE role1")
        self.superuser.execute("CREATE ROLE role2")
        self.superuser.execute("GRANT role1 TO role2")
        self.superuser.execute("GRANT role2 TO mike")
        mike = self.role('mike')
        role1 = self.role('role1', login=False)
        role2 = self.role('role2', login=False)

        assert_all(self.superuser, "LIST ROLES OF role2", [list(role1), list(role2)])
        assert_all(self.superuser, "LIST ROLES OF mike", [list(mike), list(role1), list(role2)])
        assert_all(self.superuser, "LIST ROLES OF mike NORECURSIVE", [list(mike), list(role2)])

        self.superuser.execute("REVOKE role2 FROM mike")
        assert_one(self.superuser, "LIST ROLES OF mike", list(mike))

        self.superuser.execute("REVOKE role1 FROM role2")
        assert_one(self.superuser, "LIST ROLES OF role2", list(role2))

    def test_grant_revoke_role_validation(self):
        """
        * Launch a one node cluster
        * Connect as the default superusers
        * Create ROLE mike
        * Connect as mike
        * Verify granting nonexistent roles to mike throws InvalidRequest
        * Create ROLE john
        * Verify mike cannot grant/revoke roles to/from john without the AUTHORIZE permission
        """
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        as_mike = self.get_session(user='mike', password='12345')

        assert_invalid(self.superuser, "GRANT role1 TO mike", "role1 doesn't exist")
        self.superuser.execute("CREATE ROLE role1")

        assert_invalid(self.superuser, "GRANT role1 TO john", "john doesn't exist")
        assert_invalid(self.superuser, "GRANT role2 TO john", "role2 doesn't exist")

        self.superuser.execute("CREATE ROLE john WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE role2")

        assert_unauthorized(as_mike,
                            "GRANT role2 TO john",
                            "User mike does not have sufficient privileges to perform the requested operation")

        # superusers can always grant roles
        self.superuser.execute("GRANT role1 TO john")
        # but regular users need AUTHORIZE permission on the granted role
        self.superuser.execute("GRANT AUTHORIZE ON ROLE role2 TO mike")
        as_mike.execute("GRANT role2 TO john")

        # same applies to REVOKEing roles
        assert_unauthorized(as_mike,
                            "REVOKE role1 FROM john",
                            "User mike does not have sufficient privileges to perform the requested operation")
        self.superuser.execute("REVOKE role1 FROM john")
        as_mike.execute("REVOKE role2 from john")

    def test_list_roles(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create several different ROLES
        * Verify LIST ROLES for each role is correct
        * Verify a role cannot LIST ROLES for another ROLE without being a superuser, or having the DESCRIBE permission
        """
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE role1")
        self.superuser.execute("CREATE ROLE role2")
        mike = self.role('mike')
        role1 = self.role('role1', login=False)
        role2 = self.role('role2', login=False)

        assert_all(self.superuser, "LIST ROLES", [list(self.cassandra_role), list(mike), list(role1), list(role2)])

        self.superuser.execute("GRANT role1 TO role2")
        self.superuser.execute("GRANT role2 TO mike")

        assert_all(self.superuser, "LIST ROLES OF role2", [list(role1), list(role2)])
        assert_all(self.superuser, "LIST ROLES OF mike", [list(mike), list(role1), list(role2)])
        assert_all(self.superuser, "LIST ROLES OF mike NORECURSIVE", [list(mike), list(role2)])

        as_mike = self.get_session(user='mike', password='12345')
        assert_unauthorized(as_mike,
                            "LIST ROLES OF cassandra",
                            "You are not authorized to view roles granted to cassandra")

        assert_all(as_mike, "LIST ROLES", [list(mike), list(role1), list(role2)])
        assert_all(as_mike, "LIST ROLES OF mike", [list(mike), list(role1), list(role2)])
        assert_all(as_mike, "LIST ROLES OF mike NORECURSIVE", [list(mike), list(role2)])
        assert_all(as_mike, "LIST ROLES OF role2", [list(role1), list(role2)])

        # without SELECT permission on the root level roles resource, LIST ROLES with no OF
        # returns only the roles granted to the user. With it, it includes all roles.
        assert_all(as_mike, "LIST ROLES", [list(mike), list(role1), list(role2)])
        self.superuser.execute("GRANT DESCRIBE ON ALL ROLES TO mike")
        assert_all(as_mike, "LIST ROLES", [list(self.cassandra_role), list(mike), list(role1), list(role2)])

    def test_grant_revoke_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Creates ROLES role1 and mike.
        * Grant ALL on ks.cf to role1, grant role1 to mike
        * Connect as mike.
        * Verify mike can read/write to ks.cf
        * Revoke role1 from mike, verify mike can no longer use ks.cf
        * Restore role1 to mike, but revoke role1's permissions
        * Verify mike can no longer use ks.cf
        """
        self.superuser.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        self.superuser.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE role1")
        self.superuser.execute("GRANT ALL ON table ks.cf TO role1")
        self.superuser.execute("GRANT role1 TO mike")

        as_mike = self.get_session(user='mike', password='12345')
        as_mike.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        assert_one(as_mike, "SELECT * FROM ks.cf", [0, 0])

        self.superuser.execute("REVOKE role1 FROM mike")
        assert_unauthorized(as_mike,
                            "INSERT INTO ks.cf (id, val) VALUES (0, 0)",
                            "mike has no MODIFY permission on <table ks.cf> or any of its parents")

        self.superuser.execute("GRANT role1 TO mike")
        self.superuser.execute("REVOKE ALL ON ks.cf FROM role1")

        assert_unauthorized(as_mike,
                            "INSERT INTO ks.cf (id, val) VALUES (0, 0)",
                            "mike has no MODIFY permission on <table ks.cf> or any of its parents")

    def test_filter_granted_permissions_by_resource_type(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a ks, table, function, and aggregate
        * Create ROLES mike and role1
        * Grant ALL permissions to mike for each resource. Verify they show up in LIST ALL PERMISSIONS
        * Verify you can't selectively grant invalid permissions for a given resource. ex: CREATE on an existing table
        """
        self.superuser.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        self.superuser.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE role1 WITH SUPERUSER = false AND LOGIN = false")
        self.superuser.execute("CREATE FUNCTION ks.state_func(a int, b int) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'a+b'")
        self.superuser.execute("CREATE AGGREGATE ks.agg_func(int) SFUNC state_func STYPE int")

        # GRANT ALL ON ALL KEYSPACES grants Permission.ALL_DATA

        # GRANT ALL ON KEYSPACE grants Permission.ALL_DATA
        self.superuser.execute("GRANT ALL ON KEYSPACE ks TO mike")
        self.assert_permissions_listed([("mike", "<keyspace ks>", "CREATE"),
                                        ("mike", "<keyspace ks>", "ALTER"),
                                        ("mike", "<keyspace ks>", "DROP"),
                                        ("mike", "<keyspace ks>", "SELECT"),
                                        ("mike", "<keyspace ks>", "MODIFY"),
                                        ("mike", "<keyspace ks>", "AUTHORIZE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE ALL ON KEYSPACE ks FROM mike")

        # GRANT ALL ON TABLE does not include CREATE (because the table must already be created before the GRANT)
        self.superuser.execute("GRANT ALL ON ks.cf TO MIKE")
        self.assert_permissions_listed([("mike", "<table ks.cf>", "ALTER"),
                                        ("mike", "<table ks.cf>", "DROP"),
                                        ("mike", "<table ks.cf>", "SELECT"),
                                        ("mike", "<table ks.cf>", "MODIFY"),
                                        ("mike", "<table ks.cf>", "AUTHORIZE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE ALL ON ks.cf FROM mike")
        assert_invalid(self.superuser,
                       "GRANT CREATE ON ks.cf TO MIKE",
                       "Resource type DataResource does not support any of the requested permissions",
                       SyntaxException)

        # GRANT ALL ON ALL ROLES includes SELECT & CREATE on the root level roles resource
        self.superuser.execute("GRANT ALL ON ALL ROLES TO mike")
        self.assert_permissions_listed([("mike", "<all roles>", "CREATE"),
                                        ("mike", "<all roles>", "ALTER"),
                                        ("mike", "<all roles>", "DROP"),
                                        ("mike", "<all roles>", "DESCRIBE"),
                                        ("mike", "<all roles>", "AUTHORIZE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE ALL ON ALL ROLES FROM mike")
        assert_invalid(self.superuser,
                       "GRANT SELECT ON ALL ROLES TO MIKE",
                       "Resource type RoleResource does not support any of the requested permissions",
                       SyntaxException)

        # GRANT ALL ON ROLE does not include CREATE (because the role must already be created before the GRANT)
        self.superuser.execute("GRANT ALL ON ROLE role1 TO mike")
        self.assert_permissions_listed([("mike", "<role role1>", "ALTER"),
                                        ("mike", "<role role1>", "DROP"),
                                        ("mike", "<role role1>", "AUTHORIZE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        assert_invalid(self.superuser,
                       "GRANT CREATE ON ROLE role1 TO MIKE",
                       "Resource type RoleResource does not support any of the requested permissions",
                       SyntaxException)
        self.superuser.execute("REVOKE ALL ON ROLE role1 FROM mike")

        # GRANT ALL ON ALL FUNCTIONS or on all functions for a single keyspace includes AUTHORIZE, EXECUTE and USAGE
        self.superuser.execute("GRANT ALL ON ALL FUNCTIONS TO mike")
        self.assert_permissions_listed([("mike", "<all functions>", "CREATE"),
                                        ("mike", "<all functions>", "ALTER"),
                                        ("mike", "<all functions>", "DROP"),
                                        ("mike", "<all functions>", "AUTHORIZE"),
                                        ("mike", "<all functions>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE ALL ON ALL FUNCTIONS FROM mike")

        self.superuser.execute("GRANT ALL ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        self.assert_permissions_listed([("mike", "<all functions in ks>", "CREATE"),
                                        ("mike", "<all functions in ks>", "ALTER"),
                                        ("mike", "<all functions in ks>", "DROP"),
                                        ("mike", "<all functions in ks>", "AUTHORIZE"),
                                        ("mike", "<all functions in ks>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE ALL ON ALL FUNCTIONS IN KEYSPACE ks FROM mike")

        # GRANT ALL ON FUNCTION includes AUTHORIZE, EXECUTE and USAGE for scalar functions and
        # AUTHORIZE and EXECUTE for aggregates
        self.superuser.execute("GRANT ALL ON FUNCTION ks.state_func(int, int) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.state_func(int, int)>", "ALTER"),
                                        ("mike", "<function ks.state_func(int, int)>", "DROP"),
                                        ("mike", "<function ks.state_func(int, int)>", "AUTHORIZE"),
                                        ("mike", "<function ks.state_func(int, int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE ALL ON FUNCTION ks.state_func(int, int) FROM mike")

        self.superuser.execute("GRANT ALL ON FUNCTION ks.agg_func(int) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.agg_func(int)>", "ALTER"),
                                        ("mike", "<function ks.agg_func(int)>", "DROP"),
                                        ("mike", "<function ks.agg_func(int)>", "AUTHORIZE"),
                                        ("mike", "<function ks.agg_func(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE ALL ON FUNCTION ks.agg_func(int) FROM mike")

    def test_list_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a table, ks.cf
        * Create ROLES role1, role2, and mike
        * Grant various permissions to roles
        * Verify they propagate appropriately.
        """
        self.superuser.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        self.superuser.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE role1")
        self.superuser.execute("CREATE ROLE role2")
        self.superuser.execute("GRANT SELECT ON table ks.cf TO role1")
        self.superuser.execute("GRANT ALTER ON table ks.cf TO role2")
        self.superuser.execute("GRANT MODIFY ON table ks.cf TO mike")
        self.superuser.execute("GRANT ALTER ON ROLE role1 TO role2")
        self.superuser.execute("GRANT role1 TO role2")
        self.superuser.execute("GRANT role2 TO mike")

        expected_permissions = [("mike", "<table ks.cf>", "MODIFY"),
                                ("role1", "<table ks.cf>", "SELECT"),
                                ("role2", "<table ks.cf>", "ALTER"),
                                ("role2", "<role role1>", "ALTER")]
        expected_permissions.extend(data_resource_creator_permissions('cassandra', '<keyspace ks>'))
        expected_permissions.extend(data_resource_creator_permissions('cassandra', '<table ks.cf>'))
        expected_permissions.extend(role_creator_permissions('cassandra', '<role mike>'))
        expected_permissions.extend(role_creator_permissions('cassandra', '<role role1>'))
        expected_permissions.extend(role_creator_permissions('cassandra', '<role role2>'))

        self.assert_permissions_listed(expected_permissions, self.superuser, "LIST ALL PERMISSIONS")

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF role1")

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER"),
                                        ("role2", "<role role1>", "ALTER")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF role2")

        self.assert_permissions_listed([("cassandra", "<role role1>", "ALTER"),
                                        ("cassandra", "<role role1>", "DROP"),
                                        ("cassandra", "<role role1>", "AUTHORIZE"),
                                        ("role2", "<role role1>", "ALTER")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS ON ROLE role1")
        # we didn't specifically grant DROP on role1, so only it's creator should have it
        self.assert_permissions_listed([("cassandra", "<role role1>", "DROP")],
                                       self.superuser,
                                       "LIST DROP PERMISSION ON ROLE role1")
        # but we did specifically grant ALTER role1 to role2
        # so that should be listed whether we include an OF clause or not
        self.assert_permissions_listed([("cassandra", "<role role1>", "ALTER"),
                                        ("role2", "<role role1>", "ALTER")],
                                       self.superuser,
                                       "LIST ALTER PERMISSION ON ROLE role1")
        self.assert_permissions_listed([("role2", "<role role1>", "ALTER")],
                                       self.superuser,
                                       "LIST ALTER PERMISSION ON ROLE role1 OF role2")
        # make sure ALTER on role2 is excluded properly when OF is for another role
        self.superuser.execute("CREATE ROLE role3 WITH SUPERUSER = false AND LOGIN = false")
        assert list(self.superuser.execute("LIST ALTER PERMISSION ON ROLE role1 OF role3")) == []

        # now check users can list their own permissions
        as_mike = self.get_session(user='mike', password='12345')
        self.assert_permissions_listed([("mike", "<table ks.cf>", "MODIFY"),
                                        ("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER"),
                                        ("role2", "<role role1>", "ALTER")],
                                       as_mike,
                                       "LIST ALL PERMISSIONS OF mike")

    def test_list_permissions_validation(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create several roles, including 'mike'
        * Grant some roles to mike
        * Verify mike can see the permissions of his role, and the roles granted to him
        * Verify mike cannot see all permissions, or those of roles not granted to him
        """
        self.superuser.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        self.superuser.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE john WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE role1")
        self.superuser.execute("CREATE ROLE role2")

        self.superuser.execute("GRANT SELECT ON table ks.cf TO role1")
        self.superuser.execute("GRANT ALTER ON table ks.cf TO role2")
        self.superuser.execute("GRANT MODIFY ON table ks.cf TO john")

        self.superuser.execute("GRANT role1 TO role2")
        self.superuser.execute("GRANT role2 TO mike")

        as_mike = self.get_session(user='mike', password='12345')

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER")],
                                       as_mike,
                                       "LIST ALL PERMISSIONS OF role2")

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT")],
                                       as_mike,
                                       "LIST ALL PERMISSIONS OF role1")

        assert_unauthorized(as_mike,
                            "LIST ALL PERMISSIONS",
                            "You are not authorized to view everyone's permissions")
        assert_unauthorized(as_mike,
                            "LIST ALL PERMISSIONS OF john",
                            "You are not authorized to view john's permissions")

    def test_role_caching_authenticated_user(self):
        """
        This test is to show that the role caching in AuthenticatedUser
        works correctly and revokes the roles from a logged in user
        * Launch a one node cluster, with a roles cache of 2s
        * Connect as the default superuser
        * Create ROLES role1 and mike
        * Grant permissions to role1, and role1 to mike
        * Verify mike can perform expected operations
        * Revoke role1, and thus read permissions, from mike.
        * Try reading as mike, and verify that eventually the cache expires and it fails.
        """
        # on older versions the cache is not initialized until used,
        # we need the MBean registered so let's use it
        if self.dtest_config.cassandra_version_from_build < '4.0':
            self.superuser.execute("LIST ROLES")

        mbean = make_mbean('auth', type='RolesCache')
        with JolokiaAgent(self.cluster.nodelist()[0]) as jmx:
            jmx.write_attribute(mbean, 'Validity', 2000)

        self.setup_table()
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE role1")
        self.superuser.execute("GRANT ALL ON table ks.t1 TO role1")
        self.superuser.execute("GRANT role1 TO mike")

        as_mike = self.get_session(user='mike', password='12345')
        as_mike.execute("INSERT INTO ks.t1 (k, v) VALUES (0, 0)")

        assert_one(as_mike, "SELECT * FROM ks.t1", [0, 0])

        self.superuser.execute("REVOKE role1 FROM mike")
        # mike should retain permissions until the cache expires
        unauthorized = None
        cnt = 0
        while not unauthorized and cnt < 20:
            try:
                as_mike.execute("SELECT * FROM ks.t1")
                cnt += 1
                time.sleep(.5)
            except Unauthorized as e:
                unauthorized = e

        assert unauthorized is not None

    def test_drop_non_existent_role_should_not_update_cache(self):
        """
        This test checks that dropping a nonexistent role doesn't
        create an entry in the auth cache.
        * Launch a one node cluster, with a roles cache of 10s
        * Connect as the default superuser
        * Run DROP ROLE IF EXISTS on nonexistent role
        * Create that role
        * Connect as the new role, and ensure it can issue reads.
        @jira_ticket CASSANDRA-9189
        """

        # on older versions the cache is not initialized until used,
        # we need the MBean registered so let's use it
        if self.dtest_config.cassandra_version_from_build < '4.0':
            self.superuser.execute("LIST ROLES")

        # The su status check during DROP ROLE IF EXISTS <role>
        # should not cause a non-existent role to be cached (CASSANDRA-9189)
        mbean = make_mbean('auth', type='RolesCache')
        with JolokiaAgent(self.cluster.nodelist()[0]) as jmx:
            jmx.write_attribute(mbean, 'Validity', 10000)

        self.setup_table()


        # Dropping a role which doesn't exist should be a no-op. If we cache the fact
        # that the role doesn't exist though, subsequent authz attempts which should
        # succeed will fail due to the erroneous cache entry
        self.superuser.execute("DROP ROLE IF EXISTS mike")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("GRANT ALL ON ks.t1 TO mike")

        as_mike = self.get_session(user='mike', password='12345')
        as_mike.execute("SELECT * FROM ks.t1")

    def test_prevent_circular_grants(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create several roles
        * Verify we cannot grant roles in circular chain
        """
        self.superuser.execute("CREATE ROLE mike")
        self.superuser.execute("CREATE ROLE role1")
        self.superuser.execute("CREATE ROLE role2")
        self.superuser.execute("GRANT role2 to role1")
        self.superuser.execute("GRANT role1 TO mike")
        assert_invalid(self.superuser,
                       "GRANT mike TO role1",
                       "mike is a member of role1",
                       InvalidRequest)
        assert_invalid(self.superuser,
                       "GRANT mike TO role2",
                       "mike is a member of role2",
                       InvalidRequest)

    def test_create_user_as_alias_for_create_role(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Attempt to create roles using "CREATE USER". Verify still works
        """
        self.superuser.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        assert_one(self.superuser, "LIST ROLES OF mike", list(self.role('mike')))

        self.superuser.execute("CREATE USER super_user WITH PASSWORD '12345' SUPERUSER")
        assert_one(self.superuser, "LIST ROLES OF super_user", list(self.role('super_user', superuser=True)))

    def test_role_name(self):
        """
        Simple test to verify the behaviour of quoting when creating roles & users
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify that CREATE ROLE is not case sensitive, when identifiers are unquoted
        * Verify that CREATE ROLE is case sensitive, when identifiers are quoted
        * Verify that CREATE USER is always case sensitive
        @jira_ticket CASSANDRA-10394
        """
        # unquoted identifiers and unreserved keyword do not preserve case
        # count
        self.superuser.execute("CREATE ROLE ROLE1 WITH PASSWORD = '12345' AND LOGIN = true")
        self.assert_unauthenticated('ROLE1', '12345')

        self.superuser.execute("CREATE ROLE COUNT WITH PASSWORD = '12345' AND LOGIN = true")
        self.assert_unauthenticated('COUNT', '12345')
        self.get_session(user='count', password='12345')

        # string literals and quoted names do preserve case
        self.superuser.execute("CREATE ROLE 'ROLE2' WITH PASSWORD = '12345' AND LOGIN = true")
        self.get_session(user='ROLE2', password='12345')
        self.assert_unauthenticated('Role2', '12345')

        self.superuser.execute("""CREATE ROLE "ROLE3" WITH PASSWORD = '12345' AND LOGIN = true""")
        self.get_session(user='ROLE3', password='12345')
        self.assert_unauthenticated('Role3', '12345')

        # when using legacy USER syntax, both unquoted identifiers and string literals preserve case
        self.superuser.execute("CREATE USER USER1 WITH PASSWORD '12345'")
        self.get_session(user='USER1', password='12345')
        self.assert_unauthenticated('User1', '12345')

        self.superuser.execute("CREATE USER 'USER2' WITH PASSWORD '12345'")
        self.get_session(user='USER2', password='12345')
        self.assert_unauthenticated('User2', '12345')

    def test_role_requires_login_privilege_to_authenticate(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user,'mike', with the login privilege
        * Connect as mike
        * Remove mike's login privilege. Verify mike cannot login
        * Restore mike's login privilege. Verify mike can connect again.
        """
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        assert_one(self.superuser, "LIST ROLES OF mike", list(self.role('mike')))
        self.get_session(user='mike', password='12345')

        self.superuser.execute("ALTER ROLE mike WITH LOGIN = false")
        assert_one(self.superuser, "LIST ROLES OF mike", list(self.role('mike', login=False)))
        self.assert_login_not_allowed('mike', '12345')

        self.superuser.execute("ALTER ROLE mike WITH LOGIN = true")
        assert_one(self.superuser, "LIST ROLES OF mike", list(self.role('mike', login=True)))
        self.get_session(user='mike', password='12345')

    def test_roles_do_not_inherit_login_privilege(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user who can login, and 'mike', who cannot
        * Grant the other user to mike.
        * Verify mike still cannot log in.
        """
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = false")
        self.superuser.execute("CREATE ROLE with_login WITH PASSWORD = '54321' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("GRANT with_login to mike")
        mike = self.role('mike', login=False)
        with_login = self.role('with_login')

        assert_all(self.superuser, "LIST ROLES OF mike", [list(mike), list(with_login)])
        assert_one(self.superuser, "LIST ROLES OF with_login", list(with_login))

        self.assert_login_not_allowed("mike", "12345")

    def test_role_requires_password_to_login(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user, 'mike', with the login privilege but no password
        * Verify we cannot connect as mike
        * Alter mike and add a password
        * Verify mike can now connect
        """
        self.superuser.execute("CREATE ROLE mike WITH SUPERUSER = false AND LOGIN = true")
        self.assert_unauthenticated('mike', None)
        self.superuser.execute("ALTER ROLE mike WITH PASSWORD = '12345'")
        self.get_session(user='mike', password='12345')

    def test_superuser_status_is_inherited(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a superuser role, and 'mike'.
        * Connect as mike
        * Verify that mike does not have permissions.
        * Grant the superuser role to mike.
        * Verify that mike now has all permissions
        """
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("CREATE ROLE db_admin WITH SUPERUSER = true")

        as_mike = self.get_session(user='mike', password='12345')
        assert_unauthorized(as_mike,
                            "CREATE ROLE another_role WITH SUPERUSER = false AND LOGIN = false",
                            "User mike does not have sufficient privileges to perform the requested operation")

        self.superuser.execute("GRANT db_admin TO mike")
        as_mike.execute("CREATE ROLE another_role WITH SUPERUSER = false AND LOGIN = false")
        assert_all(as_mike, "LIST ROLES", [list(self.role('another_role', superuser=False, login=False)),
                                           list(self.cassandra_role),
                                           list(self.role('db_admin', superuser=True, login=False)),
                                           list(self.role('mike'))])

    def test_list_users_considers_inherited_superuser_status(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a superuser role, and 'mike'
        * Grant the superuser role to mike
        * Verify that LIST USERS shows mike as a superuser, even though that privilege is granted indirectly
        """
        self.superuser.execute("CREATE ROLE db_admin WITH SUPERUSER = true")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        self.superuser.execute("GRANT db_admin TO mike")
        if self.dtest_config.cassandra_version_from_build >= '4.0':
            assert_all(self.superuser, "LIST USERS", [['cassandra', True, 'ALL'], ['mike', True, 'ALL']])
        else:
            assert_all(self.superuser, "LIST USERS", [['cassandra', True], ['mike', True]])

    # UDF permissions tests # TODO move to separate fixture & refactor this + auth_test.py
    def test_grant_revoke_udf_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create two UDFs
        * Selectively grant and revoke each possible UDF permission to mike, and verify those operations worked
        """
        self.setup_table()
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        self.superuser.execute("CREATE FUNCTION ks.\"plusOne\" ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")

        # grant / revoke on a specific function
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.\"plusOne\"(int) TO mike")

        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE"),
                                        ("mike", "<function ks.plusOne(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE ALL PERMISSIONS ON FUNCTION ks.plus_one(int) FROM mike")
        self.assert_permissions_listed([("mike", "<function ks.plusOne(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE EXECUTE PERMISSION ON FUNCTION ks.\"plusOne\"(int) FROM mike")
        self.assert_no_permissions(self.superuser, "LIST ALL PERMISSIONS OF mike")

        # grant / revoke on all functions in a given keyspace
        self.superuser.execute("GRANT EXECUTE PERMISSION ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        self.assert_permissions_listed([("mike", "<all functions in ks>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE EXECUTE PERMISSION ON ALL FUNCTIONS IN KEYSPACE ks FROM mike")
        self.assert_no_permissions(self.superuser, "LIST ALL PERMISSIONS OF mike")

        # grant / revoke on all (non-builtin) functions
        self.superuser.execute("GRANT EXECUTE PERMISSION ON ALL FUNCTIONS TO mike")
        self.assert_permissions_listed([("mike", "<all functions>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE EXECUTE PERMISSION ON ALL FUNCTIONS FROM mike")
        self.assert_no_permissions(self.superuser, "LIST ALL PERMISSIONS OF mike")

    def test_grant_revoke_are_idempotent(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create a UDF
        * Issue multiple grants and revokes of permissions for the UDF to mike
        * Verify the grants/revokes are idempotent, and were successful
        """
        self.setup_table()
        self.superuser.execute("CREATE ROLE mike")
        self.superuser.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE EXECUTE ON FUNCTION ks.plus_one(int) FROM mike")
        self.assert_no_permissions(self.superuser, "LIST ALL PERMISSIONS OF mike")
        self.superuser.execute("REVOKE EXECUTE ON FUNCTION ks.plus_one(int) FROM mike")
        self.assert_no_permissions(self.superuser, "LIST ALL PERMISSIONS OF mike")

    def test_function_resource_hierarchy_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create two UDFs
        * Verify that granting mike EXECUTE on one function does not affect the other
        * Verify that granting mike EXECUTE on the keyspace applies to all UDFs in that keyspace
        * Verify that revoking EXECUTE for mike on the keyspace does not affect his function specific permissions
        * Verify that granting mike EXECUTE ON ALL FUNCTIONS applies to all UDFs
        * Verify that revoking EXECUTE ON ALL FUNCTIONS for mike does not affect his function specific permissions
        * Check that if mike has keyspace level EXECUTE, that granting/revoking function specific permissions doesn't affect the keyspace level permissions
        """
        self.setup_table()
        self.superuser.execute("INSERT INTO ks.t1 (k,v) values (1,1)")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("GRANT SELECT ON ks.t1 TO mike")
        self.superuser.execute("CREATE FUNCTION ks.func_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        self.superuser.execute("CREATE FUNCTION ks.func_two ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")

        as_mike = self.get_session(user='mike', password='12345')
        select_one = "SELECT k, v, ks.func_one(v) FROM ks.t1 WHERE k = 1"
        select_two = "SELECT k, v, ks.func_two(v) FROM ks.t1 WHERE k = 1"

        # grant EXECUTE on only one of the two functions
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.func_one(int) TO mike")
        as_mike.execute(select_one)
        assert_unauthorized(as_mike, select_two,
                            r"User mike has no EXECUTE permission on <function ks.func_two\(int\)> or any of its parents")
        # granting EXECUTE on all of the parent keyspace's should enable mike to use both functions
        self.superuser.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        as_mike.execute(select_one)
        as_mike.execute(select_two)
        # revoke the keyspace level privilege and verify that the function specific perms are unaffected
        self.superuser.execute("REVOKE EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks FROM mike")
        as_mike.execute(select_one)
        assert_unauthorized(as_mike, select_two,
                            r"User mike has no EXECUTE permission on <function ks.func_two\(int\)> or any of its parents")
        # now check that EXECUTE on ALL FUNCTIONS works in the same way
        self.superuser.execute("GRANT EXECUTE ON ALL FUNCTIONS TO mike")
        as_mike.execute(select_one)
        as_mike.execute(select_two)
        self.superuser.execute("REVOKE EXECUTE ON ALL FUNCTIONS FROM mike")
        as_mike.execute(select_one)
        assert_unauthorized(as_mike, select_two,
                            r"User mike has no EXECUTE permission on <function ks.func_two\(int\)> or any of its parents")
        # finally, check that revoking function level permissions doesn't affect root/keyspace level perms
        self.superuser.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        self.superuser.execute("REVOKE EXECUTE ON FUNCTION ks.func_one(int) FROM mike")
        as_mike.execute(select_one)
        as_mike.execute(select_two)
        self.superuser.execute("REVOKE EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks FROM mike")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.func_one(int) TO mike")
        self.superuser.execute("GRANT EXECUTE ON ALL FUNCTIONS TO mike")
        as_mike.execute(select_one)
        as_mike.execute(select_two)
        self.superuser.execute("REVOKE EXECUTE ON FUNCTION ks.func_one(int) FROM mike")
        as_mike.execute(select_one)
        as_mike.execute(select_two)

    def test_udf_permissions_validation(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create a new UDF
        * Verify mike can only modify the UDF iff he has the ALTER permission on the function's keyspace
        * Verify mike can grant permissions on the UDF iff he has AUTHORIZE and EXECUTE
        * Verify mike can drop the UDF iff he has the DROP permission
        * Verify mike can create a new UDF iff he has the CREATE permission
        """
        self.setup_table()
        self.superuser.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        as_mike = self.get_session(user='mike', password='12345')

        # can't replace an existing function without ALTER permission on the parent ks
        cql = "CREATE OR REPLACE FUNCTION ks.plus_one( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript as '1 + input'"
        assert_unauthorized(as_mike, cql,
                            r"User mike has no ALTER permission on <function ks.plus_one\(int\)> or any of its parents")
        self.superuser.execute("GRANT ALTER ON FUNCTION ks.plus_one(int) TO mike")
        as_mike.execute(cql)

        # can't grant permissions on a function without AUTHORIZE (and without the grantor having EXECUTE themself)
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        self.superuser.execute("CREATE ROLE role1")
        cql = "GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO role1"
        assert_unauthorized(as_mike, cql,
                            r"User mike has no AUTHORIZE permission on <function ks.plus_one\(int\)> or any of its parents")
        self.superuser.execute("GRANT AUTHORIZE ON FUNCTION ks.plus_one(int) TO mike")
        as_mike.execute(cql)
        # now revoke AUTHORIZE from mike
        self.superuser.execute("REVOKE AUTHORIZE ON FUNCTION ks.plus_one(int) FROM mike")
        cql = "REVOKE EXECUTE ON FUNCTION ks.plus_one(int) FROM role1"
        assert_unauthorized(as_mike, cql,
                            r"User mike has no AUTHORIZE permission on <function ks.plus_one\(int\)> or any of its parents")
        self.superuser.execute("GRANT AUTHORIZE ON FUNCTION ks.plus_one(int) TO mike")
        as_mike.execute(cql)

        # can't drop a function without DROP
        cql = "DROP FUNCTION ks.plus_one(int)"
        assert_unauthorized(as_mike, cql,
                            r"User mike has no DROP permission on <function ks.plus_one\(int\)> or any of its parents")
        self.superuser.execute("GRANT DROP ON FUNCTION ks.plus_one(int) TO mike")
        as_mike.execute(cql)

        # DROP IF EXISTS on a non-existent function should return silently, DROP on a non-existent function
        # behaves like DROP TABLE and returns an "Unconfigured XXX" error
        cql = "DROP FUNCTION IF EXISTS ks.no_such_function(int,int)"
        as_mike.execute(cql)

        cql = "DROP FUNCTION ks.no_such_function(int,int)"
        assert_invalid(as_mike, cql,
                       r"Unconfigured function ks.no_such_function\(int,int\)",
                       InvalidRequest)

        # can't create a new function without CREATE on the parent keyspace's collection of functions
        cql = "CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'"
        assert_unauthorized(as_mike, cql,
                            "User mike has no CREATE permission on <all functions in ks> or any of its parents")
        self.superuser.execute("GRANT CREATE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        as_mike.execute(cql)

    def test_drop_role_cleans_up_udf_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create a UDF
        * Grant mike permissions for that UDF
        * Verify that if mike is dropped and recreated, he has no permissions for the UDF
        """
        self.setup_table()
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        self.superuser.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        self.superuser.execute("GRANT EXECUTE ON ALL FUNCTIONS TO mike")

        self.assert_permissions_listed([("mike", "<all functions>", "EXECUTE"),
                                        ("mike", "<all functions in ks>", "EXECUTE"),
                                        ("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")

        # drop and recreate the role to ensure permissions are cleared
        self.superuser.execute("DROP ROLE mike")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.assert_no_permissions(self.superuser, "LIST ALL PERMISSIONS OF mike")

    def test_drop_function_and_keyspace_cleans_up_udf_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create a UDF
        * Grant mike permissions on the UDF, and on the keyspace of the UDF
        * Verify dropping the UDF removes mike's permissions for the UDF
        * Verify dropping the keyspace removes the rest of mike's permissions
        """
        self.setup_table()
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        self.superuser.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")

        self.assert_permissions_listed([("mike", "<all functions in ks>", "EXECUTE"),
                                        ("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")

        # drop the function
        self.superuser.execute("DROP FUNCTION ks.plus_one")
        self.assert_permissions_listed([("mike", "<all functions in ks>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        # drop the keyspace
        self.superuser.execute("DROP KEYSPACE ks")
        self.assert_no_permissions(self.superuser, "LIST ALL PERMISSIONS OF mike")

    def test_udf_with_overloads_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create two overloaded UDFs (same name, different parameters)
        * Verify that granting/revoking permissions on one function, doesn't affect the other
        """
        self.setup_table()
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        self.superuser.execute("CREATE FUNCTION ks.plus_one ( input double ) CALLED ON NULL INPUT RETURNS double LANGUAGE javascript AS 'input + 1'")

        # grant execute on one variant
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")

        # and now on the other
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(double) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE"),
                                        ("mike", "<function ks.plus_one(double)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")

        # revoke permissions on one of the functions only
        self.superuser.execute("REVOKE EXECUTE ON FUNCTION ks.plus_one(double) FROM mike")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")

        # drop the function that the role has no permissions on
        self.superuser.execute("DROP FUNCTION ks.plus_one(double)")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")

        # finally, drop the function that the role does have permissions on
        self.superuser.execute("DROP FUNCTION ks.plus_one(int)")
        self.assert_no_permissions(self.superuser, "LIST ALL PERMISSIONS OF mike")

    def test_drop_keyspace_cleans_up_function_level_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike', a UDF and a UDA which uses for its state function
        * Grant mike permissions to the UDF & UDA
        * Drop the keyspace containing the UDF & UDA
        * Verify mike has no permissions
        """
        self.setup_table()
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("CREATE FUNCTION ks.state_func (a int, b int) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'a + b'")
        self.superuser.execute("CREATE AGGREGATE ks.agg_func (int) SFUNC state_func STYPE int")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.state_func(int, int) TO mike")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.agg_func(int) TO mike")

        self.assert_permissions_listed([("mike", "<function ks.state_func(int, int)>", "EXECUTE"),
                                        ("mike", "<function ks.agg_func(int)>", "EXECUTE")],
                                       self.superuser,
                                       "LIST ALL PERMISSIONS OF mike")
        # drop the keyspace
        self.superuser.execute("DROP KEYSPACE ks")
        self.assert_no_permissions(self.superuser, "LIST ALL PERMISSIONS OF mike")

    def test_udf_permissions_in_selection(self):
        """
        Verify EXECUTE permission works in a SELECT when UDF is one of the columns requested
        """
        self.verify_udf_permissions("SELECT k, v, ks.plus_one(v) FROM ks.t1 WHERE k = 1")

    def test_udf_permissions_in_select_where_clause(self):
        """
        Verify EXECUTE permission works in a SELECT when UDF is in the WHERE clause
        """
        self.verify_udf_permissions("SELECT k, v FROM ks.t1 WHERE k = ks.plus_one(0)")

    def test_udf_permissions_in_insert(self):
        """
        Verify EXECUTE permission works in an INSERT when UDF is in the VALUES
        """
        self.verify_udf_permissions("INSERT INTO ks.t1 (k, v) VALUES (1, ks.plus_one(1))")

    def test_udf_permissions_in_update(self):
        """
        Verify EXECUTE permission works in an UPDATE when UDF is in the SET and WHERE clauses
        """
        self.verify_udf_permissions("UPDATE ks.t1 SET v = ks.plus_one(2) WHERE k = ks.plus_one(0)")

    def test_udf_permissions_in_delete(self):
        """
        Verify EXECUTE permission works in a DELETE when UDF is in the WHERE clause
        """
        self.verify_udf_permissions("DELETE FROM ks.t1 WHERE k = ks.plus_one(0)")

    def verify_udf_permissions(self, cql):
        """
        Reusable test code. Runs the same logic with different statement types.
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a role and grant it EXECUTE permissions on an UDF.
        * Verify the passed in cql works iff EXECUTE is granted to the role
        @param cql The statement to verify. Should contain the UDF ks.plus_one
        """
        self.setup_table()
        self.superuser.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("GRANT ALL PERMISSIONS ON ks.t1 TO mike")
        self.superuser.execute("INSERT INTO ks.t1 (k,v) values (1,1)")
        as_mike = self.get_session(user='mike', password='12345')
        assert_unauthorized(as_mike,
                            cql,
                            r"User mike has no EXECUTE permission on <function ks.plus_one\(int\)> or any of its parents")

        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        return as_mike.execute(cql)

    def test_inheritence_of_udf_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify that if EXECUTE permissions are granted to a parent role, that roles the parent is granted to inherit EXECUTE
        """
        self.setup_table()
        self.superuser.execute("CREATE ROLE function_user")
        self.superuser.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO function_user")
        self.superuser.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        self.superuser.execute("INSERT INTO ks.t1 (k,v) VALUES (1,1)")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("GRANT SELECT ON ks.t1 TO mike")
        as_mike = self.get_session(user='mike', password='12345')
        select = "SELECT k, v, ks.plus_one(v) FROM ks.t1 WHERE k = 1"
        assert_unauthorized(as_mike,
                            select,
                            r"User mike has no EXECUTE permission on <function ks.plus_one\(int\)> or any of its parents")

        self.superuser.execute("GRANT function_user TO mike")
        assert_one(as_mike, select, [1, 1, 2])

    def test_builtin_functions_require_no_special_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify a new role can use all built in functions, with no permissions granted.
        """
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.setup_table()
        self.superuser.execute("INSERT INTO ks.t1 (k,v) VALUES (1,1)")
        as_mike = self.get_session(user='mike', password='12345')
        self.superuser.execute("GRANT ALL PERMISSIONS ON ks.t1 TO mike")
        assert_one(as_mike, "SELECT * from ks.t1 WHERE k=blobasint(intasblob(1))", [1, 1])

    def test_disallow_grant_revoke_on_builtin_functions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Verify that granting or revoking permissions for the builtin functions to mike throw InvalidRequest
        """
        self.setup_table()
        self.superuser.execute("CREATE ROLE mike")
        assert_invalid(self.superuser, "GRANT EXECUTE ON FUNCTION system.intasblob(int) TO mike",
                       "Altering permissions on builtin functions is not supported",
                       InvalidRequest)
        assert_invalid(self.superuser, "REVOKE ALL PERMISSIONS ON FUNCTION system.intasblob(int) FROM mike",
                       "Altering permissions on builtin functions is not supported",
                       InvalidRequest)
        assert_invalid(self.superuser, "GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE system TO mike",
                       "Altering permissions on builtin functions is not supported",
                       InvalidRequest)
        assert_invalid(self.superuser, "REVOKE ALL PERMISSIONS ON ALL FUNCTIONS IN KEYSPACE system FROM mike",
                       "Altering permissions on builtin functions is not supported",
                       InvalidRequest)

    def test_disallow_grant_execute_on_non_function_resources(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Verify that granting EXECUTE on non function resources to mike throws InvalidRequest
        """
        self.setup_table()
        self.superuser.execute("CREATE ROLE mike")
        self.superuser.execute("CREATE ROLE role1")

        # can't grant EXECUTE on data or role resources
        assert_invalid(self.superuser, "GRANT EXECUTE ON ALL KEYSPACES TO mike",
                       "Resource type DataResource does not support any of the requested permissions",
                       SyntaxException)
        assert_invalid(self.superuser, "GRANT EXECUTE ON KEYSPACE ks TO mike",
                       "Resource type DataResource does not support any of the requested permissions",
                       SyntaxException)
        assert_invalid(self.superuser, "GRANT EXECUTE ON TABLE ks.t1 TO mike",
                       "Resource type DataResource does not support any of the requested permissions",
                       SyntaxException)
        assert_invalid(self.superuser, "GRANT EXECUTE ON ALL ROLES TO mike",
                       "Resource type RoleResource does not support any of the requested permissions",
                       SyntaxException)
        assert_invalid(self.superuser, "GRANT EXECUTE ON ROLE mike TO role1",
                       "Resource type RoleResource does not support any of the requested permissions",
                       SyntaxException)

    def test_aggregate_function_permissions(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create two UDFs, and use them as the state & final functions in a UDA
        * Verify all UDF permissions also apply to UDAs
        * Drop the UDA and ensure that the granted permissions are removed
        """
        self.setup_table()
        self.superuser.execute("INSERT INTO ks.t1 (k,v) VALUES (1,1)")
        self.superuser.execute("INSERT INTO ks.t1 (k,v) VALUES (2,2)")
        self.superuser.execute("""CREATE FUNCTION ks.state_function( a int, b int )
                             CALLED ON NULL INPUT
                             RETURNS int
                             LANGUAGE java
                             AS 'return Integer.valueOf( (a != null ? a.intValue() : 0) + b.intValue());'""")
        self.superuser.execute("""CREATE FUNCTION ks.final_function( a int )
                             CALLED ON NULL INPUT
                             RETURNS int
                             LANGUAGE java
                             AS 'return a;'""")
        self.superuser.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.superuser.execute("GRANT CREATE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        self.superuser.execute("GRANT ALL PERMISSIONS ON ks.t1 TO mike")
        as_mike = self.get_session(user='mike', password='12345')
        create_aggregate_cql = """CREATE AGGREGATE ks.simple_aggregate(int)
                          SFUNC state_function
                          STYPE int
                          FINALFUNC final_function
                          INITCOND 0"""
        # check permissions to create the aggregate
        assert_unauthorized(as_mike,
                            create_aggregate_cql,
                            r"User mike has no EXECUTE permission on <function ks.state_function\(int, int\)> or any of its parents")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.state_function(int, int) TO mike")
        assert_unauthorized(as_mike,
                            create_aggregate_cql,
                            r"User mike has no EXECUTE permission on <function ks.final_function\(int\)> or any of its parents")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.final_function(int) TO mike")
        as_mike.execute(create_aggregate_cql)

        # without execute permissions on the state or final function we
        # cannot use the aggregate, so revoke them to verify
        self.superuser.execute("REVOKE EXECUTE ON FUNCTION ks.state_function(int, int) FROM mike")
        self.superuser.execute("REVOKE EXECUTE ON FUNCTION ks.final_function(int) FROM mike")
        execute_aggregate_cql = "SELECT ks.simple_aggregate(v) FROM ks.t1"
        assert_unauthorized(as_mike,
                            execute_aggregate_cql,
                            r"User mike has no EXECUTE permission on <function ks.state_function\(int, int\)> or any of its parents")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.state_function(int, int) TO mike")
        assert_unauthorized(as_mike,
                            execute_aggregate_cql,
                            r"User mike has no EXECUTE permission on <function ks.final_function\(int\)> or any of its parents")
        self.superuser.execute("GRANT EXECUTE ON FUNCTION ks.final_function(int) TO mike")

        # mike *does* have execute permission on the aggregate function, as its creator
        assert_one(as_mike, execute_aggregate_cql, [3])

        # check that dropping the aggregate removes all of mike's permissions on it
        # note: after dropping, we have to list *all* of mike's permissions and check
        # that they don't contain any for the aggregate as we can no longer use the
        # function name in the LIST statement
        agg_perms = list(self.superuser.execute("LIST ALL PERMISSIONS ON FUNCTION ks.simple_aggregate(int) OF mike NORECURSIVE"))
        self.superuser.execute("DROP AGGREGATE ks.simple_aggregate(int)")
        all_perms = list(self.superuser.execute("LIST ALL PERMISSIONS OF mike"))
        for p in agg_perms:
            assert not p in all_perms, "Perm {p} found, but should be removed".format(p=p)

    def test_ignore_invalid_roles(self):
        """
        The system_auth.roles table includes a set of roles of which each role
        is a member. If that list were to get out of sync, so that it indicated
        that roleA is a member of roleB, but roleB does not exist in the roles
        table, then the result of LIST ROLES OF roleA should not include roleB
        @jira_ticket CASSANDRA-9551
        """
        self.superuser.execute("CREATE ROLE mike WITH LOGIN = true")
        # hack an invalid entry into the roles table for roleA
        self.superuser.execute("UPDATE system_auth.roles SET member_of = {'role1'} where role = 'mike'")
        assert_all(self.superuser, "LIST ROLES OF mike", [list(self.role('mike'))])

    def setup_table(self):
        self.superuser.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}")
        self.superuser.execute("CREATE TABLE ks.t1 (k int PRIMARY KEY, v int)")

    def assert_unauthenticated(self, user, password):
        with pytest.raises(NoHostAvailable) as response:
            node = self.cluster.nodelist()[0]
            self.cql_connection(node, user=user, password=password)
        host, error = response._excinfo[1].errors.popitem()

        message = "Provided username {user} and/or password are incorrect".format(user=user)\
            if node.cluster.version() >= LooseVersion('3.10') \
            else "Username and/or password are incorrect"
        pattern = 'Failed to authenticate to {host}: Error from server: code=0100 ' \
                  '[Bad credentials] message="{message}"'.format(host=host, message=message)

        assert isinstance(error, AuthenticationFailed), "Expected AuthenticationFailed, got {error}".format(error=error)
        assert pattern in repr(error)

    def assert_login_not_allowed(self, user, password):
        with pytest.raises(NoHostAvailable) as response:
            node = self.cluster.nodelist()[0]
            self.cql_connection(node, user=user, password=password)
        host, error = response._excinfo[1].errors.popitem()

        pattern = 'Failed to authenticate to {host}: Error from server: code=0100 ' \
                  '[Bad credentials] message="{user} is not permitted to log in"'.format(host=host, user=user)

        assert isinstance(error, AuthenticationFailed), "Expected AuthenticationFailed, got {error}".format(error=error)
        assert pattern in repr(error)

    def get_session(self, node_idx=0, user=None, password=None):
        """
        Connect with a set of credentials to a given node. Connection is not exclusive to that node.
        @param node_idx Initial node to connect to
        @param user User to connect as
        @param password Password to use
        @return Session as user, to specified node
        """
        node = self.cluster.nodelist()[node_idx]
        session = self.patient_cql_connection(node, user=user, password=password)
        return session

    def prepare(self, nodes=1, roles_expiry=0):
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
                  'permissions_validity_in_ms': 0,
                  'roles_validity_in_ms': roles_expiry}

        if self.dtest_config.cassandra_version_from_build >= '4.0':
            config['network_authorizer'] = 'org.apache.cassandra.auth.CassandraNetworkAuthorizer'

        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start(wait_for_binary_proto=True)

        self.cluster.wait_for_any_log('Created default superuser', 25)

    def assert_permissions_listed(self, expected, session, query):
        rows = session.execute(query)
        perms = [(str(r.role), str(r.resource), str(r.permission)) for r in rows]
        assert sorted(expected) == sorted(perms)

    def assert_no_permissions(self, session, query):
        assert list(session.execute(query)) == []


@since('2.2')
class TestAuthUnavailable(Tester):
    """
    * These tests verify behavior when backends for authentication & authorization are unable to pull data from the
    * system_auth keyspace. Failure scenarios are simulated based on the assumption that internal queries for role
    * hierarchies and role properties of the "cassandra" super-user get CL=QUORUM (other roles get CL=LOCAL_ONE). And so
    * we expect these internal queries to fail when one of two nodes are down and system_auth have RF=2. Though the
    * permissions cache is used in these tests, it is always populated by permissions derived from the super-user status
    * (all applicable to resource) of the "cassandra" user. The network_authorizer is always disabled to make sure the
    * queries utilize the role/permissions cache only.
    """

    def test_authentication_handle_unavailable(self):
        """
        * Launch a two node cluster with role/permissions cache disabled
        * Connect as default super user
        * Increase the system_auth RF to 2
        * Run repair
        * Stop one of the nodes
        * Verify that attempt to login fail with AuthenticationFailed

        @jira_ticket CASSANDRA-15041
        """
        self.prepare(nodes=2)
        logger.debug("Nodes started")

        node0, node1 = self.cluster.nodelist()

        cassandra = self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        node1.stop()

        try:
            self.patient_exclusive_cql_connection(node0, timeout=2, user='cassandra', password='cassandra')
            self.fail("Expected login attempt to raise an exception.")
        except NoHostAvailable as e:
            # From driver
            assert isinstance(list(e.errors.values())[0], AuthenticationFailed)
            # AuthenticationFailed from server
            assert re.search("code=0100", str(e))
            # Message from server
            assert re.search("Unable to perform authentication:.* Cannot achieve consistency level QUORUM", str(e))

    def test_authentication_through_cache_handle_unavailable(self):
        """
        * Launch a two node cluster with role/permissions cache enabled
        * Connect as default super user
        * Increase the system_auth RF to 2
        * Run repair
        * Stop one of the nodes
        * Verify that attempt to login fail with AuthenticationFailed

        @jira_ticket CASSANDRA-15041
        """
        self.prepare(nodes=2, cache_validity=500, cache_update_interval=500)
        logger.debug("Nodes started")

        node0, node1 = self.cluster.nodelist()

        cassandra = self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        # Warm up cache
        self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        node1.stop()

        # Wait for cache to timeout
        time.sleep(1)

        try:
            self.patient_exclusive_cql_connection(node0, timeout=2, user='cassandra', password='cassandra')
            self.fail("Expected login attempt to raise an exception.")
        except NoHostAvailable as e:
            # From driver
            assert isinstance(list(e.errors.values())[0], AuthenticationFailed)
            # AuthenticationFailed from server
            assert re.search("code=0100", str(e))
            # Message from server
            assert re.search("Unable to perform authentication:.* Cannot achieve consistency level QUORUM", str(e))

    @since('4.0')
    def test_authentication_from_cache_while_unavailable(self):
        """
        * Launch a two node cluster with role/permissions cache enabled
        * Connect as default super user
        * Increase the system_auth RF to 2
        * Run repair
        * Stop one of the nodes
        * Verify that login is successful from cached entries

        @jira_ticket CASSANDRA-15041
        """
        self.prepare(nodes=2, cache_validity=60000, cache_update_interval=60000)
        logger.debug("Nodes started")

        node0, node1 = self.cluster.nodelist()

        cassandra = self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        # Warm up cache
        self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        node1.stop()

        self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

    @since('4.0')
    def test_credentials_cache_background_reload_handle_unavailable(self):
        """
        * Launch a two node cluster with role/permissions cache update interval at a fraction of validity time
        * Connect as default super user
        * Increase the system_auth RF to 2
        * Run repair
        * Stop one of the nodes
        * Wait for cache update interval to expire
        * Trigger async update of cache

        @jira_ticket CASSANDRA-15041
        """
        self.prepare(nodes=2, cache_validity=60000, cache_update_interval=10)
        logger.debug("Nodes started")

        node0, node1 = self.cluster.nodelist()

        cassandra = self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        # Warm up cache
        self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        node1.stop()

        # Trigger async update of role/permissions cache
        time.sleep(0.5)
        self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        # Give background update operation time to fail and check logs
        time.sleep(6)
        assert not self.check_logs_for_errors()

    def test_authorization_handle_unavailable(self):
        """
        * Launch a two node cluster with role/permissions cache disabled
        * Connect as default super user
        * Increase the system_auth RF to 2
        * Run repair
        * Create dummy ks/table
        * Stop one of the nodes
        * Verify that attempt to select on table fail with Unauthorized

        @jira_ticket CASSANDRA-15041
        """
        self.prepare(nodes=2)
        logger.debug("Nodes started")

        node0, node1 = self.cluster.nodelist()

        cassandra = self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        node1.stop()

        assert_exception(cassandra, "SELECT * from ks.cf", matching="Unable to perform authorization of super-user permission: Cannot achieve consistency level QUORUM", expected=Unauthorized)

    def test_authorization_through_cache_handle_unavailable(self):
        """
        * Launch a two node cluster with role/permissions cache enabled
        * Connect as default super user
        * Increase the system_auth RF to 2
        * Run repair
        * Create dummy ks/table
        * Stop one of the nodes
        * Verify that attempt to select on table fail with Unauthorized

        @jira_ticket CASSANDRA-15041
        """
        self.prepare(nodes=2, cache_validity=500, cache_update_interval=500)
        logger.debug("Nodes started")

        node0, node1 = self.cluster.nodelist()

        cassandra = self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        # Warm up cache
        cassandra.execute("SELECT * from ks.cf")

        node1.stop()

        # Wait for cache to timeout
        time.sleep(1)

        assert_exception(cassandra, "SELECT * from ks.cf", matching="Unable to perform authorization of super-user permission: Cannot achieve consistency level QUORUM", expected=Unauthorized)

    def test_authorization_from_cache_while_unavailable(self):
        """
        * Launch a two node cluster with role/permissions cache enabled
        * Connect as default super user
        * Increase the system_auth RF to 2
        * Run repair
        * Create dummy ks/table
        * Stop one of the nodes
        * Verify that select on table is authorized from cached entries

        @jira_ticket CASSANDRA-15041
        """
        self.prepare(nodes=2, cache_validity=60000, cache_update_interval=60000)
        logger.debug("Nodes started")

        node0, node1 = self.cluster.nodelist()

        cassandra = self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        # Warm up cache
        cassandra.execute("SELECT * from ks.cf")

        node1.stop()

        # Authorized from cache
        cassandra.execute("SELECT * from ks.cf")

    def test_permission_cache_background_reload_handle_unavailable(self):
        """
        * Launch a two node cluster with role/permissions cache update interval at a fraction of validity time
        * Connect as default super user
        * Increase the system_auth RF to 2
        * Run repair
        * Create dummy ks/table
        * Stop one of the nodes
        * Wait for cache update interval to expire
        * Trigger async update of cache
        * Verify that background update don't log errors

        @jira_ticket CASSANDRA-15041
        """
        self.prepare(nodes=2, cache_validity=60000, cache_update_interval=10)
        logger.debug("Nodes started")

        node0, node1 = self.cluster.nodelist()

        cassandra = self.patient_exclusive_cql_connection(node0, user='cassandra', password='cassandra')

        self.set_rf2_on_system_auth(cassandra)

        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        # Warm up cache
        cassandra.execute("SELECT * from ks.cf")

        node1.stop()

        # Trigger async update of role/permissions cache
        time.sleep(0.5)
        cassandra.execute("SELECT * from ks.cf")

        # Give background update operation time to fail and check logs
        time.sleep(6)
        assert not self.check_logs_for_errors()

    def set_rf2_on_system_auth(self, session):
        """
        Set RF=2 on system_auth and repair
        @param session The session used to alter keyspace
        """
        session.execute("""
            ALTER KEYSPACE system_auth
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};
        """)

        logger.debug("Repairing after altering RF")
        self.cluster.repair()

    def prepare(self, nodes=1, cache_validity=0, cache_update_interval=-1):
        """
        Sets up and launches C* cluster.
        Always set same cache validity and update-interval on roles, permissions and credentials to overcome differences
        in cache strategies between 4.0 and pre-4.0.
        @param nodes Number of nodes in the cluster. Default is 1
        @param cache_validity The timeout for the roles/permissions/credentials cache in ms. Default is 0.
        @param cache_update_interval The update interval for the roles/permissions/credentials cache in ms. Default is -1.
        """
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': cache_validity,
                  'permissions_update_interval_in_ms': cache_update_interval,
                  'roles_validity_in_ms': cache_validity,
                  'roles_update_interval_in_ms': cache_update_interval}
        if self.dtest_config.cassandra_version_from_build >= '3.0':
            config['enable_materialized_views'] = 'true'
        if self.dtest_config.cassandra_version_from_build >= '3.4':
            config['credentials_validity_in_ms'] = cache_validity
            config['credentials_update_interval_in_ms'] = cache_update_interval
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start()

        n = self.cluster.wait_for_any_log('Created default superuser', 25)
        logger.debug("Default role created by " + n.name)


@since('4.0')
class TestNetworkAuth(Tester):

    @pytest.fixture(autouse=True)
    def fixture_setup_auth(self, fixture_dtest_setup):
        fixture_dtest_setup.cluster.set_configuration_options(values={
            'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
            'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
            'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
            'network_authorizer': 'org.apache.cassandra.auth.CassandraNetworkAuthorizer',
            'num_tokens': 1
        })
        fixture_dtest_setup.cluster.populate([1, 1], debug=True).start(wait_for_binary_proto=True, jvm_args=['-XX:-PerfDisableSharedMem'])
        fixture_dtest_setup.dc1_node, fixture_dtest_setup.dc2_node = fixture_dtest_setup.cluster.nodelist()
        fixture_dtest_setup.superuser = fixture_dtest_setup.patient_exclusive_cql_connection(fixture_dtest_setup.dc1_node, user='cassandra', password='cassandra')

        fixture_dtest_setup.superuser.execute("ALTER KEYSPACE system_auth WITH REPLICATION={'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}")
        fixture_dtest_setup.superuser.execute("CREATE KEYSPACE ks WITH REPLICATION={'class': 'NetworkTopologyStrategy', 'dc1': 1, 'dc2': 1}")
        fixture_dtest_setup.superuser.execute("CREATE TABLE ks.tbl (k int primary key, v int)")

    def username(self):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(8));


    def create_user(self, query_fmt, username):
        """
        formats and runs the given auth query and grants permissions to the created user
        """
        self.superuser.execute(query_fmt % username)
        self.superuser.execute("GRANT ALL PERMISSIONS ON ks.tbl TO %s" % username)

    def assertConnectsTo(self, username, node):
        session = self.exclusive_cql_connection(node, user=username, password='password')
        session.execute("SELECT * FROM ks.tbl")

    def assertUnauthorized(self, func):
        try:
            func()
            pytest.fail("Expecting Unauthorized exception")
        except Unauthorized as _:
            pass
        except NoHostAvailable as e:
            cause = list(e.errors.values())[0]
            assert isinstance(cause, Unauthorized)

    def assertWontConnectTo(self, username, node):
        self.assertUnauthorized(lambda: self.exclusive_cql_connection(node, user=username, password='password'))

    def clear_network_auth_cache(self, node):
        mbean = make_mbean('auth', type='NetworkAuthCache')
        with JolokiaAgent(node) as jmx:
            jmx.execute_method(mbean, 'invalidate')

    def test_full_dc_access(self):
        username = self.username()
        self.create_user("CREATE ROLE %s WITH password = 'password' AND LOGIN = true", username)
        self.assertConnectsTo(username, self.dc1_node)
        self.assertConnectsTo(username, self.dc2_node)

    def test_single_dc_access(self):
        username = self.username()
        self.create_user("CREATE ROLE %s WITH password = 'password' AND LOGIN = true AND ACCESS TO DATACENTERS {'dc1'}", username)
        self.assertConnectsTo(username, self.dc1_node)
        self.assertWontConnectTo(username, self.dc2_node)

    def test_revoked_dc_access(self):
        """
        if a user's access to a dc is revoked while they're connected,
        all of their requests should fail once the cache is cleared
        """
        username = self.username()
        self.create_user("CREATE ROLE %s WITH password = 'password' AND LOGIN = true", username)
        self.assertConnectsTo(username, self.dc1_node)
        self.assertConnectsTo(username, self.dc2_node)

        # connect to the dc2 node, then remove permission for it
        session = self.exclusive_cql_connection(self.dc2_node, user=username, password='password')
        self.superuser.execute("ALTER ROLE %s WITH ACCESS TO DATACENTERS {'dc1'}" % username)
        self.clear_network_auth_cache(self.dc2_node)
        self.assertUnauthorized(lambda: session.execute("SELECT * FROM ks.tbl"))

    def test_create_dc_validation(self):
        """
        trying to give a user access to a dc that doesn't exist should fail
        """
        username = self.username()
        with pytest.raises(InvalidRequest):
            self.create_user("CREATE ROLE %s WITH password = 'password' AND LOGIN = true AND ACCESS TO DATACENTERS {'dc1000'}", username)

    def test_alter_dc_validation(self):
        """
        trying to give a user access to a dc that doesn't exist should fail
        """
        username = self.username()
        self.create_user("CREATE ROLE %s WITH password = 'password' AND LOGIN = true", username)
        with pytest.raises(InvalidRequest):
            self.create_user("ALTER ROLE %s WITH ACCESS TO DATACENTERS {'dc1000'}", username)

    def test_revoked_login(self):
        """
        If the login flag is set to false for a user with a current connection,
        all their requests should fail once the cache is cleared. Here because it has
        more in common with these tests that the other auth tests
        """
        username = self.username()
        superuser = self.patient_exclusive_cql_connection(self.dc1_node, user='cassandra', password='cassandra')
        self.create_user("CREATE ROLE %s WITH password = 'password' AND LOGIN = true", username)
        self.assertConnectsTo(username, self.dc1_node)
        self.assertConnectsTo(username, self.dc2_node)

        # connect to the dc2 node, then remove permission for it
        session = self.exclusive_cql_connection(self.dc2_node, user=username, password='password')
        superuser.execute("ALTER ROLE %s WITH LOGIN=false" % username)
        self.clear_network_auth_cache(self.dc2_node)
        self.assertUnauthorized(lambda: session.execute("SELECT * FROM ks.tbl"))


def role_creator_permissions(creator, role):
    return [(creator, role, perm) for perm in ('ALTER', 'DROP', 'AUTHORIZE')]


def function_resource_creator_permissions(creator, resource):
    return [(creator, resource, perm) for perm in ('ALTER', 'DROP', 'AUTHORIZE', 'EXECUTE')]
