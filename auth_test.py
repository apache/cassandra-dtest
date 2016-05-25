import re
import time
from collections import namedtuple

from cassandra import AuthenticationFailed, InvalidRequest, Unauthorized
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import SyntaxException

from assertions import (assert_all, assert_invalid, assert_one,
                        assert_unauthorized)
from dtest import CASSANDRA_VERSION_FROM_BUILD, Tester, debug
from tools import since


class TestAuth(Tester):

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def system_auth_ks_is_alterable_test(self):
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
        debug("nodes started")

        session = self.get_session(user='cassandra', password='cassandra')
        self.assertEquals(1, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

        session.execute("""
            ALTER KEYSPACE system_auth
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
        """)
        # The driver schema metadata API is async. Force a hard refresh here, before we check that the alter succeeded.
        session.cluster.refresh_schema_metadata()

        self.assertEquals(3, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

        # Run repair to workaround read repair issues caused by CASSANDRA-10655
        debug("Repairing before altering RF")
        self.cluster.repair()

        # make sure schema change is persistent
        debug("Stopping cluster..")
        self.cluster.stop()
        debug("Restarting cluster..")
        self.cluster.start(wait_other_notice=True)

        # check each node directly
        for i in range(3):
            debug('Checking node: {i}'.format(i=i))
            node = self.cluster.nodelist()[i]
            session = self.patient_exclusive_cql_connection(node, user='cassandra', password='cassandra')
            self.assertEquals(3, session.cluster.metadata.keyspaces['system_auth'].replication_strategy.replication_factor)

    def login_test(self):
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
            assert isinstance(e.errors.values()[0], AuthenticationFailed)
        try:
            self.get_session(user='doesntexist', password='doesntmatter')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)

    # from 2.2 role creation is granted by CREATE_ROLE permissions, not superuser status
    @since('1.2', max_version='2.1.x')
    def only_superuser_can_create_users_test(self):
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
        assert_unauthorized(jackob, "CREATE USER james WITH PASSWORD '54321' NOSUPERUSER", 'Only superusers are allowed to perform CREATE (\[ROLE\|USER\]|USER) queries', )

    @since('1.2', max_version='2.1.x')
    def password_authenticator_create_user_requires_password_test(self):
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

    def cant_create_existing_user_test(self):
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

    def list_users_test(self):
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
        self.assertEqual(5, len(rows))
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        self.assertTrue(users['cassandra'])
        self.assertFalse(users['alex'])
        self.assertTrue(users['bob'])
        self.assertFalse(users['cathy'])
        self.assertTrue(users['dave'])

        self.get_session(user='dave', password='12345')
        rows = list(session.execute("LIST USERS"))
        self.assertEqual(5, len(rows))
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        self.assertTrue(users['cassandra'])
        self.assertFalse(users['alex'])
        self.assertTrue(users['bob'])
        self.assertFalse(users['cathy'])
        self.assertTrue(users['dave'])

    def user_cant_drop_themselves_test(self):
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
    def only_superusers_can_drop_users_test(self):
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
        self.assertEqual(3, len(rows))

        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, 'DROP USER dave', 'Only superusers are allowed to perform DROP (\[ROLE\|USER\]|USER) queries')

        rows = list(cassandra.execute("LIST USERS"))
        self.assertEqual(3, len(rows))

        cassandra.execute('DROP USER dave')
        rows = list(cassandra.execute("LIST USERS"))
        self.assertEqual(2, len(rows))

    def dropping_nonexistent_user_throws_exception_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify that dropping a nonexistent user throws InvalidRequest
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_invalid(session, 'DROP USER nonexistent', "nonexistent doesn't exist")

    def drop_user_case_sensitive_test(self):
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
        self.assertItemsEqual(rows, ['cassandra'])

        cassandra.execute("CREATE USER test WITH PASSWORD '12345'")

        # Should be invalid, as 'TEST' does not exist
        assert_invalid(cassandra, "DROP USER TEST")

        cassandra.execute("DROP USER test")
        rows = [x[0] for x in list(cassandra.execute("LIST USERS"))]
        self.assertItemsEqual(rows, ['cassandra'])

    def alter_user_case_sensitive_test(self):
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

    def regular_users_can_alter_their_passwords_only_test(self):
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

    def users_cant_alter_their_superuser_status_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Attempt to remove our own superuser status. Assert this throws Unauthorized
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_unauthorized(session, "ALTER USER cassandra NOSUPERUSER", "You aren't allowed to alter your own superuser status")

    def only_superuser_alters_superuser_status_test(self):
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

    def altering_nonexistent_user_throws_exception_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Assert that altering a nonexistent user throws InvalidRequest
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        assert_invalid(session, "ALTER USER nonexistent WITH PASSWORD 'doesn''tmatter'", "nonexistent doesn't exist")

    def conditional_create_drop_user_test(self):
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

        assert_one(session, "LIST USERS", ['cassandra', True])

        session.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'sup'")
        session.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'ignored'")

        self.get_session(user='aleksey', password='sup')

        assert_all(session, "LIST USERS", [['aleksey', False], ['cassandra', True]])

        session.execute("DROP USER IF EXISTS aleksey")
        assert_one(session, "LIST USERS", ['cassandra', True])

        session.execute("DROP USER IF EXISTS aleksey")
        assert_one(session, "LIST USERS", ['cassandra', True])

    def create_ks_auth_test(self):
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

    def create_cf_auth_test(self):
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

    def alter_ks_auth_test(self):
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

    def alter_cf_auth_test(self):
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
    def materialized_views_auth_test(self):
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

    def drop_ks_auth_test(self):
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

    def drop_cf_auth_test(self):
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

    def modify_and_select_auth_test(self):
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
        self.assertEquals(0, len(rows))

        assert_unauthorized(cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "UPDATE ks.cf SET val = 1 WHERE id = 1", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "DELETE FROM ks.cf WHERE id = 1", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "TRUNCATE ks.cf", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("UPDATE ks.cf SET val = 1 WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEquals(2, len(rows))

        cathy.execute("DELETE FROM ks.cf WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEquals(1, len(rows))

        rows = list(cathy.execute("TRUNCATE ks.cf"))
        self.assertItemsEqual(rows, [])

    def grant_revoke_auth_test(self):
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

    def grant_revoke_nonexistent_user_or_ks_test(self):
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

    def grant_revoke_cleanup_test(self):
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
        self.assertEquals(1, len(rows))

        # drop and recreate the user, make sure permissions are gone
        cassandra.execute("DROP USER cathy")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        assert_unauthorized(cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "SELECT * FROM ks.cf", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")

        # grant all the permissions back
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        self.assertEqual(1, len(rows))

        # drop and recreate the keyspace, make sure permissions are gone
        cassandra.execute("DROP KEYSPACE ks")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        assert_unauthorized(cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)", "User cathy has no MODIFY permission on <table ks.cf> or any of its parents")

        assert_unauthorized(cathy, "SELECT * FROM ks.cf", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")

    def permissions_caching_test(self):
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

        # grant SELECT to cathy
        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        # should still fail after 1 second.
        time.sleep(1.0)
        for c in cathys:
            assert_unauthorized(c, "SELECT * FROM ks.cf", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")

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
                    self.assertEqual(0, len(rows))
                success = True
            except Unauthorized:
                pass
            cnt += 1
            time.sleep(0.1)

        self.assertTrue(success)

    def list_permissions_test(self):
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

    def type_auth_test(self):
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

    def restart_node_doesnt_lose_auth_data_test(self):
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
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        self.cluster.stop()
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer'}
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        philip = self.get_session(user='philip', password='strongpass')
        cathy = self.get_session(user='cathy', password='12345')
        assert_unauthorized(cathy, "SELECT * FROM ks.cf", "User cathy has no SELECT permission on <table ks.cf> or any of its parents")
        philip.execute("SELECT * FROM ks.cf")

    def prepare(self, nodes=1, permissions_validity=0):
        """
        Sets up and launches C* cluster.
        @param nodes Number of nodes in the cluster. Default is 1
        @param permissions_validity The timeout for the permissions cache in ms. Default is 0.
        """
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': permissions_validity}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start()

        n = self.wait_for_any_log(self.cluster.nodelist(), 'Created default superuser', 25)
        debug("Default role created by " + n.name)

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
        self.assertEqual(sorted(expected), sorted(perms))


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

# First value is the role name
# Second value is superuser status
# Third value is login status
# Fourth value is role options
# See CASSANDRA-7653 for explanations of these
Role = namedtuple('Role', ['name', 'superuser', 'login', 'options'])

mike_role = Role('mike', False, True, {})
role1_role = Role('role1', False, False, {})
role2_role = Role('role2', False, False, {})
cassandra_role = Role('cassandra', True, True, {})


@since('2.2')
class TestAuthRoles(Tester):
    """
    @jira_ticket CASSANDRA-7653
    """

    def __init__(self, *args, **kwargs):
        if CASSANDRA_VERSION_FROM_BUILD >= '3.0':
            kwargs['cluster_options'] = {'enable_user_defined_functions': 'true',
                                         'enable_scripted_user_defined_functions': 'true'}
        else:
            kwargs['cluster_options'] = {'enable_user_defined_functions': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def create_drop_role_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, check it exists
        * Drop the role, check it is gone
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        assert_one(cassandra, 'LIST ROLES', list(cassandra_role))

        cassandra.execute("CREATE ROLE role1")
        assert_all(cassandra, "LIST ROLES", [list(cassandra_role), list(role1_role)])

        cassandra.execute("DROP ROLE role1")
        assert_one(cassandra, "LIST ROLES", list(cassandra_role))

    def conditional_create_drop_role_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role twice, using IF NOT EXISTS
        * Check neither query failed, but role was only created once
        * Drop the new role twice, using IF EXISTS
        * Check neither query failed, but only superuser remains
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        assert_one(cassandra, "LIST ROLES", list(cassandra_role))

        cassandra.execute("CREATE ROLE IF NOT EXISTS role1")
        cassandra.execute("CREATE ROLE IF NOT EXISTS role1")
        assert_all(cassandra, "LIST ROLES", [list(cassandra_role), list(role1_role)])

        cassandra.execute("DROP ROLE IF EXISTS role1")
        cassandra.execute("DROP ROLE IF EXISTS role1")
        assert_one(cassandra, "LIST ROLES", list(cassandra_role))

    def create_drop_role_validation_test(self):
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
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        mike = self.get_session(user='mike', password='12345')

        assert_unauthorized(mike,
                            "CREATE ROLE role2",
                            "User mike does not have sufficient privileges to perform the requested operation")
        cassandra.execute("CREATE ROLE role1")

        assert_unauthorized(mike,
                            "DROP ROLE role1",
                            "User mike does not have sufficient privileges to perform the requested operation")

        assert_invalid(cassandra, "CREATE ROLE role1", "role1 already exists")
        cassandra.execute("DROP ROLE role1")
        assert_invalid(cassandra, "DROP ROLE role1", "role1 doesn't exist")

    def role_admin_validation_test(self):
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
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE administrator WITH SUPERUSER = false AND LOGIN = false")
        cassandra.execute("GRANT ALL ON ALL ROLES TO administrator")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT administrator TO mike")
        cassandra.execute("CREATE ROLE klaus WITH PASSWORD = '54321' AND SUPERUSER = false AND LOGIN = true")
        mike = self.get_session(user='mike', password='12345')
        klaus = self.get_session(user='klaus', password='54321')

        # roles with CREATE on ALL ROLES can create roles
        mike.execute("CREATE ROLE role1 WITH PASSWORD = '11111' AND LOGIN = false")

        # require ALTER on ALL ROLES or a SPECIFIC ROLE to modify
        self.assert_unauthenticated('role1 is not permitted to log in', 'role1', '11111')
        cassandra.execute("GRANT ALTER on ROLE role1 TO klaus")
        klaus.execute("ALTER ROLE role1 WITH LOGIN = true")
        mike.execute("ALTER ROLE role1 WITH PASSWORD = '22222'")
        role1 = self.get_session(user='role1', password='22222')

        # only superusers can set superuser status
        assert_unauthorized(mike, "ALTER ROLE role1 WITH SUPERUSER = true",
                            "Only superusers are allowed to alter superuser status")
        assert_unauthorized(mike, "ALTER ROLE mike WITH SUPERUSER = true",
                            "You aren't allowed to alter your own superuser status or that of a role granted to you")

        # roles without necessary permissions cannot create, drop or alter roles except themselves
        assert_unauthorized(role1, "CREATE ROLE role2 WITH LOGIN = false",
                            "User role1 does not have sufficient privileges to perform the requested operation")
        assert_unauthorized(role1, "ALTER ROLE mike WITH LOGIN = false",
                            "User role1 does not have sufficient privileges to perform the requested operation")
        assert_unauthorized(role1, "DROP ROLE mike",
                            "User role1 does not have sufficient privileges to perform the requested operation")
        role1.execute("ALTER ROLE role1 WITH PASSWORD = '33333'")

        # roles with roleadmin can drop roles
        mike.execute("DROP ROLE role1")
        assert_all(cassandra, "LIST ROLES", [['administrator', False, False, {}],
                                             list(cassandra_role),
                                             ['klaus', False, True, {}],
                                             list(mike_role)])

        # revoking role admin removes its privileges
        cassandra.execute("REVOKE administrator FROM mike")
        assert_unauthorized(mike, "CREATE ROLE role3 WITH LOGIN = false",
                            "User mike does not have sufficient privileges to perform the requested operation")

    def creator_of_db_resource_granted_all_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike', grant it CREATE on ALL KS and ALL ROLES
        * Connect as mike
        * Verify that mike is automatically granted permissions on any resource he creates
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO mike")
        cassandra.execute("GRANT CREATE ON ALL ROLES TO mike")

        mike = self.get_session(user='mike', password='12345')
        # mike should automatically be granted permissions on any resource he creates, i.e. tables or roles
        mike.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        mike.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        mike.execute("CREATE ROLE role1 WITH PASSWORD = '11111' AND SUPERUSER = false AND LOGIN = true")
        mike.execute("""CREATE FUNCTION ks.state_function_1(a int, b int)
                        CALLED ON NULL INPUT
                        RETURNS int
                        LANGUAGE javascript
                        AS ' a + b'""")
        mike.execute("""CREATE AGGREGATE ks.simple_aggregate_1(int)
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
                                       cassandra,
                                       "LIST ALL PERMISSIONS")

    def create_and_grant_roles_with_superuser_status_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a superuser role, a nonsuperuser role, and 'mike'
        * Grant CREATE and AUTHORIZE to mike
        * Connect as mike
        * Verify mike can create new roles, but not grant them superuser
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE another_superuser WITH SUPERUSER = true AND LOGIN = false")
        cassandra.execute("CREATE ROLE non_superuser WITH SUPERUSER = false AND LOGIN = false")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        # mike can create and grant any role, except superusers
        cassandra.execute("GRANT CREATE ON ALL ROLES TO mike")
        cassandra.execute("GRANT AUTHORIZE ON ALL ROLES TO mike")

        # mike can create roles, but not with superuser status
        # and can grant any role, including those with superuser status
        mike = self.get_session(user='mike', password='12345')
        mike.execute("CREATE ROLE role1 WITH SUPERUSER = false")
        mike.execute("GRANT non_superuser TO role1")
        mike.execute("GRANT another_superuser TO role1")
        assert_unauthorized(mike, "CREATE ROLE role2 WITH SUPERUSER = true",
                            "Only superusers can create a role with superuser status")
        assert_all(cassandra, "LIST ROLES OF role1", [['another_superuser', True, False, {}],
                                                      ['non_superuser', False, False, {}],
                                                      ['role1', False, False, {}]])

    def drop_and_revoke_roles_with_superuser_status_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create multiple roles, including 'mike'
        * Grant mike DROP and AUTHORIZE
        * Connect as mike
        * Verify mike can drop or revoke any roles, regardless of its superuser status
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE another_superuser WITH SUPERUSER = true AND LOGIN = false")
        cassandra.execute("CREATE ROLE non_superuser WITH SUPERUSER = false AND LOGIN = false")
        cassandra.execute("CREATE ROLE role1 WITH SUPERUSER = false")
        cassandra.execute("GRANT another_superuser TO role1")
        cassandra.execute("GRANT non_superuser TO role1")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT DROP ON ALL ROLES TO mike")
        cassandra.execute("GRANT AUTHORIZE ON ALL ROLES TO mike")

        # mike can drop and revoke any role, including superusers
        mike = self.get_session(user='mike', password='12345')
        mike.execute("REVOKE another_superuser FROM role1")
        mike.execute("REVOKE non_superuser FROM role1")
        mike.execute("DROP ROLE non_superuser")
        mike.execute("DROP ROLE role1")

    def drop_role_removes_memberships_test(self):
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
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT role2 TO role1")
        cassandra.execute("GRANT role1 TO mike")
        assert_all(cassandra, "LIST ROLES OF mike", [list(mike_role), list(role1_role), list(role2_role)])

        # drop the role indirectly granted
        cassandra.execute("DROP ROLE role2")
        assert_all(cassandra, "LIST ROLES OF mike", [list(mike_role), list(role1_role)])

        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT role2 to role1")
        assert_all(cassandra, "LIST ROLES OF mike", [list(mike_role), list(role1_role), list(role2_role)])
        # drop the directly granted role
        cassandra.execute("DROP ROLE role1")
        assert_one(cassandra, "LIST ROLES OF mike", list(mike_role))
        assert_all(cassandra, "LIST ROLES", [list(cassandra_role), list(mike_role), list(role2_role)])

    def drop_role_revokes_permissions_granted_on_it_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create ROLES role1, role2, and mike
        * Grant permissions to role1 and role2. Grant role1 and role2 to mike
        * List mike's permissions, verify permissions from role1 and role2 in list
        * Drop role1 and role2
        * Assert mike has no permissions remaining
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT ALTER ON ROLE role1 TO mike")
        cassandra.execute("GRANT AUTHORIZE ON ROLE role2 TO mike")

        self.assert_permissions_listed([("mike", "<role role1>", "ALTER"),
                                        ("mike", "<role role2>", "AUTHORIZE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")

        cassandra.execute("DROP ROLE role1")
        cassandra.execute("DROP ROLE role2")
        self.assertItemsEqual(list(cassandra.execute("LIST ALL PERMISSIONS OF mike")), [])

    def grant_revoke_roles_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create ROLES role1, role2, and mike
        * Grant role1 to role2, and role2 to mike
        * Verify the output of LIST ROLES
        * REVOKE various roles, verify the output of LIST ROLES
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT role1 TO role2")
        cassandra.execute("GRANT role2 TO mike")

        assert_all(cassandra, "LIST ROLES OF role2", [list(role1_role), list(role2_role)])
        assert_all(cassandra, "LIST ROLES OF mike", [list(mike_role), list(role1_role), list(role2_role)])
        assert_all(cassandra, "LIST ROLES OF mike NORECURSIVE", [list(mike_role), list(role2_role)])

        cassandra.execute("REVOKE role2 FROM mike")
        assert_one(cassandra, "LIST ROLES OF mike", list(mike_role))

        cassandra.execute("REVOKE role1 FROM role2")
        assert_one(cassandra, "LIST ROLES OF role2", list(role2_role))

    def grant_revoke_role_validation_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superusers
        * Create ROLE mike
        * Connect as mike
        * Verify granting nonexistent roles to mike throws InvalidRequest
        * Create ROLE john
        * Verify mike cannot grant/revoke roles to/from john without the AUTHORIZE permission
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        mike = self.get_session(user='mike', password='12345')

        assert_invalid(cassandra, "GRANT role1 TO mike", "role1 doesn't exist")
        cassandra.execute("CREATE ROLE role1")

        assert_invalid(cassandra, "GRANT role1 TO john", "john doesn't exist")
        assert_invalid(cassandra, "GRANT role2 TO john", "role2 doesn't exist")

        cassandra.execute("CREATE ROLE john WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role2")

        assert_unauthorized(mike,
                            "GRANT role2 TO john",
                            "User mike does not have sufficient privileges to perform the requested operation")

        # superusers can always grant roles
        cassandra.execute("GRANT role1 TO john")
        # but regular users need AUTHORIZE permission on the granted role
        cassandra.execute("GRANT AUTHORIZE ON ROLE role2 TO mike")
        mike.execute("GRANT role2 TO john")

        # same applies to REVOKEing roles
        assert_unauthorized(mike,
                            "REVOKE role1 FROM john",
                            "User mike does not have sufficient privileges to perform the requested operation")
        cassandra.execute("REVOKE role1 FROM john")
        mike.execute("REVOKE role2 from john")

    def list_roles_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create several different ROLES
        * Verify LIST ROLES for each role is correct
        * Verify a role cannot LIST ROLES for another ROLE without being a superuser, or having the DESCRIBE permission
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")

        assert_all(cassandra, "LIST ROLES", [list(cassandra_role), list(mike_role), list(role1_role), list(role2_role)])

        cassandra.execute("GRANT role1 TO role2")
        cassandra.execute("GRANT role2 TO mike")

        assert_all(cassandra, "LIST ROLES OF role2", [list(role1_role), list(role2_role)])
        assert_all(cassandra, "LIST ROLES OF mike", [list(mike_role), list(role1_role), list(role2_role)])
        assert_all(cassandra, "LIST ROLES OF mike NORECURSIVE", [list(mike_role), list(role2_role)])

        mike = self.get_session(user='mike', password='12345')
        assert_unauthorized(mike,
                            "LIST ROLES OF cassandra",
                            "You are not authorized to view roles granted to cassandra")

        assert_all(mike, "LIST ROLES", [list(mike_role), list(role1_role), list(role2_role)])
        assert_all(mike, "LIST ROLES OF mike", [list(mike_role), list(role1_role), list(role2_role)])
        assert_all(mike, "LIST ROLES OF mike NORECURSIVE", [list(mike_role), list(role2_role)])
        assert_all(mike, "LIST ROLES OF role2", [list(role1_role), list(role2_role)])

        # without SELECT permission on the root level roles resource, LIST ROLES with no OF
        # returns only the roles granted to the user. With it, it includes all roles.
        assert_all(mike, "LIST ROLES", [list(mike_role), list(role1_role), list(role2_role)])
        cassandra.execute("GRANT DESCRIBE ON ALL ROLES TO mike")
        assert_all(mike, "LIST ROLES", [list(cassandra_role), list(mike_role), list(role1_role), list(role2_role)])

    def grant_revoke_permissions_test(self):
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
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("GRANT ALL ON table ks.cf TO role1")
        cassandra.execute("GRANT role1 TO mike")

        mike = self.get_session(user='mike', password='12345')
        mike.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        assert_one(mike, "SELECT * FROM ks.cf", [0, 0])

        cassandra.execute("REVOKE role1 FROM mike")
        assert_unauthorized(mike,
                            "INSERT INTO ks.cf (id, val) VALUES (0, 0)",
                            "mike has no MODIFY permission on <table ks.cf> or any of its parents")

        cassandra.execute("GRANT role1 TO mike")
        cassandra.execute("REVOKE ALL ON ks.cf FROM role1")

        assert_unauthorized(mike,
                            "INSERT INTO ks.cf (id, val) VALUES (0, 0)",
                            "mike has no MODIFY permission on <table ks.cf> or any of its parents")

    def filter_granted_permissions_by_resource_type_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a ks, table, function, and aggregate
        * Create ROLES mike and role1
        * Grant ALL permissions to mike for each resource. Verify they show up in LIST ALL PERMISSIONS
        * Verify you can't selectively grant invalid permissions for a given resource. ex: CREATE on an existing table
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1 WITH SUPERUSER = false AND LOGIN = false")
        cassandra.execute("CREATE FUNCTION ks.state_func(a int, b int) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'a+b'")
        cassandra.execute("CREATE AGGREGATE ks.agg_func(int) SFUNC state_func STYPE int")

        # GRANT ALL ON ALL KEYSPACES grants Permission.ALL_DATA

        # GRANT ALL ON KEYSPACE grants Permission.ALL_DATA
        cassandra.execute("GRANT ALL ON KEYSPACE ks TO mike")
        self.assert_permissions_listed([("mike", "<keyspace ks>", "CREATE"),
                                        ("mike", "<keyspace ks>", "ALTER"),
                                        ("mike", "<keyspace ks>", "DROP"),
                                        ("mike", "<keyspace ks>", "SELECT"),
                                        ("mike", "<keyspace ks>", "MODIFY"),
                                        ("mike", "<keyspace ks>", "AUTHORIZE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE ALL ON KEYSPACE ks FROM mike")

        # GRANT ALL ON TABLE does not include CREATE (because the table must already be created before the GRANT)
        cassandra.execute("GRANT ALL ON ks.cf TO MIKE")
        self.assert_permissions_listed([("mike", "<table ks.cf>", "ALTER"),
                                        ("mike", "<table ks.cf>", "DROP"),
                                        ("mike", "<table ks.cf>", "SELECT"),
                                        ("mike", "<table ks.cf>", "MODIFY"),
                                        ("mike", "<table ks.cf>", "AUTHORIZE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE ALL ON ks.cf FROM mike")
        assert_invalid(cassandra,
                       "GRANT CREATE ON ks.cf TO MIKE",
                       "Resource type DataResource does not support any of the requested permissions",
                       SyntaxException)

        # GRANT ALL ON ALL ROLES includes SELECT & CREATE on the root level roles resource
        cassandra.execute("GRANT ALL ON ALL ROLES TO mike")
        self.assert_permissions_listed([("mike", "<all roles>", "CREATE"),
                                        ("mike", "<all roles>", "ALTER"),
                                        ("mike", "<all roles>", "DROP"),
                                        ("mike", "<all roles>", "DESCRIBE"),
                                        ("mike", "<all roles>", "AUTHORIZE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE ALL ON ALL ROLES FROM mike")
        assert_invalid(cassandra,
                       "GRANT SELECT ON ALL ROLES TO MIKE",
                       "Resource type RoleResource does not support any of the requested permissions",
                       SyntaxException)

        # GRANT ALL ON ROLE does not include CREATE (because the role must already be created before the GRANT)
        cassandra.execute("GRANT ALL ON ROLE role1 TO mike")
        self.assert_permissions_listed([("mike", "<role role1>", "ALTER"),
                                        ("mike", "<role role1>", "DROP"),
                                        ("mike", "<role role1>", "AUTHORIZE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        assert_invalid(cassandra,
                       "GRANT CREATE ON ROLE role1 TO MIKE",
                       "Resource type RoleResource does not support any of the requested permissions",
                       SyntaxException)
        cassandra.execute("REVOKE ALL ON ROLE role1 FROM mike")

        # GRANT ALL ON ALL FUNCTIONS or on all functions for a single keyspace includes AUTHORIZE, EXECUTE and USAGE
        cassandra.execute("GRANT ALL ON ALL FUNCTIONS TO mike")
        self.assert_permissions_listed([("mike", "<all functions>", "CREATE"),
                                        ("mike", "<all functions>", "ALTER"),
                                        ("mike", "<all functions>", "DROP"),
                                        ("mike", "<all functions>", "AUTHORIZE"),
                                        ("mike", "<all functions>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE ALL ON ALL FUNCTIONS FROM mike")

        cassandra.execute("GRANT ALL ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        self.assert_permissions_listed([("mike", "<all functions in ks>", "CREATE"),
                                        ("mike", "<all functions in ks>", "ALTER"),
                                        ("mike", "<all functions in ks>", "DROP"),
                                        ("mike", "<all functions in ks>", "AUTHORIZE"),
                                        ("mike", "<all functions in ks>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE ALL ON ALL FUNCTIONS IN KEYSPACE ks FROM mike")

        # GRANT ALL ON FUNCTION includes AUTHORIZE, EXECUTE and USAGE for scalar functions and
        # AUTHORIZE and EXECUTE for aggregates
        cassandra.execute("GRANT ALL ON FUNCTION ks.state_func(int, int) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.state_func(int, int)>", "ALTER"),
                                        ("mike", "<function ks.state_func(int, int)>", "DROP"),
                                        ("mike", "<function ks.state_func(int, int)>", "AUTHORIZE"),
                                        ("mike", "<function ks.state_func(int, int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE ALL ON FUNCTION ks.state_func(int, int) FROM mike")

        cassandra.execute("GRANT ALL ON FUNCTION ks.agg_func(int) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.agg_func(int)>", "ALTER"),
                                        ("mike", "<function ks.agg_func(int)>", "DROP"),
                                        ("mike", "<function ks.agg_func(int)>", "AUTHORIZE"),
                                        ("mike", "<function ks.agg_func(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE ALL ON FUNCTION ks.agg_func(int) FROM mike")

    def list_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a table, ks.cf
        * Create ROLES role1, role2, and mike
        * Grant various permissions to roles
        * Verify they propagate appropriately.
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT SELECT ON table ks.cf TO role1")
        cassandra.execute("GRANT ALTER ON table ks.cf TO role2")
        cassandra.execute("GRANT MODIFY ON table ks.cf TO mike")
        cassandra.execute("GRANT ALTER ON ROLE role1 TO role2")
        cassandra.execute("GRANT role1 TO role2")
        cassandra.execute("GRANT role2 TO mike")

        expected_permissions = [("mike", "<table ks.cf>", "MODIFY"),
                                ("role1", "<table ks.cf>", "SELECT"),
                                ("role2", "<table ks.cf>", "ALTER"),
                                ("role2", "<role role1>", "ALTER")]
        expected_permissions.extend(data_resource_creator_permissions('cassandra', '<keyspace ks>'))
        expected_permissions.extend(data_resource_creator_permissions('cassandra', '<table ks.cf>'))
        expected_permissions.extend(role_creator_permissions('cassandra', '<role mike>'))
        expected_permissions.extend(role_creator_permissions('cassandra', '<role role1>'))
        expected_permissions.extend(role_creator_permissions('cassandra', '<role role2>'))

        self.assert_permissions_listed(expected_permissions, cassandra, "LIST ALL PERMISSIONS")

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF role1")

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER"),
                                        ("role2", "<role role1>", "ALTER")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF role2")

        self.assert_permissions_listed([("cassandra", "<role role1>", "ALTER"),
                                        ("cassandra", "<role role1>", "DROP"),
                                        ("cassandra", "<role role1>", "AUTHORIZE"),
                                        ("role2", "<role role1>", "ALTER")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS ON ROLE role1")
        # we didn't specifically grant DROP on role1, so only it's creator should have it
        self.assert_permissions_listed([("cassandra", "<role role1>", "DROP")],
                                       cassandra,
                                       "LIST DROP PERMISSION ON ROLE role1")
        # but we did specifically grant ALTER role1 to role2
        # so that should be listed whether we include an OF clause or not
        self.assert_permissions_listed([("cassandra", "<role role1>", "ALTER"),
                                        ("role2", "<role role1>", "ALTER")],
                                       cassandra,
                                       "LIST ALTER PERMISSION ON ROLE role1")
        self.assert_permissions_listed([("role2", "<role role1>", "ALTER")],
                                       cassandra,
                                       "LIST ALTER PERMISSION ON ROLE role1 OF role2")
        # make sure ALTER on role2 is excluded properly when OF is for another role
        cassandra.execute("CREATE ROLE role3 WITH SUPERUSER = false AND LOGIN = false")
        self.assertItemsEqual(list(cassandra.execute("LIST ALTER PERMISSION ON ROLE role1 OF role3")), [])

        # now check users can list their own permissions
        mike = self.get_session(user='mike', password='12345')
        self.assert_permissions_listed([("mike", "<table ks.cf>", "MODIFY"),
                                        ("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER"),
                                        ("role2", "<role role1>", "ALTER")],
                                       mike,
                                       "LIST ALL PERMISSIONS OF mike")

    def list_permissions_validation_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create several roles, including 'mike'
        * Grant some roles to mike
        * Verify mike can see the permissions of his role, and the roles granted to him
        * Verify mike cannot see all permissions, or those of roles not granted to him
        """
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE john WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")

        cassandra.execute("GRANT SELECT ON table ks.cf TO role1")
        cassandra.execute("GRANT ALTER ON table ks.cf TO role2")
        cassandra.execute("GRANT MODIFY ON table ks.cf TO john")

        cassandra.execute("GRANT role1 TO role2")
        cassandra.execute("GRANT role2 TO mike")

        mike = self.get_session(user='mike', password='12345')

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER")],
                                       mike,
                                       "LIST ALL PERMISSIONS OF role2")

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT")],
                                       mike,
                                       "LIST ALL PERMISSIONS OF role1")

        assert_unauthorized(mike,
                            "LIST ALL PERMISSIONS",
                            "You are not authorized to view everyone's permissions")
        assert_unauthorized(mike,
                            "LIST ALL PERMISSIONS OF john",
                            "You are not authorized to view john's permissions")

    def role_caching_authenticated_user_test(self):
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
        self.prepare(roles_expiry=2000)
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("GRANT ALL ON table ks.cf TO role1")
        cassandra.execute("GRANT role1 TO mike")

        mike = self.get_session(user='mike', password='12345')
        mike.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        assert_one(mike, "SELECT * FROM ks.cf", [0, 0])

        cassandra.execute("REVOKE role1 FROM mike")
        # mike should retain permissions until the cache expires
        unauthorized = None
        cnt = 0
        while not unauthorized and cnt < 20:
            try:
                mike.execute("SELECT * FROM ks.cf")
                cnt += 1
                time.sleep(.5)
            except Unauthorized as e:
                unauthorized = e

        self.assertIsNotNone(unauthorized)

    def drop_non_existent_role_should_not_update_cache(self):
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
        # The su status check during DROP ROLE IF EXISTS <role>
        # should not cause a non-existent role to be cached (CASSANDRA-9189)
        self.prepare(roles_expiry=10000)
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        # Dropping a role which doesn't exist should be a no-op. If we cache the fact
        # that the role doesn't exist though, subsequent authz attempts which should
        # succeed will fail due to the erroneous cache entry
        cassandra.execute("DROP ROLE IF EXISTS mike")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("GRANT ALL ON ks.cf TO mike")

        mike = self.get_session(user='mike', password='12345')
        mike.execute("SELECT * FROM ks.cf")

    def prevent_circular_grants_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create several roles
        * Verify we cannot grant roles in circular chain
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT role2 to role1")
        cassandra.execute("GRANT role1 TO mike")
        assert_invalid(cassandra,
                       "GRANT mike TO role1",
                       "mike is a member of role1",
                       InvalidRequest)
        assert_invalid(cassandra,
                       "GRANT mike TO role2",
                       "mike is a member of role2",
                       InvalidRequest)

    def create_user_as_alias_for_create_role_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Attempt to create roles using "CREATE USER". Verify still works
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        assert_one(cassandra, "LIST ROLES OF mike", list(mike_role))

        cassandra.execute("CREATE USER super_user WITH PASSWORD '12345' SUPERUSER")
        assert_one(cassandra, "LIST ROLES OF super_user", ["super_user", True, True, {}])

    def role_name_test(self):
        """
        Simple test to verify the behaviour of quoting when creating roles & users
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify that CREATE ROLE is not case sensitive, when identifiers are unquoted
        * Verify that CREATE ROLE is case sensitive, when identifiers are quoted
        * Verify that CREATE USER is always case sensitive
        @jira_ticket CASSANDRA-10394
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        # unquoted identifiers and unreserved keyword do not preserve case
        # count
        cassandra.execute("CREATE ROLE ROLE1 WITH PASSWORD = '12345' AND LOGIN = true")
        self.assert_unauthenticated("Username and/or password are incorrect", 'ROLE1', '12345')
        self.get_session(user='role1', password='12345')

        cassandra.execute("CREATE ROLE COUNT WITH PASSWORD = '12345' AND LOGIN = true")
        self.assert_unauthenticated("Username and/or password are incorrect", 'COUNT', '12345')
        self.get_session(user='count', password='12345')

        # string literals and quoted names do preserve case
        cassandra.execute("CREATE ROLE 'ROLE2' WITH PASSWORD = '12345' AND LOGIN = true")
        self.get_session(user='ROLE2', password='12345')
        self.assert_unauthenticated("Username and/or password are incorrect", 'Role2', '12345')

        cassandra.execute("""CREATE ROLE "ROLE3" WITH PASSWORD = '12345' AND LOGIN = true""")
        self.get_session(user='ROLE3', password='12345')
        self.assert_unauthenticated("Username and/or password are incorrect", 'Role3', '12345')

        # when using legacy USER syntax, both unquoted identifiers and string literals preserve case
        cassandra.execute("CREATE USER USER1 WITH PASSWORD '12345'")
        self.get_session(user='USER1', password='12345')
        self.assert_unauthenticated("Username and/or password are incorrect", 'User1', '12345')

        cassandra.execute("CREATE USER 'USER2' WITH PASSWORD '12345'")
        self.get_session(user='USER2', password='12345')
        self.assert_unauthenticated("Username and/or password are incorrect", 'User2', '12345')

    def role_requires_login_privilege_to_authenticate_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user,'mike', with the login privilege
        * Connect as mike
        * Remove mike's login privilege. Verify mike cannot login
        * Restore mike's login privilege. Verify mike can connect again.
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        assert_one(cassandra, "LIST ROLES OF mike", list(mike_role))
        self.get_session(user='mike', password='12345')

        cassandra.execute("ALTER ROLE mike WITH LOGIN = false")
        assert_one(cassandra, "LIST ROLES OF mike", ["mike", False, False, {}])
        self.assert_unauthenticated('mike is not permitted to log in', 'mike', '12345')

        cassandra.execute("ALTER ROLE mike WITH LOGIN = true")
        assert_one(cassandra, "LIST ROLES OF mike", ["mike", False, True, {}])
        self.get_session(user='mike', password='12345')

    def roles_do_not_inherit_login_privilege_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user who can login, and 'mike', who cannot
        * Grant the other user to mike.
        * Verify mike still cannot log in.
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = false")
        cassandra.execute("CREATE ROLE with_login WITH PASSWORD = '54321' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT with_login to mike")

        assert_all(cassandra, "LIST ROLES OF mike", [["mike", False, False, {}],
                                                     ["with_login", False, True, {}]])
        assert_one(cassandra, "LIST ROLES OF with_login", ["with_login", False, True, {}])

        self.assert_unauthenticated("mike is not permitted to log in", "mike", "12345")

    def role_requires_password_to_login_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user, 'mike', with the login privilege but no password
        * Verify we cannot connect as mike
        * Alter mike and add a password
        * Verify mike can now connect
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH SUPERUSER = false AND LOGIN = true")
        self.assert_unauthenticated("Username and/or password are incorrect", 'mike', None)
        cassandra.execute("ALTER ROLE mike WITH PASSWORD = '12345'")
        self.get_session(user='mike', password='12345')

    def superuser_status_is_inherited_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a superuser role, and 'mike'.
        * Connect as mike
        * Verify that mike does not have permissions.
        * Grant the superuser role to mike.
        * Verify that mike now has all permissions
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE db_admin WITH SUPERUSER = true")

        mike = self.get_session(user='mike', password='12345')
        assert_unauthorized(mike,
                            "CREATE ROLE another_role WITH SUPERUSER = false AND LOGIN = false",
                            "User mike does not have sufficient privileges to perform the requested operation")

        cassandra.execute("GRANT db_admin TO mike")
        mike.execute("CREATE ROLE another_role WITH SUPERUSER = false AND LOGIN = false")
        assert_all(mike, "LIST ROLES", [["another_role", False, False, {}],
                                        list(cassandra_role),
                                        ["db_admin", True, False, {}],
                                        list(mike_role)])

    def list_users_considers_inherited_superuser_status_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a superuser role, and 'mike'
        * Grant the superuser role to mike
        * Verify that LIST USERS shows mike as a superuser, even though that privilege is granted indirectly
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE db_admin WITH SUPERUSER = true")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT db_admin TO mike")
        assert_all(cassandra, "LIST USERS", [['cassandra', True],
                                             ["mike", True]])

    # UDF permissions tests # TODO move to separate fixture & refactor this + auth_test.py
    def grant_revoke_udf_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create two UDFs
        * Selectively grant and revoke each possible UDF permission to mike, and verify those operations worked
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("CREATE FUNCTION ks.\"plusOne\" ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")

        # grant / revoke on a specific function
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.\"plusOne\"(int) TO mike")

        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE"),
                                        ("mike", "<function ks.plusOne(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE ALL PERMISSIONS ON FUNCTION ks.plus_one(int) FROM mike")
        self.assert_permissions_listed([("mike", "<function ks.plusOne(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE EXECUTE PERMISSION ON FUNCTION ks.\"plusOne\"(int) FROM mike")
        self.assert_no_permissions(cassandra, "LIST ALL PERMISSIONS OF mike")

        # grant / revoke on all functions in a given keyspace
        cassandra.execute("GRANT EXECUTE PERMISSION ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        self.assert_permissions_listed([("mike", "<all functions in ks>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE EXECUTE PERMISSION ON ALL FUNCTIONS IN KEYSPACE ks FROM mike")
        self.assert_no_permissions(cassandra, "LIST ALL PERMISSIONS OF mike")

        # grant / revoke on all (non-builtin) functions
        cassandra.execute("GRANT EXECUTE PERMISSION ON ALL FUNCTIONS TO mike")
        self.assert_permissions_listed([("mike", "<all functions>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE EXECUTE PERMISSION ON ALL FUNCTIONS FROM mike")
        self.assert_no_permissions(cassandra, "LIST ALL PERMISSIONS OF mike")

    def grant_revoke_are_idempotent_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create a UDF
        * Issue multiple grants and revokes of permissions for the UDF to mike
        * Verify the grants/revokes are idempotent, and were successful
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike")
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE EXECUTE ON FUNCTION ks.plus_one(int) FROM mike")
        self.assert_no_permissions(cassandra, "LIST ALL PERMISSIONS OF mike")
        cassandra.execute("REVOKE EXECUTE ON FUNCTION ks.plus_one(int) FROM mike")
        self.assert_no_permissions(cassandra, "LIST ALL PERMISSIONS OF mike")

    def function_resource_hierarchy_permissions_test(self):
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
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("INSERT INTO ks.t1 (k,v) values (1,1)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("GRANT SELECT ON ks.t1 TO mike")
        cassandra.execute("CREATE FUNCTION ks.func_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("CREATE FUNCTION ks.func_two ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")

        mike = self.get_session(user='mike', password='12345')
        select_one = "SELECT k, v, ks.func_one(v) FROM ks.t1 WHERE k = 1"
        select_two = "SELECT k, v, ks.func_two(v) FROM ks.t1 WHERE k = 1"

        # grant EXECUTE on only one of the two functions
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.func_one(int) TO mike")
        mike.execute(select_one)
        assert_unauthorized(mike, select_two,
                            "User mike has no EXECUTE permission on <function ks.func_two\(int\)> or any of its parents")
        # granting EXECUTE on all of the parent keyspace's should enable mike to use both functions
        cassandra.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        mike.execute(select_one)
        mike.execute(select_two)
        # revoke the keyspace level privilege and verify that the function specific perms are unaffected
        cassandra.execute("REVOKE EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks FROM mike")
        mike.execute(select_one)
        assert_unauthorized(mike, select_two,
                            "User mike has no EXECUTE permission on <function ks.func_two\(int\)> or any of its parents")
        # now check that EXECUTE on ALL FUNCTIONS works in the same way
        cassandra.execute("GRANT EXECUTE ON ALL FUNCTIONS TO mike")
        mike.execute(select_one)
        mike.execute(select_two)
        cassandra.execute("REVOKE EXECUTE ON ALL FUNCTIONS FROM mike")
        mike.execute(select_one)
        assert_unauthorized(mike, select_two,
                            "User mike has no EXECUTE permission on <function ks.func_two\(int\)> or any of its parents")
        # finally, check that revoking function level permissions doesn't affect root/keyspace level perms
        cassandra.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        cassandra.execute("REVOKE EXECUTE ON FUNCTION ks.func_one(int) FROM mike")
        mike.execute(select_one)
        mike.execute(select_two)
        cassandra.execute("REVOKE EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks FROM mike")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.func_one(int) TO mike")
        cassandra.execute("GRANT EXECUTE ON ALL FUNCTIONS TO mike")
        mike.execute(select_one)
        mike.execute(select_two)
        cassandra.execute("REVOKE EXECUTE ON FUNCTION ks.func_one(int) FROM mike")
        mike.execute(select_one)
        mike.execute(select_two)

    def udf_permissions_validation_test(self):
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
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        mike = self.get_session(user='mike', password='12345')

        # can't replace an existing function without ALTER permission on the parent ks
        cql = "CREATE OR REPLACE FUNCTION ks.plus_one( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript as '1 + input'"
        assert_unauthorized(mike, cql,
                            "User mike has no ALTER permission on <function ks.plus_one\(int\)> or any of its parents")
        cassandra.execute("GRANT ALTER ON FUNCTION ks.plus_one(int) TO mike")
        mike.execute(cql)

        # can't grant permissions on a function without AUTHORIZE (and without the grantor having EXECUTE themself)
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        cassandra.execute("CREATE ROLE role1")
        cql = "GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO role1"
        assert_unauthorized(mike, cql,
                            "User mike has no AUTHORIZE permission on <function ks.plus_one\(int\)> or any of its parents")
        cassandra.execute("GRANT AUTHORIZE ON FUNCTION ks.plus_one(int) TO mike")
        mike.execute(cql)
        # now revoke AUTHORIZE from mike
        cassandra.execute("REVOKE AUTHORIZE ON FUNCTION ks.plus_one(int) FROM mike")
        cql = "REVOKE EXECUTE ON FUNCTION ks.plus_one(int) FROM role1"
        assert_unauthorized(mike, cql,
                            "User mike has no AUTHORIZE permission on <function ks.plus_one\(int\)> or any of its parents")
        cassandra.execute("GRANT AUTHORIZE ON FUNCTION ks.plus_one(int) TO mike")
        mike.execute(cql)

        # can't drop a function without DROP
        cql = "DROP FUNCTION ks.plus_one(int)"
        assert_unauthorized(mike, cql,
                            "User mike has no DROP permission on <function ks.plus_one\(int\)> or any of its parents")
        cassandra.execute("GRANT DROP ON FUNCTION ks.plus_one(int) TO mike")
        mike.execute(cql)

        # DROP IF EXISTS on a non-existent function should return silently, DROP on a non-existent function
        # behaves like DROP TABLE and returns an "Unconfigured XXX" error
        cql = "DROP FUNCTION IF EXISTS ks.no_such_function(int,int)"
        mike.execute(cql)

        cql = "DROP FUNCTION ks.no_such_function(int,int)"
        assert_invalid(mike, cql,
                       "Unconfigured function ks.no_such_function\(int,int\)",
                       InvalidRequest)

        # can't create a new function without CREATE on the parent keyspace's collection of functions
        cql = "CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'"
        assert_unauthorized(mike, cql,
                            "User mike has no CREATE permission on <all functions in ks> or any of its parents")
        cassandra.execute("GRANT CREATE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        mike.execute(cql)

    def drop_role_cleans_up_udf_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create a UDF
        * Grant mike permissions for that UDF
        * Verify that if mike is dropped and recreated, he has no permissions for the UDF
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        cassandra.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        cassandra.execute("GRANT EXECUTE ON ALL FUNCTIONS TO mike")

        self.assert_permissions_listed([("mike", "<all functions>", "EXECUTE"),
                                        ("mike", "<all functions in ks>", "EXECUTE"),
                                        ("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")

        # drop and recreate the role to ensure permissions are cleared
        cassandra.execute("DROP ROLE mike")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.assert_no_permissions(cassandra, "LIST ALL PERMISSIONS OF mike")

    def drop_function_and_keyspace_cleans_up_udf_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create a UDF
        * Grant mike permissions on the UDF, and on the keyspace of the UDF
        * Verify dropping the UDF removes mike's permissions for the UDF
        * Verify dropping the keyspace removes the rest of mike's permissions
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        cassandra.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")

        self.assert_permissions_listed([("mike", "<all functions in ks>", "EXECUTE"),
                                        ("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")

        # drop the function
        cassandra.execute("DROP FUNCTION ks.plus_one")
        self.assert_permissions_listed([("mike", "<all functions in ks>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        # drop the keyspace
        cassandra.execute("DROP KEYSPACE ks")
        self.assert_no_permissions(cassandra, "LIST ALL PERMISSIONS OF mike")

    def udf_with_overloads_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create two overloaded UDFs (same name, different parameters)
        * Verify that granting/revoking permissions on one function, doesn't affect the other
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input double ) CALLED ON NULL INPUT RETURNS double LANGUAGE javascript AS 'input + 1'")

        # grant execute on one variant
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")

        # and now on the other
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(double) TO mike")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE"),
                                        ("mike", "<function ks.plus_one(double)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")

        # revoke permissions on one of the functions only
        cassandra.execute("REVOKE EXECUTE ON FUNCTION ks.plus_one(double) FROM mike")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")

        # drop the function that the role has no permissions on
        cassandra.execute("DROP FUNCTION ks.plus_one(double)")
        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")

        # finally, drop the function that the role does have permissions on
        cassandra.execute("DROP FUNCTION ks.plus_one(int)")
        self.assert_no_permissions(cassandra, "LIST ALL PERMISSIONS OF mike")

    def drop_keyspace_cleans_up_function_level_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike', and a UDF
        * Grant mike permissions to the UDF
        * Drop the keyspace containing the UDF
        * Verify mike has no permissions
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")

        self.assert_permissions_listed([("mike", "<function ks.plus_one(int)>", "EXECUTE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")
        # drop the keyspace
        cassandra.execute("DROP KEYSPACE ks")
        self.assert_no_permissions(cassandra, "LIST ALL PERMISSIONS OF mike")

    def udf_permissions_in_selection_test(self):
        """
        Verify EXECUTE permission works in a SELECT when UDF is one of the columns requested
        """
        self.verify_udf_permissions("SELECT k, v, ks.plus_one(v) FROM ks.t1 WHERE k = 1")

    def udf_permissions_in_select_where_clause_test(self):
        """
        Verify EXECUTE permission works in a SELECT when UDF is in the WHERE clause
        """
        self.verify_udf_permissions("SELECT k, v FROM ks.t1 WHERE k = ks.plus_one(0)")

    def udf_permissions_in_insert_test(self):
        """
        Verify EXECUTE permission works in an INSERT when UDF is in the VALUES
        """
        self.verify_udf_permissions("INSERT INTO ks.t1 (k, v) VALUES (1, ks.plus_one(1))")

    def udf_permissions_in_update_test(self):
        """
        Verify EXECUTE permission works in an UPDATE when UDF is in the SET and WHERE clauses
        """
        self.verify_udf_permissions("UPDATE ks.t1 SET v = ks.plus_one(2) WHERE k = ks.plus_one(0)")

    def udf_permissions_in_delete_test(self):
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
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("GRANT ALL PERMISSIONS ON ks.t1 TO mike")
        cassandra.execute("INSERT INTO ks.t1 (k,v) values (1,1)")
        mike = self.get_session(user='mike', password='12345')
        assert_unauthorized(mike,
                            cql,
                            "User mike has no EXECUTE permission on <function ks.plus_one\(int\)> or any of its parents")

        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.plus_one(int) TO mike")
        return mike.execute(cql)

    def inheritence_of_udf_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify that if EXECUTE permissions are granted to a parent role, that roles the parent is granted to inherit EXECUTE
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE function_user")
        cassandra.execute("GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE ks TO function_user")
        cassandra.execute("CREATE FUNCTION ks.plus_one ( input int ) CALLED ON NULL INPUT RETURNS int LANGUAGE javascript AS 'input + 1'")
        cassandra.execute("INSERT INTO ks.t1 (k,v) VALUES (1,1)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("GRANT SELECT ON ks.t1 TO mike")
        mike = self.get_session(user='mike', password='12345')
        select = "SELECT k, v, ks.plus_one(v) FROM ks.t1 WHERE k = 1"
        assert_unauthorized(mike,
                            select,
                            "User mike has no EXECUTE permission on <function ks.plus_one\(int\)> or any of its parents")

        cassandra.execute("GRANT function_user TO mike")
        assert_one(mike, select, [1, 1, 2])

    def builtin_functions_require_no_special_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Verify a new role can use all built in functions, with no permissions granted.
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.setup_table(cassandra)
        cassandra.execute("INSERT INTO ks.t1 (k,v) VALUES (1,1)")
        mike = self.get_session(user='mike', password='12345')
        cassandra.execute("GRANT ALL PERMISSIONS ON ks.t1 TO mike")
        assert_one(mike, "SELECT * from ks.t1 WHERE k=blobasint(intasblob(1))", [1, 1])

    def disallow_grant_revoke_on_builtin_functions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Verify that granting or revoking permissions for the builtin functions to mike throw InvalidRequest
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike")
        assert_invalid(cassandra, "GRANT EXECUTE ON FUNCTION system.intasblob(int) TO mike",
                       "Altering permissions on builtin functions is not supported",
                       InvalidRequest)
        assert_invalid(cassandra, "REVOKE ALL PERMISSIONS ON FUNCTION system.intasblob(int) FROM mike",
                       "Altering permissions on builtin functions is not supported",
                       InvalidRequest)
        assert_invalid(cassandra, "GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE system TO mike",
                       "Altering permissions on builtin functions is not supported",
                       InvalidRequest)
        assert_invalid(cassandra, "REVOKE ALL PERMISSIONS ON ALL FUNCTIONS IN KEYSPACE system FROM mike",
                       "Altering permissions on builtin functions is not supported",
                       InvalidRequest)

    def disallow_grant_execute_on_non_function_resources_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Verify that granting EXECUTE on non function resources to mike throws InvalidRequest
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike")
        cassandra.execute("CREATE ROLE role1")

        # can't grant EXECUTE on data or role resources
        assert_invalid(cassandra, "GRANT EXECUTE ON ALL KEYSPACES TO mike",
                       "Resource type DataResource does not support any of the requested permissions",
                       SyntaxException)
        assert_invalid(cassandra, "GRANT EXECUTE ON KEYSPACE ks TO mike",
                       "Resource type DataResource does not support any of the requested permissions",
                       SyntaxException)
        assert_invalid(cassandra, "GRANT EXECUTE ON TABLE ks.t1 TO mike",
                       "Resource type DataResource does not support any of the requested permissions",
                       SyntaxException)
        assert_invalid(cassandra, "GRANT EXECUTE ON ALL ROLES TO mike",
                       "Resource type RoleResource does not support any of the requested permissions",
                       SyntaxException)
        assert_invalid(cassandra, "GRANT EXECUTE ON ROLE mike TO role1",
                       "Resource type RoleResource does not support any of the requested permissions",
                       SyntaxException)

    def aggregate_function_permissions_test(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new role, 'mike'
        * Create two aggregate functions
        * Verify all UDF permissions also apply to aggregates
        """
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        self.setup_table(cassandra)
        cassandra.execute("INSERT INTO ks.t1 (k,v) VALUES (1,1)")
        cassandra.execute("INSERT INTO ks.t1 (k,v) VALUES (2,2)")
        cassandra.execute("""CREATE FUNCTION ks.state_function( a int, b int )
                             CALLED ON NULL INPUT
                             RETURNS int
                             LANGUAGE java
                             AS 'return Integer.valueOf( (a != null ? a.intValue() : 0) + b.intValue());'""")
        cassandra.execute("""CREATE FUNCTION ks.final_function( a int )
                             CALLED ON NULL INPUT
                             RETURNS int
                             LANGUAGE java
                             AS 'return a;'""")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("GRANT CREATE ON ALL FUNCTIONS IN KEYSPACE ks TO mike")
        cassandra.execute("GRANT ALL PERMISSIONS ON ks.t1 TO mike")
        mike = self.get_session(user='mike', password='12345')
        create_aggregate_cql = """CREATE AGGREGATE ks.simple_aggregate(int)
                          SFUNC state_function
                          STYPE int
                          FINALFUNC final_function
                          INITCOND 0"""
        # check permissions to create the aggregate
        assert_unauthorized(mike,
                            create_aggregate_cql,
                            "User mike has no EXECUTE permission on <function ks.state_function\(int, int\)> or any of its parents")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.state_function(int, int) TO mike")
        assert_unauthorized(mike,
                            create_aggregate_cql,
                            "User mike has no EXECUTE permission on <function ks.final_function\(int\)> or any of its parents")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.final_function(int) TO mike")
        mike.execute(create_aggregate_cql)

        # without execute permissions on the state or final function we
        # cannot use the aggregate, so revoke them to verify
        cassandra.execute("REVOKE EXECUTE ON FUNCTION ks.state_function(int, int) FROM mike")
        cassandra.execute("REVOKE EXECUTE ON FUNCTION ks.final_function(int) FROM mike")
        execute_aggregate_cql = "SELECT ks.simple_aggregate(v) FROM ks.t1"
        assert_unauthorized(mike,
                            execute_aggregate_cql,
                            "User mike has no EXECUTE permission on <function ks.state_function\(int, int\)> or any of its parents")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.state_function(int, int) TO mike")
        assert_unauthorized(mike,
                            execute_aggregate_cql,
                            "User mike has no EXECUTE permission on <function ks.final_function\(int\)> or any of its parents")
        cassandra.execute("GRANT EXECUTE ON FUNCTION ks.final_function(int) TO mike")

        # mike *does* have execute permission on the aggregate function, as its creator
        assert_one(mike, execute_aggregate_cql, [3])

    def ignore_invalid_roles_test(self):
        """
        The system_auth.roles table includes a set of roles of which each role
        is a member. If that list were to get out of sync, so that it indicated
        that roleA is a member of roleB, but roleB does not exist in the roles
        table, then the result of LIST ROLES OF roleA should not include roleB
        @jira_ticket CASSANDRA-9551
        """

        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH LOGIN = true")
        # hack an invalid entry into the roles table for roleA
        cassandra.execute("UPDATE system_auth.roles SET member_of = {'role1'} where role = 'mike'")
        assert_all(cassandra, "LIST ROLES OF mike", [list(mike_role)])

    def setup_table(self, session):
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("CREATE TABLE ks.t1 (k int PRIMARY KEY, v int)")

    def assert_unauthenticated(self, message, user, password):
        with self.assertRaises(NoHostAvailable) as response:
            node = self.cluster.nodelist()[0]
            self.cql_connection(node, user=user, password=password)
        host, error = response.exception.errors.popitem()
        pattern = 'Failed to authenticate to %s: Error from server: code=0100 \[Bad credentials\] message="%s"' % (host, message)
        assert isinstance(error, AuthenticationFailed), "Expected AuthenticationFailed, got %s" % error
        assert re.search(pattern, error.message), "Expected: %s, actual: %s" % (pattern, error.message)

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
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start(wait_for_binary_proto=True)

        self.wait_for_any_log(self.cluster.nodelist(), 'Created default superuser', 25)

    def assert_permissions_listed(self, expected, session, query):
        rows = session.execute(query)
        perms = [(str(r.role), str(r.resource), str(r.permission)) for r in rows]
        self.assertEqual(sorted(expected), sorted(perms))

    def assert_no_permissions(self, session, query):
        self.assertItemsEqual(list(session.execute(query)), [])


def role_creator_permissions(creator, role):
    return [(creator, role, perm) for perm in ('ALTER', 'DROP', 'AUTHORIZE')]


def function_resource_creator_permissions(creator, resource):
    return [(creator, resource, perm) for perm in ('ALTER', 'DROP', 'AUTHORIZE', 'EXECUTE')]
