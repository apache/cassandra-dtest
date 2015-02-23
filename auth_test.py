import time, re

from cassandra import Unauthorized, AuthenticationFailed
from cassandra.cluster import NoHostAvailable
from dtest import debug, Tester
from tools import since
from assertions import assert_invalid
from tools import require

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
        self.prepare(nodes=3)
        debug("nodes started")
        schema_query = """SELECT strategy_options
                          FROM system.schema_keyspaces
                          WHERE keyspace_name = 'system_auth'"""

        cursor = self.get_cursor(0, user='cassandra', password='cassandra')
        rows = cursor.execute(schema_query)
        row = rows[0]
        self.assertEqual('{"replication_factor":"1"}', row[0])


        cursor.execute("""
            ALTER KEYSPACE system_auth
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
        """)

        # make sure schema change is persistent
        debug("Stopping cluster..")
        self.cluster.stop()
        debug("Restarting cluster..")
        self.cluster.start(wait_other_notice=True)

        for i in range(3):
            debug('Checking node: {i}'.format(i=i))
            cursor = self.get_cursor(i, user='cassandra', password='cassandra')
            rows = cursor.execute(schema_query)
            row = rows[0]
            self.assertEqual('{"replication_factor":"3"}', row[0])

    def login_test(self):
        # also tests default user creation (cassandra/cassandra)
        self.prepare()
        self.get_cursor(user='cassandra', password='cassandra')
        try:
            self.get_cursor(user='cassandra', password='badpassword')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)
        try:
            self.get_cursor(user='doesntexist', password='doesntmatter')
        except NoHostAvailable as e:
            assert isinstance(e.errors.values()[0], AuthenticationFailed)

    # from 3.0 role creation is granted by CREATE_ROLE permissions, not superuser status
    @since('1.2', max_version='2.1.x')
    def only_superuser_can_create_users_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER jackob WITH PASSWORD '12345' NOSUPERUSER")

        jackob = self.get_cursor(user='jackob', password='12345')
        self.assertUnauthorized('Only superusers are allowed to perform CREATE (\[ROLE\|USER\]|USER) queries', jackob, "CREATE USER james WITH PASSWORD '54321' NOSUPERUSER")

    @since('1.2', max_version='2.1.x')
    def password_authenticator_create_user_requires_password_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        assert_invalid(cursor, "CREATE USER jackob NOSUPERUSER", 'PasswordAuthenticator requires PASSWORD option')

    def cant_create_existing_user_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        cursor.execute("CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER")
        assert_invalid(cursor, "CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER", 'james@example.com already exists')

    def list_users_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        cursor.execute("CREATE USER alex WITH PASSWORD '12345' NOSUPERUSER")
        cursor.execute("CREATE USER bob WITH PASSWORD '12345' SUPERUSER")
        cursor.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        cursor.execute("CREATE USER dave WITH PASSWORD '12345' SUPERUSER")

        rows = cursor.execute("LIST USERS")
        self.assertEqual(5, len(rows))
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        self.assertTrue(users['cassandra'])
        self.assertFalse(users['alex'])
        self.assertTrue(users['bob'])
        self.assertFalse(users['cathy'])
        self.assertTrue(users['dave'])

    def user_cant_drop_themselves_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        # handle different error messages between versions 2.x & 3.x
        assert_invalid(cursor, "DROP USER cassandra", "(Users aren't allowed to DROP themselves|Cannot DROP primary role for current login)")

    # from 3.0 role deletion is granted by DROP_ROLE permissions, not superuser status
    @since('1.2', max_version='2.1.x')
    def only_superusers_can_drop_users_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE USER dave WITH PASSWORD '12345' NOSUPERUSER")
        rows = cassandra.execute("LIST USERS")
        self.assertEqual(3, len(rows))

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized('Only superusers are allowed to perform DROP (\[ROLE\|USER\]|USER) queries',
                                cathy, 'DROP USER dave')

        rows = cassandra.execute("LIST USERS")
        self.assertEqual(3, len(rows))

        cassandra.execute('DROP USER dave')
        rows = cassandra.execute("LIST USERS")
        self.assertEqual(2, len(rows))

    def dropping_nonexistent_user_throws_exception_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        assert_invalid(cursor, 'DROP USER nonexistent', "nonexistent doesn't exist")

    def regular_users_can_alter_their_passwords_only_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cathy = self.get_cursor(user='cathy', password='12345')
        cathy.execute("ALTER USER cathy WITH PASSWORD '54321'")
        cathy = self.get_cursor(user='cathy', password='54321')
        self.assertUnauthorized("You aren't allowed to alter this user|User cathy does not have sufficient privileges to perform the requested operation",
                                cathy, "ALTER USER bob WITH PASSWORD 'cantchangeit'")

    def users_cant_alter_their_superuser_status_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        self.assertUnauthorized("You aren't allowed to alter your own superuser status",
                                cursor, "ALTER USER cassandra NOSUPERUSER")

    def only_superuser_alters_superuser_status_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("Only superusers are allowed to alter superuser status",
                                cathy, "ALTER USER cassandra NOSUPERUSER")

        cassandra.execute("ALTER USER cathy SUPERUSER")

    def altering_nonexistent_user_throws_exception_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        assert_invalid(cursor, "ALTER USER nonexistent WITH PASSWORD 'doesn''tmatter'", "nonexistent doesn't exist")

    @since('2.0')
    def conditional_create_drop_user_test(self):
        self.prepare()
        cursor = self.get_cursor(user='cassandra', password='cassandra')

        users = cursor.execute("LIST USERS")
        self.assertEqual(1, len(users)) # cassandra

        cursor.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'sup'")
        cursor.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'ignored'")

        users = cursor.execute("LIST USERS")
        self.assertEqual(2, len(users)) # cassandra + aleksey

        cursor.execute("DROP USER IF EXISTS aleksey")
        cursor.execute("DROP USER IF EXISTS aleksey")

        users = cursor.execute("LIST USERS")
        self.assertEqual(1, len(users)) # cassandra

    def create_ks_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no CREATE permission on <all keyspaces> or any of its parents",
                                cathy,
                                "CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO cathy")
        cathy.execute("""CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}""")

    def create_cf_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no CREATE permission on <keyspace ks> or any of its parents",
                                cathy, "CREATE TABLE ks.cf (id int primary key)")

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute("CREATE TABLE ks.cf (id int primary key)")

    def alter_ks_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no ALTER permission on <keyspace ks> or any of its parents",
                                cathy,
                                "ALTER KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")

        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO cathy")
        cathy.execute("ALTER KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")

    def alter_cf_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "ALTER TABLE ks.cf ADD val int")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf ADD val int")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "CREATE INDEX ON ks.cf(val)")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("CREATE INDEX ON ks.cf(val)")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")

        cathy.execute("USE ks")
        self.assertUnauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                                cathy, "DROP INDEX cf_val_idx")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("DROP INDEX cf_val_idx")

    def drop_ks_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no DROP permission on <keyspace ks> or any of its parents",
                                cathy, "DROP KEYSPACE ks")

        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute("DROP KEYSPACE ks")

    def drop_cf_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no DROP permission on <table ks.cf> or any of its parents",
                                cathy, "DROP TABLE ks.cf")

        cassandra.execute("GRANT DROP ON ks.cf TO cathy")
        cathy.execute("DROP TABLE ks.cf")

    def modify_and_select_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        rows = cathy.execute("SELECT * FROM ks.cf")
        self.assertEquals(0, len(rows))

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "UPDATE ks.cf SET val = 1 WHERE id = 1")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "DELETE FROM ks.cf WHERE id = 1")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "TRUNCATE ks.cf")

        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("UPDATE ks.cf SET val = 1 WHERE id = 1")
        rows = cathy.execute("SELECT * FROM ks.cf")
        self.assertEquals(2, len(rows))

        cathy.execute("DELETE FROM ks.cf WHERE id = 1")
        rows = cathy.execute("SELECT * FROM ks.cf")
        self.assertEquals(1, len(rows))

        rows = cathy.execute("TRUNCATE ks.cf")
        assert rows == None

    def grant_revoke_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cathy = self.get_cursor(user='cathy', password='12345')
        # missing both SELECT and AUTHORIZE
        self.assertUnauthorized("User cathy has no AUTHORIZE permission on <all keyspaces> or any of its parents",
                                cathy, "GRANT SELECT ON ALL KEYSPACES TO bob")

        cassandra.execute("GRANT AUTHORIZE ON ALL KEYSPACES TO cathy")

        # still missing SELECT
        self.assertUnauthorized("User cathy has no SELECT permission on <all keyspaces> or any of its parents",
                                cathy, "GRANT SELECT ON ALL KEYSPACES TO bob")

        cassandra.execute("GRANT SELECT ON ALL KEYSPACES TO cathy")

        # should succeed now with both SELECT and AUTHORIZE
        cathy.execute("GRANT SELECT ON ALL KEYSPACES TO bob")

    def grant_revoke_validation_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        assert_invalid(cassandra, "GRANT ALL ON KEYSPACE nonexistent TO cathy", "<keyspace nonexistent> doesn't exist")

        assert_invalid(cassandra, "GRANT ALL ON KEYSPACE ks TO nonexistent", "(User|Role) nonexistent doesn't exist")

        assert_invalid(cassandra, "REVOKE ALL ON KEYSPACE nonexistent FROM cathy", "<keyspace nonexistent> doesn't exist")

        assert_invalid(cassandra, "REVOKE ALL ON KEYSPACE ks FROM nonexistent", "(User|Role) nonexistent doesn't exist")

    def grant_revoke_cleanup_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")

        cathy = self.get_cursor(user='cathy', password='12345')
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        rows = cathy.execute("SELECT * FROM ks.cf")
        self.assertEquals(1, len(rows))

        # drop and recreate the user, make sure permissions are gone
        cassandra.execute("DROP USER cathy")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

        # grant all the permissions back
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        rows = cathy.execute("SELECT * FROM ks.cf")
        self.assertEqual(1, len(rows))

        # drop and recreate the keyspace, make sure permissions are gone
        cassandra.execute("DROP KEYSPACE ks")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        self.assertUnauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

    def permissions_caching_test(self):
        self.prepare(permissions_validity=2000)

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_cursor(user='cathy', password='12345')
        # another user to make sure the cache is at user level
        cathy2 = self.get_cursor(user='cathy', password='12345')
        cathys = [cathy, cathy2]

        self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

        # grant SELECT to cathy
        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        # should still fail after 1 second.
        time.sleep(1.0)
        for c in cathys:
            self.assertUnauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                    c, "SELECT * FROM ks.cf")

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
                    rows = c.execute("SELECT * FROM ks.cf")
                    self.assertEqual(0, len(rows))
                success = True
            except Unauthorized:
                pass
            cnt += 1
            time.sleep(0.1)

        assert success

    def list_permissions_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
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
                           ('bob', '<table ks.cf2>', 'MODIFY')];

        # CASSANDRA-7216 automatically grants permissions on a role to its creator
        if self.cluster.cassandra_version() >= '3.0.0':
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
        if self.cluster.cassandra_version() >= '3.0.0':
            expected_permissions.extend(data_resource_creator_permissions('cassandra', '<table ks.cf>'))
        self.assertPermissionsListed(expected_permissions, cassandra, "LIST ALL PERMISSIONS ON ks.cf NORECURSIVE")

        expected_permissions = [('cathy', '<table ks.cf2>', 'SELECT')]
        # CASSANDRA-7216 automatically grants permissions on a role to its creator
        if self.cluster.cassandra_version() >= '3.0.0':
            expected_permissions.append(('cassandra', '<table ks.cf2>', 'SELECT'))
            expected_permissions.append(('cassandra', '<keyspace ks>', 'SELECT'))
        debug(sorted(expected_permissions))
        self.assertPermissionsListed(expected_permissions, cassandra, "LIST SELECT ON ks.cf2")

        self.assertPermissionsListed([('cathy', '<all keyspaces>', 'CREATE'),
                                      ('cathy', '<table ks.cf>', 'MODIFY')],
                                     cassandra, "LIST ALL ON ks.cf OF cathy")

        bob = self.get_cursor(user='bob', password='12345')
        self.assertPermissionsListed([('bob', '<keyspace ks>', 'ALTER'),
                                      ('bob', '<table ks.cf>', 'DROP'),
                                      ('bob', '<table ks.cf2>', 'MODIFY')],
                                     bob, "LIST ALL PERMISSIONS OF bob")

        self.assertUnauthorized("You are not authorized to view everyone's permissions",
                                bob, "LIST ALL PERMISSIONS")

        self.assertUnauthorized("You are not authorized to view cathy's permissions",
                                bob, "LIST ALL PERMISSIONS OF cathy")

    @since('2.1')
    def type_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no CREATE permission on <keyspace ks> or any of its parents",
                                cathy, "CREATE TYPE ks.address (street text, city text)")
        self.assertUnauthorized("User cathy has no ALTER permission on <keyspace ks> or any of its parents",
                                cathy, "ALTER TYPE ks.address ADD zip_code int")
        self.assertUnauthorized("User cathy has no DROP permission on <keyspace ks> or any of its parents",
                                cathy, "DROP TYPE ks.address")

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute("CREATE TYPE ks.address (street text, city text)")
        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO cathy")
        cathy.execute("ALTER TYPE ks.address ADD zip_code int")
        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute("DROP TYPE ks.address")

    @since('3.0')
    @require('https://issues.apache.org/jira/browse/CASSANDRA-7557')
    def func_auth_test(self):
        self.prepare()
        udf = "CREATE FUNCTION sin ( input double ) RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.sin(input.doubleValue()));'"
        dropUdf = "DROP FUNCTION sin"

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_cursor(user='cathy', password='12345')
        self.assertUnauthorized("User cathy has no CREATE permission on <keyspace ks> or any of its parents",
                                cathy, udf)
        self.assertUnauthorized("User cathy has no DROP permission on <keyspace ks> or any of its parents",
                                cathy, dropUdf)

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute(udf)
        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute(dropUdf)


    def prepare(self, nodes=1, permissions_validity=0):
        config = {'authenticator' : 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer' : 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms' : permissions_validity}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start(no_wait=True)
        # default user setup is delayed by 10 seconds to reduce log spam
        if nodes == 1:
            self.cluster.nodelist()[0].watch_log_for('Created default superuser')
        else:
            # can' just watch for log - the line will appear in just one of the nodes' logs
            # only one test uses more than 1 node, though, so some sleep is fine.
            time.sleep(15)

    def get_cursor(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.patient_cql_connection(node, version="3.0.1", user=user, password=password)
        return conn

    def assertPermissionsListed(self, expected, cursor, query):
        # from cassandra.query import named_tuple_factory
        # cursor.row_factory = named_tuple_factory
        rows = cursor.execute(query)
        perms = [(str(r.username), str(r.resource), str(r.permission)) for r in rows]
        self.assertEqual(sorted(expected), sorted(perms))

    def assertUnauthorized(self, message, cursor, query):
        with self.assertRaises(Unauthorized) as cm:
            cursor.execute(query)
        assert re.search(message, cm.exception.message), "Expected: %s" % message


def data_resource_creator_permissions(creator, resource):
    permissions = []
    for perm in 'SELECT', 'MODIFY', 'ALTER', 'DROP', 'AUTHORIZE':
        permissions.append((creator, resource, perm))
    if resource.startswith("<keyspace "):
        permissions.append((creator, resource, 'CREATE'))
    return permissions


def role_creator_permissions(creator, role):
    permissions = []
    for perm in 'ALTER', 'DROP', 'AUTHORIZE':
        permissions.append((creator, role, perm))
    return permissions