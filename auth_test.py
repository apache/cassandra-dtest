import time

from cql import ProgrammingError
from cql.cassandra.ttypes import AuthenticationException
from dtest import Tester
from tools import since

class TestAuth(Tester):

    @since('1.2')
    def system_auth_ks_is_alterable_test(self):
        self.prepare(3)

        schema_query = """SELECT strategy_options
                          FROM system.schema_keyspaces
                          WHERE keyspace_name = 'system_auth'"""

        cursor = self.get_cursor(1, user='cassandra', password='cassandra')
        cursor.execute(schema_query)
        row = cursor.fetchone()
        self.assertEqual('{"replication_factor":"1"}', row[0])

        cursor.execute("""
            ALTER KEYSPACE system_auth
                WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};
        """)

        # make sure schema change is persistent
        self.cluster.stop()
        self.cluster.start()
        self.cluster.repair()

        for i in range(3):
            cursor = self.get_cursor(i, user='cassandra', password='cassandra')
            cursor.execute(schema_query)
            row = cursor.fetchone()
            self.assertEqual('{"replication_factor":"3"}', row[0])

    @since('1.2')
    def login_test(self):
        # also tests default user creation (cassandra/cassandra)
        self.prepare()
        self.get_cursor(user='cassandra', password='cassandra')
        with self.assertRaises(AuthenticationException):
            self.get_cursor(user='cassandra', password='badpassword')
        with self.assertRaises(AuthenticationException):
            self.get_cursor(user='doesntexist', password='doesntmatter')

    @since('1.2')
    def only_superuser_can_create_users_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER jackob WITH PASSWORD '12345' NOSUPERUSER")

        jackob = self.get_cursor(user='jackob', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            jackob.execute("CREATE USER james WITH PASSWORD '54321' NOSUPERUSER")
        self.assertEqual('Bad Request: Only superusers are allowed to perfrom CREATE USER queries',
                         cm.exception.message)

    @since('1.2')
    def password_authenticator_create_user_requires_password_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute("CREATE USER jackob NOSUPERUSER")
        self.assertEqual('Bad Request: PasswordAuthenticator requires PASSWORD option',
                         cm.exception.message)

    @since('1.2')
    def cant_create_existing_user_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        cursor.execute("CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER")
        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute("CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER")
        self.assertEqual('Bad Request: User james@example.com already exists',
                         cm.exception.message)

    @since('1.2')
    def list_users_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        cursor.execute("CREATE USER alex WITH PASSWORD '12345' NOSUPERUSER")
        cursor.execute("CREATE USER bob WITH PASSWORD '12345' SUPERUSER")
        cursor.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        cursor.execute("CREATE USER dave WITH PASSWORD '12345' SUPERUSER")

        cursor.execute("LIST USERS")
        rows = cursor.fetchall()
        self.assertEqual(5, len(rows))
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        self.assertTrue(users['cassandra'])
        self.assertFalse(users['alex'])
        self.assertTrue(users['bob'])
        self.assertFalse(users['cathy'])
        self.assertTrue(users['dave'])

    @since('1.2')
    def user_cant_drop_themselves_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute("DROP USER cassandra")
        self.assertEqual("Bad Request: Users aren't allowed to DROP themselves",
                         cm.exception.message)

    @since('1.2')
    def only_superusers_can_drop_users_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("CREATE USER dave WITH PASSWORD '12345' NOSUPERUSER")
        cassandra.execute("LIST USERS")
        self.assertEqual(3, cassandra.rowcount)

        cathy = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute('DROP USER dave')
        self.assertEqual('Bad Request: Only superusers are allowed to perfrom DROP USER queries',
                         cm.exception.message)
        cassandra.execute("LIST USERS")
        self.assertEqual(3, cassandra.rowcount)

        cassandra.execute('DROP USER dave')
        cassandra.execute("LIST USERS")
        self.assertEqual(2, cassandra.rowcount)

    @since('1.2')
    def dropping_nonexistent_user_throws_exception_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute('DROP USER nonexistent')
        self.assertEqual("Bad Request: User nonexistent doesn't exist",
                         cm.exception.message)

    @since('1.2')
    def regular_users_can_alter_their_passwords_only_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cathy = self.get_cursor(user='cathy', password='12345')
        cathy.execute("ALTER USER cathy WITH PASSWORD '54321'")
        cathy = self.get_cursor(user='cathy', password='54321')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("ALTER USER bob WITH PASSWORD 'cantchangeit'")
        self.assertEqual("Bad Request: You aren't allowed to alter this user",
                         cm.exception.message)

    @since('1.2')
    def users_cant_alter_their_superuser_status_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute("ALTER USER cassandra NOSUPERUSER")
        self.assertEqual("Bad Request: You aren't allowed to alter your own superuser status",
                         cm.exception.message)

    @since('1.2')
    def only_superuser_alters_superuser_status_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("ALTER USER cassandra NOSUPERUSER")
        self.assertEqual("Bad Request: Only superusers are allowed to alter superuser status",
                         cm.exception.message)

        cassandra.execute("ALTER USER cathy SUPERUSER")

    @since('1.2')
    def altering_nonexistent_user_throws_exception_test(self):
        self.prepare()

        cursor = self.get_cursor(user='cassandra', password='cassandra')
        with self.assertRaises(ProgrammingError) as cm:
            cursor.execute("ALTER USER nonexistent WITH PASSWORD 'doesn''tmatter'")
        self.assertEqual("Bad Request: User nonexistent doesn't exist",
                         cm.exception.message)

    @since('1.2')
    def create_ks_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("""CREATE KEYSPACE ks
                             WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}""")
        self.assertEqual("Bad Request: User cathy has no CREATE permission on <all keyspaces> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO cathy")
        cathy.execute("""CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}""")

    @since('1.2')
    def create_cf_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("CREATE TABLE ks.cf (id int primary key)")
        self.assertEqual("Bad Request: User cathy has no CREATE permission on <keyspace ks> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute("CREATE TABLE ks.cf (id int primary key)")

    @since('1.2')
    def alter_ks_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("ALTER KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")
        self.assertEqual("Bad Request: User cathy has no ALTER permission on <keyspace ks> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO cathy")
        cathy.execute("ALTER KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':2}")

    @since('1.2')
    def alter_cf_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("ALTER TABLE ks.cf ADD val int")
        self.assertEqual("Bad Request: User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf ADD val int")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("CREATE INDEX ON ks.cf(val)")
        self.assertEqual("Bad Request: User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("CREATE INDEX ON ks.cf(val)")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")

        cathy.execute("USE ks")
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("DROP INDEX cf_val_idx")
        self.assertEqual("Bad Request: User cathy has no ALTER permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("DROP INDEX cf_val_idx")

    @since('1.2')
    def drop_ks_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        cathy = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("DROP KEYSPACE ks")
        self.assertEqual("Bad Request: User cathy has no DROP permission on <keyspace ks> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute("DROP KEYSPACE ks")

    @since('1.2')
    def drop_cf_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("DROP TABLE ks.cf")
        self.assertEqual("Bad Request: User cathy has no DROP permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT DROP ON ks.cf TO cathy")
        cathy.execute("DROP TABLE ks.cf")

    @since('1.2')
    def modify_and_select_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("SELECT * FROM ks.cf")
        self.assertEqual("Bad Request: User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        cathy.execute("SELECT * FROM ks.cf")
        self.assertEquals(0, cathy.rowcount)

        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        self.assertEqual("Bad Request: User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("UPDATE ks.cf SET val = 1 WHERE id = 1")
        self.assertEqual("Bad Request: User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("DELETE FROM ks.cf WHERE id = 1")
        self.assertEqual("Bad Request: User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("TRUNCATE ks.cf")
        self.assertEqual("Bad Request: User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("UPDATE ks.cf SET val = 1 WHERE id = 1")
        cathy.execute("SELECT * FROM ks.cf")
        self.assertEquals(2, cathy.rowcount)

        cathy.execute("DELETE FROM ks.cf WHERE id = 1")
        cathy.execute("SELECT * FROM ks.cf")
        self.assertEquals(1, cathy.rowcount)

        cathy.execute("TRUNCATE ks.cf")
        self.assertEquals(0, cathy.rowcount)

    @since('1.2')
    def grant_revoke_auth_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cathy = self.get_cursor(user='cathy', password='12345')
        # missing both SELECT and AUTHORIZE
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("GRANT SELECT ON ALL KEYSPACES TO bob")
        self.assertEqual("Bad Request: User cathy has no AUTHORIZE permission on <all keyspaces> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT AUTHORIZE ON ALL KEYSPACES TO cathy")

        # still missing SELECT
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("GRANT SELECT ON ALL KEYSPACES TO bob")
        self.assertEqual("Bad Request: User cathy has no SELECT permission on <all keyspaces> or any of its parents",
                         cm.exception.message)

        cassandra.execute("GRANT SELECT ON ALL KEYSPACES TO cathy")

        # should succeed now with both SELECT and AUTHORIZE
        cathy.execute("GRANT SELECT ON ALL KEYSPACES TO bob")

    @since('1.2')
    def grant_revoke_validation_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")

        with self.assertRaises(ProgrammingError) as cm:
            cassandra.execute("GRANT ALL ON KEYSPACE nonexistent TO cathy")
        self.assertEquals("Bad Request: <keyspace nonexistent> doesn't exist",
                          cm.exception.message)
        with self.assertRaises(ProgrammingError) as cm:
            cassandra.execute("GRANT ALL ON KEYSPACE ks TO nonexistent")
        self.assertEquals("Bad Request: User nonexistent doesn't exist",
                          cm.exception.message)

        with self.assertRaises(ProgrammingError) as cm:
            cassandra.execute("REVOKE ALL ON KEYSPACE nonexistent FROM cathy")
        self.assertEquals("Bad Request: <keyspace nonexistent> doesn't exist",
                          cm.exception.message)
        with self.assertRaises(ProgrammingError) as cm:
            cassandra.execute("REVOKE ALL ON KEYSPACE ks FROM nonexistent")
        self.assertEquals("Bad Request: User nonexistent doesn't exist",
                          cm.exception.message)

    @since('1.2')
    def grant_revoke_cleanup_test(self):
        self.prepare()

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")

        cathy = self.get_cursor(user='cathy', password='12345')
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("SELECT * FROM ks.cf")
        self.assertEquals(1, cathy.rowcount)

        # drop and recreate the user, make sure permissions are gone
        cassandra.execute("DROP USER cathy")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        self.assertEqual("Bad Request: User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("SELECT * FROM ks.cf")
        self.assertEqual("Bad Request: User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        # grant all the permissions back
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("SELECT * FROM ks.cf")
        self.assertEqual(1, cathy.rowcount)

        # drop and recreate the keyspace, make sure permissions are gone
        cassandra.execute("DROP KEYSPACE ks")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        self.assertEqual("Bad Request: User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("SELECT * FROM ks.cf")
        self.assertEqual("Bad Request: User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

    @since('1.2')
    def permissions_caching_test(self):
        self.prepare(permissions_expiry=2000)

        cassandra = self.get_cursor(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_cursor(user='cathy', password='12345')

        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("SELECT * FROM ks.cf")
        self.assertEqual("Bad Request: User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        # grant SELECT to cathy
        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")

        # start another client, sleep for about 2 seconds, retry the request
        # should still see a failure
        time.sleep(1.7)
        cathy2 = self.get_cursor(user='cathy', password='12345')
        with self.assertRaises(ProgrammingError) as cm:
            cathy2.execute("SELECT * FROM ks.cf")
        self.assertEqual("Bad Request: User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

        # wait a bit more until the cache expires
        time.sleep(0.4)
        cathy2.execute("SELECT * FROM ks.cf")
        self.assertEqual(0, cathy2.rowcount)

        # revoke SELECT from cathy
        cassandra.execute("REVOKE SELECT ON ks.cf FROM cathy")

        # wait for about 2 seconds and retry - SELECT should still be in the cache
        time.sleep(1.7)
        cathy.execute("SELECT * FROM ks.cf")
        self.assertEqual(0, cathy.rowcount)

        # wait a bit more until the cache expires
        time.sleep(0.4)

        # the changes (SELECT revocation) should kick in now
        with self.assertRaises(ProgrammingError) as cm:
            cathy.execute("SELECT * FROM ks.cf")
        self.assertEqual("Bad Request: User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                         cm.exception.message)

    @since('1.2')
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

        cassandra.execute("LIST ALL PERMISSIONS")
        self.assertPermissions([('cathy', '<all keyspaces>', 'CREATE'),
                                ('cathy', '<table ks.cf>', 'MODIFY'),
                                ('cathy', '<table ks.cf2>', 'SELECT'),
                                ('bob', '<keyspace ks>', 'ALTER'),
                                ('bob', '<table ks.cf>', 'DROP'),
                                ('bob', '<table ks.cf2>', 'MODIFY')],
                               cassandra.fetchall())

        cassandra.execute("LIST ALL PERMISSIONS OF cathy")
        self.assertPermissions([('cathy', '<all keyspaces>', 'CREATE'),
                                ('cathy', '<table ks.cf>', 'MODIFY'),
                                ('cathy', '<table ks.cf2>', 'SELECT')],
                               cassandra.fetchall())

        cassandra.execute("LIST ALL PERMISSIONS ON ks.cf NORECURSIVE")
        self.assertPermissions([('cathy', '<table ks.cf>', 'MODIFY'),
                                ('bob', '<table ks.cf>', 'DROP')],
                               cassandra.fetchall())

        cassandra.execute("LIST SELECT ON ks.cf2")
        self.assertPermissions([('cathy', '<table ks.cf2>', 'SELECT')],
                               cassandra.fetchall())

        cassandra.execute("LIST ALL ON ks.cf OF cathy")
        self.assertPermissions([('cathy', '<all keyspaces>', 'CREATE'),
                                ('cathy', '<table ks.cf>', 'MODIFY')],
                               cassandra.fetchall())

        bob = self.get_cursor(user='bob', password='12345')
        bob.execute("LIST ALL PERMISSIONS OF bob")
        self.assertPermissions([('bob', '<keyspace ks>', 'ALTER'),
                                ('bob', '<table ks.cf>', 'DROP'),
                                ('bob', '<table ks.cf2>', 'MODIFY')],
                               bob.fetchall())

        with self.assertRaises(ProgrammingError) as cm:
            bob.execute("LIST ALL PERMISSIONS")
        self.assertEqual("Bad Request: You are not authorized to view everyone's permissions",
                         cm.exception.message)

        with self.assertRaises(ProgrammingError) as cm:
            bob.execute("LIST ALL PERMISSIONS OF cathy")
        self.assertEqual("Bad Request: You are not authorized to view cathy's permissions",
                         cm.exception.message)

    def prepare(self, nodes=1, permissions_expiry=0):
        config = {'authenticator' : 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer' : 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms' : permissions_expiry}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start()
        time.sleep(11) # default user setup is delayed by 10 seconds to reduce log spam

    def get_cursor(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.cql_connection(node, version="3.0.1", user=user, password=password)
        return conn.cursor()

    def assertPermissions(self, perms, rows):
        rows_to_perms = [(str(r[0]), str(r[1]), str(r[2])) for r in rows]
        self.assertEqual(sorted(perms), sorted(rows_to_perms))
