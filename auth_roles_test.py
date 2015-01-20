import time

from cassandra import AuthenticationFailed, Unauthorized
from cassandra.cluster import NoHostAvailable
from dtest import Tester
from assertions import *
from tools import since

#Second value is superuser status
#Third value is login status, See #7653 for explanation.
mike_role = ['mike', False, True]
role1_role = ['role1', False, False]
role2_role = ['role2', False, False]
cassandra_role = ['cassandra', True, True]


class TestAuthRoles(Tester):

    @since('3.0')
    def create_drop_role_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        assert_one(cassandra, 'LIST ROLES', cassandra_role)

        cassandra.execute("CREATE ROLE role1")
        assert_all(cassandra, "LIST ROLES", [cassandra_role, role1_role])

        cassandra.execute("DROP ROLE role1")
        assert_one(cassandra, "LIST ROLES", cassandra_role)

    @since('3.0')
    def conditional_create_drop_role_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        assert_one(cassandra, "LIST ROLES", cassandra_role)

        cassandra.execute("CREATE ROLE IF NOT EXISTS role1")
        cassandra.execute("CREATE ROLE IF NOT EXISTS role1")
        assert_all(cassandra, "LIST ROLES", [cassandra_role, role1_role])

        cassandra.execute("DROP ROLE IF EXISTS role1")
        cassandra.execute("DROP ROLE IF EXISTS role1")
        assert_one(cassandra, "LIST ROLES", cassandra_role)

    @since('3.0')
    def create_drop_role_validation_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        mike = self.get_session(user='mike', password='12345')

        assert_invalid(mike,
                       "CREATE ROLE role2",
                       "Only superusers are allowed to perform CREATE \[ROLE\|USER\] queries",
                       Unauthorized)
        cassandra.execute("CREATE ROLE role1")

        assert_invalid(mike,
                       "DROP ROLE role1",
                       "Only superusers are allowed to perform DROP \[ROLE\|USER\] queries",
                       Unauthorized)
        assert_invalid(cassandra, "CREATE ROLE role1", "role1 already exists")
        cassandra.execute("DROP ROLE role1")
        assert_invalid(cassandra, "DROP ROLE role1", "role1 doesn't exist")

    @since('3.0')
    def drop_role_removes_memberships_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("GRANT role2 TO role1")
        cassandra.execute("GRANT role1 TO mike")
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])

        # drop the role indirectly granted
        cassandra.execute("DROP ROLE role2")
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role])

        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT role2 to role1")
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])
        # drop the directly granted role
        cassandra.execute("DROP ROLE role1")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)
        assert_all(cassandra, "LIST ROLES", [cassandra_role, mike_role, role2_role])

    @since('3.0')
    def grant_revoke_roles_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT role1 TO role2")
        cassandra.execute("GRANT role2 TO mike")

        assert_all(cassandra, "LIST ROLES OF role2", [role1_role, role2_role])
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])
        assert_all(cassandra, "LIST ROLES OF mike NORECURSIVE", [mike_role, role2_role])

        cassandra.execute("REVOKE role2 FROM mike")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)

        cassandra.execute("REVOKE role1 FROM role2")
        assert_one(cassandra, "LIST ROLES OF role2", role2_role)

    @since('3.0')
    def grant_revoke_role_validation_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        mike = self.get_session(user='mike', password='12345')

        assert_invalid(cassandra, "GRANT role1 TO mike", "role1 doesn't exist")
        cassandra.execute("CREATE ROLE role1")

        assert_invalid(cassandra, "GRANT role1 TO john", "john doesn't exist")
        assert_invalid(cassandra, "GRANT role1 TO role2", "role2 doesn't exist")

        cassandra.execute("CREATE ROLE john WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE role2")

        assert_invalid(mike,
                       "GRANT role1 TO john",
                       "Only superusers are allowed to perform role management queries",
                       Unauthorized)
        assert_invalid(mike,
                       "GRANT role1 TO role2",
                       "Only superusers are allowed to perform role management queries",
                       Unauthorized)

        cassandra.execute("GRANT role1 TO john")
        cassandra.execute("GRANT role1 TO role2")

        assert_invalid(mike,
                       "REVOKE role1 FROM john",
                       "Only superusers are allowed to perform role management queries",
                       Unauthorized)
        assert_invalid(mike,
                       "REVOKE role1 FROM role2",
                       "Only superusers are allowed to perform role management queries",
                       Unauthorized)

    @since('3.0')
    def list_roles_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")

        assert_all(cassandra, "LIST ROLES", [cassandra_role, mike_role, role1_role, role2_role])

        cassandra.execute("GRANT role1 TO role2")
        cassandra.execute("GRANT role2 TO mike")

        assert_all(cassandra, "LIST ROLES OF role2", [role1_role, role2_role])
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])
        assert_all(cassandra, "LIST ROLES OF mike NORECURSIVE", [mike_role, role2_role])

        mike = self.get_session(user='mike', password='12345')
        assert_invalid(mike,
                       "LIST ROLES OF cassandra",
                       "You are not authorized to view roles granted to cassandra",
                       Unauthorized)
        assert_all(mike, "LIST ROLES", [mike_role, role1_role, role2_role])
        assert_all(mike, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])
        assert_all(mike, "LIST ROLES OF mike NORECURSIVE", [mike_role, role2_role])
        assert_all(mike, "LIST ROLES OF role2", [role1_role, role2_role])


    @since('3.0')
    def grant_revoke_permissions_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("GRANT ALL ON table ks.cf TO role1")
        cassandra.execute("GRANT role1 TO mike")

        mike = self.get_session(user='mike', password='12345')
        mike.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        assert_one(mike, "SELECT * FROM ks.cf", [0, 0])

        cassandra.execute("REVOKE role1 FROM mike")
        assert_invalid(mike,
                       "INSERT INTO ks.cf (id, val) VALUES (0, 0)",
                       "mike has no MODIFY permission on <table ks.cf> or any of its parents",
                       Unauthorized)

        cassandra.execute("GRANT role1 TO mike")
        cassandra.execute("REVOKE ALL ON ks.cf FROM role1")

        assert_invalid(mike,
                       "INSERT INTO ks.cf (id, val) VALUES (0, 0)",
                       "mike has no MODIFY permission on <table ks.cf> or any of its parents",
                       Unauthorized)

    @since('3.0')
    def list_permissions_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT SELECT ON table ks.cf TO role1")
        cassandra.execute("GRANT ALTER ON table ks.cf TO role2")
        cassandra.execute("GRANT MODIFY ON table ks.cf TO mike")
        cassandra.execute("GRANT role1 TO role2")
        cassandra.execute("GRANT role2 TO mike")

        self.assert_permissions_listed([("mike", "<table ks.cf>", "MODIFY"),
                                        ("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS")

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF role1")

        self.assert_permissions_listed([("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF role2")

        mike = self.get_session(user='mike', password='12345')
        self.assert_permissions_listed([("mike", "<table ks.cf>", "MODIFY"),
                                        ("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER")],
                                       mike,
                                       "LIST ALL PERMISSIONS OF mike")

    @since('3.0')
    def list_permissions_validation_test(self):
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE john WITH PASSWORD '12345' NOSUPERUSER LOGIN")
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

        assert_invalid(mike,
                       "LIST ALL PERMISSIONS",
                       "You are not authorized to view everyone's permissions",
                       Unauthorized)
        assert_invalid(mike,
                       "LIST ALL PERMISSIONS OF john",
                       "You are not authorized to view john's permissions",
                       Unauthorized)

    @since('3.0')
    def role_caching_authenticated_user_test(self):
        # This test is to show that the role caching in AuthenticatedUser
        # works correctly and revokes the roles from a logged in user
        self.prepare(roles_expiry=2000)
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
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

        assert unauthorized is not None

    @since('3.0')
    def prevent_circular_grants_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
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

    @since('3.0')
    def create_user_as_alias_for_create_role_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)

        cassandra.execute("CREATE USER super_user WITH PASSWORD '12345' SUPERUSER")
        assert_one(cassandra, "LIST ROLES OF super_user", ["super_user", True, True])

    @since('3.0')
    def role_requires_login_privilege_to_authenticate_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)
        self.get_session(user='mike', password='12345')

        cassandra.execute("ALTER ROLE mike NOLOGIN")
        assert_one(cassandra, "LIST ROLES OF mike", ["mike", False, False])
        self.assert_unauthenticated('mike is not permitted to log in', 'mike', '12345')

        cassandra.execute("ALTER ROLE mike LOGIN")
        assert_one(cassandra, "LIST ROLES OF mike", ["mike", False, True])
        self.get_session(user='mike', password='12345')

    @since('3.0')
    def roles_do_not_inherit_login_privilege_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER NOLOGIN")
        cassandra.execute("CREATE ROLE with_login WITH PASSWORD '54321' NOSUPERUSER LOGIN")
        cassandra.execute("GRANT with_login to mike")

        assert_all(cassandra, "LIST ROLES OF mike", [["mike", False, False],
                                                     ["with_login", False, True]])
        assert_one(cassandra, "LIST ROLES OF with_login", ["with_login", False, True])

        self.assert_unauthenticated("mike is not permitted to log in", "mike", "12345")

    @since('3.0')
    def role_requires_password_to_login_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike NOSUPERUSER LOGIN")
        self.assert_unauthenticated("Username and/or password are incorrect", 'mike', None)
        cassandra.execute("ALTER ROLE mike WITH PASSWORD '12345'")
        self.get_session(user='mike', password='12345')

    @since('3.0')
    def superuser_status_is_inherited_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE db_admin SUPERUSER")

        mike = self.get_session(user='mike', password='12345')
        assert_invalid(mike,
                       "CREATE ROLE another_role NOSUPERUSER NOLOGIN",
                       "Only superusers are allowed to perform CREATE \[ROLE\|USER\] queries",
                       Unauthorized)

        cassandra.execute("GRANT db_admin TO mike")
        mike.execute("CREATE ROLE another_role NOSUPERUSER NOLOGIN")
        assert_all(mike, "LIST ROLES", [["another_role", False, False],
                                        ["cassandra", True, True],
                                        ["db_admin", True, False],
                                        ["mike", False, True]])

    def assert_unauthenticated(self, message, user, password):
        with self.assertRaises(NoHostAvailable) as response:
            node = self.cluster.nodelist()[0]
            self.cql_connection(node, version="3.1.7", user=user, password=password)
        host, error = response.exception.errors.popitem()
        pattern = 'Failed to authenticate to %s: code=0100 \[Bad credentials\] message="%s"' % (host, message)
        assert type(error) == AuthenticationFailed, "Expected AuthenticationFailed, got %s" % type(error)
        assert re.search(pattern, error.message), "Expected: %s" % pattern

    def prepare(self, nodes=1, roles_expiry=0):
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
                  'permissions_validity_in_ms': 0,
                  'roles_validity_in_ms': roles_expiry}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start(no_wait=True)
        # default user setup is delayed by 10 seconds to reduce log spam

        if nodes == 1:
            self.cluster.nodelist()[0].watch_log_for("Created default superuser")
        else:
            # can' just watch for log - the line will appear in just one of the nodes' logs
            # only one test uses more than 1 node, though, so some sleep is fine.
            time.sleep(15)

    def get_session(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.patient_cql_connection(node, version="3.1.7", user=user, password=password)
        return conn

    def assert_permissions_listed(self, expected, cursor, query):
        # from cassandra.query import named_tuple_factory
        # cursor.row_factory = named_tuple_factory
        rows = cursor.execute(query)
        perms = [(str(r.role), str(r.resource), str(r.permission)) for r in rows]
        self.assertEqual(sorted(expected), sorted(perms))
