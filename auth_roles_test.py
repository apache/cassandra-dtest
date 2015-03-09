import time

from cassandra import AuthenticationFailed, Unauthorized
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import SyntaxException
from auth_test import data_resource_creator_permissions, role_creator_permissions
from dtest import Tester
from assertions import *
from tools import since

#Second value is superuser status
#Third value is login status, See #7653 for explanation.
mike_role = ['mike', False, True, {}]
role1_role = ['role1', False, False, {}]
role2_role = ['role2', False, False, {}]
cassandra_role = ['cassandra', True, True, {}]


@since('3.0')
class TestAuthRoles(Tester):

    def create_drop_role_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        assert_one(cassandra, 'LIST ROLES', cassandra_role)

        cassandra.execute("CREATE ROLE role1")
        assert_all(cassandra, "LIST ROLES", [cassandra_role, role1_role])

        cassandra.execute("DROP ROLE role1")
        assert_one(cassandra, "LIST ROLES", cassandra_role)

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

    def create_drop_role_validation_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        mike = self.get_session(user='mike', password='12345')

        assert_invalid(mike,
                       "CREATE ROLE role2",
                       "User mike does not have sufficient privileges to perform the requested operation",
                       Unauthorized)
        cassandra.execute("CREATE ROLE role1")

        assert_invalid(mike,
                       "DROP ROLE role1",
                       "User mike does not have sufficient privileges to perform the requested operation",
                       Unauthorized)

        assert_invalid(cassandra, "CREATE ROLE role1", "role1 already exists")
        cassandra.execute("DROP ROLE role1")
        assert_invalid(cassandra, "DROP ROLE role1", "role1 doesn't exist")

    def role_admin_validation_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE administrator NOSUPERUSER NOLOGIN")
        cassandra.execute("GRANT ALL ON ALL ROLES TO administrator")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("GRANT administrator TO mike")
        cassandra.execute("CREATE ROLE klaus WITH PASSWORD '54321' NOSUPERUSER LOGIN")
        mike = self.get_session(user='mike', password='12345')
        klaus = self.get_session(user='klaus', password='54321')

        # roles with CREATE on ALL ROLES can create roles
        mike.execute("CREATE ROLE role1 WITH PASSWORD '11111' NOLOGIN")

        # require ALTER on ALL ROLES or a SPECIFIC ROLE to modify
        self.assert_unauthenticated('role1 is not permitted to log in', 'role1', '11111')
        cassandra.execute("GRANT ALTER on ROLE role1 TO klaus")
        klaus.execute("ALTER ROLE role1 LOGIN")
        mike.execute("ALTER ROLE role1 WITH PASSWORD '22222'")
        role1 = self.get_session(user='role1', password='22222')

        # only superusers can set superuser status
        assert_invalid(mike, "ALTER ROLE role1 SUPERUSER",
                       "Only superusers are allowed to alter superuser status",
                       Unauthorized)
        assert_invalid(mike, "ALTER ROLE mike SUPERUSER",
                       "You aren't allowed to alter your own superuser status or that of a role granted to you",
                       Unauthorized)

        # roles without necessary permissions cannot create, drop or alter roles except themselves
        assert_invalid(role1, "CREATE ROLE role2 NOLOGIN",
                       "User role1 does not have sufficient privileges to perform the requested operation",
                       Unauthorized)
        assert_invalid(role1, "ALTER ROLE mike NOLOGIN",
                       "User role1 does not have sufficient privileges to perform the requested operation",
                       Unauthorized)
        assert_invalid(role1, "DROP ROLE mike",
                       "User role1 does not have sufficient privileges to perform the requested operation",
                       Unauthorized)
        role1.execute("ALTER ROLE role1 WITH PASSWORD '33333'")

        # roles with roleadmin can drop roles
        mike.execute("DROP ROLE role1")
        assert_all(cassandra, "LIST ROLES", [['administrator', False, False, {}],
                                             cassandra_role,
                                             ['klaus', False, True, {}],
                                             mike_role])

        # revoking role admin removes its privileges
        cassandra.execute("REVOKE administrator FROM mike")
        assert_invalid(mike, "CREATE ROLE role3 NOLOGIN",
                       "User mike does not have sufficient privileges to perform the requested operation",
                       Unauthorized)

    def creator_of_db_resource_granted_all_permissions_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO mike")
        cassandra.execute("GRANT CREATE ON ALL ROLES TO mike")

        mike = self.get_session(user='mike', password='12345')
        # mike should automatically be granted permissions on any resource he creates, i.e. tables or roles
        mike.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        mike.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        mike.execute("CREATE ROLE role1 WITH PASSWORD '11111' NOSUPERUSER LOGIN")

        cassandra_permissions = role_creator_permissions('cassandra', '<role mike>')
        mike_permissions = [('mike', '<all roles>', 'CREATE'), ('mike', '<all keyspaces>', 'CREATE')]
        mike_permissions.extend(role_creator_permissions('mike', '<role role1>'))
        mike_permissions.extend(data_resource_creator_permissions('mike', '<keyspace ks>'))
        mike_permissions.extend(data_resource_creator_permissions('mike', '<table ks.cf>'))

        self.assert_permissions_listed(cassandra_permissions + mike_permissions,
                                       cassandra,
                                       "LIST ALL PERMISSIONS")

    def create_and_grant_roles_with_superuser_status_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE another_superuser SUPERUSER NOLOGIN")
        cassandra.execute("CREATE ROLE non_superuser NOSUPERUSER NOLOGIN")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        # mike can create and grant any role, except superusers
        cassandra.execute("GRANT CREATE ON ALL ROLES TO mike")
        cassandra.execute("GRANT AUTHORIZE ON ALL ROLES TO mike")

        # mike can create roles, but not with superuser status
        # and can grant any role, including those with superuser status
        mike = self.get_session(user='mike', password='12345')
        mike.execute("CREATE ROLE role1 NOSUPERUSER")
        mike.execute("GRANT non_superuser TO role1")
        mike.execute("GRANT another_superuser TO role1")
        assert_invalid(mike, "CREATE ROLE role2 SUPERUSER",
                       "Only superusers can create a role with superuser status",
                       Unauthorized)
        assert_all(cassandra, "LIST ROLES OF role1", [['another_superuser', True, False, {}],
                                                      ['non_superuser', False, False, {}],
                                                      ['role1', False, False, {}]])

    def drop_and_revoke_roles_with_superuser_status_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE another_superuser SUPERUSER NOLOGIN")
        cassandra.execute("CREATE ROLE non_superuser NOSUPERUSER NOLOGIN")
        cassandra.execute("CREATE ROLE role1 NOSUPERUSER")
        cassandra.execute("GRANT another_superuser TO role1")
        cassandra.execute("GRANT non_superuser TO role1")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("GRANT DROP ON ALL ROLES TO mike")
        cassandra.execute("GRANT AUTHORIZE ON ALL ROLES TO mike")

        # mike can drop and revoke any role, including superusers
        mike = self.get_session(user='mike', password='12345')
        mike.execute("REVOKE another_superuser FROM role1")
        mike.execute("REVOKE non_superuser FROM role1")
        mike.execute("DROP ROLE non_superuser")
        mike.execute("DROP ROLE role1")


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

    def drop_role_revokes_permissions_granted_on_it_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("GRANT ALTER ON ROLE role1 TO mike")
        cassandra.execute("GRANT AUTHORIZE ON ROLE role2 TO mike")

        self.assert_permissions_listed([("mike", "<role role1>", "ALTER"),
                                        ("mike", "<role role2>", "AUTHORIZE")],
                                       cassandra,
                                       "LIST ALL PERMISSIONS OF mike")

        cassandra.execute("DROP ROLE role1")
        cassandra.execute("DROP ROLE role2")
        assert cassandra.execute("LIST ALL PERMISSIONS OF mike") is None

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

    def grant_revoke_role_validation_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        mike = self.get_session(user='mike', password='12345')

        assert_invalid(cassandra, "GRANT role1 TO mike", "role1 doesn't exist")
        cassandra.execute("CREATE ROLE role1")

        assert_invalid(cassandra, "GRANT role1 TO john", "john doesn't exist")
        assert_invalid(cassandra, "GRANT role2 TO john", "role2 doesn't exist")

        cassandra.execute("CREATE ROLE john WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE role2")

        assert_invalid(mike,
                       "GRANT role2 TO john",
                       "User mike does not have sufficient privileges to perform the requested operation",
                       Unauthorized)

        # superusers can always grant roles
        cassandra.execute("GRANT role1 TO john")
        # but regular users need AUTHORIZE permission on the granted role
        cassandra.execute("GRANT AUTHORIZE ON ROLE role2 TO mike")
        mike.execute("GRANT role2 TO john")

        # same applies to REVOKEing roles
        assert_invalid(mike,
                       "REVOKE role1 FROM john",
                       "User mike does not have sufficient privileges to perform the requested operation",
                       Unauthorized)
        cassandra.execute("REVOKE role1 FROM john")
        mike.execute("REVOKE role2 from john")

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

        # without SELECT permission on the root level roles resource, LIST ROLES with no OF
        # returns only the roles granted to the user. With it, it includes all roles.
        assert_all(mike, "LIST ROLES", [mike_role, role1_role, role2_role])
        cassandra.execute("GRANT DESCRIBE ON ALL ROLES TO mike")
        assert_all(mike, "LIST ROLES", [cassandra_role, mike_role, role1_role, role2_role])


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

    def filter_granted_permissions_by_resource_type_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE role1 NOSUPERUSER NOLOGIN")

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
        cassandra.execute("CREATE ROLE role3 NOSUPERUSER NOLOGIN")
        assert cassandra.execute("LIST ALTER PERMISSION ON ROLE role1 OF role3") is None

        # now check users can list their own permissions
        mike = self.get_session(user='mike', password='12345')
        self.assert_permissions_listed([("mike", "<table ks.cf>", "MODIFY"),
                                        ("role1", "<table ks.cf>", "SELECT"),
                                        ("role2", "<table ks.cf>", "ALTER"),
                                        ("role2", "<role role1>", "ALTER")],
                                       mike,
                                       "LIST ALL PERMISSIONS OF mike")

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

    def create_user_as_alias_for_create_role_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)

        cassandra.execute("CREATE USER super_user WITH PASSWORD '12345' SUPERUSER")
        assert_one(cassandra, "LIST ROLES OF super_user", ["super_user", True, True, {}])

    def role_requires_login_privilege_to_authenticate_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)
        self.get_session(user='mike', password='12345')

        cassandra.execute("ALTER ROLE mike NOLOGIN")
        assert_one(cassandra, "LIST ROLES OF mike", ["mike", False, False, {}])
        self.assert_unauthenticated('mike is not permitted to log in', 'mike', '12345')

        cassandra.execute("ALTER ROLE mike LOGIN")
        assert_one(cassandra, "LIST ROLES OF mike", ["mike", False, True, {}])
        self.get_session(user='mike', password='12345')

    def roles_do_not_inherit_login_privilege_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER NOLOGIN")
        cassandra.execute("CREATE ROLE with_login WITH PASSWORD '54321' NOSUPERUSER LOGIN")
        cassandra.execute("GRANT with_login to mike")

        assert_all(cassandra, "LIST ROLES OF mike", [["mike", False, False, {}],
                                                     ["with_login", False, True, {}]])
        assert_one(cassandra, "LIST ROLES OF with_login", ["with_login", False, True, {}])

        self.assert_unauthenticated("mike is not permitted to log in", "mike", "12345")

    def role_requires_password_to_login_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike NOSUPERUSER LOGIN")
        self.assert_unauthenticated("Username and/or password are incorrect", 'mike', None)
        cassandra.execute("ALTER ROLE mike WITH PASSWORD '12345'")
        self.get_session(user='mike', password='12345')

    def superuser_status_is_inherited_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("CREATE ROLE db_admin SUPERUSER")

        mike = self.get_session(user='mike', password='12345')
        assert_invalid(mike,
                       "CREATE ROLE another_role NOSUPERUSER NOLOGIN",
                       "User mike does not have sufficient privileges to perform the requested operation",
                       Unauthorized)

        cassandra.execute("GRANT db_admin TO mike")
        mike.execute("CREATE ROLE another_role NOSUPERUSER NOLOGIN")
        assert_all(mike, "LIST ROLES", [["another_role", False, False, {}],
                                        cassandra_role,
                                        ["db_admin", True, False, {}],
                                        mike_role])

    @since('3.0')
    def list_users_considers_inherited_superuser_status_test(self):
        self.prepare()
        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE ROLE db_admin SUPERUSER")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD '12345' NOSUPERUSER LOGIN")
        cassandra.execute("GRANT db_admin TO mike")
        assert_all(cassandra, "LIST USERS", [['cassandra', True],
                                             ["mike", True]])

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
