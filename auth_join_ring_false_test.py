import pytest

from cassandra import AuthenticationFailed, Unauthorized
from cassandra.cluster import NoHostAvailable

from dtest import Tester

since = pytest.mark.since

@since('2.2')
class TestAuth(Tester):


    def test_login_existing_node(self):
        """
        * Launch a three node cluster
        * Restart the third node in `join_ring=false` mode
        * Connect as the default user/password
        * Verify that default user w/ bad password gives AuthenticationFailed exception
        * Verify that bad user gives AuthenticationFailed exception
        """
        # also tests default user creation (cassandra/cassandra)
        self.prepare(nodes=3)
        node1, node2, node3 = self.cluster.nodelist()
        node3.stop(wait_other_notice=True)
        node3.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)

        self.patient_exclusive_cql_connection(node=node3, user='cassandra', password='cassandra')
        try:
            self.patient_exclusive_cql_connection(node=node3, user='cassandra', password='badpassword')
        except NoHostAvailable as e:
            assert isinstance(list(e.errors.values())[0], AuthenticationFailed)
        try:
            self.patient_exclusive_cql_connection(node=node3, user='doesntexist', password='doesntmatter')
        except NoHostAvailable as e:
            assert isinstance(list(e.errors.values())[0], AuthenticationFailed)

    def test_login_new_node(self):
        """
        * Launch a two node cluster
        * Add a third node in `join_ring=false` mode
        * Connect as the default user/password
        * Verify that default user w/ bad password gives AuthenticationFailed exception
        * Verify that bad user gives AuthenticationFailed exception
        """
        # also tests default user creation (cassandra/cassandra)
        self.prepare(nodes=2)

        node3 = self.cluster.create_node('node3', False,
                                    ('127.0.0.3', 9160),
                                    ('127.0.0.3', 7000),
                                    '7300', '2002', None,
                                    binary_interface=('127.0.0.3', 9042))

        self.cluster.add(node3, False, data_center="dc1")
        node3.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)

        self.patient_exclusive_cql_connection(node=node3, user='cassandra', password='cassandra')
        try:
            self.patient_exclusive_cql_connection(node=node3, user='cassandra', password='badpassword')
        except NoHostAvailable as e:
            assert isinstance(list(e.errors.values())[0], AuthenticationFailed)
        try:
            self.patient_exclusive_cql_connection(node=node3, user='doesntexist', password='doesntmatter')
        except NoHostAvailable as e:
            assert isinstance(list(e.errors.values())[0], AuthenticationFailed)

    def test_list_users(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create two new users, and two new superusers.
        * Verify that LIST USERS shows all five users.
        * Verify that the correct users are listed as super users.
        * Add a second node in `join_ring=false` mode
        * Connect (through the non-ring node) as one of the new users, and check that the LIST USERS behavior is also correct there.
        """
        self.prepare()

        session = self.get_session(user='cassandra', password='cassandra')
        session.execute("CREATE USER alex WITH PASSWORD '12345' NOSUPERUSER")
        session.execute("CREATE USER bob WITH PASSWORD '12345' SUPERUSER")
        session.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        session.execute("CREATE USER dave WITH PASSWORD '12345' SUPERUSER")

        node2 = self.cluster.create_node('node2', False,
                                    ('127.0.0.2', 9160),
                                    ('127.0.0.2', 7000),
                                    '7200', '2001', None,
                                    binary_interface=('127.0.0.2', 9042))
                                    
        self.cluster.add(node2, False, data_center="dc1")
        node2.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)

        self.patient_exclusive_cql_connection(node=node2, user='cassandra', password='cassandra')
        session = self.get_session(user='cassandra', password='cassandra')

        rows = list(session.execute("LIST USERS"))
        assert 5 == len(rows)
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        assert users['cassandra']
        assert not users['alex']
        assert users['bob']
        assert not users['cathy']
        assert users['dave']

        self.get_session(node_idx=1, user='dave', password='12345')
        rows = list(session.execute("LIST USERS"))
        assert 5 == len(rows)
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        assert users['cassandra']
        assert not users['alex']
        assert users['bob']
        assert not users['cathy']
        assert users['dave']

    def test_modify_and_select_auth(self):
        self.prepare()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        node2 = self.cluster.create_node('node2', False,
                                    ('127.0.0.2', 9160),
                                    ('127.0.0.2', 7000),
                                    '7200', '2001', None,
                                    binary_interface=('127.0.0.2', 9042))

        self.cluster.add(node2, False, data_center="dc1")
        node2.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)

        cathy = self.get_session(node_idx=1, user='cathy', password='12345')

        self.assert_unauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents",
                                cathy, "SELECT * FROM ks.cf")

        node2.stop()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")

        node2.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)
        cathy = self.get_session(node_idx=1, user='cathy', password='12345')

        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 0 == len(rows)

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "UPDATE ks.cf SET val = 1 WHERE id = 1")

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "DELETE FROM ks.cf WHERE id = 1")

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents",
                                cathy, "TRUNCATE ks.cf")

        node2.stop()

        cassandra = self.get_session(user='cassandra', password='cassandra')
        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")

        node2.start(join_ring=False, wait_other_notice=False, wait_for_binary_proto=True)
        cathy = self.get_session(node_idx=1, user='cathy', password='12345')

        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("UPDATE ks.cf SET val = 1 WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 2 == len(rows)

        cathy.execute("DELETE FROM ks.cf WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 1 == len(rows)

        rows = list(cathy.execute("TRUNCATE ks.cf"))
        assert len(rows) == 0


    def assert_unauthorized(self, message, session, query):
        with pytest.raises(Unauthorized) as cm:
            session.execute(query)
            assert_regexp_matches(repr(cm._excinfo[1]), message)
            
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
        self.cluster.populate(nodes).start()

        self.cluster.wait_for_any_log('Created default superuser', 25)

