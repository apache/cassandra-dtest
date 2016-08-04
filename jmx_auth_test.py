from distutils.version import LooseVersion

from ccmlib.node import ToolError

from dtest import Tester
from jmxutils import apply_jmx_authentication
from tools import known_failure, since


@since('3.6')
class TestJMXAuth(Tester):

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11730',
                   flaky=False, notes='windows')
    def basic_auth_test(self):
        """
        Some basic smoke testing of JMX authentication and authorization.
        Uses nodetool as a means of exercising the JMX interface as JolokiaAgent
        exposes its own connector which bypasses the in-built security features
        @jira_ticket CASSANDRA-10091
        """
        self.prepare()
        [node] = self.cluster.nodelist()
        node.nodetool('-u cassandra -pw cassandra status')

        session = self.patient_cql_connection(node, user='cassandra', password='cassandra')
        # the jmx_user role has no login privilege but give it a password anyway
        # to demonstrate that LOGIN is required for JMX authentication
        session.execute("CREATE ROLE jmx_user WITH LOGIN=false AND PASSWORD='321cba'")
        session.execute("GRANT SELECT ON MBEAN 'org.apache.cassandra.net:type=FailureDetector' TO jmx_user")
        session.execute("GRANT DESCRIBE ON ALL MBEANS TO jmx_user")
        session.execute("CREATE ROLE test WITH LOGIN=true and PASSWORD='abc123'")

        with self.assertRaisesRegexp(ToolError, self.authentication_fail_message(node, 'baduser')):
            node.nodetool('-u baduser -pw abc123 gossipinfo')

        with self.assertRaisesRegexp(ToolError, self.authentication_fail_message(node, 'test')):
            node.nodetool('-u test -pw badpassword gossipinfo')

        with self.assertRaisesRegexp(ToolError, "Required key 'username' is missing"):
            node.nodetool('gossipinfo')

        # role must have LOGIN attribute
        with self.assertRaisesRegexp(ToolError, 'jmx_user is not permitted to log in'):
            node.nodetool('-u jmx_user -pw 321cba gossipinfo')

        # test doesn't yet have any privileges on the necessary JMX resources
        with self.assertRaisesRegexp(ToolError, 'Access Denied'):
            node.nodetool('-u test -pw abc123 gossipinfo')

        session.execute("GRANT jmx_user TO test")
        node.nodetool('-u test -pw abc123 gossipinfo')

        # superuser status applies to JMX authz too
        node.nodetool('-u cassandra -pw cassandra gossipinfo')

    def prepare(self, nodes=1, permissions_validity=0):
        config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                  'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                  'permissions_validity_in_ms': permissions_validity}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes)
        [node] = self.cluster.nodelist()
        apply_jmx_authentication(node)
        node.start()
        node.watch_log_for('Created default superuser')

    def authentication_fail_message(self, node, username):
        return "Provided username {user} and/or password are incorrect".format(user=username) \
            if LooseVersion(node.cluster.version()) >= LooseVersion('3.10') \
            else "Username and/or password are incorrect"
