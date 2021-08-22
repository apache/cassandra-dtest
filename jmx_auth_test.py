import random
import string
import pytest
import logging
from distutils.version import LooseVersion

from ccmlib.node import ToolError
from dtest import Tester
from tools.jmxutils import apply_jmx_authentication

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('3.6')
class TestJMXAuth(Tester):
    """
    Uses nodetool as a means of exercising the JMX interface as JolokiaAgent
    exposes its own connector which bypasses the in-built security features
    """

    def test_basic_auth(self):
        """
        Some basic smoke testing of JMX authentication and authorization.
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

        with pytest.raises(ToolError, match=self.authentication_fail_message(node, 'baduser')):
            node.nodetool('-u baduser -pw abc123 gossipinfo')

        with pytest.raises(ToolError, match=self.authentication_fail_message(node, 'test')):
            node.nodetool('-u test -pw badpassword gossipinfo')

        with pytest.raises(ToolError, match="Required key 'username' is missing"):
            node.nodetool('gossipinfo')

        # role must have LOGIN attribute
        with pytest.raises(ToolError, match='jmx_user is not permitted to log in'):
            node.nodetool('-u jmx_user -pw 321cba gossipinfo')

        # test doesn't yet have any privileges on the necessary JMX resources
        with pytest.raises(ToolError, match='Access Denied'):
            node.nodetool('-u test -pw abc123 gossipinfo')

        session.execute("GRANT jmx_user TO test")
        node.nodetool('-u test -pw abc123 gossipinfo')

        # superuser status applies to JMX authz too
        node.nodetool('-u cassandra -pw cassandra gossipinfo')

    @since('4.1')
    def test_revoked_jmx_access(self):
        """
        if a user's access to a JMX MBean is revoked while they're connected,
        all of their requests should fail once the cache is cleared.
        @jira_ticket CASSANDRA-16404
        """
        self.prepare(permissions_validity=60000)
        [node] = self.cluster.nodelist()

        def test_revoked_access(cache_name):
            logger.debug('Testing with cache name: %s' % cache_name)
            username = self.username()
            session = self.patient_cql_connection(node, user='cassandra', password='cassandra')
            session.execute("CREATE ROLE %s WITH LOGIN=true AND PASSWORD='abc123'" % username)
            session.execute("GRANT SELECT ON MBEAN 'org.apache.cassandra.net:type=FailureDetector' TO %s" % username)
            session.execute("GRANT DESCRIBE ON ALL MBEANS TO %s" % username)

            # works fine
            node.nodetool('-u %s -pw abc123 gossipinfo' % username)

            session.execute("REVOKE SELECT ON MBEAN 'org.apache.cassandra.net:type=FailureDetector' FROM %s" % username)
            # works fine because the JMX permission is cached
            node.nodetool('-u %s -pw abc123 gossipinfo' % username)

            node.nodetool('-u cassandra -pw cassandra invalidatejmxpermissionscache')
            # the user has no permissions to the JMX resource anymore
            with pytest.raises(ToolError, match='Access Denied'):
                node.nodetool('-u %s -pw abc123 gossipinfo' % username)

        test_revoked_access("JmxPermissionsCache")

        # deprecated cache name, scheduled for removal in 5.0
        if self.dtest_config.cassandra_version_from_build < '5.0':
            test_revoked_access("JMXPermissionsCache")

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
            if node.cluster.version() >= LooseVersion('3.10') else "Username and/or password are incorrect"

    def username(self):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(8))