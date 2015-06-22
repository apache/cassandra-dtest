from cassandra import Unauthorized
from dtest import Tester, debug
from assertions import assert_all, assert_invalid
from tools import since
import time

from ccmlib import repository

@since('3.0')
class TestAuthUpgrade(Tester):

    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]

        # Force cluster options that are common among versions:
        kwargs['cluster_options'] = {'authenticator': 'PasswordAuthenticator',
                                     'authorizer': 'CassandraAuthorizer'}
        Tester.__init__(self, *args, **kwargs)

    def upgrade_with_internal_auth_test(self):
        """
        Tests upgrade between 2.1->3.0 as the schema and apis around authn/authz changed

        @jira_ticket CASSANDRA-7653
        """
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="git:cassandra-2.1")
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()

        # wait for default superuser creation
        # The log message
        # node.watch_log_for('Created default superuser')
        # will only appear on one of the three nodes, and we don't know
        # which ahead of time. Grepping all three in parallel is unpleasant.
        # See auth_test and auth_roles test for instances of this as well.
        # Should be fixed by C*-6177
        time.sleep(15)

        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        session.execute("CREATE USER klaus WITH PASSWORD '12345' SUPERUSER")
        session.execute("CREATE USER michael WITH PASSWORD '54321' NOSUPERUSER")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
        session.execute("CREATE TABLE ks.cf1 (id int primary key, val int)")
        session.execute("CREATE TABLE ks.cf2 (id int primary key, val int)")
        session.execute("GRANT MODIFY ON ks.cf1 TO michael")
        session.execute("GRANT SELECT ON ks.cf2 TO michael")

        self.check_permissions(node1)
        session.shutdown()
        # upgrade the cluster to 3.0, restarting node 1 in the process
        # note - will need changing when we branch for 3.0
        self.upgrade_to_version("git:trunk", nodes=[node1])
        # conversion of legacy auth info won't complete on the first upgraded node
        node1.watch_log_for('Unable to complete conversion of legacy permissions')
        # run the permissions checking queries on the upgraded node
        # this will be using the legacy tables as the conversion didn't complete
        self.check_permissions(node1)
        # and check on those still on the old version
        self.check_permissions(node2)
        self.check_permissions(node3)

        # now upgrade the remaining nodes
        self.restart_nodes_to_upgrade([node2, node3])
        self.check_permissions(node2)
        self.check_permissions(node3)

        # we should now be able to drop the old auth tables
        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        session.execute('DROP TABLE system_auth.users')
        session.execute('DROP TABLE system_auth.credentials')
        session.execute('DROP TABLE system_auth.permissions')
        # and we should still be able to authenticate and check authorization
        self.check_permissions(node1)

        debug('Test completed successfully')

    def check_permissions(self, node):
        klaus = self.patient_cql_connection(node, user='klaus', password='12345')
        # klaus is a superuser, so should be able to list all permissions
        assert_all(klaus,
                   'LIST ALL PERMISSIONS',
                   [['michael', '<table ks.cf1>', 'MODIFY'],
                    ['michael', '<table ks.cf2>', 'SELECT']])
        klaus.shutdown()

        michael = self.patient_cql_connection(node, user='michael', password='54321')
        michael.execute('INSERT INTO ks.cf1 (id, val) VALUES (0,0)')
        michael.execute('SELECT * FROM ks.cf2')
        assert_invalid(michael,
                       'SELECT * FROM ks.cf1',
                       'User michael has no SELECT permission on <table ks.cf1> or any of its parents',
                       Unauthorized)
        michael.shutdown()

    def restart_nodes_to_upgrade(self, nodes):
        for node in nodes:
            node.flush()
            time.sleep(.5)
            node.stop(wait_other_notice=True)
            self.set_node_to_current_version(node)
            node.start(wait_other_notice=True)
            time.sleep(.5)

    def upgrade_to_version(self, tag, nodes=None):
        debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            repository.clean_all()
            debug(repository.GIT_REPO)
            node.set_install_dir(version=tag, verbose=True)
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True)
            node.nodetool('upgradesstables -a')
