import time
from unittest import skipIf

from cassandra import Unauthorized
from ccmlib.common import is_win

from dtest import OFFHEAP_MEMTABLES, Tester, debug
from tools.assertions import assert_all, assert_invalid
from tools.decorators import known_failure, since
from tools.misc import ImmutableMapping


@since('2.2')
class TestAuthUpgrade(Tester):
    cluster_options = ImmutableMapping({'authenticator': 'PasswordAuthenticator',
                                        'authorizer': 'CassandraAuthorizer'})
    ignore_log_patterns = (
        # This one occurs if we do a non-rolling upgrade, the node
        # it's trying to send the migration to hasn't started yet,
        # and when it does, it gets replayed and everything is fine.
        r'Can\'t send migration request: node.*is down',
    )

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11469',
                   flaky=True,
                   notes='Fails ~30% of the time.')
    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11250',
                   flaky=True,
                   notes='windows')
    def upgrade_to_22_test(self):
        self.do_upgrade_with_internal_auth("git:cassandra-2.2")

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11267',
                   flaky=True,
                   notes='windows')
    @since('3.0')
    @skipIf(OFFHEAP_MEMTABLES, 'offheap_objects are not available in 3.0')
    def upgrade_to_30_test(self):
        self.do_upgrade_with_internal_auth("git:cassandra-3.0")

    def do_upgrade_with_internal_auth(self, target_version):
        """
        Tests upgrade between 2.1->2.2 & 2.1->3.0 as the schema and apis around authn/authz changed

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

        self.check_permissions(node1, False)
        session.cluster.shutdown()
        # upgrade node1 to 2.2
        self.upgrade_to_version(target_version, node1)
        # run the permissions checking queries on the upgraded node
        # this will be using the legacy tables as the conversion didn't complete
        # but the output format should be updated on the upgraded node
        self.check_permissions(node1, True)
        # and check on those still on the old version
        self.check_permissions(node2, False)
        self.check_permissions(node3, False)

        # now upgrade the remaining nodes
        self.upgrade_to_version(target_version, node2)
        self.upgrade_to_version(target_version, node3)

        self.check_permissions(node2, True)
        self.check_permissions(node3, True)

        # we should now be able to drop the old auth tables
        session = self.patient_cql_connection(node1, user='cassandra', password='cassandra')
        session.execute('DROP TABLE system_auth.users')
        session.execute('DROP TABLE system_auth.credentials')
        session.execute('DROP TABLE system_auth.permissions')
        # and we should still be able to authenticate and check authorization
        self.check_permissions(node1, True)
        debug('Test completed successfully')

    def check_permissions(self, node, upgraded):
        # use an exclusive connection to ensure we only talk to the specified node
        klaus = self.patient_exclusive_cql_connection(node, user='klaus', password='12345', timeout=20)
        # klaus is a superuser, so should be able to list all permissions
        # the output of LIST PERMISSIONS changes slightly with #7653 adding
        # a new role column to results, so we need to tailor our check
        # based on whether the node has been upgraded or not
        if not upgraded:
            assert_all(klaus,
                       'LIST ALL PERMISSIONS',
                       [['michael', '<table ks.cf1>', 'MODIFY'],
                        ['michael', '<table ks.cf2>', 'SELECT']])
        else:
            assert_all(klaus,
                       'LIST ALL PERMISSIONS',
                       [['michael', 'michael', '<table ks.cf1>', 'MODIFY'],
                        ['michael', 'michael', '<table ks.cf2>', 'SELECT']])

        klaus.cluster.shutdown()

        michael = self.patient_exclusive_cql_connection(node, user='michael', password='54321')
        michael.execute('INSERT INTO ks.cf1 (id, val) VALUES (0,0)')
        michael.execute('SELECT * FROM ks.cf2')
        assert_invalid(michael,
                       'SELECT * FROM ks.cf1',
                       'User michael has no SELECT permission on <table ks.cf1> or any of its parents',
                       Unauthorized)
        michael.cluster.shutdown()

    def upgrade_to_version(self, tag, node):
        format_args = {'node': node.name, 'tag': tag}
        debug('Upgrading node {node} to {tag}'.format(**format_args))
        # drain and shutdown
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop(wait_other_notice=False)
        debug('{node} stopped'.format(**format_args))

        # Ignore errors before upgrade on Windows
        if is_win():
            node.mark_log_for_errors()

        # Update Cassandra Directory
        debug('Updating version to tag {tag}'.format(**format_args))
        node.set_install_dir(version=tag, verbose=True)
        debug('Set new cassandra dir for {node}: {tag}'.format(**format_args))

        # Restart node on new version
        debug('Starting {node} on new version ({tag})'.format(**format_args))
        # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
        node.set_log_level("INFO")
        node.start(wait_other_notice=True)
        # wait for the conversion of legacy data to either complete or fail
        # (because not enough upgraded nodes are available yet)
        debug('Waiting for conversion of legacy data to complete or fail')
        node.watch_log_for('conversion of legacy permissions')

        debug('Running upgradesstables')
        node.nodetool('upgradesstables -a')
        debug('Upgrade of {node} complete'.format(**format_args))
