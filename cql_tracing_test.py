# coding: utf-8

from distutils.version import LooseVersion

from dtest import Tester, debug
from tools import known_failure, since


class TestCqlTracing(Tester):
    """
    Smoke test that the default implementation for tracing works. Also test
    that Cassandra falls back to the default tracing implementation when the
    user specifies an invalid implementation.

    # TODO write a mock Tracing implementation and assert, at least, it can be
    #      instantiated when specified as a custom tracing implementation.
    """

    def prepare(self, create_keyspace=True, nodes=3, rf=3, protocol_version=3, jvm_args=None, **kwargs):
        if jvm_args is None:
            jvm_args = []

        cluster = self.cluster
        cluster.populate(nodes).start(wait_for_binary_proto=True, jvm_args=jvm_args)

        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            if self._preserve_cluster:
                session.execute("DROP KEYSPACE IF EXISTS ks")
            self.create_ks(session, 'ks', rf)
        return session

    def trace(self, session):
        """
        * CREATE a table
        * enable TRACING
        * SELECT on a known system table and assert it ran with tracing by checking the output
        * INSERT a row into the created system table and assert it ran with tracing
        * SELECT from the table and assert it ran with tracing

        @param session The Session object to use to create a table.
        @jira_ticket CASSANDRA-10392
        """

        node1 = self.cluster.nodelist()[0]

        # Create
        session.execute("""
            CREATE TABLE ks.users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        out, err = node1.run_cqlsh('TRACING ON', return_output=True)
        self.assertIn('Tracing is enabled', out)

        out, err = node1.run_cqlsh('TRACING ON; SELECT * from system.peers', return_output=True)
        self.assertIn('Tracing session: ', out)
        self.assertIn('Request complete ', out)

        # Inserts
        out, err = node1.run_cqlsh(
            "CONSISTENCY ALL; TRACING ON; "
            "INSERT INTO ks.users (userid, firstname, lastname, age) "
            "VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)",
            return_output=True)
        debug(out)
        self.assertIn('Tracing session: ', out)

        # Restricted to 2.2+ due to flakiness on 2.1.  See CASSANDRA-11598 for details.
        if LooseVersion(self.cluster.version()) >= LooseVersion('2.2'):
            self.assertIn('127.0.0.1', out)
            self.assertIn('127.0.0.2', out)
            self.assertIn('127.0.0.3', out)

        self.assertIn('Parsing INSERT INTO ks.users ', out)
        self.assertIn('Request complete ', out)

        # Queries
        out, err = node1.run_cqlsh('CONSISTENCY ALL; TRACING ON; '
                                   'SELECT firstname, lastname '
                                   'FROM ks.users WHERE userid = 550e8400-e29b-41d4-a716-446655440000',
                                   return_output=True)
        debug(out)
        self.assertIn('Tracing session: ', out)
        self.assertIn(' 127.0.0.1 ', out)
        self.assertIn(' 127.0.0.2 ', out)
        self.assertIn(' 127.0.0.3 ', out)
        self.assertIn('Request complete ', out)
        self.assertIn(" Frodo |  Baggins", out)

    def tracing_simple_test(self):
        """
        Test tracing using the default tracing class. See trace().

        @jira_ticket CASSANDRA-10392
        """
        session = self.prepare()
        self.trace(session)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11465',
                   flaky=True)
    @since('3.4')
    def tracing_unknown_impl_test(self):
        """
        Test that Cassandra logs an error, but keeps its default tracing
        behavior, when a nonexistent tracing class is specified.

        * set a nonexistent custom tracing class
        * run trace()
        * if running the test on a version with custom tracing classes
          implemented, check that an error about the nonexistent class was
          logged.

        @jira_ticket CASSANDRA-10392
        """
        expected_error = 'Cannot use class junk for tracing'
        self.ignore_log_patterns = [expected_error]
        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=junk'])
        self.trace(session)

        errs = self.cluster.nodelist()[0].grep_log_for_errors()
        debug('Errors after attempted trace with unknown tracing class: {errs}'.format(errs=errs))
        self.assertEqual(len(errs), 1)
        self.assertEqual(len(errs[0]), 1)
        err = errs[0][0]
        self.assertIn(expected_error, err)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11465',
                   flaky=True)
    @since('3.4')
    def tracing_default_impl_test(self):
        """
        Test that Cassandra logs an error, but keeps its default tracing
        behavior, when the default tracing class is specified.

        This doesn't work because the constructor for the default
        implementation isn't accessible.

        * set the default tracing class as a custom tracing class
        * run trace()
        * if running the test on a version with custom tracing classes
          implemented, check that an error about the class was
          logged.

        @jira_ticket CASSANDRA-10392
        """
        expected_error = 'Cannot use class org.apache.cassandra.tracing.TracingImpl'
        self.ignore_log_patterns = [expected_error]
        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=org.apache.cassandra.tracing.TracingImpl'])
        self.trace(session)

        errs = self.cluster.nodelist()[0].grep_log_for_errors()
        debug('Errors after attempted trace with default tracing class: {errs}'.format(errs=errs))
        self.assertEqual(len(errs), 1)
        self.assertEqual(len(errs[0]), 1)
        err = errs[0][0]
        self.assertIn(expected_error, err)
        # make sure it logged the error for the correct reason. this isn't
        # part of the expected error to avoid having to escape parens and
        # periods for regexes.
        self.assertIn("Default constructor for Tracing class "
                      "'org.apache.cassandra.tracing.TracingImpl' is inaccessible.",
                      err)
