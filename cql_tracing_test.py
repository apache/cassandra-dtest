# coding: utf-8

from dtest import Tester, debug


def post_cassandra_10392(version):
    """
    Return a bool indicating whether or not `version` has had CASSANDRA-10392
    merged or not. It was merged into trunk after 3.0.3 on the 3.0 release
    track and after 3.3 on the v3 tick-tock release track, so this simply
    checks the passed-in version against those versions.

    @param version A C* version, typically a Version object from
                   distutils.version
    """
    return ('3.0.3' < version < '3.1' or '3.3' < version)


class TestCqlTracing(Tester):

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
        self.assertIn('Tracing session: ', out)
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
        Test tracing using the default tracing class.
        """
        session = self.prepare()
        self.trace(session)

    def tracing_unknown_impl_test(self):
        """
        Test that Cassandra logs an error, but keeps its default tracing
        behavior when a nonexistent tracing class is specified.
        """
        expected_error = 'Cannot use class junk for tracing'
        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=junk'])
        self.ignore_log_patterns = [expected_error]
        self.trace(session)

        if post_cassandra_10392(self.cluster.version()):
            errs = self.cluster.nodelist()[0].grep_log_for_errors()
            self.assertEqual(len(errs), 1)
            self.assertEqual(len(errs[0]), 1)
            err = errs[0][0]
            self.assertIn(expected_error, err)

    def tracing_default_impl_test(self):
        """
        Test default Tracing class
        """
        expected_error = 'Cannot use class org.apache.cassandra.tracing.TracingImpl'
        session = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=org.apache.cassandra.tracing.TracingImpl'])
        self.ignore_log_patterns = [expected_error]
        self.trace(session)

        if post_cassandra_10392(self.cluster.version()):
            errs = self.cluster.nodelist()[0].grep_log_for_errors()
            self.assertEqual(len(errs), 1)
            self.assertEqual(len(errs[0]), 1)
            err = errs[0][0]
            self.assertIn(expected_error, err)
