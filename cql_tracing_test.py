# coding: utf-8

import os
import subprocess
import sys
from ccmlib import common
from dtest import Tester, debug


class TestCqlTracing(Tester):

    def prepare(self, create_keyspace=True, nodes=3, rf=3, protocol_version=3, jvm_args=[], **kwargs):
        cluster = self.cluster
        cluster.populate(nodes).start(wait_for_binary_proto=True, jvm_args=jvm_args)
        cluster.nodelist()[0].watch_log_for(' listening ')

        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            if self._preserve_cluster:
                session.execute("DROP KEYSPACE IF EXISTS ks")
            self.create_ks(session, 'ks', rf)
        return session

    def run_cqlsh(self, node, cmds, cqlsh_options=[]):
        cdir = node.get_install_dir()
        cli = os.path.join(cdir, 'bin', common.platform_binary('cqlsh'))
        env = common.make_cassandra_env(cdir, node.get_path())
        env['LANG'] = 'en_US.UTF-8'
        host = node.network_interfaces['binary'][0]
        port = node.network_interfaces['binary'][1]
        args = cqlsh_options + [host, str(port)]
        sys.stdout.flush()
        p = subprocess.Popen([cli] + args, env=env, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        for cmd in cmds.split(';'):
            p.stdin.write(cmd + ';\n')
        p.stdin.write("quit;\n")
        return p.communicate()

    def trace(self, cursor):

        node1 = self.cluster.nodelist()[0]

        # Create
        cursor.execute("""
            CREATE TABLE ks.users (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );
        """)

        out, err = self.run_cqlsh(node1, 'TRACING ON')
        self.assertIn('Tracing is enabled', out)

        out, err = self.run_cqlsh(node1, 'TRACING ON; SELECT * from system.peers')
        self.assertIn('Tracing session: ', out)
        self.assertIn('Request complete ', out)

        # Inserts
        out, err = self.run_cqlsh(node1, "CONSISTENCY ALL; INSERT INTO ks.users (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)")

        # Queries
        out, err = self.run_cqlsh(node1, 'CONSISTENCY ALL; TRACING ON; SELECT firstname, lastname FROM ks.users WHERE userid = 550e8400-e29b-41d4-a716-446655440000')
        debug(out)
        self.assertIn('Tracing session: ', out)
        self.assertIn(' 127.0.0.1 ', out)
        self.assertIn(' 127.0.0.2 ', out)
        self.assertIn(' 127.0.0.3 ', out)
        self.assertIn('Request complete ', out)
        self.assertIn(" Frodo |  Baggins", out)

    def tracing_simple_test(self):
        """ Test Simple Tracing """
        cursor = self.prepare()
        self.trace(cursor)

    def tracing_unknown_impl_test(self):
        """ Test unknown Tracing class """
        cursor = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=junk'])
        self.ignore_log_patterns = ["Cannot use class junk for tracing"]
        self.trace(cursor)

    def tracing_default_impl_test(self):
        """ Test default Tracing class """
        cursor = self.prepare(jvm_args=['-Dcassandra.custom_tracing_class=org.apache.cassandra.tracing.TracingImpl'])
        self.ignore_log_patterns = ["Cannot use class org.apache.cassandra.tracing.TracingImpl"]
        self.trace(cursor)
