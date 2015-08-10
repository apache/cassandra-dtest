from dtest import Tester
from tools import since


class TestCompression(Tester):

    @since("3.0")
    def disable_compression_cql_test(self):
        """
        @jira_ticket CASSANDRA-8384
        using new cql create table syntax to disable compression
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute("create table disabled_compression_table (id uuid PRIMARY KEY ) WITH compression = {'enabled': false};")
        meta = session.cluster.metadata.keyspaces['ks'].tables['disabled_compression_table']
        self.assertEqual('false', meta.options['compression']['enabled'])

    @since("3.0")
    def compression_cql_options_test(self):
        """
        @jira_ticket CASSANDRA-8384
        using new cql create table syntax to configure compression
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute("""
            create table compression_opts_table
                (id uuid PRIMARY KEY )
                WITH compression = {
                    'class': 'SnappyCompressor',
                    'chunk_length_in_kb': 256,
                    'crc_check_chance': 0.25
                };
            """)
        meta = session.cluster.metadata.keyspaces['ks'].tables['compression_opts_table']
        self.assertEqual('org.apache.cassandra.io.compress.SnappyCompressor', meta.options['compression']['class'])
        self.assertEqual('256', meta.options['compression']['chunk_length_in_kb'])
        self.assertEqual('0.25', meta.options['compression']['crc_check_chance'])

    @since("3.0")
    def compression_cql_disabled_with_alter_test(self):
        """
        @jira_ticket CASSANDRA-8384
        starting with compression enabled then disabling it
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute("""
            create table start_enabled_compression_table
                (id uuid PRIMARY KEY )
                WITH compression = {
                    'class': 'SnappyCompressor',
                    'chunk_length_in_kb': 256,
                    'crc_check_chance': 0.25
                };
            """)
        meta = session.cluster.metadata.keyspaces['ks'].tables['start_enabled_compression_table']
        self.assertEqual('org.apache.cassandra.io.compress.SnappyCompressor', meta.options['compression']['class'])
        self.assertEqual('256', meta.options['compression']['chunk_length_in_kb'])
        self.assertEqual('0.25', meta.options['compression']['crc_check_chance'])
        session.execute("alter table start_enabled_compression_table with compression = {'enabled': false};")
        meta = session.cluster.metadata.keyspaces['ks'].tables['start_enabled_compression_table']
        self.assertEqual('false', meta.options['compression']['enabled'])

    @since("3.0")
    def compression_cql_enabled_with_alter_test(self):
        """
        @jira_ticket CASSANDRA-8384
        starting with compression disabled and enabling it
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute("create table start_disabled_compression_table (id uuid PRIMARY KEY ) WITH compression = {'enabled': false};")
        meta = session.cluster.metadata.keyspaces['ks'].tables['start_disabled_compression_table']
        self.assertEqual('false', meta.options['compression']['enabled'])
        session.execute("""alter table start_disabled_compression_table
                                WITH compression = {
                                        'class': 'SnappyCompressor',
                                        'chunk_length_in_kb': 256,
                                        'crc_check_chance': 0.25
                                    };""")
        meta = session.cluster.metadata.keyspaces['ks'].tables['start_disabled_compression_table']
        self.assertEqual('org.apache.cassandra.io.compress.SnappyCompressor', meta.options['compression']['class'])
        self.assertEqual('256', meta.options['compression']['chunk_length_in_kb'])
        self.assertEqual('0.25', meta.options['compression']['crc_check_chance'])
