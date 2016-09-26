import os

from dtest import create_ks
from scrub_test import TestHelper
from tools.assertions import assert_crc_check_chance_equal
from tools.decorators import since


class TestCompression(TestHelper):

    def _get_compression_type(self, file):
        types = {
            '0010': 'NONE',
            '789c': 'DEFLATE'
        }

        with open(file, 'rb') as fh:
            file_start = fh.read(2)
            return types.get(file_start.encode('hex'), 'UNKNOWN')

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
        create_ks(session, 'ks', 1)
        session.execute("create table disabled_compression_table (id uuid PRIMARY KEY ) WITH compression = {'enabled': false};")
        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces['ks'].tables['disabled_compression_table']
        self.assertEqual('false', meta.options['compression']['enabled'])

        for n in range(0, 100):
            session.execute("insert into disabled_compression_table (id) values (uuid());")

        sstables = self.flush('disabled_compression_table')
        sstable_paths = self.get_table_paths('disabled_compression_table')
        found = False
        for sstable_path in sstable_paths:
            sstable = os.path.join(sstable_path, sstables['disabled_compression_table'][1])
            if os.path.exists(sstable):
                self.assertEqual('NONE', self._get_compression_type(sstable))
                found = True
        self.assertTrue(found)

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
        create_ks(session, 'ks', 1)
        session.execute("""
            create table compression_opts_table
                (id uuid PRIMARY KEY )
                WITH compression = {
                    'class': 'DeflateCompressor',
                    'chunk_length_in_kb': 256
                }
                AND crc_check_chance = 0.25;
            """)

        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces['ks'].tables['compression_opts_table']
        self.assertEqual('org.apache.cassandra.io.compress.DeflateCompressor', meta.options['compression']['class'])
        self.assertEqual('256', meta.options['compression']['chunk_length_in_kb'])
        assert_crc_check_chance_equal(session, "compression_opts_table", 0.25)

        warn = node.grep_log("The option crc_check_chance was deprecated as a compression option.")
        self.assertEqual(len(warn), 0)
        session.execute("""
            alter table compression_opts_table
                WITH compression = {
                    'class': 'DeflateCompressor',
                    'chunk_length_in_kb': 256,
                    'crc_check_chance': 0.6
                }
            """)
        warn = node.grep_log("The option crc_check_chance was deprecated as a compression option.")
        self.assertEqual(len(warn), 1)

        # check metadata again after crc_check_chance_update
        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces['ks'].tables['compression_opts_table']
        self.assertEqual('org.apache.cassandra.io.compress.DeflateCompressor', meta.options['compression']['class'])
        self.assertEqual('256', meta.options['compression']['chunk_length_in_kb'])
        assert_crc_check_chance_equal(session, "compression_opts_table", 0.6)

        for n in range(0, 100):
            session.execute("insert into compression_opts_table (id) values (uuid());")

        sstables = self.flush('compression_opts_table')
        sstable_paths = self.get_table_paths('compression_opts_table')
        found = False
        for sstable_path in sstable_paths:
            sstable = os.path.join(sstable_path, sstables['compression_opts_table'][1])
            if os.path.exists(sstable):
                self.assertEqual('DEFLATE', self._get_compression_type(sstable))
                found = True
        self.assertTrue(found)

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
        create_ks(session, 'ks', 1)
        session.execute("""
            create table start_enabled_compression_table
                (id uuid PRIMARY KEY )
                WITH compression = {
                    'class': 'SnappyCompressor',
                    'chunk_length_in_kb': 256
                }
                AND crc_check_chance = 0.25;
            """)
        meta = session.cluster.metadata.keyspaces['ks'].tables['start_enabled_compression_table']
        self.assertEqual('org.apache.cassandra.io.compress.SnappyCompressor', meta.options['compression']['class'])
        self.assertEqual('256', meta.options['compression']['chunk_length_in_kb'])
        assert_crc_check_chance_equal(session, "start_enabled_compression_table", 0.25)
        session.execute("alter table start_enabled_compression_table with compression = {'enabled': false};")

        session.cluster.refresh_schema_metadata()
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
        create_ks(session, 'ks', 1)
        session.execute("create table start_disabled_compression_table (id uuid PRIMARY KEY ) WITH compression = {'enabled': false};")
        meta = session.cluster.metadata.keyspaces['ks'].tables['start_disabled_compression_table']
        self.assertEqual('false', meta.options['compression']['enabled'])
        session.execute("""alter table start_disabled_compression_table
                                WITH compression = {
                                        'class': 'SnappyCompressor',
                                        'chunk_length_in_kb': 256
                                    } AND crc_check_chance = 0.25;""")

        session.cluster.refresh_schema_metadata()
        meta = session.cluster.metadata.keyspaces['ks'].tables['start_disabled_compression_table']
        self.assertEqual('org.apache.cassandra.io.compress.SnappyCompressor', meta.options['compression']['class'])
        self.assertEqual('256', meta.options['compression']['chunk_length_in_kb'])
        assert_crc_check_chance_equal(session, "start_disabled_compression_table", 0.25)
