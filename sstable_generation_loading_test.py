import os
import subprocess
import time
from distutils import dir_util

from ccmlib import common as ccmcommon

from dtest import Tester, debug, create_ks, create_cf
from tools.assertions import assert_all, assert_none, assert_one
from tools.decorators import since


# WARNING: sstableloader tests should be added to TestSSTableGenerationAndLoading (below),
# and not to BaseSStableLoaderTest (which is shared with upgrade tests)


# Also used by upgrade_tests/storage_engine_upgrade_test
# to test loading legacy sstables
class BaseSStableLoaderTest(Tester):
    __test__ = False
    upgrade_from = None
    compact = False
    jvm_args = ()
    allow_log_errors = True

    def create_schema(self, session, ks, compression):
        create_ks(session, ks, rf=2)
        create_cf(session, "standard1", compression=compression, compact_storage=self.compact)
        create_cf(session, "counter1", compression=compression, columns={'v': 'counter'},
                  compact_storage=self.compact)

    def sstableloader_compression_none_to_none_test(self):
        self.load_sstable_with_configuration(None, None)

    def sstableloader_compression_none_to_snappy_test(self):
        self.load_sstable_with_configuration(None, 'Snappy')

    def sstableloader_compression_none_to_deflate_test(self):
        self.load_sstable_with_configuration(None, 'Deflate')

    def sstableloader_compression_snappy_to_none_test(self):
        self.load_sstable_with_configuration('Snappy', None)

    def sstableloader_compression_snappy_to_snappy_test(self):
        self.load_sstable_with_configuration('Snappy', 'Snappy')

    def sstableloader_compression_snappy_to_deflate_test(self):
        self.load_sstable_with_configuration('Snappy', 'Deflate')

    def sstableloader_compression_deflate_to_none_test(self):
        self.load_sstable_with_configuration('Deflate', None)

    def sstableloader_compression_deflate_to_snappy_test(self):
        self.load_sstable_with_configuration('Deflate', 'Snappy')

    def sstableloader_compression_deflate_to_deflate_test(self):
        self.load_sstable_with_configuration('Deflate', 'Deflate')

    def sstableloader_with_mv_test(self):
        """
        @jira_ticket CASSANDRA-11275
        """
        def create_schema_with_mv(session, ks, compression):
            self.create_schema(session, ks, compression)
            # create a materialized view
            session.execute("CREATE MATERIALIZED VIEW mv1 AS "
                            "SELECT key FROM standard1 WHERE key IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL "
                            "PRIMARY KEY (v)")

        self.load_sstable_with_configuration(ks='"Keyspace1"', create_schema=create_schema_with_mv)

    def copy_sstables(self, cluster, node):
        for x in xrange(0, cluster.data_dir_count):
            data_dir = os.path.join(node.get_path(), 'data{0}'.format(x))
            copy_root = os.path.join(node.get_path(), 'data{0}_copy'.format(x))
            for ddir in os.listdir(data_dir):
                keyspace_dir = os.path.join(data_dir, ddir)
                if os.path.isdir(keyspace_dir) and ddir != 'system':
                    copy_dir = os.path.join(copy_root, ddir)
                    dir_util.copy_tree(keyspace_dir, copy_dir)

    def load_sstables(self, cluster, node, ks):
        cdir = node.get_install_dir()
        sstableloader = os.path.join(cdir, 'bin', ccmcommon.platform_binary('sstableloader'))
        env = ccmcommon.make_cassandra_env(cdir, node.get_path())
        host = node.address()
        for x in xrange(0, cluster.data_dir_count):
            sstablecopy_dir = os.path.join(node.get_path(), 'data{0}_copy'.format(x), ks.strip('"'))
            for cf_dir in os.listdir(sstablecopy_dir):
                full_cf_dir = os.path.join(sstablecopy_dir, cf_dir)
                if os.path.isdir(full_cf_dir):
                    cmd_args = [sstableloader, '--nodes', host, full_cf_dir]
                    p = subprocess.Popen(cmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
                    exit_status = p.wait()
                    debug('stdout: {out}'.format(out=p.stdout))
                    debug('stderr: {err}'.format(err=p.stderr))
                    self.assertEqual(0, exit_status,
                                     "sstableloader exited with a non-zero status: {}".format(exit_status))

    def load_sstable_with_configuration(self, pre_compression=None, post_compression=None, ks="ks", create_schema=create_schema):
        """
        tests that the sstableloader works by using it to load data.
        Compression of the columnfamilies being loaded, and loaded into
        can be specified.

        pre_compression and post_compression can be these values:
        None, 'Snappy', or 'Deflate'.
        """
        NUM_KEYS = 1000

        for compression_option in (pre_compression, post_compression):
            self.assertIn(compression_option, (None, 'Snappy', 'Deflate'))

        debug("Testing sstableloader with pre_compression=%s and post_compression=%s" % (pre_compression, post_compression))
        if self.upgrade_from:
            debug("Testing sstableloader with upgrade_from=%s and compact=%s" % (self.upgrade_from, self.compact))

        cluster = self.cluster
        if self.upgrade_from:
            debug("Generating sstables with version %s" % (self.upgrade_from))
            default_install_dir = self.cluster.get_install_dir()
            # Forcing cluster version on purpose
            cluster.set_install_dir(version=self.upgrade_from)
        debug("Using jvm_args={}".format(self.jvm_args))
        cluster.populate(2).start(jvm_args=list(self.jvm_args))
        node1, node2 = cluster.nodelist()
        time.sleep(.5)

        debug("creating keyspace and inserting")
        session = self.cql_connection(node1)
        self.create_schema(session, ks, pre_compression)

        for i in range(NUM_KEYS):
            session.execute("UPDATE standard1 SET v='{}' WHERE KEY='{}' AND c='col'".format(i, i))
            session.execute("UPDATE counter1 SET v=v+1 WHERE KEY='{}'".format(i))

        node1.nodetool('drain')
        node1.stop()
        node2.nodetool('drain')
        node2.stop()

        debug("Making a copy of the sstables")
        # make a copy of the sstables
        self.copy_sstables(cluster, node1)

        debug("Wiping out the data and restarting cluster")
        # wipe out the node data.
        cluster.clear()

        if self.upgrade_from:
            debug("Running sstableloader with version from %s" % (default_install_dir))
            # Return to previous version
            cluster.set_install_dir(install_dir=default_install_dir)

        cluster.start(jvm_args=list(self.jvm_args))
        time.sleep(5)  # let gossip figure out what is going on

        debug("re-creating the keyspace and column families.")
        session = self.cql_connection(node1)
        self.create_schema(session, ks, post_compression)
        time.sleep(2)

        debug("Calling sstableloader")
        # call sstableloader to re-load each cf.
        self.load_sstables(cluster, node1, ks)

        def read_and_validate_data(session):
            for i in range(NUM_KEYS):
                query = "SELECT * FROM standard1 WHERE KEY='{}'".format(i)
                assert_one(session, query, [str(i), 'col', str(i)])
                query = "SELECT * FROM counter1 WHERE KEY='{}'".format(i)
                assert_one(session, query, [str(i), 1])

        debug("Reading data back")
        # Now we should have sstables with the loaded data, and the existing
        # data. Lets read it all to make sure it is all there.
        read_and_validate_data(session)

        debug("scrubbing, compacting, and repairing")
        # do some operations and try reading the data again.
        node1.nodetool('scrub')
        node1.nodetool('compact')
        node1.nodetool('repair')

        debug("Reading data back one more time")
        read_and_validate_data(session)

        # check that RewindableDataInputStreamPlus spill files are properly cleaned up
        if self.upgrade_from:
            for x in xrange(0, cluster.data_dir_count):
                data_dir = os.path.join(node1.get_path(), 'data{0}'.format(x))
                for ddir in os.listdir(data_dir):
                    keyspace_dir = os.path.join(data_dir, ddir)
                    temp_files = self.glob_data_dirs(os.path.join(keyspace_dir, '*', "tmp", "*.dat"))
                    debug("temp files: " + str(temp_files))
                    self.assertEquals(0, len(temp_files), "Temporary files were not cleaned up.")


class TestSSTableGenerationAndLoading(BaseSStableLoaderTest):
    __test__ = True

    def sstableloader_uppercase_keyspace_name_test(self):
        """
        Make sure sstableloader works with upper case keyspace
        @jira_ticket CASSANDRA-10806
        """
        self.load_sstable_with_configuration(ks='"Keyspace1"')

    def incompressible_data_in_compressed_table_test(self):
        """
        tests for the bug that caused #3370:
        https://issues.apache.org/jira/browse/CASSANDRA-3370

        @jira_ticket CASSANDRA-3370

        inserts random data into a compressed table. The compressed SSTable was
        compared to the uncompressed and was found to indeed be larger then
        uncompressed.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(.5)

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        create_cf(session, 'cf', compression="Deflate")

        # make unique column names, and values that are incompressible
        for col in xrange(10):
            col_name = str(col)
            col_val = os.urandom(5000)
            col_val = col_val.encode('hex')
            cql = "UPDATE cf SET v='%s' WHERE KEY='0' AND c='%s'" % (col_val, col_name)
            # print cql
            session.execute(cql)

        node1.flush()
        time.sleep(2)
        rows = list(session.execute("SELECT * FROM cf WHERE KEY = '0' AND c < '8'"))
        self.assertGreater(len(rows), 0)

    def remove_index_file_test(self):
        """
        tests for situations similar to that found in #343:
        https://issues.apache.org/jira/browse/CASSANDRA-343

        @jira_ticket CASSANDRA-343
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # Makinge sure the cluster is ready to accept the subsequent
        # stress connection. This was an issue on Windows.
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8'])
        node1.flush()
        node1.compact()
        node1.stop()
        time.sleep(1)
        paths = []
        for data_dir in node1.data_directories():
            basepath = os.path.join(data_dir, 'keyspace1')
            for x in os.listdir(basepath):
                if x.startswith("standard1"):
                    path = os.path.join(basepath, x)

            os.system('rm %s/*Index.db' % path)
            os.system('rm %s/*Filter.db' % path)
            os.system('rm %s/*Statistics.db' % path)
            os.system('rm %s/*Digest.sha1' % path)
            paths.append(path)

        node1.start()

        time.sleep(10)

        data_found = 0
        for path in paths:
            for fname in os.listdir(path):
                if fname.endswith('Data.db'):
                    data_found += 1
        self.assertGreater(data_found, 0, "After removing index, filter, stats, and digest files, the data file was deleted!")

    def sstableloader_with_mv_test(self):
        """
        @jira_ticket CASSANDRA-11275
        """
        def create_schema_with_mv(session, ks, compression):
            self.create_schema(session, ks, compression)
            # create a materialized view
            session.execute("CREATE MATERIALIZED VIEW mv1 AS "
                            "SELECT key FROM standard1 WHERE key IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL "
                            "PRIMARY KEY (v)")

        self.load_sstable_with_configuration(ks='"Keyspace1"', create_schema=create_schema_with_mv)

    @since('4.0')
    def sstableloader_with_failing_2i_test(self):
        """
        @jira_ticket CASSANDRA-10130

        Simulates an index building failure during SSTables load.
        The table data should be loaded and the index should be marked for rebuilding during the next node start.
        """
        def create_schema_with_2i(session):
            create_ks(session, 'k', 1)
            session.execute("CREATE TABLE k.t (p int, c int, v int, PRIMARY KEY(p, c))")
            session.execute("CREATE INDEX idx ON k.t(v)")

        cluster = self.cluster
        cluster.populate(1, install_byteman=True).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_schema_with_2i(session)
        session.execute("INSERT INTO k.t(p, c, v) VALUES (0, 1, 8)")

        # Stop node and copy SSTables
        node.nodetool('drain')
        node.stop()
        self.copy_sstables(cluster, node)

        # Wipe out data and restart
        cluster.clear()
        cluster.start()

        # Restore the schema
        session = self.patient_cql_connection(node)
        create_schema_with_2i(session)

        # The table should exist and be empty, and the index should be empty and marked as built
        assert_one(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx'])
        assert_none(session, "SELECT * FROM k.t")
        assert_none(session, "SELECT * FROM k.t WHERE v = 8")

        # Add some additional data before loading the SSTable, to check that it will be still accessible
        session.execute("INSERT INTO k.t(p, c, v) VALUES (0, 2, 8)")
        assert_one(session, "SELECT * FROM k.t", [0, 2, 8])
        assert_one(session, "SELECT * FROM k.t WHERE v = 8", [0, 2, 8])

        # Load SSTables with a failure during index creation
        node.byteman_submit(['./byteman/index_build_failure.btm'])
        with self.assertRaises(Exception):
            self.load_sstables(cluster, node, 'k')

        # Check that the index isn't marked as built and the old SSTable data has been loaded but not indexed
        assert_none(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""")
        assert_all(session, "SELECT * FROM k.t", [[0, 1, 8], [0, 2, 8]])
        assert_one(session, "SELECT * FROM k.t WHERE v = 8", [0, 2, 8])

        # Restart the node to trigger index rebuild
        node.nodetool('drain')
        node.stop()
        cluster.start()
        session = self.patient_cql_connection(node)

        # Check that the index is marked as built and the index has been rebuilt
        assert_one(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx', None])
        assert_all(session, "SELECT * FROM k.t", [[0, 1, 8], [0, 2, 8]])
        assert_all(session, "SELECT * FROM k.t WHERE v = 8", [[0, 1, 8], [0, 2, 8]])
