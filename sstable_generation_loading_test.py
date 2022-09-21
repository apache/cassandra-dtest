import os
import subprocess
import time
import distutils.dir_util
from distutils.version import LooseVersion

import pytest
import logging

from ccmlib import common as ccmcommon
from ccmlib.node import ToolError

from dtest import Tester, create_ks, create_cf, mk_bman_path, MAJOR_VERSION_4
from tools.assertions import assert_all, assert_none, assert_one

since = pytest.mark.since
logger = logging.getLogger(__name__)

# WARNING: sstableloader tests should be added to TestSSTableGenerationAndLoading (below),
# and not to BaseSStableLoaderTest (which is shared with upgrade tests)


# Also used by upgrade_tests/storage_engine_upgrade_test
# to test loading legacy sstables
class BaseSStableLoaderTester(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.allow_log_errors = True

    upgrade_from = None
    test_compact = False

    def compact(self):
        return self.fixture_dtest_setup.cluster.version() < MAJOR_VERSION_4 and self.test_compact

    def create_schema(self, session, ks, compression):
        create_ks(session, ks, rf=2)
        create_cf(session, "standard1", compression=compression, compact_storage=self.compact())
        create_cf(session, "counter1", compression=compression, columns={'v': 'counter'},
                  compact_storage=self.compact())

    def skip_base_class_test(self):
        if self.__class__.__name__ != 'TestBasedSSTableLoader' and self.upgrade_from is None:
            pytest.skip("Don't need to run base class test, only derived classes")

    def create_schema_40(self, session, ks, compression):
        create_ks(session, ks, rf=2)
        create_cf(session, "standard1", compression=compression, compact_storage=self.compact())
        create_cf(session, "counter1", key_type='text', compression=compression, columns={'column1': 'text',
                                                                                    'v': 'counter static',
                                                                                    'value': 'counter'},
                  primary_key="key, column1", clustering='column1 ASC', compact_storage=self.compact())

    def test_sstableloader_compression_none_to_none(self):
        self.skip_base_class_test()
        self.load_sstable_with_configuration(None, None)

    def test_sstableloader_compression_none_to_snappy(self):
        self.skip_base_class_test()
        self.load_sstable_with_configuration(None, 'Snappy')

    def test_sstableloader_compression_none_to_deflate(self):
        self.skip_base_class_test()
        self.load_sstable_with_configuration(None, 'Deflate')

    def test_sstableloader_compression_snappy_to_none(self):
        self.skip_base_class_test()
        self.load_sstable_with_configuration('Snappy', None)

    def test_sstableloader_compression_snappy_to_snappy(self):
        self.skip_base_class_test()
        self.load_sstable_with_configuration('Snappy', 'Snappy')

    def test_sstableloader_compression_snappy_to_deflate(self):
        self.skip_base_class_test()
        self.load_sstable_with_configuration('Snappy', 'Deflate')

    def test_sstableloader_compression_deflate_to_none(self):
        self.skip_base_class_test()
        self.load_sstable_with_configuration('Deflate', None)

    def test_sstableloader_compression_deflate_to_snappy(self):
        self.skip_base_class_test()
        self.load_sstable_with_configuration('Deflate', 'Snappy')

    def test_sstableloader_compression_deflate_to_deflate(self):
        self.skip_base_class_test()
        self.load_sstable_with_configuration('Deflate', 'Deflate')

    def test_sstableloader_with_mv(self):
        """
        @jira_ticket CASSANDRA-11275
        """
        self.skip_base_class_test()
        def create_schema_with_mv(session, ks, compression):
            self.create_schema(session, ks, compression)
            # create a materialized view
            session.execute("CREATE MATERIALIZED VIEW mv1 AS "
                            "SELECT key FROM standard1 WHERE key IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL "
                            "PRIMARY KEY (v)")

        self.load_sstable_with_configuration(ks='"Keyspace1"', create_schema=create_schema_with_mv)

    def copy_sstables(self, cluster, node):
        for x in range(0, cluster.data_dir_count):
            data_dir = os.path.join(node.get_path(), 'data{0}'.format(x))
            copy_root = os.path.join(node.get_path(), 'data{0}_copy'.format(x))
            for ddir in os.listdir(data_dir):
                keyspace_dir = os.path.join(data_dir, ddir)
                if os.path.isdir(keyspace_dir) and ddir != 'system':
                    copy_dir = os.path.join(copy_root, ddir)
                    distutils.dir_util.copy_tree(keyspace_dir, copy_dir)

    def load_sstables(self, cluster, node, ks):
        cdir = node.get_install_dir()
        sstableloader = os.path.join(cdir, 'bin', ccmcommon.platform_binary('sstableloader'))
        env = ccmcommon.make_cassandra_env(cdir, node.get_path())
        host = node.address()
        for x in range(0, cluster.data_dir_count):
            sstablecopy_dir = os.path.join(node.get_path(), 'data{0}_copy'.format(x), ks.strip('"'))
            for cf_dir in os.listdir(sstablecopy_dir):
                full_cf_dir = os.path.join(sstablecopy_dir, cf_dir)
                if os.path.isdir(full_cf_dir):
                    cmd_args = [sstableloader, '--nodes', host, full_cf_dir]
                    p = subprocess.Popen(cmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
                    stdout, stderr = p.communicate()
                    exit_status = p.returncode
                    logger.debug('stdout: {out}'.format(out=stdout.decode("utf-8")))
                    logger.debug('stderr: {err}'.format(err=stderr.decode("utf-8")))
                    assert 0 == exit_status, \
                        "sstableloader exited with a non-zero status: {}".format(exit_status)

    def load_sstables_from_another_node(self, cluster, node_from, node_to, ks):
        cdir = node_to.get_install_dir()
        sstableloader = os.path.join(cdir, 'bin', ccmcommon.platform_binary('sstableloader'))
        env = ccmcommon.make_cassandra_env(cdir, node_to.get_path())
        host = node_to.address()
        ret = []
        for x in range(cluster.data_dir_count):
            sstable_dir = os.path.join(node_from.get_path(), 'data' + str(x), ks.strip('"'))
            for cf_dir in os.listdir(sstable_dir):
                full_cf_dir = os.path.join(sstable_dir, cf_dir)
                if os.path.isdir(full_cf_dir):
                    cmd_args = [sstableloader, '--verbose', '--nodes', host, full_cf_dir]
                    p = subprocess.Popen(cmd_args, stderr=subprocess.PIPE, stdout=subprocess.PIPE, env=env)
                    stdout, stderr = p.communicate()
                    exit_status = p.returncode
                    ret.append((exit_status, stdout.decode("utf-8"), stderr.decode("utf-8")))
        return ret

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
            assert compression_option in (None, 'Snappy', 'Deflate')

        logger.debug("Testing sstableloader with pre_compression=%s and post_compression=%s" % (pre_compression, post_compression))
        if self.upgrade_from:
            logger.debug("Testing sstableloader with upgrade_from=%s and compact=%s" % (self.upgrade_from, self.compact))

        cluster = self.cluster
        if self.upgrade_from:
            logger.debug("Generating sstables with version %s" % (self.upgrade_from))
            default_install_version = self.cluster.version()
            default_install_dir = self.cluster.get_install_dir()
            # Forcing cluster version on purpose
            cluster.set_install_dir(version=self.upgrade_from)
            self.fixture_dtest_setup.reinitialize_cluster_for_different_version()
        self.install_nodetool_legacy_parsing()
        logger.debug("Using jvm_args={}".format(self.jvm_args))
        cluster.populate(2).start(jvm_args=list(self.jvm_args))
        self.install_nodetool_legacy_parsing()
        node1, node2 = cluster.nodelist()
        time.sleep(.5)

        logger.debug("creating keyspace and inserting")
        session = self.cql_connection(node1)
        self.create_schema(session, ks, pre_compression)

        for i in range(NUM_KEYS):
            session.execute("UPDATE standard1 SET v='{}' WHERE KEY='{}' AND c='col'".format(i, i))
            session.execute("UPDATE counter1 SET v=v+1 WHERE KEY='{}'".format(i))

        #Will upgrade to a version that doesn't support compact storage so revert the compact
        #storage, this doesn't actually fix it yet
        if self.compact() and default_install_version >= MAJOR_VERSION_4:
            session.execute('alter table standard1 drop compact storage');
            session.execute('alter table counter1 drop compact storage');
            node1.nodetool('rebuild')
            node1.nodetool('cleanup')
            node2.nodetool('rebuild')
            node2.nodetool('cleanup')

        node1.nodetool('drain')
        node1.stop()
        node2.nodetool('drain')
        node2.stop()

        logger.debug("Making a copy of the sstables")
        # make a copy of the sstables
        self.copy_sstables(cluster, node1)

        logger.debug("Wiping out the data and restarting cluster")
        # wipe out the node data.
        cluster.clear()

        if self.upgrade_from:
            logger.debug("Running sstableloader with version from %s" % (default_install_dir))
            # Return to previous version
            cluster.set_install_dir(install_dir=default_install_dir)
            self.fixture_dtest_setup.reinitialize_cluster_for_different_version()

        self.install_nodetool_legacy_parsing()
        cluster.start(jvm_args=list(self.jvm_args))
        time.sleep(5)  # let gossip figure out what is going on

        logger.debug("re-creating the keyspace and column families.")
        session = self.cql_connection(node1)

        if self.test_compact and default_install_version >= MAJOR_VERSION_4:
            self.create_schema_40(session, ks, post_compression)
        else:
            self.create_schema(session, ks, post_compression)
        time.sleep(2)

        logger.debug("Calling sstableloader")
        # call sstableloader to re-load each cf.
        self.load_sstables(cluster, node1, ks)

        def read_and_validate_data(session):
            for i in range(NUM_KEYS):
                query = "SELECT * FROM standard1 WHERE KEY='{}'".format(i)
                assert_one(session, query, [str(i), 'col', str(i)])
                query = "SELECT key, v FROM counter1 WHERE KEY='{}'".format(i)
                assert_one(session, query, [str(i), 1])

        logger.debug("Reading data back")
        # Now we should have sstables with the loaded data, and the existing
        # data. Lets read it all to make sure it is all there.
        read_and_validate_data(session)

        logger.debug("scrubbing, compacting, and repairing")
        # do some operations and try reading the data again.
        node1.nodetool('scrub')
        node1.nodetool('compact')
        try:
            node1.nodetool('repair')
        except ToolError as e:
            print("Caught ToolError")

        logger.debug("Reading data back one more time")
        read_and_validate_data(session)

        # check that RewindableDataInputStreamPlus spill files are properly cleaned up
        if self.upgrade_from:
            for x in range(0, cluster.data_dir_count):
                data_dir = os.path.join(node1.get_path(), 'data{0}'.format(x))
                for ddir in os.listdir(data_dir):
                    keyspace_dir = os.path.join(data_dir, ddir)
                    temp_files = self.glob_data_dirs(os.path.join(keyspace_dir, '*', "tmp", "*.dat"))
                    logger.debug("temp files: " + str(temp_files))
                    assert 0 == len(temp_files), "Temporary files were not cleaned up."


class TestSSTableGenerationAndLoading(BaseSStableLoaderTester):

    def test_sstableloader_uppercase_keyspace_name(self):
        """
        Make sure sstableloader works with upper case keyspace
        @jira_ticket CASSANDRA-10806
        """
        self.load_sstable_with_configuration(ks='"Keyspace1"')

    def test_incompressible_data_in_compressed_table(self):
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
        for col in range(10):
            col_name = str(col)
            col_val = os.urandom(5000)
            col_val = col_val.hex()
            cql = "UPDATE cf SET v='%s' WHERE KEY='0' AND c='%s'" % (col_val, col_name)
            # print cql
            session.execute(cql)

        node1.flush()
        time.sleep(2)
        rows = list(session.execute("SELECT * FROM cf WHERE KEY = '0' AND c < '8'"))
        assert len(rows) > 0

    def test_remove_index_file(self):
        """
        tests for situations similar to that found in #343:
        https://issues.apache.org/jira/browse/CASSANDRA-343

        @jira_ticket CASSANDRA-343
        """
        cluster = self.cluster
        cluster.populate(1).start()
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
        assert data_found > 0, "After removing index, filter, stats, and digest files > the data file was deleted!"

    def test_sstableloader_with_mv(self):
        """
        @jira_ticket CASSANDRA-11275
        """
        def create_schema_with_mv(session, ks, compression):
            self.cluster.nodelist()[0].set_configuration_options({'enable_materialized_views': 'true'})
            self.create_schema(session, ks, compression)
            # create a materialized view
            session.execute("CREATE MATERIALIZED VIEW mv1 AS "
                            "SELECT key FROM standard1 WHERE key IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL "
                            "PRIMARY KEY (v)")

        self.load_sstable_with_configuration(ks='"Keyspace1"', create_schema=create_schema_with_mv)

    @since('4.0')
    def test_sstableloader_with_failing_2i(self):
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
        cluster.populate(1, install_byteman=True).start()
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
        assert_one(session, """SELECT * FROM system."IndexInfo" WHERE table_name='k'""", ['k', 'idx', None])
        assert_none(session, "SELECT * FROM k.t")
        assert_none(session, "SELECT * FROM k.t WHERE v = 8")

        # Add some additional data before loading the SSTable, to check that it will be still accessible
        session.execute("INSERT INTO k.t(p, c, v) VALUES (0, 2, 8)")
        assert_one(session, "SELECT * FROM k.t", [0, 2, 8])
        assert_one(session, "SELECT * FROM k.t WHERE v = 8", [0, 2, 8])

        # Load SSTables with a failure during index creation
        node.byteman_submit([mk_bman_path('index_build_failure.btm')])
        with pytest.raises(Exception):
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

    @since("3.0")
    def test_sstableloader_empty_stream(self):
        """
        @jira_ticket CASSANDRA-16349

        Tests that sstableloader does not throw if SSTables it attempts to load do not
        intersect with the node's ranges.
        """
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        create_ks(session, 'k', 1)
        session.execute("CREATE TABLE k.t (k int PRIMARY KEY, v int)")
        for i in range(10):
            session.execute("INSERT INTO k.t (k, v) VALUES ({0}, {0})".format(i))
        node1.nodetool('flush')

        ret = self.load_sstables_from_another_node(cluster, node1, node2, "k")
        assert len(ret) > 0, "Expected to stream at least 1 table"
        for exit_status, _, stderr in ret:
            assert exit_status == 0, "Expected exit code 0, got {}".format(exit_status)
            # Below warning is emitted in trunk/4.1 because of CASSANDRA-15234. We exploit the backward compatibility
            # framework with DTests instead of changing config in all old tests.
            if len(stderr) > 0 and stderr is not "parameters have been deprecated. They have new names and/or value format":
                "Expected empty stderr, got {}".format(stderr)
