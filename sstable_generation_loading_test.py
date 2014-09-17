import time, os
from distutils import dir_util
import subprocess

from dtest import Tester, debug
from ccmlib import common as ccmcommon

class TestSSTableGenerationAndLoading(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestSSTableGenerationAndLoading, self).__init__(*argv, **kwargs)
        self.allow_log_errors = True

    def incompressible_data_in_compressed_table_test(self):
        """
        tests for the bug that caused #3370:
        https://issues.apache.org/jira/browse/CASSANDRA-3370

        inserts random data into a compressed table. The compressed SSTable was
        compared to the uncompressed and was found to indeed be larger then
        uncompressed.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(.5)

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', compression="Deflate")

        # make unique column names, and values that are incompressible
        rnd = open('/dev/urandom', 'rb')
        for col in xrange(10):
            col_name = str(col)
            col_val = rnd.read(5000)
            col_val = col_val.encode('hex')
            cql = "UPDATE cf SET v='%s' WHERE KEY='0' AND c='%s'" % (col_val, col_name)
            # print cql
            cursor.execute(cql)
        rnd.close()

        node1.flush()
        time.sleep(2)
        rows = cursor.execute("SELECT * FROM cf WHERE KEY = '0' AND c < '8'")
        assert len(rows) > 0

    def remove_index_file_test(self):
        """
        tests for situations similar to that found in #343:
        https://issues.apache.org/jira/browse/CASSANDRA-343
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        # Makinge sure the cluster is ready to accept the subsequent
        # stress connection. This was an issue on Windows.
        node1.watch_log_for('thrift clients...')
        version = cluster.version()
        if version < "2.1":
            node1.stress(['--num-keys=10000'])
        else:
            node1.stress(['write', 'n=10000', '-rate', 'threads=8'])
        node1.flush()
        node1.compact()
        node1.stop()
        time.sleep(1)
        path = ""
        if version < "2.1":
            path = os.path.join(node1.get_path(), 'data', 'Keyspace1', 'Standard1')
        else:
            basepath = os.path.join(node1.get_path(), 'data', 'Keyspace1')
            for x in os.listdir(basepath):
                if x.startswith("Standard1"):
                    path = os.path.join(basepath, x)

        os.system('rm %s/*Index.db' % path)
        os.system('rm %s/*Filter.db' % path)
        os.system('rm %s/*Statistics.db' % path)
        os.system('rm %s/*Digest.sha1' % path)

        node1.start()

        time.sleep(10)

        data_found = 0
        for fname in os.listdir(path):
            if fname.endswith('Data.db'):
                data_found += 1
        assert data_found > 0, "After removing index, filter, stats, and digest files, the data file was deleted!"

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

    def load_sstable_with_configuration(self, pre_compression=None, post_compression=None):
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

        debug("Testing sstableloader with pre_compression=%s and post_compression=%s" % (pre_compression, post_compression))

        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()
        time.sleep(.5)

        def create_schema(cursor, compression):
            self.create_ks(cursor, "ks", rf=2)
            self.create_cf(cursor, "standard1", compression=compression)
            self.create_cf(cursor, "counter1", compression=compression, columns={'v': 'counter'})

        debug("creating keyspace and inserting")
        cursor = self.cql_connection(node1)
        create_schema(cursor, pre_compression)

        for i in range(NUM_KEYS):
            cursor.execute("UPDATE standard1 SET v='%d' WHERE KEY='%d' AND c='col'" % (i, i))
            cursor.execute("UPDATE counter1 SET v=v+1 WHERE KEY='%d'" % i)

        node1.nodetool('drain')
        node1.stop()
        node2.nodetool('drain')
        node2.stop()

        debug("Making a copy of the sstables")
        # make a copy of the sstables
        data_dir = os.path.join(node1.get_path(), 'data')
        copy_root = os.path.join(node1.get_path(), 'data_copy')
        for ddir in os.listdir(data_dir):
            keyspace_dir = os.path.join(data_dir, ddir)
            if os.path.isdir(keyspace_dir) and ddir != 'system':
                copy_dir = os.path.join(copy_root, ddir)
                dir_util.copy_tree(keyspace_dir, copy_dir)

        debug("Wiping out the data and restarting cluster")
        # wipe out the node data.
        cluster.clear()
        cluster.start()
        time.sleep(5) # let gossip figure out what is going on

        debug("re-creating the keyspace and column families.")
        cursor = self.cql_connection(node1)
        create_schema(cursor, post_compression)
        time.sleep(2)

        debug("Calling sstableloader")
        # call sstableloader to re-load each cf.
        cdir = node1.get_cassandra_dir()
        sstableloader = os.path.join(cdir, 'bin', ccmcommon.platform_binary('sstableloader'))
        env = ccmcommon.make_cassandra_env(cdir, node1.get_path())
        host = node1.address()
        sstablecopy_dir = copy_root + '/ks'
        for cf_dir in os.listdir(sstablecopy_dir):
            full_cf_dir = os.path.join(sstablecopy_dir, cf_dir)
            if os.path.isdir(full_cf_dir):
                cmd_args = [sstableloader, '--nodes', host, full_cf_dir]
                p = subprocess.Popen(cmd_args, env=env)
                exit_status = p.wait()
                self.assertEqual(0, exit_status,
                        "sstableloader exited with a non-zero status: %d" % exit_status)

        def read_and_validate_data(cursor):
            for i in range(NUM_KEYS):
                rows = cursor.execute("SELECT * FROM standard1 WHERE KEY='%d'" % i)
                self.assertEquals([str(i), 'col', str(i)], list(rows[0]))
                rows = cursor.execute("SELECT * FROM counter1 WHERE KEY='%d'" % i)
                self.assertEquals([str(i), 1], list(rows[0]))

        debug("Reading data back")
        # Now we should have sstables with the loaded data, and the existing
        # data. Lets read it all to make sure it is all there.
        read_and_validate_data(cursor)

        debug("scrubbing, compacting, and repairing")
        # do some operations and try reading the data again.
        node1.nodetool('scrub')
        node1.nodetool('compact')
        node1.nodetool('repair')

        debug("Reading data back one more time")
        read_and_validate_data(cursor)
