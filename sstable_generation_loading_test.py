import time
import os
import sys
import re
from distutils import dir_util
import subprocess
import itertools
import zlib

from dtest import Tester, debug
from tools import since
from ccmlib.cluster import Cluster
from ccmlib.node import Node
from ccmlib import common as ccmcommon

class TestSSTableGenerationAndLoading(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestSSTableGenerationAndLoading, self).__init__(*argv, **kwargs)
        self.allow_log_errors = True

    def incompressible_data_in_compressed_table_test(self):
        """
        tests for the bug that caused this:
        https://issues.apache.org/jira/browse/CASSANDRA-3370

        inserts random data into a compressed table. The compressed SSTable was
        compared to the uncompressed and was found to indeed be larger then
        uncompressed.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(.5)

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', compression="Deflate", validation='blob', comparator='bigint')

        # make unique column names, and values that are incompressible
        rnd = open('/dev/urandom', 'rb')
        for key in xrange(1):
            for col in xrange(10):
                col_name = col
                col_val = rnd.read(5000)
                col_val = col_val.encode('hex')
                cql = u"UPDATE cf SET %d = '%s' WHERE KEY='%s'" % (
                        col_name,
                        col_val,
                        str(key))
                cursor.execute(cql)
        rnd.close()

        node1.flush()
        time.sleep(2)
        cursor.execute("SELECT 1..8 FROM cf ")
        cursor.fetchone()

    @since('1.1')
    def remove_index_file_test(self):
        """
        tests for situations similar to that found in this issue:
        https://issues.apache.org/jira/browse/CASSANDRA-343
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        node1.stress(['--num-keys=10000'])
        node1.flush()
        node1.compact()
        node1.stop()
        time.sleep(1)

        path = os.path.join(node1.get_path(), 'data', 'Keyspace1', 'Standard1')
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
        self.load_sstable_with_configuration(None, 'SnappyCompressor')
    def sstableloader_compression_none_to_deflate_test(self):
        self.load_sstable_with_configuration(None, 'DeflateCompressor')

    def sstableloader_compression_snappy_to_none_test(self):
        self.load_sstable_with_configuration('SnappyCompressor', None)
    def sstableloader_compression_snappy_to_snappy_test(self):
        self.load_sstable_with_configuration('SnappyCompressor', 'SnappyCompressor')
    def sstableloader_compression_snappy_to_deflate_test(self):
        self.load_sstable_with_configuration('SnappyCompressor', 'DeflateCompressor')

    def sstableloader_compression_deflate_to_none_test(self):
        self.load_sstable_with_configuration('DeflateCompressor', None)
    def sstableloader_compression_deflate_to_snappy_test(self):
        self.load_sstable_with_configuration('DeflateCompressor', 'SnappyCompressor')
    def sstableloader_compression_deflate_to_deflate_test(self):
        self.load_sstable_with_configuration('DeflateCompressor', 'DeflateCompressor')

    def load_sstable_with_configuration(self, pre_compression=None, post_compression=None):
        """
        tests that the sstableloader works by using it to load data.
        Compression of the columnfamilies being loaded, and loaded into
        can be specified.

        pre_compression and post_compression can be these values:
        None, 'SnappyCompressor', or 'DeflateCompressor'.
        """
        NUM_KEYS = 1000

        def read_and_validate_data():
            debug("READ")
            node1.stress(['--operation=READ', '--num-keys=%d'%NUM_KEYS])
            debug("RANGE_SLICE")
            node1.stress(['--operation=RANGE_SLICE', '--num-keys=%d'%NUM_KEYS])
            debug("READ SUPER")
            node1.stress(['--operation=READ', '--num-keys=%d'%NUM_KEYS, '--family-type=Super'])
            debug("COUNTER GET")
            node1.stress(['--operation=COUNTER_GET', '--num-keys=%d'%NUM_KEYS])
            debug("COUNTER GET SUPER")
            node1.stress(['--operation=COUNTER_GET', '--num-keys=%d'%NUM_KEYS, '--family-type=Super'])
            

        assert pre_compression in (None, 'SnappyCompressor', 'DeflateCompressor'), 'Invalid input pre_compression: ' + str(pre_compression)
        assert post_compression in (None, 'SnappyCompressor', 'DeflateCompressor'), 'Invalid input post_compression: ' + str(post_compression)

        debug("Testing sstableloader with pre_compression=%s and post_compression=%s" % (pre_compression, post_compression))

        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()
        time.sleep(.5)

        compression_str = '--compression='+pre_compression if pre_compression else ''

        debug("creating keyspace and inserting")
        # create the keyspace
        node1.stress(['--replication-factor=2', '--num-keys=1', compression_str])
        node1.stress(['--num-keys=%d'%NUM_KEYS, compression_str])
        node1.stress(['--num-keys=%d'%NUM_KEYS, '--family-type=Super', compression_str])
        node1.stress(['--num-keys=%d'%NUM_KEYS, '--operation=COUNTER_ADD', compression_str])
        node1.stress(['--num-keys=%d'%NUM_KEYS, '--family-type=Super', '--operation=COUNTER_ADD', compression_str])
        time.sleep(2)
        
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
        node1.clear()
        node2.clear()
        node1.start()
        node2.start()
        time.sleep(20) # let gossip figure out what is going on

        debug("re-creating the keyspace and column families.")
        # now re-create the keyspace and column families, but don't insert much data.
        # we'll want this data to get overwritten.
        compression_str = '--compression='+post_compression if post_compression else ''
        node1.stress(['--replication-factor=1', '--num-keys=1', compression_str])
        node1.stress(['--num-keys=1', compression_str])
        node1.stress(['--num-keys=1', '--family-type=Super', compression_str])
        node1.stress(['--num-keys=1', '--operation=COUNTER_ADD', compression_str])
        node1.stress(['--num-keys=1', '--family-type=Super', '--operation=COUNTER_ADD', compression_str])
        time.sleep(2)

        debug("Calling sstableloader")
        # call sstableloader to re-load each cf.
        cdir = node1.get_cassandra_dir()
        sstableloader = os.path.join(cdir, 'bin', 'sstableloader')
        env = ccmcommon.make_cassandra_env(cdir, node1.get_path())
        host = node1.address()
        for cf_dir in os.listdir(copy_dir):
            full_cf_dir = os.path.join(copy_dir, cf_dir)
            if os.path.isdir(full_cf_dir):
                cmd_args = [sstableloader, '--nodes', host, full_cf_dir]
                p = subprocess.Popen(cmd_args, env=env)
                p.wait()

        debug("Reading data back")
        # Now we should have sstables with the loaded data, and the existing
        # data. Lets read it all to make sure it is all there.
        read_and_validate_data()

        debug("scrubbing, compacting, and repairing")
        # do some operations and try reading the data again.
        node1.nodetool('scrub')
        node1.nodetool('compact')
        node1.nodetool('repair')

        debug("Reading data back one more time")
        read_and_validate_data()
