from dtest import Tester, debug
from tools import since, require, InterruptCompaction
from ccmlib import common

import subprocess
import glob
import os

# These must match the stress schema names
KeyspaceName = 'keyspace1'
TableName = 'standard1'

@require('7066')
@since('3.0')
class SSTableListTest(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def listfiles_oncompaction_test(self):
        """
        Check we can list the sstable files after successfull compaction (no temporary sstable files)
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        self._create_data(node, KeyspaceName, TableName, 100000)
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        node.compact();
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

    def listfiles_onabortedcompaction_test(self):
        """
        Check we can list the sstable files after aborted compaction (temporary sstable files)
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        numrecords = 200000

        self._create_data(node, KeyspaceName, TableName, numrecords)
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        t = InterruptCompaction(node, TableName)
        t.start()

        node.compact()
        t.join()

        # should compaction finish before the node is killed, this test would fail,
        # in which case try increasing numrecords
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName, finalfiles)
        assert len(tmpfiles) > 0

        # restart and make sure tmp files are gone and the data can be read
        node.start(wait_for_binary_proto=True)
        node.watch_log_for("Compacted(.*)%s"%(TableName,))

        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        debug("Run stress to ensure data is readable")
        self._read_data(node, numrecords)

    def _create_data(self, node, ks, table, numrecords):
        # This is just to create the schema so we can disable compaction
        node.stress( ['write', 'n=1', '-rate', 'threads=1'] )
        node.nodetool('disableautocompaction %s %s' % (ks, table))

        node.stress( ['write', 'n=%d' % (numrecords), '-rate', 'threads=50'] )
        node.flush()

    def _read_data(self, node, numrecords):
        node.stress( ['read', 'n=%d' % (numrecords,), '-rate', 'threads=25'] )

    def _check_files(self, node, ks, table, expected_finalfiles=[], expected_tmpfiles=[]):
        sstablefiles = self._get_sstable_files(node, ks, table)

        debug("Comparing all files...")
        allfiles = self._list_sstable_files(ks, table, type='all')
        self.assertEqual(sstablefiles, allfiles)

        if len(expected_finalfiles) == 0:
            expected_finalfiles = allfiles

        debug("Comparing final files...")
        finalfiles = self._list_sstable_files(ks, table, type='final')
        self.assertEqual(expected_finalfiles, finalfiles)

        if len(expected_tmpfiles) == 0:
            expected_tmpfiles = sorted(list(set(allfiles) - set(finalfiles)))

        debug("Comparing tmp files...")
        tmpfiles = self._list_sstable_files(ks, table, type='tmp')
        self.assertEqual(expected_tmpfiles, tmpfiles)

        debug("Comparing op logs...")
        expectedoplogs = sorted(self._get_sstable_operation_logs(node, ks, table))
        oplogs = sorted(list(set(self._list_sstable_files(ks, table, type='tmp', oplogs=True)) - set(tmpfiles)))
        self.assertEqual(expectedoplogs, oplogs)

        return finalfiles, tmpfiles

    # Use sstablelister to list sstable files
    def _list_sstable_files(self, ks, table, type='all', oplogs=False):
        debug("About to invoke sstablelister...")
        node1 = self.cluster.nodelist()[0]
        env = common.make_cassandra_env(node1.get_install_cassandra_root(), node1.get_node_cassandra_root())
        tool_bin = node1.get_tool('sstablelister')

        if oplogs:
            args = [ tool_bin, '--type', type, '--oplog', ks, table]
        else:
            args = [ tool_bin, '--type', type, ks, table]

        p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        (stdin, stderr) = p.communicate()

        if len(stderr) > 0:
            debug(stderr)
            assert False, "Error invoking sstablelister"

        ret = stdin.split('\n')
        debug("Got %d files" % (len(ret),))
        return sorted(filter(None, ret))

    # Read sstable files directly from disk
    def _get_sstable_files(self, node, ks, table):
        keyspace_dir = os.path.join(node.get_path(), 'data', ks)

        ret = []
        for ext in ('*.db', '*.txt', '*.adler32'):
            ret.extend(glob.glob(os.path.join(keyspace_dir, table + '-*', ext)))

        return sorted(ret)

    def _get_sstable_operation_logs(self, node, ks, table):
        keyspace_dir = os.path.join(node.get_path(), 'data', ks)
        ret = glob.glob(os.path.join(keyspace_dir, table + '-*', "operations", "*.log"))

        return sorted(ret)
