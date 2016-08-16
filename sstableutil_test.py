import glob
import os
import subprocess

from ccmlib import common
from ccmlib.node import ToolError

from dtest import Tester, debug
from tools import InterruptCompaction, since

# These must match the stress schema names
KeyspaceName = 'keyspace1'
TableName = 'standard1'


def _normcase_all(xs):
    """
    Return a list of the elements in xs, each with its casing normalized for
    use as a filename.
    """
    return [os.path.normcase(x) for x in xs]


@since('3.0')
class SSTableUtilTest(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def compaction_test(self):
        """
        @jira_ticket CASSANDRA-7066

        Check that we can list sstable files after a successful compaction (no temporary sstable files)
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        self._create_data(node, KeyspaceName, TableName, 100000)
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        node.compact()
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

    def abortedcompaction_test(self):
        """
        @jira_ticket CASSANDRA-7066
        @jira_ticket CASSANDRA-11497

        Check that we can cleanup temporary files after a compaction is aborted.
        """
        log_file_name = 'debug.log'
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        numrecords = 250000

        self._create_data(node, KeyspaceName, TableName, numrecords)
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertTrue(len(finalfiles) > 0, "Expected to find some final files")
        self.assertEqual(0, len(tmpfiles), "Expected no tmp files")

        t = InterruptCompaction(node, TableName, filename=log_file_name, delay=2)
        t.start()

        try:
            debug("Compacting...")
            node.compact()
        except ToolError:
            pass  # expected to fail

        t.join()

        finalfiles = _normcase_all(self._invoke_sstableutil(KeyspaceName, TableName, type='final'))
        tmpfiles = _normcase_all(self._invoke_sstableutil(KeyspaceName, TableName, type='tmp'))

        # In most cases we should end up with some temporary files to clean up, but it may happen
        # that no temporary files are created if compaction finishes too early or starts too late
        # see CASSANDRA-11497
        debug("Got {} final files and {} tmp files after compaction was interrupted"
              .format(len(finalfiles), len(tmpfiles)))

        self._invoke_sstableutil(KeyspaceName, TableName, cleanup=True)

        self._check_files(node, KeyspaceName, TableName, finalfiles, [])

        # restart to make sure not data is lost
        debug("Restarting node...")
        node.start(wait_for_binary_proto=True)
        # in some environments, a compaction may start that would change sstable files. We should wait if so
        node.wait_for_compactions()

        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        debug("Running stress to ensure data is readable")
        self._read_data(node, numrecords)

    def _create_data(self, node, ks, table, numrecords):
        """
         This is just to create the schema so we can disable compaction
        """
        node.stress(['write', 'n=1', 'no-warmup', '-rate', 'threads=1'])
        node.nodetool('disableautocompaction %s %s' % (ks, table))

        node.stress(['write', 'n=%d' % (numrecords), 'no-warmup', '-rate', 'threads=50'])
        node.flush()

    def _read_data(self, node, numrecords):
        node.stress(['read', 'n=%d' % (numrecords,), 'no-warmup', '-rate', 'threads=25'])

    def _check_files(self, node, ks, table, expected_finalfiles=None, expected_tmpfiles=None):
        sstablefiles = _normcase_all(self._get_sstable_files(node, ks, table))
        allfiles = _normcase_all(self._invoke_sstableutil(ks, table, type='all'))
        finalfiles = _normcase_all(self._invoke_sstableutil(ks, table, type='final'))
        tmpfiles = _normcase_all(self._invoke_sstableutil(ks, table, type='tmp'))
        expected_oplogs = _normcase_all(self._get_sstable_transaction_logs(node, ks, table))
        tmpfiles_with_oplogs = _normcase_all(self._invoke_sstableutil(ks, table, type='tmp', oplogs=True))
        oplogs = _normcase_all(sorted(list(set(tmpfiles_with_oplogs) - set(tmpfiles))))

        if expected_finalfiles is None:
            expected_finalfiles = allfiles
        else:
            expected_finalfiles = _normcase_all(expected_finalfiles)

        if expected_tmpfiles is None:
            expected_tmpfiles = sorted(set(allfiles) - set(finalfiles))
        else:
            expected_tmpfiles = _normcase_all(expected_tmpfiles)

        debug("Comparing all files...")
        self.assertEqual(sstablefiles, allfiles)

        debug("Comparing final files...")
        self.assertEqual(expected_finalfiles, finalfiles)

        debug("Comparing tmp files...")
        self.assertEqual(expected_tmpfiles, tmpfiles)

        debug("Comparing op logs...")
        self.assertEqual(expected_oplogs, oplogs)

        return finalfiles, tmpfiles

    def _invoke_sstableutil(self, ks, table, type='all', oplogs=False, cleanup=False):
        """
        Invoke sstableutil and return the list of files, if any
        """
        debug("About to invoke sstableutil with type {}...".format(type))
        node1 = self.cluster.nodelist()[0]
        env = common.make_cassandra_env(node1.get_install_cassandra_root(), node1.get_node_cassandra_root())
        tool_bin = node1.get_tool('sstableutil')

        args = [tool_bin, '--type', type]

        if oplogs:
            args.extend(['--oplog'])
        if cleanup:
            args.extend(['--cleanup'])

        args.extend([ks, table])

        p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        (stdout, stderr) = p.communicate()

        self.assertEqual(p.returncode, 0, "Error invoking sstableutil; returned {code}".format(code=p.returncode))

        if stdout:
            debug(stdout)

        match = ks + os.sep + table + '-'
        ret = sorted(filter(lambda s: match in s, stdout.splitlines()))
        debug("Got {} files of type {}".format(len(ret), type))
        return ret

    def _get_sstable_files(self, node, ks, table):
        """
        Read sstable files directly from disk
        """
        ret = []
        for data_dir in node.data_directories():
            keyspace_dir = os.path.join(data_dir, ks)
            for ext in ('*.db', '*.txt', '*.adler32', '*.crc32'):
                ret.extend(glob.glob(os.path.join(keyspace_dir, table + '-*', ext)))

        return sorted(ret)

    def _get_sstable_transaction_logs(self, node, ks, table):
        ret = []
        for data_dir in node.data_directories():
            keyspace_dir = os.path.join(data_dir, ks)
            ret.extend(glob.glob(os.path.join(keyspace_dir, table + '-*', "*.log")))

        return sorted(ret)
