import glob
import os
import subprocess

from ccmlib import common
from ccmlib.node import NodetoolError

from dtest import Tester, debug
from tools import InterruptCompaction, known_failure, since

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
        Check we can list the sstable files after successfull compaction (no temporary sstable files)
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

    @known_failure(failure_source='cassandra',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-10919',
                   flaky=True)
    def abortedcompaction_test(self):
        """
        @jira_ticket CASSANDRA-7066
        Check we can list the sstable files after aborted compaction (temporary sstable files)
        Then perform a cleanup and verify the temporary files are gone
        """
        log_file_name = 'debug.log'
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        numrecords = 400000

        self._create_data(node, KeyspaceName, TableName, numrecords)
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        t = InterruptCompaction(node, TableName, filename=log_file_name)
        t.start()

        try:
            node.compact()
            assert False, "Compaction should have failed"
        except NodetoolError:
            pass  # expected to fail

        t.join()

        # should compaction finish before the node is killed, this test would fail,
        # in which case try increasing numrecords
        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName, finalfiles)
        self.assertTrue(len(tmpfiles) > 0)

        self._invoke_sstableutil(KeyspaceName, TableName, cleanup=True)

        self._check_files(node, KeyspaceName, TableName, finalfiles, [])

        # restart to make sure not data is lost
        node.start(wait_for_binary_proto=True)

        finalfiles, tmpfiles = self._check_files(node, KeyspaceName, TableName)
        self.assertEqual(0, len(tmpfiles))

        debug("Run stress to ensure data is readable")
        self._read_data(node, numrecords)

    def _create_data(self, node, ks, table, numrecords):
        """
         This is just to create the schema so we can disable compaction
        """
        node.stress(['write', 'n=1', '-rate', 'threads=1'])
        node.nodetool('disableautocompaction %s %s' % (ks, table))

        node.stress(['write', 'n=%d' % (numrecords), '-rate', 'threads=50'])
        node.flush()

    def _read_data(self, node, numrecords):
        node.stress(['read', 'n=%d' % (numrecords,), '-rate', 'threads=25'])

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
        debug("About to invoke sstableutil...")
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

        if p.returncode != 0:
            debug(stderr)
            assert False, "Error invoking sstableutil; returned {code}".format(code=p.returncode)

        debug(stdout)
        match = ks + os.sep + table + '-'
        ret = sorted(filter(lambda s: match in s, stdout.splitlines()))
        debug("Got %d files" % (len(ret),))
        return ret

    def _get_sstable_files(self, node, ks, table):
        """
        Read sstable files directly from disk
        """
        ret = []
        for x in xrange(0, self.cluster.data_dir_count):
            keyspace_dir = os.path.join(node.get_path(), 'data{0}'.format(x), ks)
            for ext in ('*.db', '*.txt', '*.adler32', '*.crc32'):
                ret.extend(glob.glob(os.path.join(keyspace_dir, table + '-*', ext)))

        return sorted(ret)

    def _get_sstable_transaction_logs(self, node, ks, table):
        ret = []
        for x in xrange(0, self.cluster.data_dir_count):
            keyspace_dir = os.path.join(node.get_path(), 'data{0}'.format(x), ks)
            ret.extend(glob.glob(os.path.join(keyspace_dir, table + '-*', "*.log")))

        return sorted(ret)
