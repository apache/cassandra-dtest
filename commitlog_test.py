import os
import stat
import glob
import time
import platform
import subprocess
import ccmlib
from tools import since
from cassandra.cluster import NoHostAvailable, OperationTimedOut
from dtest import Tester, debug
from assertions import assert_one, assert_none, assert_almost_equal


class TestCommitLog(Tester):
    """ CommitLog Tests """

    def __init__(self, *argv, **kwargs):
        super(TestCommitLog, self).__init__(*argv, **kwargs)
        self.allow_log_errors = True

    def setUp(self):
        super(TestCommitLog, self).setUp()
        self.cluster.populate(1)
        [self.node1] = self.cluster.nodelist()

    def tearDown(self):
        self._change_commitlog_perms(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)
        super(TestCommitLog, self).tearDown()

    def prepare(self, configuration={}, create_test_keyspace=True, **kwargs):
        conf = {'commitlog_sync_period_in_ms': 1000}
        if self.cluster.version() >= "2.1":
            conf['memtable_heap_space_in_mb'] = 512
        conf.update(configuration)
        self.cluster.set_configuration_options(values=conf, **kwargs)
        self.cluster.start()
        self.cursor1 = self.patient_cql_connection(self.node1)
        if create_test_keyspace:
            self.cursor1.execute("DROP KEYSPACE IF EXISTS ks;")
            self.create_ks(self.cursor1, 'ks', 1)
            self.cursor1.execute("DROP TABLE IF EXISTS test;")
            query = """
              CREATE TABLE test (
                key int primary key,
                col1 int
              )
            """
            self.cursor1.execute(query)

    def _change_commitlog_perms(self, mod):
        path = self._get_commitlog_path()
        os.chmod(path, mod)
        commitlogs = glob.glob(path+'/*')
        for commitlog in commitlogs:
            os.chmod(commitlog, mod)

    def _get_commitlog_path(self):
        """ Returns the commitlog path """

        return os.path.join(self.node1.get_path(), 'commitlogs')

    def _get_commitlog_files(self):
        """ Returns the number of commitlog files in the directory """

        path = self._get_commitlog_path()
        return [os.path.join(path, p) for p in os.listdir(path)]

    def _get_commitlog_size(self):
        """ Returns the commitlog directory size in MB """

        path = self._get_commitlog_path()
        cmd_args = ['du', '-B', '1', path]
        p = subprocess.Popen(cmd_args, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.returncode
        self.assertEqual(0, exit_status,
                         "du exited with a non-zero status: %d" % exit_status)
        size = int(stdout.split('\t')[0])
        return size/1024/1024  # in MB

    def _commitlog_test(self, segment_size_in_mb, commitlog_size,
                        num_commitlog_files, compressed=False,
                        size_error=0.05, files_error=0):
        """ Execute a basic commitlog test and validate the commitlog files """

        if compressed:
            segment_size_in_mb *= 0.7
        segment_size = segment_size_in_mb * 1024 * 1024
        self.node1.stress(['write', 'n=150000', '-rate', 'threads=25'])
        time.sleep(1)

        if not ccmlib.common.is_win():
            assert_almost_equal(self._get_commitlog_size(), commitlog_size,
                                error=size_error)
        commitlogs = self._get_commitlog_files()
        assert_almost_equal(len(commitlogs), num_commitlog_files,
                            error=files_error)
        for f in commitlogs:
            size = os.path.getsize(f)
            size_in_mb = int(size/1024/1024)
            if size_in_mb < 1 or size < (segment_size*0.1):
                continue   # commitlog not yet used
            assert_almost_equal(size, segment_size, error=0.05)

    def _provoke_commitlog_failure(self):
        """ Provoke the commitlog failure """

        # Test things are ok at this point
        self.cursor1.execute("""
            INSERT INTO test (key, col1) VALUES (1, 1);
        """)
        assert_one(
            self.cursor1,
            "SELECT * FROM test where key=1;",
            [1, 1]
        )

        self._change_commitlog_perms(0)

        if self.cluster.version() < "2.1":
            with open(os.devnull, 'w') as devnull:
                self.node1.stress(['--num-keys=500000'], stdout=devnull, stderr=subprocess.STDOUT)
        else:
            with open(os.devnull, 'w') as devnull:
                self.node1.stress(['write', 'n=500000', '-rate', 'threads=25'], stdout=devnull, stderr=subprocess.STDOUT)

    @since('2.1')
    def default_segment_size_test(self):
        """ Test default commitlog_segment_size_in_mb (32MB) """

        self.prepare(create_test_keyspace=False)
        self._commitlog_test(32, 60, 2, files_error=0.5)

    @since('2.1')
    def small_segment_size_test(self):
        """ Test a small commitlog_segment_size_in_mb (5MB) """
        segment_size_in_mb = 5
        self.prepare(configuration={
            'commitlog_segment_size_in_mb': segment_size_in_mb
        }, create_test_keyspace=False)
        self._commitlog_test(segment_size_in_mb, 60, 13, files_error=0.12)

    @since('3.0')
    def default_compressed_segment_size_test(self):
        """ Test default compressed commitlog_segment_size_in_mb (32MB) """

        self.prepare(configuration={
            'commitlog_compression': [{'class_name': 'LZ4Compressor'}]
        }, create_test_keyspace=False)
        self._commitlog_test(32, 23, 2, compressed=True, files_error=0.5)

    @since('3.0')
    def small_compressed_segment_size_test(self):
        """ Test a small compressed commitlog_segment_size_in_mb (5MB) """
        segment_size_in_mb = 5
        self.prepare(configuration={
            'commitlog_segment_size_in_mb': segment_size_in_mb,
            'commitlog_compression': [{'class_name': 'LZ4Compressor'}]
        }, create_test_keyspace=False)
        self._commitlog_test(segment_size_in_mb, 42, 14, compressed=True, files_error=0.12)

    def stop_failure_policy_test(self):
        """ Test the stop commitlog failure policy (default one) """
        self.prepare()

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log("Failed .+ commit log segments. Commit disk failure policy is stop; terminating thread")
        debug(failure)
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertTrue(self.node1.is_running(), "Node1 should still be running")

        # Cannot write anymore after the failure
        with self.assertRaises(NoHostAvailable) as cm:
            self.cursor1.execute("""
              INSERT INTO test (key, col1) VALUES (2, 2);
            """)

        # Should not be able to read neither
        with self.assertRaises(NoHostAvailable) as cm:
            self.cursor1.execute("""
              "SELECT * FROM test;"
            """)

    def stop_commit_failure_policy_test(self):
        """ Test the stop_commit commitlog failure policy """
        self.prepare(configuration={
            'commit_failure_policy': 'stop_commit'
        })

        self.cursor1.execute("""
            INSERT INTO test (key, col1) VALUES (2, 2);
        """)

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log("Failed .+ commit log segments. Commit disk failure policy is stop_commit; terminating thread")
        debug(failure)
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertTrue(self.node1.is_running(), "Node1 should still be running")

        # Cannot write anymore after the failure
        with self.assertRaises(OperationTimedOut) as cm:
            self.cursor1.execute("""
              INSERT INTO test (key, col1) VALUES (2, 2);
            """)

        # Should be able to read
        assert_one(
            self.cursor1,
            "SELECT * FROM test where key=2;",
            [2, 2]
        )

    @since('2.1')
    def die_failure_policy_test(self):
        """ Test the die commitlog failure policy """
        self.prepare(configuration={
            'commit_failure_policy': 'die'
        })

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log("ERROR \[COMMIT-LOG-ALLOCATOR\].+JVM state determined to be unstable.  Exiting forcefully")
        debug(failure)
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertFalse(self.node1.is_running(), "Node1 should not be running")

    def ignore_failure_policy_test(self):
        """ Test the ignore commitlog failure policy """
        self.prepare(configuration={
            'commit_failure_policy': 'ignore'
        })

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log("ERROR \[COMMIT-LOG-ALLOCATOR\].+Failed .+ commit log segments")
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertTrue(self.node1.is_running(), "Node1 should still be running")

        with self.assertRaises(OperationTimedOut) as cm:
            self.cursor1.execute("""
              INSERT INTO test (key, col1) VALUES (2, 2);
            """)
        # Should not exists
        assert_none(self.cursor1, "SELECT * FROM test where key=2;")

        # bring back the node commitlogs
        self._change_commitlog_perms(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)

        self.cursor1.execute("""
          INSERT INTO test (key, col1) VALUES (3, 3);
        """)
        assert_one(
            self.cursor1,
            "SELECT * FROM test where key=3;",
            [3, 3]
        )

        time.sleep(2)
        assert_one(
            self.cursor1,
            "SELECT * FROM test where key=2;",
            [2, 2]
        )
