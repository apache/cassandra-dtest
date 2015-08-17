import glob
import os
import stat
import subprocess
import time

from cassandra import WriteTimeout
from cassandra.cluster import NoHostAvailable, OperationTimedOut

import ccmlib
from ccmlib.common import is_win
from assertions import assert_almost_equal, assert_none, assert_one
from dtest import Tester, debug
from tools import since, rows_to_list


class TestCommitLog(Tester):
    """ CommitLog Tests """

    def __init__(self, *argv, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
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
        self.session1 = self.patient_cql_connection(self.node1)
        if create_test_keyspace:
            self.session1.execute("DROP KEYSPACE IF EXISTS ks;")
            self.create_ks(self.session1, 'ks', 1)
            self.session1.execute("DROP TABLE IF EXISTS test;")
            query = """
              CREATE TABLE test (
                key int primary key,
                col1 int
              )
            """
            self.session1.execute(query)

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
        cmd_args = ['du', '-m', path]
        p = subprocess.Popen(cmd_args, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.returncode
        self.assertEqual(0, exit_status,
                         "du exited with a non-zero status: %d" % exit_status)
        size = int(stdout.split('\t')[0])
        return size

    def _commitlog_test(self, segment_size_in_mb, commitlog_size,
                        num_commitlog_files, compressed=False,
                        files_error=0):
        """ Execute a basic commitlog test and validate the commitlog files """

        if compressed:
            segment_size_in_mb *= 0.7
        segment_size = segment_size_in_mb * 1024 * 1024
        self.node1.stress(['write', 'n=150000', '-rate', 'threads=25'])
        time.sleep(1)

        commitlogs = self._get_commitlog_files()
        assert_almost_equal(len(commitlogs), num_commitlog_files,
                            error=files_error)

        if not ccmlib.common.is_win():
            tolerated_error = 0.25 if compressed else 0.15
            assert_almost_equal(sum([int(os.path.getsize(f)/1024/1024) for f in commitlogs]),
                                commitlog_size,
                                error=tolerated_error)

        # the most recently-written segment of the commitlog may be smaller
        # than the expected size, so we allow exactly one segment to be smaller
        smaller_found = False
        for i, f in enumerate(commitlogs):
            size = os.path.getsize(f)
            size_in_mb = int(size/1024/1024)
            debug('segment file {} {}; smaller already found: {}'.format(f, size_in_mb, smaller_found))
            if size_in_mb < 1 or size < (segment_size*0.1):
                continue  # commitlog not yet used

            tolerated_error = 0.15 if compressed else 0.05

            try:
                # in general, the size will be close to what we expect
                assert_almost_equal(size, segment_size, error=tolerated_error)
            except AssertionError as e:
                # but segments may be smaller with compression enabled,
                # or the last segment may be smaller
                if (not smaller_found) or compressed:
                    self.assertLessEqual(size, segment_size)
                    smaller_found = True
                else:
                    raise e

    def _provoke_commitlog_failure(self):
        """ Provoke the commitlog failure """

        # Test things are ok at this point
        self.session1.execute("""
            INSERT INTO test (key, col1) VALUES (1, 1);
        """)
        assert_one(
            self.session1,
            "SELECT * FROM test where key=1;",
            [1, 1]
        )

        self._change_commitlog_perms(0)

        if self.cluster.version() < "2.1":
            with open(os.devnull, 'w') as devnull:
                self.node1.stress(['--num-keys=1000000', '-S', '1000'],
                                  stdout=devnull, stderr=subprocess.STDOUT)
        else:
            with open(os.devnull, 'w') as devnull:
                self.node1.stress(['write', 'n=1M', '-col', 'size=FIXED(1000)', '-rate', 'threads=25'],
                                  stdout=devnull, stderr=subprocess.STDOUT)

    def test_commitlog_replay_on_startup(self):
        """ Test commit log replay """
        node1 = self.node1
        node1.set_configuration_options(batch_commitlog=True)
        node1.start()

        debug("Insert data")
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'Test', 1)
        session.execute("""
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """)
        session.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) "
                        "VALUES('gandalf', 'p@$$', 'male', 'WA', 1955);")

        debug("Verify data is present")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT * FROM Test. users")
        self.assertItemsEqual(rows_to_list(res),
                              [[u'gandalf', 1955, u'male', u'p@$$', u'WA']])

        debug("Stop node abruptly")
        node1.stop(gently=False)

        debug("Verify commitlog was written before abrupt stop")
        commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
        commitlog_files = os.listdir(commitlog_dir)
        self.assertTrue(len(commitlog_files) > 0)

        debug("Verify no SSTables were flushed before abrupt stop")
        data_dir = os.path.join(node1.get_path(), 'data')
        cf_id = [s for s in os.listdir(os.path.join(data_dir, "test")) if s.startswith("users")][0]
        cf_data_dir = glob.glob("{data_dir}/test/{cf_id}".format(**locals()))[0]
        cf_data_dir_files = os.listdir(cf_data_dir)
        if "backups" in cf_data_dir_files:
            cf_data_dir_files.remove("backups")
        self.assertEqual(0, len(cf_data_dir_files))

        debug("Verify commit log was replayed on startup")
        node1.start()
        node1.watch_log_for("Log replay complete", timeout=5)
        # Here we verify there was more than 0 replayed mutations
        zero_replays = node1.grep_log("0 replayed mutations")
        self.assertEqual(0, len(zero_replays))

        debug("Make query and ensure data is present")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT * FROM Test. users")
        self.assertItemsEqual(rows_to_list(res),
                              [[u'gandalf', 1955, u'male', u'p@$$', u'WA']])

    @since('2.1')
    def default_segment_size_test(self):
        """ Test default commitlog_segment_size_in_mb (32MB) """

        self.prepare(create_test_keyspace=False)
        self._commitlog_test(32, 64, 2, files_error=0.5)

    @since('2.1')
    def small_segment_size_test(self):
        """ Test a small commitlog_segment_size_in_mb (5MB) """
        segment_size_in_mb = 5
        self.prepare(configuration={
            'commitlog_segment_size_in_mb': segment_size_in_mb
        }, create_test_keyspace=False)
        self._commitlog_test(segment_size_in_mb, 62.5, 13, files_error=0.2)

    @since('2.2')
    def default_compressed_segment_size_test(self):
        """ Test default compressed commitlog_segment_size_in_mb (32MB) """

        self.prepare(configuration={
            'commitlog_compression': [{'class_name': 'LZ4Compressor'}]
        }, create_test_keyspace=False)
        self._commitlog_test(32, 42, 2, compressed=True, files_error=0.5)

    @since('2.2')
    def small_compressed_segment_size_test(self):
        """ Test a small compressed commitlog_segment_size_in_mb (5MB) """
        segment_size_in_mb = 5
        self.prepare(configuration={
            'commitlog_segment_size_in_mb': segment_size_in_mb,
            'commitlog_compression': [{'class_name': 'LZ4Compressor'}]
        }, create_test_keyspace=False)
        (expected_commitlog_files,
         expected_commitlog_size) = ((12, 33)
                                     if self.cluster.version() >= '3.0'
                                     else (10, 42))
        self._commitlog_test(segment_size_in_mb, expected_commitlog_size, expected_commitlog_files, compressed=True, files_error=0.12)

    def stop_failure_policy_test(self):
        """ Test the stop commitlog failure policy (default one) """
        self.prepare()

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log("Failed .+ commit log segments. Commit disk failure policy is stop; terminating thread")
        debug(failure)
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertTrue(self.node1.is_running(), "Node1 should still be running")

        # Cannot write anymore after the failure
        with self.assertRaises(NoHostAvailable):
            self.session1.execute("""
              INSERT INTO test (key, col1) VALUES (2, 2);
            """)

        # Should not be able to read neither
        with self.assertRaises(NoHostAvailable):
            self.session1.execute("""
              "SELECT * FROM test;"
            """)

    def stop_commit_failure_policy_test(self):
        """ Test the stop_commit commitlog failure policy """
        self.prepare(configuration={
            'commit_failure_policy': 'stop_commit'
        })

        self.session1.execute("""
            INSERT INTO test (key, col1) VALUES (2, 2);
        """)

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log("Failed .+ commit log segments. Commit disk failure policy is stop_commit; terminating thread")
        debug(failure)
        self.assertTrue(failure, "Cannot find the commitlog failure message in logs")
        self.assertTrue(self.node1.is_running(), "Node1 should still be running")

        # Cannot write anymore after the failure
        with self.assertRaises((OperationTimedOut, WriteTimeout)):
            self.session1.execute("""
              INSERT INTO test (key, col1) VALUES (2, 2);
            """)

        # Should be able to read
        assert_one(
            self.session1,
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

        # on Windows, we can't delete the segments if they're chmod to 0 so they'll still be available for use by CLSM,
        # and we can still create new segments since os.chmod is limited to stat.S_IWRITE and stat.S_IREAD to set files
        # as read-only. New mutations will still be allocated and WriteTimeouts will not be raised. It's sufficient that
        # we confirm that a) the node isn't dead (stop) and b) the node doesn't terminate the thread (stop_commit)
        query = "INSERT INTO test (key, col1) VALUES (2, 2);"
        if is_win():
            # We expect this to succeed
            self.session1.execute(query)
            self.assertFalse(self.node1.grep_log("terminating thread"), "thread was terminated but CL error should have been ignored.")
            self.assertTrue(self.node1.is_running(), "Node1 should still be running after an ignore error on CL")
        else:
            with self.assertRaises((OperationTimedOut, WriteTimeout)):
                self.session1.execute(query)

            # Should not exist
            assert_none(self.session1, "SELECT * FROM test where key=2;")

        # bring back the node commitlogs
        self._change_commitlog_perms(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)

        self.session1.execute("""
          INSERT INTO test (key, col1) VALUES (3, 3);
        """)
        assert_one(
            self.session1,
            "SELECT * FROM test where key=3;",
            [3, 3]
        )

        time.sleep(2)
        assert_one(
            self.session1,
            "SELECT * FROM test where key=2;",
            [2, 2]
        )
