import binascii
import glob
import os
import stat
import struct
import time
from distutils.version import LooseVersion
import pytest
import logging

from cassandra import WriteTimeout
from cassandra.cluster import NoHostAvailable, OperationTimedOut
from ccmlib.common import is_win
from ccmlib.node import Node, TimeoutError, NodeError
from parse import parse

from dtest import Tester, create_ks
from tools.assertions import (assert_almost_equal, assert_none, assert_one, assert_lists_equal_ignoring_order)
from tools.data import rows_to_list

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestCommitLog(Tester):
    """
    CommitLog Tests
    """
    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.allow_log_errors = True

    @pytest.fixture(scope='function', autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        if fixture_dtest_setup.dtest_config.cassandra_version_from_build >= '3.0':
            fixture_dtest_setup.cluster.set_configuration_options({'enable_materialized_views': 'true'})
        fixture_dtest_setup.cluster.populate(1)
        [self.node1] = fixture_dtest_setup.cluster.nodelist()

        yield

        # Some of the tests change commitlog permissions to provoke failure
        # so this changes them back so we can delete them.
        self._change_commitlog_perms(stat.S_IWRITE | stat.S_IREAD | stat.S_IEXEC)

    def prepare(self, configuration=None, create_test_keyspace=True, **kwargs):
        if configuration is None:
            configuration = {}
        default_conf = {'commitlog_sync_period_in_ms': 1000}

        set_conf = dict(default_conf, **configuration)
        logger.debug('setting commitlog configuration with the following values: {set_conf}'
                     ' and the following kwargs: {kwargs}'.format(set_conf=set_conf, kwargs=kwargs))
        self.cluster.set_configuration_options(values=set_conf, **kwargs)
        self.cluster.start()
        self.session1 = self.patient_cql_connection(self.node1)
        if create_test_keyspace:
            self.session1.execute("DROP KEYSPACE IF EXISTS ks;")
            create_ks(self.session1, 'ks', 1)
            self.session1.execute("DROP TABLE IF EXISTS test;")
            query = """
              CREATE TABLE test (
                key int primary key,
                col1 int
              )
            """
            self.session1.execute(query)

    def _change_commitlog_perms(self, mod):
        for path in self._get_commitlog_paths():
            logger.debug('changing permissions to {perms} on {path}'.format(perms=oct(mod), path=path))
            os.chmod(path, mod)
            commitlogs = glob.glob(path + '/*')

            if commitlogs:
                logger.debug('changing permissions to {perms} on the following files:'
                             '\n  {files}'.format(perms=oct(mod), files='\n  '.join(commitlogs)))
            else:
                logger.debug(self._change_commitlog_perms.__name__ + ' called on empty commitlog directory '
                                                                     '{path} with permissions {perms}'.format(path=path, perms=oct(mod)))

            for commitlog in commitlogs:
                os.chmod(commitlog, mod)

    def _get_commitlog_paths(self):
        """
        Returns the list of commitlog and cdc paths
        """
        # TODO: this does not account for non-default commitlog/cdc paths
        # specified in cassandra.yaml
        return [d for d in [os.path.join(self.node1.get_path(), 'commitlogs'),
                            os.path.join(self.node1.get_path(), 'cdc')]
                if os.path.isdir(d)]

    def _get_commitlog_files(self):
        """
        Returns the paths to commitlog files
        """
        return [os.path.join(path, filename)
                for path in self._get_commitlog_paths()
                for filename in os.listdir(path)]

    def _segment_size_test(self, segment_size_in_mb, compressed=False):
        """
        Execute a basic commitlog test and validate the commitlog files
        """
        conf = {'commitlog_segment_size_in_mb': segment_size_in_mb}
        if compressed:
            conf['commitlog_compression'] = [{'class_name': 'LZ4Compressor'}]
        conf['memtable_heap_space_in_mb'] = 512
        self.prepare(configuration=conf, create_test_keyspace=False)

        segment_size = segment_size_in_mb * 1024 * 1024
        self.node1.stress(['write', 'n=150k', 'no-warmup', '-rate', 'threads=25'])
        time.sleep(1)

        commitlogs = self._get_commitlog_files()
        assert len(commitlogs) > 0, 'No commit log files were created'

        # the most recently-written segment of the commitlog may be smaller
        # than the expected size, so we allow exactly one segment to be smaller
        smaller_found = False
        for i, f in enumerate(commitlogs):
            size = os.path.getsize(f)
            size_in_mb = int(size / 1024 / 1024)
            logger.debug('segment file {} {}; smaller already found: {}'.format(f, size_in_mb, smaller_found))
            if size_in_mb < 1 or size < (segment_size * 0.1):
                logger.debug('segment file not yet used; moving to next file')
                continue  # commitlog not yet used

            try:
                if compressed:
                    # if compression is used, we assume there will be at most a 50% compression ratio
                    assert size < segment_size
                    assert size > segment_size / 2
                else:
                    # if no compression is used, the size will be close to what we expect
                    assert_almost_equal(size, segment_size, error=0.05)
            except AssertionError as e:
                # the last segment may be smaller
                if not smaller_found:
                    assert size <= segment_size
                    smaller_found = True
                else:
                    raise e

    def _provoke_commitlog_failure(self):
        """
        Provoke the commitlog failure
        """
        logger.debug('Provoking commitlog failure')
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

        # Use stress_process to skip internal error handling in ccm. Grep node logs for specific errors in test method.
        self.node1.stress_process(['write', 'n=1M', 'no-warmup', '-col', 'size=FIXED(1000)', '-rate', 'threads=25']).communicate()

    @since('3.0.7')
    def test_mv_lock_contention_during_replay(self):
        """
        Ensure that we don't generate WriteTimeoutExceptions during commitlog
        replay due to MV lock contention.  Fixed in 3.0.7 and 3.7.
        @jira_ticket CASSANDRA-11891
        """
        cluster_ver = self.cluster.version()
        if LooseVersion('3.1') <= cluster_ver < LooseVersion('3.7'):
            pytest.skip("Fixed in 3.0.7 and 3.7")

        node1 = self.node1
        node1.set_batch_commitlog(enabled=True)
        node1.start()
        session = self.patient_cql_connection(node1)

        logger.debug("Creating schema")
        create_ks(session, 'Test', 1)
        session.execute("""
            CREATE TABLE mytable (
                a int,
                b int,
                c int,
                PRIMARY KEY (a, b)
            );
        """)

        session.execute("""
            CREATE MATERIALIZED VIEW myview
            AS SELECT * FROM mytable
            WHERE a IS NOT NULL AND b IS NOT NULL
            PRIMARY KEY (a, b);
        """)

        logger.debug("Insert data")
        num_rows = 1024  # maximum number of mutations replayed at once by the commit log
        for i in range(num_rows):
            session.execute("INSERT INTO Test.mytable (a, b, c) VALUES (0, {i}, {i})".format(i=i))

        node1.stop(gently=False)
        node1.mark_log_for_errors()

        logger.debug("Verify commitlog was written before abrupt stop")
        commitlog_files = os.listdir(os.path.join(node1.get_path(), 'commitlogs'))
        assert [] != commitlog_files

        # set a short timeout to ensure lock contention will generally exceed this
        node1.set_configuration_options({'write_request_timeout_in_ms': 30})
        logger.debug("Starting node again")
        node1.start()

        logger.debug("Verify commit log was replayed on startup")
        start_time, replay_complete = time.time(), False
        while not replay_complete:
            matches = node1.grep_log(r".*WriteTimeoutException.*")
            assert [] == matches

            replay_complete = node1.grep_log("Log replay complete")
            assert time.time() - start_time < 120, "Did not finish commitlog replay within 120 seconds"

        logger.debug("Reconnecting to node")
        session = self.patient_cql_connection(node1)
        logger.debug("Make query to ensure data is present")
        res = list(session.execute("SELECT * FROM Test.mytable"))
        assert num_rows == len(res), res

    def test_commitlog_replay_on_startup(self):
        """
        Test commit log replay
        """
        node1 = self.node1
        node1.set_batch_commitlog(enabled=True)
        node1.start()

        logger.debug("Insert data")
        session = self.patient_cql_connection(node1)
        create_ks(session, 'Test', 1)
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

        logger.debug("Verify data is present")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT * FROM Test. users")
        assert rows_to_list(res) == [['gandalf', 1955, 'male', 'p@$$', 'WA']]

        logger.debug("Stop node abruptly")
        node1.stop(gently=False)

        logger.debug("Verify commitlog was written before abrupt stop")
        commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
        commitlog_files = os.listdir(commitlog_dir)
        assert len(commitlog_files) > 0

        logger.debug("Verify no SSTables were flushed before abrupt stop")
        assert 0 == len(node1.get_sstables('test', 'users'))

        logger.debug("Verify commit log was replayed on startup")
        node1.start()
        node1.watch_log_for("Log replay complete")
        # Here we verify from the logs that some mutations were replayed
        replays = [match_tuple[0] for match_tuple in node1.grep_log(r" \d+ replayed mutations")]
        logger.debug('The following log lines indicate that mutations were replayed: {msgs}'.format(msgs=replays))
        num_replayed_mutations = [
            parse('{} {num_mutations:d} replayed mutations{}', line).named['num_mutations']
            for line in replays
        ]
        # assert there were some lines where more than zero mutations were replayed
        assert [m for m in num_replayed_mutations if m > 0] != []

        logger.debug("Make query and ensure data is present")
        session = self.patient_cql_connection(node1)
        res = session.execute("SELECT * FROM Test. users")
        assert_lists_equal_ignoring_order(rows_to_list(res), [['gandalf', 1955, 'male', 'p@$$', 'WA']])

    def test_default_segment_size(self):
        """
        Test default commitlog_segment_size_in_mb (32MB)
        """
        self._segment_size_test(32)

    def test_small_segment_size(self):
        """
        Test a small commitlog_segment_size_in_mb (5MB)
        """
        self._segment_size_test(5)

    @since('2.2')
    def test_default_compressed_segment_size(self):
        """
        Test default compressed commitlog_segment_size_in_mb (32MB)
        """
        self._segment_size_test(32, compressed=True)

    @since('2.2')
    def test_small_compressed_segment_size(self):
        """
        Test a small compressed commitlog_segment_size_in_mb (5MB)
        """
        self._segment_size_test(5, compressed=True)

    def test_stop_failure_policy(self):
        """
        Test the stop commitlog failure policy (default one)
        """
        self.prepare()

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log("Failed .+ commit log segments. Commit disk failure policy is stop; terminating thread")
        logger.debug(failure)
        assert failure, "Cannot find the commitlog failure message in logs"
        assert self.node1.is_running(), "Node1 should still be running"

        # Cannot write anymore after the failure
        with pytest.raises(NoHostAvailable):
            self.session1.execute("""
              INSERT INTO test (key, col1) VALUES (2, 2);
            """)

        # Should not be able to read neither
        with pytest.raises(NoHostAvailable):
            self.session1.execute("""
              "SELECT * FROM test;"
            """)

    def test_stop_commit_failure_policy(self):
        """
        Test the stop_commit commitlog failure policy
        """
        self.prepare(configuration={
            'commit_failure_policy': 'stop_commit'
        })

        self.session1.execute("""
            INSERT INTO test (key, col1) VALUES (2, 2);
        """)

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log("Failed .+ commit log segments. Commit disk failure policy is stop_commit; terminating thread")
        logger.debug(failure)
        assert failure, "Cannot find the commitlog failure message in logs"
        assert self.node1.is_running(), "Node1 should still be running"

        # Cannot write anymore after the failure
        logger.debug('attempting to insert to node with failing commitlog; should fail')
        with pytest.raises((OperationTimedOut, WriteTimeout)):
            self.session1.execute("""
              INSERT INTO test (key, col1) VALUES (2, 2);
            """)

        # Should be able to read
        logger.debug('attempting to read from node with failing commitlog; should succeed')
        assert_one(
            self.session1,
            "SELECT * FROM test where key=2;",
            [2, 2]
        )

    def test_die_failure_policy(self):
        """
        Test the die commitlog failure policy
        """
        self.prepare(configuration={
            'commit_failure_policy': 'die'
        })

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log(r"ERROR \[COMMIT-LOG-ALLOCATOR\].+JVM state determined to be unstable.  Exiting forcefully")
        logger.debug(failure)
        assert failure, "Cannot find the commitlog failure message in logs"
        assert not self.node1.is_running(), "Node1 should not be running"

    def test_ignore_failure_policy(self):
        """
        Test the ignore commitlog failure policy
        """
        self.prepare(configuration={
            'commit_failure_policy': 'ignore'
        })

        self._provoke_commitlog_failure()
        failure = self.node1.grep_log(r"ERROR \[COMMIT-LOG-ALLOCATOR\].+Failed .+ commit log segments")
        assert failure, "Cannot find the commitlog failure message in logs"
        assert self.node1.is_running(), "Node1 should still be running"

        # on Windows, we can't delete the segments if they're chmod to 0 so they'll still be available for use by CLSM,
        # and we can still create new segments since os.chmod is limited to stat.S_IWRITE and stat.S_IREAD to set files
        # as read-only. New mutations will still be allocated and WriteTimeouts will not be raised. It's sufficient that
        # we confirm that a) the node isn't dead (stop) and b) the node doesn't terminate the thread (stop_commit)
        query = "INSERT INTO test (key, col1) VALUES (2, 2);"
        if is_win():
            # We expect this to succeed
            self.session1.execute(query)
            assert not self.node1.grep_log("terminating thread"), "thread was terminated but CL error should have been ignored."
            assert self.node1.is_running(), "Node1 should still be running after an ignore error on CL"
        else:
            with pytest.raises((OperationTimedOut, WriteTimeout)):
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

    @since('2.2')
    def test_bad_crc(self):
        """
        if the commit log header crc (checksum) doesn't match the actual crc of the header data,
        and the commit_failure_policy is stop, C* shouldn't startup
        @jira_ticket CASSANDRA-9749
        """
        expected_error = "Exiting due to error while processing commit log during initialization."
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            expected_error]
        node = self.node1
        assert isinstance(node, Node)
        node.set_configuration_options({'commit_failure_policy': 'stop', 'commitlog_sync_period_in_ms': 1000})
        self.cluster.start()

        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(cursor, 'ks', 1)
        cursor.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for i in range(10):
            cursor.execute("INSERT INTO ks.tbl (k, v) VALUES ({0}, {0})".format(i))

        results = list(cursor.execute("SELECT * FROM ks.tbl"))
        assert len(results) == 10

        # with the commitlog_sync_period_in_ms set to 1000,
        # this sleep guarantees that the commitlog data is
        # actually flushed to disk before we kill -9 it
        time.sleep(1)

        node.stop(gently=False)

        # check that ks.tbl hasn't been flushed
        path = node.get_path()
        for data_dir in node.data_directories():
            ks_dir = os.path.join(data_dir, 'ks')
            db_dir = os.listdir(ks_dir)[0]
            sstables = len([f for f in os.listdir(os.path.join(ks_dir, db_dir)) if f.endswith('.db')])
            assert sstables == 0

        # modify the commit log crc values
        cl_dir = os.path.join(path, 'commitlogs')
        assert len(os.listdir(cl_dir)) > 0
        for cl in os.listdir(cl_dir):
            # locate the CRC location
            with open(os.path.join(cl_dir, cl), 'rb') as f:
                f.seek(0)
                version = struct.unpack('>i', f.read(4))[0]
                crc_pos = 12
                if version >= 5:
                    f.seek(crc_pos)
                    psize = struct.unpack('>h', f.read(2))[0] & 0xFFFF
                    crc_pos += 2 + psize

            # rewrite it with crap
            with open(os.path.join(cl_dir, cl), 'wb') as f:
                f.seek(crc_pos)
                f.write(struct.pack('>i', 123456))

            # verify said crap
            with open(os.path.join(cl_dir, cl), 'rb') as f:
                f.seek(crc_pos)
                crc = struct.unpack('>i', f.read(4))[0]
                assert crc == 123456

        mark = node.mark_log()
        node.start()
        node.watch_log_for(expected_error, from_mark=mark)
        with pytest.raises((TimeoutError, NodeError)):
            node.wait_for_binary_interface(from_mark=mark, timeout=20)
        assert not node.is_running()

    @since('2.2')
    def test_compression_error(self):
        """
        @jira_ticket CASSANDRA-7886
        if the commit log header refers to an unknown compression class, and
        the commit_failure_policy is stop, C* shouldn't start up
        """
        expected_error = 'Could not create Compression for type org.apache.cassandra.io.compress.LZ5Compressor'
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            expected_error]
        node = self.node1
        assert isinstance(node, Node)
        node.set_configuration_options({'commit_failure_policy': 'stop',
                                        'commitlog_compression': [{'class_name': 'LZ4Compressor'}],
                                        'commitlog_sync_period_in_ms': 1000})
        self.cluster.start()

        cursor = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(cursor, 'ks1', 1)
        cursor.execute("CREATE TABLE ks1.tbl (k INT PRIMARY KEY, v INT)")

        for i in range(10):
            cursor.execute("INSERT INTO ks1.tbl (k, v) VALUES ({0}, {0})".format(i))

        results = list(cursor.execute("SELECT * FROM ks1.tbl"))
        assert len(results) == 10

        # with the commitlog_sync_period_in_ms set to 1000,
        # this sleep guarantees that the commitlog data is
        # actually flushed to disk before we kill -9 it
        time.sleep(1)

        node.stop(gently=False)

        # check that ks1.tbl hasn't been flushed
        sstables = 0
        for data_dir in node.data_directories():
            ks_dir = os.path.join(data_dir, 'ks1')
            db_dir = os.listdir(ks_dir)[0]
            sstables = sstables + len([f for f in os.listdir(os.path.join(ks_dir, db_dir)) if f.endswith('.db')])
        assert sstables == 0

        def get_header_crc(header):
            """
            When calculating the header crc, C* splits up the 8b id, first adding the 4 least significant
            bytes to the crc, then the 5 most significant bytes, so this splits them and calculates the same way
            """
            new_header = bytearray(header[:4])
            # C* evaluates most and least significant 4 bytes out of order
            new_header.extend(header[8:12])
            new_header.extend(header[4:8])
            # C* evaluates the short parameter length as an int
            new_header.extend(b'\x00\x00')
            new_header.extend(header[12:14])  # the
            new_header.extend(header[14:])

            # https://docs.python.org/2/library/binascii.html
            # "Changed in version 2.6: The return value is in the range [-2**31, 2**31-1] regardless
            # of platform. In the past the value would be signed on some platforms and unsigned on
            # others. Use & 0xffffffff on the value if you want it to match Python 3 behavior."
            return binascii.crc32(new_header) & 0xffffffff

        # modify the compression parameters to look for a compressor that isn't there
        # while this scenario is pretty unlikely, if a jar or lib got moved or something,
        # you'd have a similar situation, which would be fixable by the user
        path = node.get_path()
        cl_dir = os.path.join(path, 'commitlogs')
        assert len(os.listdir(cl_dir)) > 0
        for cl in os.listdir(cl_dir):
            # read the header and find the crc location
            with open(os.path.join(cl_dir, cl), 'rb') as f:
                f.seek(0)
                crc_pos = 12
                f.seek(crc_pos)
                psize = struct.unpack('>h', f.read(2))[0] & 0xFFFF
                crc_pos += 2 + psize

                header_length = crc_pos
                f.seek(crc_pos)
                crc = struct.unpack('>i', f.read(4))[0]

                # check that we're going this right
                f.seek(0)
                header_bytes = f.read(header_length)

                # https://docs.python.org/2/library/binascii.html
                # "Changed in version 2.6: The return value is in the range [-2**31, 2**31-1] regardless
                # of platform. In the past the value would be signed on some platforms and unsigned on
                # others. Use & 0xffffffff on the value if you want it to match Python 3 behavior."
                assert get_header_crc(header_bytes) == (crc & 0xffffffff)

            # rewrite it with imaginary compressor
            assert 'LZ4Compressor'.encode("ascii") in header_bytes
            header_bytes = header_bytes.replace('LZ4Compressor'.encode("ascii"), 'LZ5Compressor'.encode("ascii"))
            assert 'LZ4Compressor'.encode("ascii") not in header_bytes
            assert 'LZ5Compressor'.encode("ascii") in header_bytes
            with open(os.path.join(cl_dir, cl), 'wb') as f:
                f.seek(0)
                f.write(header_bytes)
                f.seek(crc_pos)
                f.write(struct.pack('>I', get_header_crc(header_bytes)))

            # verify we wrote everything correctly
            with open(os.path.join(cl_dir, cl), 'rb') as f:
                f.seek(0)
                assert f.read(header_length) == header_bytes
                f.seek(crc_pos)
                crc = struct.unpack('>i', f.read(4))[0]

                # https://docs.python.org/2/library/binascii.html
                # "Changed in version 2.6: The return value is in the range [-2**31, 2**31-1] regardless
                # of platform. In the past the value would be signed on some platforms and unsigned on
                # others. Use & 0xffffffff on the value if you want it to match Python 3 behavior."
                assert (crc & 0xffffffff) == get_header_crc(header_bytes)

        mark = node.mark_log()
        node.start()
        node.watch_log_for(expected_error, from_mark=mark)
        with pytest.raises((TimeoutError, NodeError)):
            node.wait_for_binary_interface(from_mark=mark, timeout=20)
