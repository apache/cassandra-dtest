import distutils.dir_util
import glob
import os
import shutil
import subprocess
import time

from cassandra.concurrent import execute_concurrent_with_args

from dtest import Tester, debug
from tools import known_failure, replace_in_file, safe_mkdtemp


class SnapshotTester(Tester):

    def create_schema(self, session):
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')

    def insert_rows(self, session, start, end):
        insert_statement = session.prepare("INSERT INTO ks.cf (key, val) VALUES (?, 'asdf')")
        args = [(r,) for r in range(start, end)]
        execute_concurrent_with_args(session, insert_statement, args, concurrency=20)

    def make_snapshot(self, node, ks, cf, name):
        debug("Making snapshot....")
        node.flush()
        snapshot_cmd = 'snapshot {ks} -cf {cf} -t {name}'.format(ks=ks, cf=cf, name=name)
        debug("Running snapshot cmd: {snapshot_cmd}".format(snapshot_cmd=snapshot_cmd))
        node.nodetool(snapshot_cmd)
        tmpdir = safe_mkdtemp()
        os.mkdir(os.path.join(tmpdir, ks))
        os.mkdir(os.path.join(tmpdir, ks, cf))

        # Find the snapshot dir, it's different in various C*
        x = 0
        for data_dir in node.data_directories():
            snapshot_dir = "{data_dir}/{ks}/{cf}/snapshots/{name}".format(data_dir=data_dir, ks=ks, cf=cf, name=name)
            if not os.path.isdir(snapshot_dir):
                snapshot_dirs = glob.glob("{data_dir}/{ks}/{cf}-*/snapshots/{name}".format(data_dir=data_dir, ks=ks, cf=cf, name=name))
                if len(snapshot_dirs) > 0:
                    snapshot_dir = snapshot_dirs[0]
                else:
                    continue
            debug("snapshot_dir is : " + snapshot_dir)
            debug("snapshot copy is : " + tmpdir)

            # Copy files from the snapshot dir to existing temp dir
            distutils.dir_util.copy_tree(str(snapshot_dir), os.path.join(tmpdir, str(x), ks, cf))
            x += 1

        return tmpdir

    def restore_snapshot(self, snapshot_dir, node, ks, cf):
        debug("Restoring snapshot....")
        for x in xrange(0, self.cluster.data_dir_count):
            snap_dir = os.path.join(snapshot_dir, str(x), ks, cf)
            if os.path.exists(snap_dir):
                ip = node.address()

                args = [node.get_tool('sstableloader'), '-d', ip, snap_dir]
                p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = p.communicate()
                exit_status = p.wait()

                if exit_status != 0:
                    raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                                    (" ".join(args), exit_status, stdout, stderr))


class TestSnapshot(SnapshotTester):

    def test_basic_snapshot_and_restore(self):
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_schema(session)

        self.insert_rows(session, 0, 100)
        snapshot_dir = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200)
        rows = session.execute('SELECT count(*) from ks.cf')
        self.assertEqual(rows[0][0], 200)

        # Drop the keyspace, make sure we have no data:
        session.execute('DROP KEYSPACE ks')
        self.create_schema(session)
        rows = session.execute('SELECT count(*) from ks.cf')
        self.assertEqual(rows[0][0], 0)

        # Restore data from snapshot:
        self.restore_snapshot(snapshot_dir, node1, 'ks', 'cf')
        node1.nodetool('refresh ks cf')
        rows = session.execute('SELECT count(*) from ks.cf')

        # clean up
        debug("removing snapshot_dir: " + snapshot_dir)
        shutil.rmtree(snapshot_dir)

        self.assertEqual(rows[0][0], 100)


class TestArchiveCommitlog(SnapshotTester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'commitlog_segment_size_in_mb': 1}
        SnapshotTester.__init__(self, *args, **kwargs)

    def make_snapshot(self, node, ks, cf, name):
        debug("Making snapshot....")
        node.flush()
        snapshot_cmd = 'snapshot {ks} -cf {cf} -t {name}'.format(ks=ks, cf=cf, name=name)
        debug("Running snapshot cmd: {snapshot_cmd}".format(snapshot_cmd=snapshot_cmd))
        node.nodetool(snapshot_cmd)
        tmpdirs = []
        base_tmpdir = safe_mkdtemp()
        for x in xrange(0, self.cluster.data_dir_count):
            tmpdir = os.path.join(base_tmpdir, str(x))
            os.mkdir(tmpdir)
            # Copy files from the snapshot dir to existing temp dir
            distutils.dir_util.copy_tree(os.path.join(node.get_path(), 'data{0}'.format(x), ks), tmpdir)
            tmpdirs.append(tmpdir)

        return tmpdirs

    def restore_snapshot(self, snapshot_dir, node, ks, cf, name):
        debug("Restoring snapshot for cf ....")
        data_dir = os.path.join(node.get_path(), 'data{0}'.format(os.path.basename(snapshot_dir)))
        cfs = [s for s in os.listdir(snapshot_dir) if s.startswith(cf + "-")]
        if len(cfs) > 0:
            cf_id = cfs[0]
            glob_path = "{snapshot_dir}/{cf_id}/snapshots/{name}".format(snapshot_dir=snapshot_dir, cf_id=cf_id, name=name)
            globbed = glob.glob(glob_path)
            if len(globbed) > 0:
                snapshot_dir = globbed[0]
                if not os.path.exists(os.path.join(data_dir, ks)):
                    os.mkdir(os.path.join(data_dir, ks))
                os.mkdir(os.path.join(data_dir, ks, cf_id))

                debug("snapshot_dir is : " + snapshot_dir)
                distutils.dir_util.copy_tree(snapshot_dir, os.path.join(data_dir, ks, cf_id))

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11811',
                   flaky=False,
                   notes='Fails on windows.')
    def test_archive_commitlog(self):
        self.run_archive_commitlog(restore_point_in_time=False)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11811',
                   flaky=False,
                   notes='Fails on windows.')
    def test_archive_commitlog_with_active_commitlog(self):
        """
        Copy the active commitlogs to the archive directory before restoration
        """
        self.run_archive_commitlog(restore_point_in_time=False, archive_active_commitlogs=True)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12139',
                   flaky=True)
    def dont_test_archive_commitlog(self):
        """
        Run the archive commitlog test, but forget to add the restore commands
        """
        self.run_archive_commitlog(restore_point_in_time=False, restore_archived_commitlog=False)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11811',
                   flaky=False,
                   notes='Fails on windows.')
    def test_archive_commitlog_point_in_time(self):
        """
        Test archive commit log with restore_point_in_time setting
        """
        self.run_archive_commitlog(restore_point_in_time=True)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11811',
                   flaky=False,
                   notes='Fails on windows.')
    def test_archive_commitlog_point_in_time_with_active_commitlog(self):
        """
        Test archive commit log with restore_point_in_time setting
        """
        self.run_archive_commitlog(restore_point_in_time=True, archive_active_commitlogs=True)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11811',
                   flaky=False,
                   notes='Fails on windows.')
    def test_archive_commitlog_point_in_time_with_active_commitlog_ln(self):
        """
        Test archive commit log with restore_point_in_time setting
        """
        self.run_archive_commitlog(restore_point_in_time=True, archive_active_commitlogs=True, archive_command='ln')

    def run_archive_commitlog(self, restore_point_in_time=False, restore_archived_commitlog=True, archive_active_commitlogs=False, archive_command='cp'):
        """
        Run archive commit log restoration test
        """

        cluster = self.cluster
        cluster.populate(1)
        (node1,) = cluster.nodelist()

        # Create a temp directory for storing commitlog archives:
        tmp_commitlog = safe_mkdtemp()
        debug("tmp_commitlog: " + tmp_commitlog)

        # Edit commitlog_archiving.properties and set an archive
        # command:
        replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                        [(r'^archive_command=.*$', 'archive_command={archive_command} %path {tmp_commitlog}/%name'.format(
                            tmp_commitlog=tmp_commitlog, archive_command=archive_command))])

        cluster.start()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key bigint PRIMARY KEY, val text);')
        debug("Writing first 30,000 rows...")
        self.insert_rows(session, 0, 30000)
        # Record when this first set of inserts finished:
        insert_cutoff_times = [time.gmtime()]

        # Delete all commitlog backups so far:
        for f in glob.glob(tmp_commitlog + "/*"):
            os.remove(f)

        snapshot_dirs = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        if self.cluster.version() >= '3.0':
            system_ks_snapshot_dirs = self.make_snapshot(node1, 'system_schema', 'keyspaces', 'keyspaces')
        else:
            system_ks_snapshot_dirs = self.make_snapshot(node1, 'system', 'schema_keyspaces', 'keyspaces')

        if self.cluster.version() >= '3.0':
            system_col_snapshot_dirs = self.make_snapshot(node1, 'system_schema', 'columns', 'columns')
        else:
            system_col_snapshot_dirs = self.make_snapshot(node1, 'system', 'schema_columns', 'columns')

        if self.cluster.version() >= '3.0':
            system_ut_snapshot_dirs = self.make_snapshot(node1, 'system_schema', 'types', 'usertypes')
        else:
            system_ut_snapshot_dirs = self.make_snapshot(node1, 'system', 'schema_usertypes', 'usertypes')

        if self.cluster.version() >= '3.0':
            system_cfs_snapshot_dirs = self.make_snapshot(node1, 'system_schema', 'tables', 'cfs')
        else:
            system_cfs_snapshot_dirs = self.make_snapshot(node1, 'system', 'schema_columnfamilies', 'cfs')

        try:
            # Write more data:
            debug("Writing second 30,000 rows...")
            self.insert_rows(session, 30000, 60000)
            node1.flush()
            time.sleep(10)
            # Record when this second set of inserts finished:
            insert_cutoff_times.append(time.gmtime())

            debug("Writing final 5,000 rows...")
            self.insert_rows(session, 60000, 65000)
            # Record when the third set of inserts finished:
            insert_cutoff_times.append(time.gmtime())

            rows = session.execute('SELECT count(*) from ks.cf')
            # Make sure we have the same amount of rows as when we snapshotted:
            self.assertEqual(rows[0][0], 65000)

            # Check that there are at least one commit log backed up that
            # is not one of the active commit logs:
            commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
            debug("node1 commitlog dir: " + commitlog_dir)
            debug("node1 commitlog dir contents: " + str(os.listdir(commitlog_dir)))
            debug("tmp_commitlog contents: " + str(os.listdir(tmp_commitlog)))

            self.assertNotEqual(set(os.listdir(tmp_commitlog)) - set(os.listdir(commitlog_dir)),
                                set())

            cluster.flush()
            cluster.compact()
            node1.drain()
            if archive_active_commitlogs:
                # restart the node which causes the active commitlogs to be archived
                node1.stop()
                node1.start(wait_for_binary_proto=True)

            # Destroy the cluster
            cluster.stop()
            self.copy_logs(name=self.id().split(".")[0] + "_pre-restore")
            self._cleanup_cluster()
            cluster = self.cluster = self._get_cluster()
            cluster.populate(1)
            node1, = cluster.nodelist()

            # Restore schema from snapshots:
            for system_ks_snapshot_dir in system_ks_snapshot_dirs:
                if self.cluster.version() >= '3.0':
                    self.restore_snapshot(system_ks_snapshot_dir, node1, 'system_schema', 'keyspaces', 'keyspaces')
                else:
                    self.restore_snapshot(system_ks_snapshot_dir, node1, 'system', 'schema_keyspaces', 'keyspaces')
            for system_col_snapshot_dir in system_col_snapshot_dirs:
                if self.cluster.version() >= '3.0':
                    self.restore_snapshot(system_col_snapshot_dir, node1, 'system_schema', 'columns', 'columns')
                else:
                    self.restore_snapshot(system_col_snapshot_dir, node1, 'system', 'schema_columns', 'columns')
            for system_ut_snapshot_dir in system_ut_snapshot_dirs:
                if self.cluster.version() >= '3.0':
                    self.restore_snapshot(system_ut_snapshot_dir, node1, 'system_schema', 'types', 'usertypes')
                else:
                    self.restore_snapshot(system_ut_snapshot_dir, node1, 'system', 'schema_usertypes', 'usertypes')

            for system_cfs_snapshot_dir in system_cfs_snapshot_dirs:
                if self.cluster.version() >= '3.0':
                    self.restore_snapshot(system_cfs_snapshot_dir, node1, 'system_schema', 'tables', 'cfs')
                else:
                    self.restore_snapshot(system_cfs_snapshot_dir, node1, 'system', 'schema_columnfamilies', 'cfs')
            for snapshot_dir in snapshot_dirs:
                self.restore_snapshot(snapshot_dir, node1, 'ks', 'cf', 'basic')

            cluster.start(wait_for_binary_proto=True)

            session = self.patient_cql_connection(node1)
            node1.nodetool('refresh ks cf')

            rows = session.execute('SELECT count(*) from ks.cf')
            # Make sure we have the same amount of rows as when we snapshotted:
            self.assertEqual(rows[0][0], 30000)

            # Edit commitlog_archiving.properties. Remove the archive
            # command  and set a restore command and restore_directories:
            if restore_archived_commitlog:
                replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                                [(r'^archive_command=.*$', 'archive_command='),
                                 (r'^restore_command=.*$', 'restore_command=cp -f %from %to'),
                                 (r'^restore_directories=.*$', 'restore_directories={tmp_commitlog}'.format(
                                     tmp_commitlog=tmp_commitlog))])

                if restore_point_in_time:
                    restore_time = time.strftime("%Y:%m:%d %H:%M:%S", insert_cutoff_times[1])
                    replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                                    [(r'^restore_point_in_time=.*$', 'restore_point_in_time={restore_time}'.format(restore_time=restore_time))])

            debug("Restarting node1..")
            node1.stop()
            node1.start(wait_for_binary_proto=True)

            node1.nodetool('flush')
            node1.nodetool('compact')

            session = self.patient_cql_connection(node1)
            rows = session.execute('SELECT count(*) from ks.cf')
            # Now we should have 30000 rows from the snapshot + 30000 rows
            # from the commitlog backups:
            if not restore_archived_commitlog:
                self.assertEqual(rows[0][0], 30000)
            elif restore_point_in_time:
                self.assertEqual(rows[0][0], 60000)
            else:
                self.assertEqual(rows[0][0], 65000)

        finally:
            # clean up
            debug("removing snapshot_dir: " + ",".join(snapshot_dirs))
            for snapshot_dir in snapshot_dirs:
                shutil.rmtree(snapshot_dir)
            debug("removing snapshot_dir: " + ",".join(system_ks_snapshot_dirs))
            for system_ks_snapshot_dir in system_ks_snapshot_dirs:
                shutil.rmtree(system_ks_snapshot_dir)
            debug("removing snapshot_dir: " + ",".join(system_cfs_snapshot_dirs))
            for system_cfs_snapshot_dir in system_cfs_snapshot_dirs:
                shutil.rmtree(system_cfs_snapshot_dir)
            debug("removing snapshot_dir: " + ",".join(system_ut_snapshot_dirs))
            for system_ut_snapshot_dir in system_ut_snapshot_dirs:
                shutil.rmtree(system_ut_snapshot_dir)
            debug("removing snapshot_dir: " + ",".join(system_col_snapshot_dirs))
            for system_col_snapshot_dir in system_col_snapshot_dirs:
                shutil.rmtree(system_col_snapshot_dir)

            debug("removing tmp_commitlog: " + tmp_commitlog)
            shutil.rmtree(tmp_commitlog)

    def test_archive_and_restore_commitlog_repeatedly(self):
        """
        @jira_ticket CASSANDRA-10593
        Run archive commit log restoration test repeatedly to make sure it is idempotent
        and doesn't fail if done repeatedly
        """

        cluster = self.cluster
        cluster.populate(1)
        node1 = cluster.nodelist()[0]

        # Create a temp directory for storing commitlog archives:
        tmp_commitlog = safe_mkdtemp()
        debug("tmp_commitlog: {}".format(tmp_commitlog))

        # Edit commitlog_archiving.properties and set an archive
        # command:
        replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                        [(r'^archive_command=.*$', 'archive_command=ln %path {tmp_commitlog}/%name'.format(
                            tmp_commitlog=tmp_commitlog)),
                         (r'^restore_command=.*$', 'restore_command=cp -f %from %to'),
                         (r'^restore_directories=.*$', 'restore_directories={tmp_commitlog}'.format(
                          tmp_commitlog=tmp_commitlog))])

        cluster.start(wait_for_binary_proto=True)

        debug("Creating initial connection")
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key bigint PRIMARY KEY, val text);')
        debug("Writing 30,000 rows...")
        self.insert_rows(session, 0, 60000)

        try:
            # Check that there are at least one commit log backed up that
            # is not one of the active commit logs:
            commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
            debug("node1 commitlog dir: " + commitlog_dir)

            cluster.flush()

            self.assertNotEqual(set(os.listdir(tmp_commitlog)) - set(os.listdir(commitlog_dir)),
                                set())

            debug("Flushing and doing first restart")
            cluster.compact()
            node1.drain()
            # restart the node which causes the active commitlogs to be archived
            node1.stop()
            node1.start(wait_for_binary_proto=True)

            debug("Stopping and second restart")
            node1.stop()
            node1.start(wait_for_binary_proto=True)

            # Shouldn't be any additional data since it's replaying the same stuff repeatedly
            session = self.patient_cql_connection(node1)

            rows = session.execute('SELECT count(*) from ks.cf')
            self.assertEqual(rows[0][0], 60000)
        finally:
            debug("removing tmp_commitlog: " + tmp_commitlog)
            shutil.rmtree(tmp_commitlog)
