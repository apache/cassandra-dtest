import distutils.dir_util
import glob
import os
import shutil
import subprocess
import tempfile
import time

from cassandra.concurrent import execute_concurrent_with_args

from dtest import Tester, debug
from tools import replace_in_file, require, since


class SnapshotTester(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def insert_rows(self, session, start, end):
        insert_statement = session.prepare("INSERT INTO ks.cf (key, val) VALUES (?, 'asdf')")
        args = [(r,) for r in range(start, end)]
        execute_concurrent_with_args(session, insert_statement, args, concurrency=20)

    def make_snapshot(self, node, ks, cf, name):
        debug("Making snapshot....")
        node.flush()
        snapshot_cmd = 'snapshot {ks} -cf {cf} -t {name}'.format(**locals())
        debug("Running snapshot cmd: {snapshot_cmd}".format(snapshot_cmd=snapshot_cmd))
        node.nodetool(snapshot_cmd)
        tmpdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(tmpdir, ks))
        os.mkdir(os.path.join(tmpdir, ks, cf))
        node_dir = node.get_path()

        # Find the snapshot dir, it's different in various C* versions:
        snapshot_dir = "{node_dir}/data/{ks}/{cf}/snapshots/{name}".format(**locals())
        if not os.path.isdir(snapshot_dir):
            snapshot_dir = glob.glob("{node_dir}/data/{ks}/{cf}-*/snapshots/{name}".format(**locals()))[0]
        debug("snapshot_dir is : " + snapshot_dir)
        debug("snapshot copy is : " + tmpdir)

        # Copy files from the snapshot dir to existing temp dir
        distutils.dir_util.copy_tree(str(snapshot_dir), os.path.join(tmpdir, ks, cf))

        return tmpdir

    def restore_snapshot(self, snapshot_dir, node, ks, cf):
        debug("Restoring snapshot....")
        snapshot_dir = os.path.join(snapshot_dir, ks, cf)
        ip = node.address()

        args = [node.get_tool('sstableloader'), '-d', ip, snapshot_dir]
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        exit_status = p.wait()

        if exit_status != 0:
            raise Exception("sstableloader command '%s' failed; exit status: %d'; stdout: %s; stderr: %s" %
                            (" ".join(args), exit_status, stdout, stderr))


class TestSnapshot(SnapshotTester):

    def __init__(self, *args, **kwargs):
        SnapshotTester.__init__(self, *args, **kwargs)

    def test_basic_snapshot_and_restore(self):
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')

        self.insert_rows(session, 0, 100)
        snapshot_dir = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        # Write more data after the snapshot, this will get thrown
        # away when we restore:
        self.insert_rows(session, 100, 200)
        rows = session.execute('SELECT count(*) from ks.cf')
        self.assertEqual(rows[0][0], 200)

        # Drop the keyspace, make sure we have no data:
        session.execute('DROP KEYSPACE ks')
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')
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


@require('dtest issue #393', broken_in='3.0')
class TestArchiveCommitlog(SnapshotTester):
    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'commitlog_segment_size_in_mb': 1}
        SnapshotTester.__init__(self, *args, **kwargs)

    def make_snapshot(self, node, ks, cf, name):
        debug("Making snapshot....")
        node.flush()
        snapshot_cmd = 'snapshot {ks} -cf {cf} -t {name}'.format(**locals())
        debug("Running snapshot cmd: {snapshot_cmd}".format(snapshot_cmd=snapshot_cmd))
        node.nodetool(snapshot_cmd)
        tmpdir = tempfile.mkdtemp()
        node_dir = node.get_path()

        # Copy files from the snapshot dir to existing temp dir
        distutils.dir_util.copy_tree(os.path.join(node.get_path(), 'data', ks), tmpdir)

        return tmpdir

    def restore_snapshot(self, snapshot_dir, node, ks, cf, name):
        debug("Restoring snapshot for cf ....")
        data_dir = os.path.join(node.get_path(), 'data')
        cf_id = [s for s in os.listdir(snapshot_dir) if cf in s][0]
        snapshot_dir = glob.glob("{snapshot_dir}/{cf_id}/snapshots/{name}".format(**locals()))[0]
        if not os.path.exists(os.path.join(data_dir, ks)):
            os.mkdir(os.path.join(data_dir, ks))
        os.mkdir(os.path.join(data_dir, ks, cf_id))

        debug("snapshot_dir is : " + snapshot_dir)
        distutils.dir_util.copy_tree(snapshot_dir, os.path.join(data_dir, ks, cf_id))

    @since('2.1')
    def test_archive_commitlog(self):
        self.run_archive_commitlog(restore_point_in_time=False)

    def test_archive_commitlog_with_active_commitlog(self):
        """Copy the active commitlogs to the archive directory before restoration"""
        self.run_archive_commitlog(restore_point_in_time=False, archive_active_commitlogs=True)

    def dont_test_archive_commitlog(self):
        """Run the archive commitlog test, but forget to add the restore commands:"""
        self.run_archive_commitlog(restore_point_in_time=False, restore_archived_commitlog=False)

    def test_archive_commitlog_point_in_time(self):
        """Test archive commit log with restore_point_in_time setting"""
        self.run_archive_commitlog(restore_point_in_time=True)

    def test_archive_commitlog_point_in_time_with_active_commitlog(self):
        """Test archive commit log with restore_point_in_time setting"""
        self.run_archive_commitlog(restore_point_in_time=True, archive_active_commitlogs=True)

    def run_archive_commitlog(self, restore_point_in_time=False, restore_archived_commitlog=True, archive_active_commitlogs=False):
        """Run archive commit log restoration test"""

        cluster = self.cluster
        cluster.populate(1)
        (node1,) = cluster.nodelist()

        # Create a temp directory for storing commitlog archives:
        tmp_commitlog = tempfile.mkdtemp()
        debug("tmp_commitlog: " + tmp_commitlog)

        # Edit commitlog_archiving.properties and set an archive
        # command:
        replace_in_file(os.path.join(node1.get_path(), 'conf', 'commitlog_archiving.properties'),
                        [(r'^archive_command=.*$', 'archive_command=cp %path {tmp_commitlog}/%name'.format(
                            tmp_commitlog=tmp_commitlog))])

        cluster.start()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE ks.cf ( key bigint PRIMARY KEY, val text);')
        debug("Writing first 30,000 rows...")
        self.insert_rows(session, 0, 30000)
        # Record when this first set of inserts finished:
        insert_cutoff_times = [time.gmtime()]

        # Delete all commitlog backups so far:
        for f in glob.glob(tmp_commitlog+"/*"):
            os.remove(f)

        snapshot_dir = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        if self.cluster.version() >= '3.0':
            system_ks_snapshot_dir = self.make_snapshot(node1, 'system_schema', 'keyspaces', 'keyspaces')
        else:
            system_ks_snapshot_dir = self.make_snapshot(node1, 'system', 'schema_keyspaces', 'keyspaces')

        if self.cluster.version() >= '3.0':
            system_col_snapshot_dir = self.make_snapshot(node1, 'system_schema', 'columns', 'columns')
        else:
            system_col_snapshot_dir = self.make_snapshot(node1, 'system', 'schema_columns', 'columns')

        if self.cluster.version() >= '3.0':
            system_ut_snapshot_dir = self.make_snapshot(node1, 'system_schema', 'types', 'usertypes')
        elif self.cluster.version() >= '2.1':
            system_ut_snapshot_dir = self.make_snapshot(node1, 'system', 'schema_usertypes', 'usertypes')

        if self.cluster.version() >= '3.0':
            system_cfs_snapshot_dir = self.make_snapshot(node1, 'system_schema', 'tables', 'cfs')
        else:
            system_cfs_snapshot_dir = self.make_snapshot(node1, 'system', 'schema_columnfamilies', 'cfs')

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

            self.assertTrue(len(set(os.listdir(tmp_commitlog)) - set(os.listdir(commitlog_dir))) > 0)

            cluster.flush()
            cluster.compact()
            node1.drain()
            if archive_active_commitlogs:
                # restart the node which causes the active commitlogs to be archived
                node1.stop()
                node1.start(wait_for_binary_proto=True)

            # Destroy the cluster
            cluster.stop()
            self.copy_logs(name=self.id().split(".")[0]+"_pre-restore")
            self._cleanup_cluster()
            cluster = self.cluster = self._get_cluster()
            cluster.populate(1)
            node1, = cluster.nodelist()

            # Restore schema from snapshots:
            if self.cluster.version() >= '3.0':
                self.restore_snapshot(system_ks_snapshot_dir, node1, 'system_schema', 'keyspaces', 'keyspaces')
            else:
                self.restore_snapshot(system_ks_snapshot_dir, node1, 'system', 'schema_keyspaces', 'keyspaces')

            if self.cluster.version() >= '3.0':
                self.restore_snapshot(system_col_snapshot_dir, node1, 'system_schema', 'columns', 'columns')
            else:
                self.restore_snapshot(system_col_snapshot_dir, node1, 'system', 'schema_columns', 'columns')

            if self.cluster.version() >= '3.0':
                self.restore_snapshot(system_ut_snapshot_dir, node1, 'system_schema', 'types', 'usertypes')
            if self.cluster.version() >= '2.1':
                self.restore_snapshot(system_ut_snapshot_dir, node1, 'system', 'schema_usertypes', 'usertypes')

            if self.cluster.version() >= '3.0':
                self.restore_snapshot(system_cfs_snapshot_dir, node1, 'system_schema', 'tables', 'cfs')
            else:
                self.restore_snapshot(system_cfs_snapshot_dir, node1, 'system', 'schema_columnfamilies', 'cfs')

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
                                    [(r'^restore_point_in_time=.*$', 'restore_point_in_time={restore_time}'.format(**locals()))])

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
            debug("removing snapshot_dir: " + snapshot_dir)
            shutil.rmtree(snapshot_dir)
            debug("removing snapshot_dir: " + system_ks_snapshot_dir)
            shutil.rmtree(system_ks_snapshot_dir)
            debug("removing snapshot_dir: " + system_cfs_snapshot_dir)
            shutil.rmtree(system_cfs_snapshot_dir)
            if self.cluster.version() >= '2.1':
                debug("removing snapshot_dir: " + system_ut_snapshot_dir)
                shutil.rmtree(system_ut_snapshot_dir)
            debug("removing snapshot_dir: " + system_col_snapshot_dir)
            shutil.rmtree(system_col_snapshot_dir)
            debug("removing tmp_commitlog: " + tmp_commitlog)
            shutil.rmtree(tmp_commitlog)
