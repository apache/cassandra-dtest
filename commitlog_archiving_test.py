from dtest import Tester, debug
from pytools import since, replace_in_file
import tempfile, shutil, glob, os, time
import distutils.dir_util


class CommitLogArchivingTest(Tester):
    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def insert_rows(self, cursor, start, end):
        for r in range(start, end):
            cursor.execute("INSERT INTO ks.cf (key, val) VALUES ({r}, 'asdf');".format(r=r))

    @since('2.0')
    def test_active_commitlog_segments_archived_at_startup(self):
        """Test archive commit log segments are automatically archived at node startup"""
        cluster = self.cluster
        cluster.populate(1)
        (node1,) = cluster.nodelist()

        # Create a temp directory for storing commitlog archives:
        tmp_commitlog = tempfile.mkdtemp()
        debug("tmp_commitlog: " + tmp_commitlog)

        # Edit commitlog_archiving.properties and set an archive
        # command:
        replace_in_file(os.path.join(node1.get_path(),'conf','commitlog_archiving.properties'),
                        [(r'^archive_command=.*$', 'archive_command=cp %path {tmp_commitlog}/%name'.format(
                            tmp_commitlog=tmp_commitlog))])

        cluster.start()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('CREATE TABLE ks.cf ( key bigint PRIMARY KEY, val text);')
        debug("Writing 30,000 rows...")
        self.insert_rows(cursor, 0, 30000)

        node1.stop()
        # assert that we didn't yet archive the active commitlog segments
        commitlog_dir = os.path.join(node1.get_path(), 'commitlogs')
        active_segments = set(os.listdir(commitlog_dir))
        self.assertFalse(set(os.listdir(tmp_commitlog)).issuperset(active_segments))

        # start up the node & verify that the active commitlog segments were archived
        node1.start()
        self.assertTrue(set(os.listdir(tmp_commitlog)).issuperset(active_segments))
