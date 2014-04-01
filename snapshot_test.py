from dtest import Tester, debug
import tempfile
import shutil
import glob
import os

class TestSnapshot(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def insert_rows(self, cursor, start, end):
        for r in range(start, end):
            cursor.execute("INSERT INTO ks.cf (key, val) VALUES ({r}, 'asdf');".format(r=r))

    def make_snapshot(self, node, ks, cf, name):
        node.flush()
        node.nodetool('snapshot {ks} -cf {cf} -t {name}'.format(**locals()))
        tmpdir = tempfile.mkdtemp()
        os.mkdir(os.path.join(tmpdir,ks))
        os.mkdir(os.path.join(tmpdir,ks,cf))
        node_dir = node.get_path()
        
        # Find the snapshot dir, it's different in various C* versions:
        snapshot_dir = "{node_dir}/data/{ks}/{cf}/snapshots/{name}".format(**locals())
        if not os.path.isdir(snapshot_dir):
            snapshot_dir = glob.glob("{node_dir}/flush/{ks}/{cf}-*/snapshots/{name}".format(**locals()))[0]
        debug("snapshot_dir is : " + snapshot_dir)
        debug("snapshot copy is : " + tmpdir)

        os.system('cp -a {snapshot_dir}/* {tmpdir}/{ks}/{cf}/'.format(**locals()))
        return tmpdir

    def restore_snapshot(self, snapshot_dir, node, ks, cf):
        node_dir = node.get_path()
        snapshot_dir = os.path.join(snapshot_dir, ks, cf)
        ip = node.address()
        os.system('{node_dir}/bin/sstableloader -d {ip} {snapshot_dir}'.format(**locals()))

    def test_basic_snapshot_and_restore(self):
        cluster = self.cluster
        cluster.populate(1).start()
        (node1,) = cluster.nodelist()
        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')

        self.insert_rows(cursor, 0, 100)
        snapshot_dir = self.make_snapshot(node1, 'ks', 'cf', 'basic')

        # Drop the keyspace, make sure we have no data:
        cursor.execute('DROP KEYSPACE ks')
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('CREATE TABLE ks.cf ( key int PRIMARY KEY, val text);')
        cursor.execute('SELECT count(*) from ks.cf')
        self.assertEqual(cursor.fetchone()[0], 0)

        # Restore data from snapshot:
        self.restore_snapshot(snapshot_dir, node1, 'ks', 'cf')
        node1.nodetool('refresh ks cf')
        cursor.execute('SELECT count(*) from ks.cf')
        self.assertEqual(cursor.fetchone()[0], 100)
        
        shutil.rmtree(snapshot_dir)
