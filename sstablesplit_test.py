from dtest import Tester, debug
from tools import since

from os.path import getsize
import time
import subprocess
import tempfile

class TestSSTableSplit(Tester):

    def split_test(self):
        """
        Check that after running compaction, sstablessplit can succesfully split
        The resultant sstable.  Check that split is reversable and that data is readable
        after carrying out these operations.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        version = cluster.version()

        debug("Run stress to insert data")
        if version < "2.1":
            node.stress( ['-o', 'insert'] )
        else:
            node.stress( ['write', 'n=1000000', '-rate', 'threads=50'] )

        self._do_compaction(node)
        self._do_split(node, version)
        self._do_compaction(node)
        self._do_split(node, version)

        debug("Run stress to ensure data is readable")
        if version < "2.1":
            node.stress( ['-o', 'read'] )
        else:
            node.stress( ['read', 'n=1000000', '-rate', 'threads=25'] )

    def _do_compaction(self, node):
        debug("Compact sstables.")
        node.flush()
        node.compact()
        node.flush()
        keyspace = 'keyspace1' if self.cluster.version() >= '2.1' else 'Keyspace1'
        sstables = node.get_sstables(keyspace, '')
        debug("Number of sstables after compaction: %s" % len(sstables))

    def _do_split(self, node, version):
        debug("Run sstablesplit")
        time.sleep(5.0)
        node.stop()
        keyspace = 'keyspace1' if self.cluster.version() >= '2.1' else 'Keyspace1'
        origsstable = node.get_sstables(keyspace, '')
        debug("Original sstable before split: %s" % origsstable)
        node.run_sstablesplit( keyspace=keyspace )
        sstables = node.get_sstables(keyspace, '')
        debug("Number of sstables after split: %s" % len(sstables))
        if version < "2.1":
            assert len(sstables) == 6, "Incorrect number of sstables after running sstablesplit."
            assert max( [ getsize( sstable ) for sstable in sstables ] ) <= 52428960, "Max sstables size should be 52428960."
        else:
            assert len(sstables) == 7, "Incorrect number of sstables after running sstablesplit."
            sstables.remove(origsstable[0])  # newer sstablesplit does not remove the original sstable after split
            assert max( [ getsize( sstable ) for sstable in sstables ] ) <= 52428980, "Max sstables size should be 52428980."
        node.start()

    @since("2.1")
    def single_file_split_test(self):
        """
        Covers CASSANDRA-8623

        Check that sstablesplit doesn't crash when splitting a single sstable at the time.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]
        version = cluster.version()

        debug("Run stress to insert data")
        node.stress(['write', 'n=2000000', '-rate', 'threads=50',
                     '-schema', 'compaction(strategy=LeveledCompactionStrategy, sstable_size_in_mb=10)'])
        self._do_compaction(node)
        with tempfile.TemporaryFile(mode='w+') as tmpfile:
            node.run_sstablesplit(keyspace='keyspace1', size=2, no_snapshot=True,
                                  stdout=tmpfile, stderr=subprocess.STDOUT)
            tmpfile.seek(0)
            output = tmpfile.read()

        debug(output)
        failure = output.find("java.lang.AssertionError: Data component is missing")
        self.assertEqual(failure, -1, "Error during sstablesplit")
