from dtest import Tester, debug
from assertions import *
from tools import *

from os.path import getsize
import time

class TestCounters(Tester):

    def split_test(self):
        """
        Check that after running compaction, sstablessplit can succesfully split
        The resultant sstable.  Check that split is reversable and that data is readable
        after carrying out these operations.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        version = cluster.version()
        debug("Run stress to insert data")
        if version < "2.1":
            node.stress( ['-o', 'insert'] )
        else:
            node.stress( ['write', 'n=1500000', '-rate', 'threads=8'] )
        
        self._do_compaction(node)
        self._do_split(node, version)
        self._do_compaction(node)
        self._do_split(node, version)

        debug("Run stress to ensure data is readable")
        if version < "2.1":
            node.stress( ['-o', 'read'] )
        else:
            node.stress( ['read', 'n=1500000', '-rate', 'threads=8'] )

    def _do_compaction(self, node):
        debug("Compact sstables.")
        node.compact()
        sstables = node.get_sstables('Keyspace1', '')

    def _do_split(self, node, version):
        debug("run sstablesplit")
        time.sleep(5.0)
        node.stop()
        node.run_sstablesplit( keyspace='Keyspace1' )
        sstables = node.get_sstables('Keyspace1', '')
        assert len(sstables) == 6, "Incorrect number of sstables after running sstablesplit."
        if version < "2.1":
            assert max( [ getsize( sstable ) for sstable in sstables ] ) <= 52428960, "Max sstables size should be 52428960."
        else:
            assert max( [ getsize( sstable ) for sstable in sstables ] ) <= 52428980, "Max sstables size should be 52428980."
        node.start()
