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
        debug("Run stress to insert data")
        node.stress( ['-o', 'insert'] )
        
        self._do_compaction(node)
        self._do_split(node)
        self._do_compaction(node)
        self._do_split(node)

        debug("Run stress to ensure data is readable")
        node.stress( ['-o', 'read'] )

    def _do_compaction(self, node):
        debug("Compact sstables.")
        node.compact()
        sstables = node.get_sstables('Keyspace1', '')

    def _do_split(self, node):
        debug("run sstablesplit")
        time.sleep(5.0)
        node.stop()
        node.run_sstablesplit( keyspace='Keyspace1' )
        sstables = node.get_sstables('Keyspace1', '')
        assert len(sstables) == 6, "Incorrect number of sstables after running sstablesplit."
        assert max( [ getsize( sstable ) for sstable in sstables ] ) <= 52428960, "Max sstables size should be 52428960."
        node.start()
