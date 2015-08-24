from __future__ import division

import time
from math import floor
from os.path import getsize

from dtest import Tester, debug
from tools import since


class TestSSTableSplit(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

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
            node.stress(['-o', 'insert'])
        else:
            node.stress(['write', 'n=1000000', '-rate', 'threads=10'])

        self._do_compaction(node)
        self._do_split(node, version)
        self._do_compaction(node)
        self._do_split(node, version)

        debug("Run stress to ensure data is readable")
        if version < "2.1":
            node.stress(['-o', 'read'])
        else:
            node.stress(['read', 'n=1000000', '-rate', 'threads=25'])

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

        # default split size is 50MB
        splitmaxsize = 10 
        expected_sstable_size = (10 * 1024 * 1024) 
        keyspace = 'keyspace1' if self.cluster.version() >= '2.1' else 'Keyspace1'

        # get the initial sstables and their total size
        origsstables = node.get_sstables(keyspace, '')
        origsstable_size = sum([getsize(sstable) for sstable in origsstables])
        debug("Original sstable and sizes before split: {}".format([(name, getsize(name)) for name in origsstables]))

        # calculate the expected number of sstables post-split
        expected_num_sstables = floor(origsstable_size / expected_sstable_size) 

        # split the sstables
        result = node.run_sstablesplit(keyspace=keyspace, size=splitmaxsize,
                                       no_snapshot=True, debug=True)

        for (out, error, rc) in result:
            debug("stdout: {}".format(out))
            debug("stderr: {}".format(error))
            debug("rc: {}".format(rc))

        # get the sstables post-split and their total size
        sstables = node.get_sstables(keyspace, '')
        debug("Number of sstables after split: %s. expected %s" % (len(sstables), expected_num_sstables))
        self.assertLessEqual(expected_num_sstables, len(sstables) + 1)
        self.assertLessEqual(1, len(sstables))

        # make sure none of the tables are bigger than the max expected size
        sstable_sizes = [getsize(sstable) for sstable in sstables]
        # add a bit extra for overhead
        self.assertLessEqual(max(sstable_sizes), expected_sstable_size + 512)
        # make sure node can start with changed sstables
        node.start(wait_for_binary_proto=True)

    @since("2.1")
    def single_file_split_test(self):
        """
        Covers CASSANDRA-8623

        Check that sstablesplit doesn't crash when splitting a single sstable at the time.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[0]

        debug("Run stress to insert data")
        node.stress(['write', 'n=2000000', '-rate', 'threads=10',
                     '-schema', 'compaction(strategy=LeveledCompactionStrategy, sstable_size_in_mb=10)'])
        self._do_compaction(node)
        node.stop()
        result = node.run_sstablesplit(keyspace='keyspace1', size=2, no_snapshot=True)

        for (stdout, stderr, rc) in result:
            debug(stderr)
            failure = stderr.find("java.lang.AssertionError: Data component is missing")
            self.assertEqual(failure, -1, "Error during sstablesplit")
