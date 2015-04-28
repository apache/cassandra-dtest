from dtest import Tester, debug
from tools import new_node, since
import time

"""
Check that inserting and reading large columns to the database doesn't cause off heap memory usage
that is proportional to the size of the memory read/written.
"""
@since('3.0')
class TestLargeColumn(Tester):

    def stress_with_col_size(self, cluster, node, size):
        size = str(size);
        node.stress(['write', 'n=5', "no-warmup", "cl=ALL", "-pop", "seq=1...5", "-schema", "replication(factor=2)", "-col", "n=fixed(1)", "size=fixed(" + size + ")", "-rate", "threads=1"])
        node.stress(['read', 'n=5', "no-warmup", "cl=ALL", "-pop", "seq=1...5", "-schema", "replication(factor=2)", "-col", "n=fixed(1)", "size=fixed(" + size + ")", "-rate", "threads=1"])

    def directbytes(self, node):
        return node.nodetool( "gcstats", capture_output=True)[0].split("\n")[1].split()[6]

    def cleanup_test(self):
        """
        See CASSANDRA-8670
        """
        cluster = self.cluster
        #Commit log segment size needs to increase for the database to be willing to accept columns that large
        #internode compression is disabled because the regression being tested occurs in NIO buffer pooling without compression
        cluster.set_configuration_options( { 'commitlog_segment_size_in_mb' : 128, 'internode_compression' : 'none' })
        #Have Netty allocate memory on heap so it is clear if memory used for large columns is related to intracluster messaging
        cluster.populate(2).start(jvm_args=[" -Dcassandra.netty_use_heap_allocator=true "])
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        debug("Before stress {0}".format(self.directbytes(node1)))
        debug("Running stress")
        #Run the full stack to see how much memory is utilized for "small" columns
        self.stress_with_col_size(cluster, node1, 1)
        debug("Ran stress once {0}".format(self.directbytes(node1)))

        #Now run the full stack to see how much memory is utilized for "large" columns
        self.stress_with_col_size(cluster, node1, 1024 * 1024 * 63)

        output = node1.nodetool( "gcstats", capture_output=True);
        afterStress = self.directbytes(node1)
        debug("After stress {0}".format(afterStress))

        #Any growth in memory usage should not be proportional column size. Really almost no memory should be used
        #since Netty was instructed to use a heap allocator
        assert int(afterStress) < 1024 * 1024 * 2, int(afterStress)
