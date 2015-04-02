from dtest import Tester, debug
from tools import new_node, since
import time

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
        cluster.set_configuration_options( { 'commitlog_segment_size_in_mb' : 128, 'internode_compression' : 'none' })
        cluster.populate(2).start(jvm_args=[" -Dcassandra.netty_use_heap_allocator=true "])
        node1, node2 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        debug("Before stress {0}".format(self.directbytes(node1)))
        debug("Running stress")
        self.stress_with_col_size(cluster, node1, 1)
        debug("Ran stress once {0}".format(self.directbytes(node1)))

        self.stress_with_col_size(cluster, node1, 1024 * 1024 * 63)

        output = node1.nodetool( "gcstats", capture_output=True);
        afterStress = self.directbytes(node1)
        debug("After stress {0}".format(afterStress))
        assert int(afterStress) < 1024 * 1024 * 2, int(afterStress)
