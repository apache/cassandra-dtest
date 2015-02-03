from dtest import Tester
from tools import new_node

class TestNodetool(Tester):

    def stress(self, cluster, node):
        if cluster.version() < "2.1":
            node.stress(['-n', '25000'])
        else:
            node.stress(['write', 'n=25000'])

    def cleanup_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1, = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.stress(cluster, node1)

        node1.flush()
        node2 = new_node(cluster)
        node2.start()
        node2.watch_log_for("Bootstrap completed")

        node1.cleanup()