from cassandra import ConsistencyLevel

from dtest import Tester, debug, create_ks
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.decorators import no_vnodes
from tools.misc import new_node


class TestBootstrapConsistency(Tester):

    @no_vnodes()
    def consistent_reads_after_move_test(self):
        debug("Creating a ring")
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'write_request_timeout_in_ms': 60000,
                                                  'read_request_timeout_in_ms': 60000, 'dynamic_snitch_badness_threshold': 0.0})
        cluster.set_batch_commitlog(enabled=True)

        cluster.populate(3, tokens=[0, 2**48, 2**62]).start()
        node1, node2, node3 = cluster.nodelist()

        debug("Set to talk to node 2")
        n2session = self.patient_cql_connection(node2)
        create_ks(n2session, 'ks', 2)
        create_c1c2_table(self, n2session)

        debug("Generating some data for all nodes")
        insert_c1c2(n2session, keys=range(10, 20), consistency=ConsistencyLevel.ALL)

        node1.flush()
        debug("Taking down node1")
        node1.stop(wait_other_notice=True)

        debug("Writing data to node2")
        insert_c1c2(n2session, keys=range(30, 1000), consistency=ConsistencyLevel.ONE)
        node2.flush()

        debug("Restart node1")
        node1.start(wait_other_notice=True)

        debug("Move token on node3")
        node3.move(2)

        debug("Checking that no data was lost")
        for n in xrange(10, 20):
            query_c1c2(n2session, n, ConsistencyLevel.ALL)

        for n in xrange(30, 1000):
            query_c1c2(n2session, n, ConsistencyLevel.ALL)

    def consistent_reads_after_bootstrap_test(self):
        debug("Creating a ring")
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'write_request_timeout_in_ms': 60000,
                                                  'read_request_timeout_in_ms': 60000, 'dynamic_snitch_badness_threshold': 0.0})
        cluster.set_batch_commitlog(enabled=True)

        cluster.populate(2)
        node1, node2 = cluster.nodelist()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        debug("Set to talk to node 2")
        n2session = self.patient_cql_connection(node2)
        create_ks(n2session, 'ks', 2)
        create_c1c2_table(self, n2session)

        debug("Generating some data for all nodes")
        insert_c1c2(n2session, keys=range(10, 20), consistency=ConsistencyLevel.ALL)

        node1.flush()
        debug("Taking down node1")
        node1.stop(wait_other_notice=True)

        debug("Writing data to only node2")
        insert_c1c2(n2session, keys=range(30, 1000), consistency=ConsistencyLevel.ONE)
        node2.flush()

        debug("Restart node1")
        node1.start(wait_other_notice=True)

        debug("Bootstraping node3")
        node3 = new_node(cluster)
        node3.start(wait_for_binary_proto=True)

        n3session = self.patient_cql_connection(node3)
        n3session.execute("USE ks")
        debug("Checking that no data was lost")
        for n in xrange(10, 20):
            query_c1c2(n3session, n, ConsistencyLevel.ALL)

        for n in xrange(30, 1000):
            query_c1c2(n3session, n, ConsistencyLevel.ALL)
