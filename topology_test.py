from dtest import Tester
from pytools import insert_c1c2, query_c1c2, no_vnodes, new_node
from pyassertions import assert_almost_equal

import os, sys, time
from ccmlib.cluster import Cluster
from cassandra import ConsistencyLevel

class TestTopology(Tester):

    @no_vnodes()
    def movement_test(self):
        cluster = self.cluster

        # Create an unbalanced ring
        cluster.populate(3, tokens=[0, 2**48, 2**62]).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', columns={'c1': 'text', 'c2': 'text'})

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, ConsistencyLevel.ONE)

        cluster.flush()

        # Move nodes to balance the cluster
        balancing_tokens = cluster.balanced_tokens(3)
        escformat = '\\%s'
        if cluster.version() >= '2.1':
            escformat = '%s'
        node1.move(escformat % balancing_tokens[0]) # can't assume 0 is balanced with m3p
        node2.move(escformat % balancing_tokens[1])
        node3.move(escformat % balancing_tokens[2])
        time.sleep(1)

        cluster.cleanup()

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, ConsistencyLevel.ONE)

        # Now the load should be basically even
        sizes = [ node.data_size() for node in [node1, node2, node3] ]

        assert_almost_equal(sizes[0], sizes[1])
        assert_almost_equal(sizes[0], sizes[2])
        assert_almost_equal(sizes[1], sizes[2])

    @no_vnodes()
    def decomission_test(self):
        cluster = self.cluster

        tokens = cluster.balanced_tokens(4)
        cluster.populate(4, tokens=tokens).start()
        [node1, node2, node3, node4] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 2)
        self.create_cf(cursor, 'cf',columns={'c1': 'text', 'c2': 'text'})

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, ConsistencyLevel.QUORUM)

        cluster.flush()
        sizes = [ node.data_size() for node in cluster.nodelist() if node.is_running()]
        init_size = sizes[0]
        assert_almost_equal(*sizes)

        time.sleep(.5)
        node4.decommission()
        node4.stop()
        cluster.cleanup()
        time.sleep(.5)

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, ConsistencyLevel.QUORUM)

        sizes = [ node.data_size() for node in cluster.nodelist() if node.is_running() ]
        three_node_sizes = sizes
        assert_almost_equal(sizes[0], sizes[1])
        assert_almost_equal((2.0/3.0) * sizes[0], sizes[2])
        assert_almost_equal(sizes[2], init_size)

        if cluster.version() <= '1.2':
            node3.stop(wait_other_notice=True)
            node1.removeToken(tokens[2])
            time.sleep(.5)
            cluster.cleanup()
            time.sleep(.5)

            # Check we can get all the keys
            for n in xrange(0, 10000):
                query_c1c2(cursor, n, ConsistencyLevel.QUORUM)

            sizes = [ node.data_size() for node in cluster.nodelist() if node.is_running() ]
            assert_almost_equal(*sizes)
            assert_almost_equal(sizes[0], 2 * init_size)

            node5 = new_node(cluster, token=(tokens[2]+1)).start()
            time.sleep(.5)
            cluster.cleanup()
            time.sleep(.5)
            cluster.compact()
            time.sleep(.5)

            # Check we can get all the keys
            for n in xrange(0, 10000):
                query_c1c2(cursor, n, ConsistencyLevel.QUORUM)

            sizes = [ node.data_size() for node in cluster.nodelist() if node.is_running() ]
            # We should be back to the earlir 3 nodes situation
            for i in xrange(0, len(sizes)):
                assert_almost_equal(sizes[i], three_node_sizes[i])

    @no_vnodes()
    def replace_test(self):
        cluster = self.cluster

        tokens = cluster.balanced_tokens(3)
        cluster.populate(3, tokens=tokens).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', columns={'c1': 'text', 'c2': 'text'})

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, ConsistencyLevel.QUORUM)

        cluster.flush()

        node3.stop(wait_other_notice=True)
        time.sleep(.5)

        node4 = new_node(cluster, token=tokens[2])
        node4.start(replace_token=tokens[2])
        time.sleep(.5)
        cluster.cleanup()
        time.sleep(.5)

        for n in xrange(0, 10000):
            query_c1c2(cursor, n, ConsistencyLevel.QUORUM)

        sizes = [ node.data_size() for node in cluster.nodelist() if node.is_running()]
        assert_almost_equal(*sizes)

    @no_vnodes()
    def move_single_node_test(self):
        """ Test moving a node in a single-node cluster (#4200) """
        cluster = self.cluster

        # Create an unbalanced ring
        cluster.populate(1, tokens=[0]).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', columns={'c1': 'text', 'c2': 'text'})

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, ConsistencyLevel.ONE)

        cluster.flush()

        node1.move(2**25)
        time.sleep(1)

        cluster.cleanup()

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, ConsistencyLevel.ONE)
