from dtest import Tester
from tools import *
from assertions import *

import os, sys, time
from ccmlib.cluster import Cluster

class TestTopology(Tester):

    def movement_test(self):
        cluster = self.cluster

        # Create an unbalanced ring
        cluster.populate(3, tokens=[0, 2**125, 2**124]).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf')

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, "ONE")

        cluster.flush()
        sizes = [ node.data_size() for node in [node1, node2, node3] ]
        # Given the assigned token, node2 and node3 should have approximately
        # the same amount of data and node1 must have 6 times as much
        assert_almost_equal(sizes[1], sizes[2])
        assert_almost_equal(sizes[0], 6 * sizes[1])

        # Move nodes to balance the cluster
        balancing_tokens = cluster.balanced_tokens(3)
        node2.move(balancing_tokens[1])
        node3.move(balancing_tokens[2])
        time.sleep(1)

        cluster.cleanup()

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, "ONE")

        # Now the load should be basically even
        sizes = [ node.data_size() for node in [node1, node2, node3] ]

        assert_almost_equal(sizes[0], sizes[1])
        assert_almost_equal(sizes[0], sizes[2])
        assert_almost_equal(sizes[1], sizes[2])

    def decomission_test(self):
        cluster = self.cluster

        tokens = cluster.balanced_tokens(4)
        cluster.populate(4, tokens=tokens).start()
        [node1, node2, node3, node4] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 2)
        self.create_cf(cursor, 'cf')

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, "QUORUM")

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
            query_c1c2(cursor, n, "QUORUM")

        sizes = [ node.data_size() for node in cluster.nodelist() if node.is_running() ]
        three_node_sizes = sizes
        assert_almost_equal(sizes[0], sizes[1])
        assert_almost_equal((2.0/3.0) * sizes[0], sizes[2])
        assert_almost_equal(sizes[2], init_size)

        node3.stop(wait_other_notice=True)
        node1.removeToken(tokens[2])
        time.sleep(.5)
        cluster.cleanup()
        time.sleep(.5)

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, "QUORUM")

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
            query_c1c2(cursor, n, "QUORUM")

        sizes = [ node.data_size() for node in cluster.nodelist() if node.is_running() ]
        # We should be back to the earlir 3 nodes situation
        for i in xrange(0, len(sizes)):
            assert_almost_equal(sizes[i], three_node_sizes[i])

    def replace_test(self):
        cluster = self.cluster

        tokens = cluster.balanced_tokens(3)
        cluster.populate(3, tokens=tokens).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf')

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, "QUORUM")

        cluster.flush()

        node3.stop(wait_other_notice=True)
        time.sleep(.5)

        node4 = new_node(cluster, token=tokens[2])
        node4.start(replace_token=tokens[2])
        time.sleep(.5)
        cluster.cleanup()
        time.sleep(.5)

        for n in xrange(0, 10000):
            query_c1c2(cursor, n, "QUORUM")

        sizes = [ node.data_size() for node in cluster.nodelist() if node.is_running()]
        assert_almost_equal(*sizes)

    def move_single_node_test(self):
        """ Test moving a node in a single-node cluster (#4200) """
        cluster = self.cluster

        # Create an unbalanced ring
        cluster.populate(1, tokens=[0]).start()
        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf')

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, "ONE")

        cluster.flush()

        node1.move(2**125)
        time.sleep(1)

        cluster.cleanup()

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, "ONE")
