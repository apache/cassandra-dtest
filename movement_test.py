from dtest import Tester
from tools import *
from assertions import *

import os, sys, time
from ccmlib.cluster import Cluster

class TestMovement(Tester):

    def movement_test(self):
        cluster = self.cluster

        # Create an unbalanced ring
        cluster.populate(3, tokens=[0, 2**125, 2**124]).start()
        [node1, node2, node3] = [ cluster.nodes[n] for n in ["node1", "node2", "node3"] ]

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
        balancing_tokens = Cluster.balanced_tokens(3)
        node2.move(balancing_tokens[1])
        node3.move(balancing_tokens[2])
        time.sleep(.5)

        cluster.cleanup()

        # Check we can get all the keys
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, "ONE")

        # Now the load should be basically even
        sizes = [ node.data_size() for node in [node1, node2, node3] ]

        assert_almost_equal(sizes[0], sizes[1])
        assert_almost_equal(sizes[0], sizes[2])
        assert_almost_equal(sizes[1], sizes[2])
