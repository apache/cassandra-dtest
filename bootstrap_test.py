import random, time
from dtest import Tester
from tools import *
from assertions import *
from ccmlib.cluster import Cluster


class TestBoostrap(Tester):

    def simple_bootstrap_test(self):
        cluster = self.cluster
        if cluster.version() >= '1.2':
            tokens = Cluster.balanced_tokens(2, 64)
        else:
            tokens = Cluster.balanced_tokens(2, 128)

        keys = 10000

        # Create a single node cluster
        cluster.populate(1, tokens=[tokens[0]]).start()
        node1 = cluster.nodes["node1"]

        time.sleep(.5)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', columns={ 'c1' : 'text', 'c2' : 'text' })

        for n in xrange(0, keys):
            insert_c1c2(cursor, n, "ONE")

        node1.flush()
        initial_size = node1.data_size()

        # Reads inserted data all during the boostrap process. We shouldn't
        # get any error
        reader = self.go(lambda _: query_c1c2(cursor, random.randint(0, keys-1), "ONE"))

        # Boostraping a new node
        node2 = new_node(cluster, token=tokens[1])
        node2.start()
        time.sleep(.5)

        reader.check()
        node1.cleanup()
        time.sleep(.5)
        reader.check()

        size1 = node1.data_size()
        size2 = node2.data_size()
        assert_almost_equal(size1, size2)
        assert_almost_equal(initial_size, 2 * size1)
