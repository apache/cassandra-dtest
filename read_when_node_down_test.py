from dtest import Tester
from ccmlib.cluster import Cluster
import time
import logging
from tools import insert_c1c2, query_c1c2

class TestReadWhenNodeDown(Tester):

    def read_when_node_down_test(self):
        CL = 'QUORUM'
        RF = 3

        print "Creating a ring"
        cluster = self.cluster
        cluster.set_cassandra_dir(cassandra_version="1.0.6")
        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
        [node1, node2, node3] = cluster.nodelist()
        cluster.start()
        time.sleep(.5)

        print "Set to talk to node 2"
        cursor = self.cql_connection(node2).cursor()
        self.create_ks(cursor, 'ks', RF)
        self.create_cf(cursor, 'cf')

        print "Generating some data"
        insert_c1c2(cursor, 100, CL)

        print "Taking down node1"
        node1.nodetool('drain')
        node1.stop()

        # Test will pass if this is un-commented.
        # I suspect it is because gossip detects the node as down
#        time.sleep(20)

        print "Reading back data."
        try:
            start_time = time.time()
            query_c1c2(cursor, 100, CL)
        except Exception, e:
            # Time how long it took to fail. Compare this time with
            # rpc_timeout set in cassandra.yaml
            fail = "reading failed in %.4f seconds." % (time.time() - start_time)
            e.args = e.args + (fail ,)
            raise

        cluster.cleanup()


