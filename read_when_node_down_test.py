from dtest import Tester, debug
from ccmlib.cluster import Cluster
import time
from tools import insert_c1c2, query_c1c2

class TestReadWhenNodeDown(Tester):

    def read_when_node_down_test(self):
        CL = 'QUORUM'
        RF = 3

        debug("Creating a ring")
        cluster = self.cluster
        cluster.set_cassandra_dir(cassandra_version="1.0.6")
        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
        [node1, node2, node3] = cluster.nodelist()
        cluster.start()
        time.sleep(.5)

        debug("Set to talk to node 2")
        cursor = self.cql_connection(node2).cursor()
        self.create_ks(cursor, 'ks', RF)
        self.create_cf(cursor, 'cf')

        debug("Generating some data")
        insert_c1c2(cursor, 100, CL)

        debug("Taking down node1")
        node1.nodetool('drain')
        node1.stop()

        # Reads will fail if gossip hasn't noticed the node is down.
        debug("Sleeping to let gossip notice the node is down..")
        time.sleep(25) 

        debug("Reading back data.")
        query_c1c2(cursor, 100, CL)

        cluster.cleanup()


