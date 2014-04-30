from dtest import Tester, debug
import unittest
from tools import *
from ccmlib.cluster import Cluster
import time
from cql import OperationalError
from cql.cassandra.ttypes import UnavailableException

class NodeUnavailable(Exception):
    pass

class TestReplaceAddress(Tester):

    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def replace_node_test(self):
        """Check that the replace address function correctly replaces a node that has failed in a cluster. 
        Create a cluster, cause a node to fail, and bring up a new node with the replace_address parameter.
        Check that tokens are migrated and that data is replicated properly.
        """

        cluster = self.cluster
        cluster.populate(3).start()
        [node1,node2, node3] = cluster.nodelist()

        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])
        cursor = self.patient_cql_connection(node1).cursor()
        cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1', consistency_level='THREE')
        initialData = cursor.fetchall()
        
        #stop node, query should time out with consistency 3
        node3.stop(gently=False)
        time.sleep(5)
        with self.assertRaises(NodeUnavailable) as cm:
            try:
                cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1', consistency_level='THREE')
            except (UnavailableException, OperationalError):
                raise NodeUnavailable("Node could not be queried.")

        debug(cm.exception)

        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4',9042))
        cluster.add(node4, False)
        node4.start(replace_address='127.0.0.3')

        #query should work again
        cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1', consistency_level='THREE')
        finalData = cursor.fetchall()
        self.assertListEqual(initialData, finalData)
        
        movedTokensList = node4.grep_log("changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertGreaterEqual(len(movedTokensList), 1)

        #check that restarting node 3 doesn't work
        node3.start() 
        checkCollision = node1.grep_log("between /127.0.0.3 and /127.0.0.4; /127.0.0.4 is the new owner")
        debug(checkCollision)
        self.assertGreaterEqual(len(checkCollision), 1)

        updatedSeedList = node4.grep_log("FatClient /127.0.0.3 has been silent for 30000ms, removing from gossip")
        debug(updatedSeedList)
        self.assertGreaterEqual(len(updatedSeedList), 1)
