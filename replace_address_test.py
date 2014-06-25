from dtest import Tester, debug, DISABLE_VNODES
import unittest
from tools import *
from ccmlib.cluster import Cluster
from ccmlib.node import NodeError
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
            # This is caused by starting a node improperly (replacing active/nonexistent)
            r'Exception encountered during startup',
            # This is caused by trying to replace a nonexistent node
            r'Exception in thread Thread'
        ]
        Tester.__init__(self, *args, **kwargs)

    def replace_stopped_node_test(self):
        """Check that the replace address function correctly replaces a node that has failed in a cluster. 
        Create a cluster, cause a node to fail, and bring up a new node with the replace_address parameter.
        Check that tokens are migrated and that data is replicated properly.
        """
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        [node1,node2, node3] = cluster.nodelist()

        debug("Inserting Data...")
        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=10000', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])
        cursor = self.patient_cql_connection(node1).cursor()
        cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1', consistency_level='THREE')
        initialData = cursor.fetchall()
        
        #stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=False)
        time.sleep(5)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1', consistency_level='THREE')
            except (UnavailableException, OperationalError):
                raise NodeUnavailable("Node could not be queried.")

        #replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4',9042))
        cluster.add(node4, False)
        node4.start(replace_address='127.0.0.3')

        #query should work again
        debug("Verifying querying works again.")
        cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1', consistency_level='THREE')
        finalData = cursor.fetchall()
        self.assertListEqual(initialData, finalData)
        
        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        if DISABLE_VNODES:
            self.assertEqual(len(movedTokensList), 1)
        else:
            self.assertEqual(len(movedTokensList), 256)

        #check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start() 
        checkCollision = node1.grep_log("between /127.0.0.3 and /127.0.0.4; /127.0.0.4 is the new owner")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

    def replace_active_node_test(self):

        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        [node1,node2, node3] = cluster.nodelist()

        debug("Inserting Data...")
        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=10000', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])
        cursor = self.patient_cql_connection(node1).cursor()
        cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1', consistency_level='THREE')
        initialData = cursor.fetchall()

        #replace active node 3 with node 4
        debug("Starting node 4 to replace active node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4',9042))
        cluster.add(node4, False)
        
        with self.assertRaises(NodeError):
            node4.start(replace_address='127.0.0.3')

        checkError = node4.grep_log("java.lang.UnsupportedOperationException: Cannnot replace a live node...")

        self.assertEqual(len(checkError), 1)

    def replace_nonexistent_node_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        [node1,node2, node3] = cluster.nodelist()

        debug("Inserting Data...")
        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=10000', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])
        cursor = self.patient_cql_connection(node1).cursor()
        cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1', consistency_level='THREE')
        initialData = cursor.fetchall()

        debug('Start node 4 and replace an address with no node')
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4',9042))
        cluster.add(node4, False)
        
        #try to replace an unassigned ip address
        with self.assertRaises(NodeError):
            node4.start(replace_address='127.0.0.5')
