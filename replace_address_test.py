from dtest import Tester, debug
import unittest
from tools import *
from ccmlib.cluster import Cluster
import time
from cql import OperationalError

class TestReplaceAddress(Tester):


	def replace_stopped_node_test(self):
		"""Check that the replace address function correctly replaces a node that has failed in a cluster. 
		Create a cluster, cause a node to fail, and bring up a new node with the replace_address parameter.
		Check that tokens are migrated and that data is replicated properly.
		"""

		cluster = self.cluster
		cluster.populate(3).start()
		[node1,node2, node3] = cluster.nodelist()

		node1.stress(['write', 'n=1000', '-schema', 'replication(factor=3)'])
		cursor = self.patient_cql_connection(node1).cursor()
		node1.run_cqlsh(cmds="CONSISTENCY three", show_output=True)
		cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1')
		initialData = list(cursor)
		
		node3.stop(gently=False)
		node1.run_cqlsh(cmds="CONSISTENCY three", show_output=True)
		with self.assertRaises(OperationalError):
			cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1') # should time out

		node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4',9042))
		cluster.add(node4, False)
		node4.start(replace_address='127.0.0.3')

		node1.run_cqlsh(cmds="CONSISTENCY three", show_output=True)
		cursor.execute('select * from "Keyspace1"."Standard1" LIMIT 1') # should work
		finalData = list(cursor)
		self.assertListEqual(initialData, finalData)
		
		movedTokensList = node4.grep_log("changing ownership from /127.0.0.3 to /127.0.0.4")
		time.sleep(2)
		debug(movedTokensList[0])
		self.assertGreaterEqual(len(movedTokensList), 1)

		node3.start() #check that doesn't work
		checkCollision = node1.grep_log("between /127.0.0.3 and /127.0.0.4; /127.0.0.4 is the new owner")
		debug(checkCollision)
		self.assertGreaterEqual(len(checkCollision), 1)

		updatedSeedList = node4.grep_log("removed /127.0.0.3 from seeds, updated seeds list = [/127.0.0.1, /127.0.0.2]")
		debug(updatedSeedList)
		self.assertGreaterEqual(len(updatedSeedList), 1)
