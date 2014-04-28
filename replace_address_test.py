from dtest import Tester, debug
from assertions import *
from tools import *
from ccmlib.cluster import Cluster
import time

def wait(delay=2):
	time.sleep(delay)

class TestReplaceAddress(Tester):

	def replace_stopped_node_test(self):	
		#start three nodes
		#bring down one node
		#introduce new node replacing downed node
		cluster = self.cluster

		cluster.populate(3).start()
		[node1,node2, node3] = cluster.nodelist()

		node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])

		node3.stop(gently=False)

		node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4',9042))
		cluster.add(node4, False)

		#node4 = new_node(cluster)
		
		debug(cluster.show(True))

		node4.start(replace_address='127.0.0.3')

		debug(cluster.show(True))

		assert node4.address() == '127.0.0.4'