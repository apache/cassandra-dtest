import time
import pprint

from dtest import Tester, debug
from ccmlib.cluster import Cluster
from ccmlib.node import Node
from tools import since

from cql.connection import Connection as ThriftConnection

class TestUnavailableSchemaOnDecommission(Tester):
    """
    demonstrates JIRA #4115 
    """


    def validate_schema_consistent(self, node):
        """ Makes sure that there is only one schema """

        host, port = node.network_interfaces['thrift']
        conn = ThriftConnection(host, port, keyspace=None)
        schemas = conn.client.describe_schema_versions()
        num_schemas = len(schemas)
        assert num_schemas == 1, "There were multiple schema versions: " + pprint.pformat(schemas)

    
    def decommission_node_schema_check_test(self):
        cluster = self.cluster

        cluster.populate(1)
        # create and add a non-seed node.
        node2 = Node('node2', 
                    cluster,
                    True,
                    ('127.0.0.2', 9160),
                    ('127.0.0.2', 7000),
                    '7200',
                    None)
        cluster.add(node2, False)

        [node1, node2] = cluster.nodelist()
        node1.start()
        node2.start()
        time.sleep(2)

        node2.decommission()
        time.sleep(30)

        self.validate_schema_consistent(node1)


