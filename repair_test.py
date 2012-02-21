import time, re
from dtest import Tester
from tools import *

class TestRepair(Tester):

    def check_rows_on_node(self, node_to_check, rows, found=[], missings=[], restart=True):
        stopped_nodes = []
        for node in self.cluster.nodes.values():
            if node.is_running() and node is not node_to_check:
                stopped_nodes.append(node)
                node.stop(wait_other_notice=True)

        cursor = self.cql_connection(node_to_check, 'ks').cursor()
        cursor.execute("SELECT * FROM cf LIMIT %d" % (rows * 2))
        assert cursor.rowcount == rows

        for k in found:
            query_c1c2(cursor, k, "ONE")

        for k in missings:
            cursor.execute('SELECT c1, c2 FROM cf USING CONSISTENCY ONE WHERE key=k%d' % k)
            res = cursor.fetchall()
            assert len(filter(lambda x: len(x) != 0, res)) == 0, res

        cursor.close()

        if restart:
            for node in stopped_nodes:
                node.start(wait_other_notice=True)

    def simple_repair_test(self, ):
        self._simple_repair()

    def simple_repair_order_preserving_test(self, ):
        self._simple_repair(order_preserving_partitioner=True)

    def _simple_repair(self, order_preserving_partitioner=False):
        cluster = self.cluster

        if order_preserving_partitioner:
            cluster.set_partitioner('org.apache.cassandra.dht.ByteOrderedPartitioner')

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0)

        # Insert 1000 keys, kill node 3, insert 1 key, restart node 3, insert 1000 more keys
        for i in xrange(0, 1000):
            insert_c1c2(cursor, i, "ONE")
        time.sleep(.5)
        node3.flush();
        node3.stop()
        insert_c1c2(cursor, 1000, "ONE")
        time.sleep(.5)
        node3.start(wait_other_notice=True)
        for i in xrange(1001, 2001):
            insert_c1c2(cursor, i, "ONE")
        time.sleep(.5)
        cursor.close()

        cluster.flush()

        # Verify that node3 has only 2000 keys
        self.check_rows_on_node(node3, 2000, missings=[1000])


        # Verify that node1 has 2001 keys
        self.check_rows_on_node(node1, 2001, found=[1000])

        # Verify that node2 has 2001 keys
        self.check_rows_on_node(node2, 2001, found=[1000])

        # Run repair
        node1.repair()

        # Validate that only one range was transfered
        l = node1.grep_log("/([0-9.]+) and /([0-9.]+) have ([0-9]+) range\(s\) out of sync")
        if cluster.version() > "1":
            assert len(l) == 2, "Lines matching: " + str([elt[0] for elt in l])
        else:
            # In pre-1.0, we should have only one line
            assert len(l) == 1, "Lines matching: " + str([elt[0] for elt in l])
        valid = [(node1.address(), node3.address()), (node3.address(), node1.address()),
                 (node2.address(), node3.address()), (node3.address(), node2.address())]
        for line, m in l:
            assert int(m.group(3)) == 1, "Expecting 1 range out of sync, got " + int(m.group(1))
            assert (m.group(1), m.group(2)) in valid, str((m.group(1), m.group(2)))
            valid.remove((m.group(1), m.group(2)))
            valid.remove((m.group(2), m.group(1)))

        # Check node3 now has the key
        self.check_rows_on_node(node3, 2001, found=[1000], restart=False)

