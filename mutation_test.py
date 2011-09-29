from time import sleep
from dtest import Tester
from assertions import *
from tools import *

class TestMutation(Tester):

    def quorum_quorum_test(self):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodes.values()

        cursor1 = self.cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        self.create_cf(cursor1, 'cf')

        cursor2 = self.cql_connection(node2, 'ks').cursor()

        # insert and get at CL.QUORUM
        for n in xrange(0, 100):
            insert_c1c2(cursor1, n, "QUORUM")
            query_c1c2(cursor2, n, "QUORUM")


        # shutdown a node an test again
        node3.stop(wait_other_notice=True)
        for n in xrange(100, 200):
            insert_c1c2(cursor1, n, "QUORUM")
            query_c1c2(cursor2, n, "QUORUM")

        # shutdown another node and test we get unavailabe exception
        node2.stop(wait_other_notice=True)
        assert_unavailable(insert_c1c2, cursor1, 200, "QUORUM")

    def all_all_test(self):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodes.values()

        cursor1 = self.cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        self.create_cf(cursor1, 'cf')

        cursor2 = self.cql_connection(node2, 'ks').cursor()

        # insert and get at CL.ALL
        for n in xrange(0, 100):
            insert_c1c2(cursor1, n, "ALL")
            query_c1c2(cursor2, n, "ALL")

        # shutdown one node and test we get unavailabe exception
        node3.stop(wait_other_notice=True)
        assert_unavailable(insert_c1c2, cursor1, 100, "ALL")

    def one_one_test(self):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodes.values()

        cursor1 = self.cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        self.create_cf(cursor1, 'cf')

        cursor2 = self.cql_connection(node2, 'ks').cursor()

        # insert and get at CL.ONE
        for n in xrange(0, 100):
            insert_c1c2(cursor1, n, "ONE")
            retry_till_success(query_c1c2, cursor2, n, "ONE", timeout=5)

        # shutdown a node an test again
        node3.stop(wait_other_notice=True)
        for n in xrange(100, 200):
            insert_c1c2(cursor1, n, "ONE")
            retry_till_success(query_c1c2, cursor2, n, "ONE", timeout=5)

        # shutdown a second node an test again
        node2.stop(wait_other_notice=True)
        for n in xrange(200, 300):
            insert_c1c2(cursor1, n, "ONE")
            retry_till_success(query_c1c2, cursor1, n, "ONE", timeout=5)

    def one_all_test(self):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodes.values()

        cursor1 = self.cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        self.create_cf(cursor1, 'cf')

        cursor2 = self.cql_connection(node2, 'ks').cursor()

        # insert and get at CL.ONE
        for n in xrange(0, 100):
            insert_c1c2(cursor1, n, "ONE")
            query_c1c2(cursor2, n, "ALL")

        # shutdown a node an test again
        node3.stop(wait_other_notice=True)
        insert_c1c2(cursor1, 100, "ONE")
        assert_unavailable(query_c1c2, cursor2, 100, "ALL")

    def all_one(self):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodes.values()

        cursor1 = self.cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        self.create_cf(cursor1, 'cf')

        cursor2 = self.cql_connection(node2, 'ks').cursor()

        # insert and get at CL.ONE
        for n in xrange(0, 100):
            insert_c1c2(cursor1, n, "ALL")
            query_c1c2(cursor2, n, "ONE")

        # shutdown a node an test again
        node3.stop(wait_other_notice=True)
        _assert_unavailable(insert_c1c2, cursor1, 100, "ALL")
