from time import sleep
from dtest import Tester
from assertions import *
from tools import *

class TestConsistency(Tester):

    def quorum_quorum_test(self):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

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
        [node1, node2, node3] = cluster.nodelist()

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
        [node1, node2, node3] = cluster.nodelist()

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
        [node1, node2, node3] = cluster.nodelist()

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

    def all_one_test(self):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

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
        assert_unavailable(insert_c1c2, cursor1, 100, "ALL")

    def short_read_test(self):
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()
        time.sleep(.5)

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0)
        # insert 9 columns in one row
        insert_columns(cursor, 0, 9)
        cursor.close()

        # Deleting 3 first columns with a different node dead each time
        self.stop_delete_and_restart(1, 0)
        self.stop_delete_and_restart(2, 1)
        self.stop_delete_and_restart(3, 2)

        # Query 3 firsts columns
        cursor = self.cql_connection(node1, 'ks').cursor()
        cursor.execute('SELECT FIRST 3 * FROM cf USING CONSISTENCY QUORUM WHERE key=k0')
        assert cursor.rowcount == 1
        res = cursor.fetchone()
        # the key is returned
        assert len(res) - 1 == 3, 'Expecting 3 values (excluding the key), got %d (%s)' % (len(res) - 1, str(res))
        assert res[0] == 'k0', str(res)
        # value 0, 1 and 2 have been deleted
        for i in xrange(1, 4):
            assert res[i] == 'value%d' % (i+2), 'Expecting value%d, got %s (%s)' % (i+2, res[i], str(res))

    def short_read_reversed_test(self):
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test (this must be after the populate)
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()
        time.sleep(.5)

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0)
        # insert 9 columns in one row
        insert_columns(cursor, 0, 9)
        cursor.close()

        # Deleting 3 last columns with a different node dead each time
        self.stop_delete_and_restart(1, 6)
        self.stop_delete_and_restart(2, 7)
        self.stop_delete_and_restart(3, 8)

        # Query 3 firsts columns
        cursor = self.cql_connection(node1, 'ks').cursor()
        cursor.execute('SELECT FIRST 3 REVERSED * FROM cf USING CONSISTENCY QUORUM WHERE key=k0')
        assert cursor.rowcount == 1
        res = cursor.fetchone()
        # the key is returned
        assert len(res) - 1 == 3, 'Expecting 3 values (excluding the key), got %d (%s)' % (len(res) - 1, str(res))
        assert res[0] == 'k0', str(res)
        # value 6, 7 and 8 have been deleted
        for i in xrange(1, 4):
            assert res[i] == 'value%d' % (6-i), 'Expecting value%d, got %s (%s)' % (6-i, res[i], str(res))

    def stop_delete_and_restart(self, node_number, column):
        to_stop = self.cluster.nodes["node%d" % node_number]
        next_node = self.cluster.nodes["node%d" % (((node_number + 1) % 3) + 1)]
        to_stop.flush()
        to_stop.stop(wait_other_notice=True)
        cursor = self.cql_connection(next_node, 'ks').cursor()
        cursor.execute('DELETE c%d, c2 FROM cf USING CONSISTENCY QUORUM WHERE key=k0' % column)
        cursor.close()
        to_stop.set_log_level("DEBUG")
        to_stop.start(wait_other_notice=True)

