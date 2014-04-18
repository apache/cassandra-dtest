import time

from dtest import Tester, debug, DISABLE_VNODES
from assertions import assert_unavailable
from tools import (create_c1c2_table, insert_c1c2, query_c1c2, retry_till_success,
                   insert_columns)

class TestConsistency(Tester):

    def quorum_quorum_test(self):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor1 = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        create_c1c2_table(self, cursor1)

        cursor2 = self.patient_cql_connection(node2, 'ks').cursor()

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

        cursor1 = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        create_c1c2_table(self, cursor1)

        cursor2 = self.patient_cql_connection(node2, 'ks').cursor()

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

        cursor1 = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        create_c1c2_table(self, cursor1)

        cursor2 = self.patient_cql_connection(node2, 'ks').cursor()

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

        cursor1 = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        create_c1c2_table(self, cursor1)

        cursor2 = self.patient_cql_connection(node2, 'ks').cursor()

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

        cursor1 = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor1, 'ks', 3)
        create_c1c2_table(self, cursor1)

        cursor2 = self.patient_cql_connection(node2, 'ks').cursor()

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
        # interfer with the test
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()
        time.sleep(.5)

        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0)
        # insert 9 columns in one row
        insert_columns(self, cursor, 0, 9)
        cursor.close()

        # Deleting 3 first columns with a different node dead each time
        self.stop_delete_and_restart(1, 0)
        self.stop_delete_and_restart(2, 1)
        self.stop_delete_and_restart(3, 2)

        # Query 3 firsts columns
        cursor = self.patient_cql_connection(node1, 'ks').cursor()
        if self.cluster.version() >= "1.2":
            cursor.execute('SELECT c, v FROM cf WHERE key=\'k0\' LIMIT 3', consistency_level="QUORUM")
            res = cursor.fetchall()
            assert len(res) == 3, 'Expecting 3 values, got %d (%s)' % (len(res), str(res))
            # value 0, 1 and 2 have been deleted
            for i in xrange(1, 4):
                assert res[i-1][1] == 'value%d' % (i+2), 'Expecting value%d, got %s (%s)' % (i+2, res[i-1][1], str(res))
        else:
            cursor.execute('SELECT FIRST 3 * FROM cf USING CONSISTENCY QUORUM WHERE key=k0')
            assert cursor.rowcount == 1
            res = cursor.fetchone()
            # the key is returned
            assert len(res) == 3, 'Expecting 3 values, got %d (%s)' % (len(res), str(res))
            # value 0, 1 and 2 have been deleted
            for i in xrange(1, 4):
                assert res[i] == 'value%d' % (i+2), 'Expecting value%d, got %s (%s)' % (i+2, res[i], str(res))

    def short_read_delete_test(self):
        """ Test short reads ultimately leaving no columns alive [#4000] """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)

        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()
        time.sleep(.5)

        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0)
        # insert 2 columns in one row
        insert_columns(self, cursor, 0, 2)
        cursor.close()

        # Delete the row while first node is dead
        node1.flush()
        node1.stop(wait_other_notice=True)
        cursor = self.patient_cql_connection(node2, 'ks').cursor()
        if self.cluster.version() >= "1.2":
            cursor.execute('DELETE FROM cf WHERE key=\'k0\'', consistency_level="ONE")
        else:
            cursor.execute('DELETE FROM cf USING CONSISTENCY ONE WHERE key=k0')
        cursor.close()
        node1.start(wait_other_notice=True)
        time.sleep(.5)

        # Query first column
        cursor = self.patient_cql_connection(node1, 'ks').cursor()
        if self.cluster.version() >= "1.2":
            cursor.execute('SELECT c, v FROM cf WHERE key=\'k0\' LIMIT 1', consistency_level="QUORUM")
            res = cursor.fetchone()
            assert cursor.rowcount == 0, res
        else:
            cursor.execute('SELECT FIRST 1 * FROM cf USING CONSISTENCY QUORUM WHERE key=k0')
            assert cursor.rowcount == 1
            res = cursor.fetchone()
            assert len(res) == 0, 'Expecting no value, got %d (%s)' % (len(res), str(res))

    def hintedhandoff_test(self):
        cluster = self.cluster

        if DISABLE_VNODES:
            cluster.populate(2).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate(2, tokens=tokens).start()
        [node1, node2] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 2)
        create_c1c2_table(self, cursor)

        node2.stop(wait_other_notice=True)

        for n in xrange(0, 100):
            insert_c1c2(cursor, n, "ONE")

        log_mark = node1.mark_log()
        node2.start()
        node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=90)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been delivered via HH
        cursor = self.patient_cql_connection(node2, keyspace='ks').cursor()
        for n in xrange(0, 100):
            query_c1c2(cursor, n, "ONE")

    def readrepair_test(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False})

        if DISABLE_VNODES:
            cluster.populate(2).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate(2, tokens=tokens).start()
        [node1, node2] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 2)
        create_c1c2_table(self, cursor, read_repair=1.0)

        node2.stop(wait_other_notice=True)

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, "ONE")

        node2.start(wait_other_notice=True)
        time.sleep(5)
       # query everything to cause RR
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, "QUORUM")

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been repaired
        cursor = self.patient_cql_connection(node2, keyspace='ks').cursor()
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, "ONE")

    def short_read_reversed_test(self):
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()
        time.sleep(.5)

        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0)
        # insert 9 columns in one row
        insert_columns(self, cursor, 0, 9)
        cursor.close()

        # Deleting 3 last columns with a different node dead each time
        self.stop_delete_and_restart(1, 6)
        self.stop_delete_and_restart(2, 7)
        self.stop_delete_and_restart(3, 8)

        # Query 3 firsts columns
        cursor = self.patient_cql_connection(node1, 'ks').cursor()
        if self.cluster.version() >= "1.2":
            cursor.execute('SELECT c, v FROM cf WHERE key=\'k0\' ORDER BY c DESC LIMIT 3', consistency_level="QUORUM")
            res = cursor.fetchall()
            assert len(res) == 3, 'Expecting 3 values, got %d (%s)' % (len(res), str(res))
            # value 6, 7 and 8 have been deleted
            for i in xrange(0, 3):
                assert res[i][1] == 'value%d' % (5-i), 'Expecting value%d, got %s (%s)' % (5-i, res[i][1], str(res))
        else:
            cursor.execute('SELECT FIRST 3 REVERSED * FROM cf USING CONSISTENCY QUORUM WHERE key=k0')
            assert cursor.rowcount == 1
            res = cursor.fetchone()
            # the key is returned
            assert len(res) == 3, 'Expecting 3 values, got %d (%s)' % (len(res), str(res))
            # value 6, 7 and 8 have been deleted
            for i in xrange(0, 3):
                assert res[i] == 'value%d' % (5-i), 'Expecting value%d, got %s (%s)' % (5-i, res[i], str(res))

    def quorum_available_during_failure_test(self):
        CL = 'QUORUM'
        RF = 3

        debug("Creating a ring")
        cluster = self.cluster
        if DISABLE_VNODES:
            cluster.populate(3).start()
        else:
            tokens = cluster.balanced_tokens(3)
            cluster.populate(3, tokens=tokens).start()
        [node1, node2, node3] = cluster.nodelist()
        cluster.start()

        debug("Set to talk to node 2")
        cursor = self.patient_cql_connection(node2).cursor()
        self.create_ks(cursor, 'ks', RF)
        create_c1c2_table(self, cursor)

        debug("Generating some data")
        for n in xrange(100):
            insert_c1c2(cursor, n, CL)

        debug("Taking down node1")
        node1.stop(wait_other_notice=True)

        debug("Reading back data.")
        for n in xrange(100):
            query_c1c2(cursor, n, CL)

    def stop_delete_and_restart(self, node_number, column):
        to_stop = self.cluster.nodes["node%d" % node_number]
        next_node = self.cluster.nodes["node%d" % (((node_number + 1) % 3) + 1)]
        to_stop.flush()
        to_stop.stop(wait_other_notice=True)
        cursor = self.patient_cql_connection(next_node, 'ks').cursor()
        if self.cluster.version() >= "1.2":
            query = 'BEGIN BATCH '
            query = query + 'DELETE FROM cf WHERE key=\'k0\' AND c=\'c%06d\'; ' % column
            query = query + 'DELETE FROM cf WHERE key=\'k0\' AND c=\'c2\'; '
            query = query + 'APPLY BATCH;'
            cursor.execute(query, consistency_level="QUORUM")
        else:
            cursor.execute('DELETE c%06d, c2 FROM cf USING CONSISTENCY QUORUM WHERE key=k0' % column)
        cursor.close()
        to_stop.start(wait_other_notice=True)

