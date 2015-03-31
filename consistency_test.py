from dtest import Tester, debug, DISABLE_VNODES
from assertions import assert_unavailable, assert_none
from tools import (create_c1c2_table, insert_c1c2, query_c1c2, insert_columns)
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

class TestConsistency(Tester):

    def cl_cl_prepare(self, write_cl, read_cl, tolerate_missing=False):
        cluster = self.cluster

        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        create_c1c2_table(self, session)

        session2 = self.patient_cql_connection(node2, 'ks')

        # insert and get at CL.QUORUM
        for n in xrange(0, 100):
            insert_c1c2(session, n, write_cl)
            query_c1c2(session2, n, read_cl, tolerate_missing)

        return session, session2

    def quorum_quorum_test(self):
        session, session2 = self.cl_cl_prepare(ConsistencyLevel.QUORUM, ConsistencyLevel.QUORUM)

        #Stop a node and retest
        self.cluster.nodelist()[2].stop()
        for n in xrange(0, 100):
            insert_c1c2(session, n, ConsistencyLevel.QUORUM)
            query_c1c2(session2, n, ConsistencyLevel.QUORUM)

        self.cluster.nodelist()[1].stop()
        assert_unavailable(insert_c1c2, session, 100, ConsistencyLevel.QUORUM)

    def all_all_test(self):
        session, session2 = self.cl_cl_prepare(ConsistencyLevel.ALL, ConsistencyLevel.ALL)

        self.cluster.nodelist()[2].stop()
        assert_unavailable(insert_c1c2, session, 100, ConsistencyLevel.ALL)

    def one_one_test(self):
        session, session2 = self.cl_cl_prepare(
            ConsistencyLevel.ONE, ConsistencyLevel.ONE, tolerate_missing=True)

        #Stop a node and retest
        self.cluster.nodelist()[2].stop()
        for n in xrange(0, 100):
            insert_c1c2(session, n, ConsistencyLevel.ONE)
            query_c1c2(session2, n, ConsistencyLevel.ONE, tolerate_missing=True)


        #Stop a node and retest
        self.cluster.nodelist()[1].stop()
        for n in xrange(0, 100):
            insert_c1c2(session, n, ConsistencyLevel.ONE)
            query_c1c2(session2, n, ConsistencyLevel.ONE, tolerate_missing=False)

    def one_all_test(self):
        session, session2 = self.cl_cl_prepare(ConsistencyLevel.ONE, ConsistencyLevel.ALL)

        #Stop a node and retest
        self.cluster.nodelist()[2].stop()
        for n in xrange(0, 100):
            insert_c1c2(session, n, ConsistencyLevel.ONE)
        assert_unavailable(query_c1c2, session2, 100, ConsistencyLevel.ALL)


        #Stop a node and retest
        self.cluster.nodelist()[1].stop()
        for n in xrange(0, 100):
            insert_c1c2(session, n, ConsistencyLevel.ONE)
        assert_unavailable(query_c1c2, session2, 100, ConsistencyLevel.ALL)

    def all_one_test(self):
        session, session2 = self.cl_cl_prepare(ConsistencyLevel.ALL, ConsistencyLevel.ONE)

        #Stop a node and retest
        self.cluster.nodelist()[2].stop()
        assert_unavailable(insert_c1c2, session, 100, ConsistencyLevel.ALL)
        for n in xrange(0, 100):
            query_c1c2(session2, n, ConsistencyLevel.ONE)

        #Stop a node and retest
        self.cluster.nodelist()[1].stop()
        assert_unavailable(insert_c1c2, session, 100, ConsistencyLevel.ALL)
        for n in xrange(0, 100):
            query_c1c2(session2, n, ConsistencyLevel.ONE)

    def quorum_two_test(self):
        session, session2 = self.cl_cl_prepare(ConsistencyLevel.QUORUM, ConsistencyLevel.TWO)

        #Stop a node and retest
        self.cluster.nodelist()[2].stop()
        for n in xrange(0, 100):
            insert_c1c2(session, n, ConsistencyLevel.QUORUM)
            query_c1c2(session2, n, ConsistencyLevel.TWO)

        self.cluster.nodelist()[1].stop()
        assert_unavailable(insert_c1c2, session, 100, ConsistencyLevel.QUORUM)
        assert_unavailable(query_c1c2, session2, 100, ConsistencyLevel.TWO)

    def quorum_three_test(self):
        session, session2 = self.cl_cl_prepare(ConsistencyLevel.QUORUM, ConsistencyLevel.THREE)

        #Stop a node and retest
        self.cluster.nodelist()[2].stop()
        for n in xrange(0, 100):
            insert_c1c2(session, n, ConsistencyLevel.QUORUM)
        assert_unavailable(query_c1c2, session2, 100, ConsistencyLevel.THREE)

        self.cluster.nodelist()[1].stop()
        assert_unavailable(insert_c1c2, session, 100, ConsistencyLevel.QUORUM)
        assert_unavailable(query_c1c2, session2, 100, ConsistencyLevel.THREE)

    def two_two_test(self):
        session, session2 = self.cl_cl_prepare(ConsistencyLevel.TWO, ConsistencyLevel.TWO)

        #Stop a node and retest
        self.cluster.nodelist()[2].stop()
        for n in xrange(0, 100):
            insert_c1c2(session, n, ConsistencyLevel.TWO)
            query_c1c2(session2, n, ConsistencyLevel.TWO)

        self.cluster.nodelist()[1].stop()
        assert_unavailable(insert_c1c2, session, 100, ConsistencyLevel.TWO)
        assert_unavailable(query_c1c2, session2, 100, ConsistencyLevel.TWO)

    def three_one_test(self):
        session, session2 = self.cl_cl_prepare(ConsistencyLevel.THREE, ConsistencyLevel.ONE)

        #Stop a node and retest
        self.cluster.nodelist()[2].stop()
        assert_unavailable(insert_c1c2, session, 100, ConsistencyLevel.THREE)
        for n in xrange(0, 100):
            query_c1c2(session2, n, ConsistencyLevel.ONE)

        #Stop a node and retest
        self.cluster.nodelist()[1].stop()
        assert_unavailable(insert_c1c2, session, 100, ConsistencyLevel.THREE)
        for n in xrange(0, 100):
            query_c1c2(session2, n, ConsistencyLevel.ONE)

    def short_read_test(self):
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)

        cluster.populate(3).start(wait_other_notice=True)
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0)
        # insert 9 columns in one row
        insert_columns(self, cursor, 0, 9)

        # Deleting 3 first columns with a different node dead each time
        self.stop_delete_and_restart(1, 0)
        self.stop_delete_and_restart(2, 1)
        self.stop_delete_and_restart(3, 2)

        # Query 3 firsts columns
        cursor = self.patient_cql_connection(node1, 'ks')
        query = SimpleStatement('SELECT c, v FROM cf WHERE key=\'k0\' LIMIT 3', consistency_level=ConsistencyLevel.QUORUM)
        rows = cursor.execute(query)
        res = rows
        assert len(res) == 3, 'Expecting 3 values, got %d (%s)' % (len(res), str(res))
        # value 0, 1 and 2 have been deleted
        for i in xrange(1, 4):
            assert res[i-1][1] == 'value%d' % (i+2), 'Expecting value%d, got %s (%s)' % (i+2, res[i-1][1], str(res))

    def short_read_delete_test(self):
        """ Test short reads ultimately leaving no columns alive [#4000] """
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)

        cluster.populate(2).start(wait_other_notice=True)
        [node1, node2] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0)
        # insert 2 columns in one row
        insert_columns(self, cursor, 0, 2)

        # Delete the row while first node is dead
        node1.flush()
        node1.stop(wait_other_notice=True)
        cursor = self.patient_cql_connection(node2, 'ks')

        query = SimpleStatement('DELETE FROM cf WHERE key=\'k0\'', consistency_level=ConsistencyLevel.ONE)
        cursor.execute(query)

        node1.start(wait_other_notice=True)

        # Query first column
        cursor = self.patient_cql_connection(node1, 'ks')

        query = SimpleStatement('SELECT c, v FROM cf WHERE key=\'k0\' LIMIT 1', consistency_level=ConsistencyLevel.QUORUM)
        res = cursor.execute(query)
        assert len(res) == 0, res

    def short_read_quorum_delete_test(self):
        """Test CASSANDRA-8933"""
        cluster = self.cluster
        #Consider however 3 nodes A, B, C (RF=3), and following sequence of operations (all done at QUORUM):

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfere with the test
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)

        cluster.populate(3).start(wait_other_notice=True)
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)

        session.execute("CREATE TABLE t (id int, v int, PRIMARY KEY(id, v)) WITH read_repair_chance = 0.0")
        # we write 1 and 2 in a partition: all nodes get it.
        session.execute(SimpleStatement("INSERT INTO t (id, v) VALUES (0, 1)", consistency_level=ConsistencyLevel.ALL))
        session.execute(SimpleStatement("INSERT INTO t (id, v) VALUES (0, 2)", consistency_level=ConsistencyLevel.ALL))

        # we delete 1: only A and C get it.
        node2.flush()
        node2.stop(wait_other_notice=True)
        session.execute(SimpleStatement("DELETE FROM t WHERE id = 0 AND v = 1", consistency_level=ConsistencyLevel.QUORUM))
        node2.start(wait_other_notice=True)

        # we delete 2: only B and C get it.
        node1.flush()
        node1.stop(wait_other_notice=True)
        session.execute(SimpleStatement("DELETE FROM t WHERE id = 0 AND v = 2", consistency_level=ConsistencyLevel.QUORUM))
        node1.start(wait_other_notice=True)

        # we read the first row in the partition (so with a LIMIT 1) and A and B answer first.
        node3.stop()
        assert_none(session, "SELECT * FROM t WHERE id = 0 LIMIT 1", cl=ConsistencyLevel.QUORUM)

    def hintedhandoff_test(self):
        cluster = self.cluster

        if DISABLE_VNODES:
            cluster.populate(2).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate(2, tokens=tokens).start()
        [node1, node2] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 2)
        create_c1c2_table(self, cursor)

        node2.stop(wait_other_notice=True)

        for n in xrange(0, 100):
            insert_c1c2(cursor, n, ConsistencyLevel.ONE)

        log_mark = node1.mark_log()
        node2.start()
        node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=120)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been delivered via HH
        cursor = self.patient_cql_connection(node2, keyspace='ks')
        for n in xrange(0, 100):
            query_c1c2(cursor, n, ConsistencyLevel.ONE)

    def readrepair_test(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False})

        if DISABLE_VNODES:
            cluster.populate(2).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate(2, tokens=tokens).start()
        [node1, node2] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 2)
        create_c1c2_table(self, cursor, read_repair=1.0)

        node2.stop(wait_other_notice=True)

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, ConsistencyLevel.ONE)

        node2.start(wait_other_notice=True)

       # query everything to cause RR
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, ConsistencyLevel.QUORUM)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been repaired
        cursor = self.patient_cql_connection(node2, keyspace='ks')
        for n in xrange(0, 10000):
            query_c1c2(cursor, n, ConsistencyLevel.ONE)

    def short_read_reversed_test(self):
        cluster = self.cluster

        # Disable hinted handoff and set batch commit log so this doesn't
        # interfer with the test
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False}, batch_commitlog=True)

        cluster.populate(3).start(wait_other_notice=True)
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0)
        # insert 9 columns in one row
        insert_columns(self, cursor, 0, 9)

        # Deleting 3 last columns with a different node dead each time
        self.stop_delete_and_restart(1, 6)
        self.stop_delete_and_restart(2, 7)
        self.stop_delete_and_restart(3, 8)

        # Query 3 firsts columns
        cursor = self.patient_cql_connection(node1, 'ks')
        query = SimpleStatement('SELECT c, v FROM cf WHERE key=\'k0\' ORDER BY c DESC LIMIT 3', consistency_level=ConsistencyLevel.QUORUM)
        rows = cursor.execute(query)
        res = rows
        assert len(res) == 3, 'Expecting 3 values, got %d (%s)' % (len(res), str(res))
        # value 6, 7 and 8 have been deleted
        for i in xrange(0, 3):
            assert res[i][1] == 'value%d' % (5-i), 'Expecting value%d, got %s (%s)' % (5-i, res[i][1], str(res))

    def quorum_available_during_failure_test(self):
        CL = ConsistencyLevel.QUORUM
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
        cursor = self.patient_cql_connection(node2)
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
        cursor = self.patient_cql_connection(next_node, 'ks')
        query = 'BEGIN BATCH '
        query = query + 'DELETE FROM cf WHERE key=\'k0\' AND c=\'c%06d\'; ' % column
        query = query + 'DELETE FROM cf WHERE key=\'k0\' AND c=\'c2\'; '
        query = query + 'APPLY BATCH;'
        simple_query = SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)
        cursor.execute(simple_query)

        to_stop.start(wait_other_notice=True)

