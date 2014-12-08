from dtest import Tester, debug
from pytools import insert_c1c2, since
from cassandra import ConsistencyLevel
import unittest

class TestRepairCompaction(Tester):

    @since('2.1')
    def repair_compaction_fine_test(self):
        """Check that we do not stream data for repairs past the last repair.
        Check cases i) node goes down, data inserted, node comes up run repair
        ii) after case i, do similar with a different node down (since compaction has seperated repaired)
        iii) check that repair works appropriately where a new node is replacing
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={ 'hinted_handoff_enabled' : False, 'auto_snapshot': False}, batch_commitlog=False)
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        debug("insert data into all")

        for x in range(1, 5):
            insert_c1c2(cursor, x, ConsistencyLevel.ALL)
        node1.flush()

        debug("bringing down node 3")
        node3.flush()
        node3.stop(gently=False)

        debug("inserting additional data into node 1 and 2")
        for y in range(5, 10):
            insert_c1c2(cursor, y, ConsistencyLevel.TWO)
        node1.flush()
        node2.flush()

        debug("restarting and repairing node 3")
        node3.start()
        node3.repair()

        sstableNode1 = node3.grep_log("reading file from /127.0.0.1, repairedAt = 0")
        sstableNode2 = node3.grep_log("reading file from /127.0.0.2, repairedAt = 0")
        catchBadSSReads = node3.grep_log("reading file from .* repairedAt = ([1-9])")
        self.assertGreaterEqual(len(sstableNode1), 1)
        self.assertGreaterEqual(len(sstableNode2), 1)
        self.assertLess(len(catchBadSSReads), 1)

        debug("stopping node 2")
        node2.stop(gently=False)

        debug("inserting data in nodes 1 and 3")
        for z in range(10, 15):
            insert_c1c2(cursor, z, ConsistencyLevel.TWO)
        node1.flush()

        debug("start and repair node 2")
        node2.flush()
        node2.start()
        node2.repair()

        fileFromNode1 = node2.grep_log("reading file from /127.0.0.1, repairedAt = 0")
        fileFromNode3 = node2.grep_log("reading file from /127.0.0.3, repairedAt = 0")
        catchBadReads = node2.grep_log("reading file from .* repairedAt = ([1-9])")
        self.assertGreaterEqual(len(fileFromNode1), 1)
        self.assertGreaterEqual(len(fileFromNode3), 1)
        self.assertLess(len(catchBadReads), 1)

        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4',9042))
        node4.start()

        debug("replace node and check repair-like process")
        node3.stop(gently=False)
        node5 = Node('node5', cluster, True, ('127.0.0.5', 9160), ('127.0.0.5', 7000), '7500', '0', None, ('127.0.0.5',9042))
        cluster.add(node5, False)
        node5.start(replace_address = '127.0.0.3', wait_other_notice=True)

        fileRead = node5.grep_log("reading file from .*, repairedAt = 0")
        self.assertGreaterEqual(len(fileRead), 1)

        #additionally should see 14 distinct keys in data(this prints to command line)
        debug((node2.run_sstable2json()))

        rows = cursor.execute("SELECT COUNT(*) FROM ks.cf LIMIT 100")

        results = rows[0]
        debug(results)

        self.assertEqual(results[0], 14)