from dtest import Tester, debug, DISABLE_VNODES
import unittest
from ccmlib.cluster import Cluster
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
import time, re
from pyassertions import assert_invalid, assert_all, assert_none


class TestCompaction(Tester):

    __test__= False

    def compaction_delete_test(self):
        """Test that executing a delete properly tombstones a row.
        Insert data, delete a partition of data and check that the requesite rows are tombstoned
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)

        self.create_ks(cursor, 'ks', 1)
        cursor.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace = 0 and compaction= {'class':" + self.strategy + "};")

        for x in range(0, 100):
            cursor.execute('insert into ks.cf (key, val) values (' + str(x) + ',1)')

        node1.flush()

        for x in range(0, 10):
            cursor.execute('delete from cf where key = ' + str(x))

        node1.flush()

        for x in range(0, 10):
            assert_none(cursor.execute('select * from cf where key = ' + str(x)))

        json_path = tempfile.mktemp(suffix='.json')
        with open(json_path, 'w') as f:
            node1.run_sstable2json(f)

        with open(json_path, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("true]]")
        debug(numfound)

        self.assertEqual(numfound, 10)


    def data_size_test(self):
        """Ensure that data size does not have unwarranted increases after compaction.
        Insert data and check data size before and after a compaction.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=3)'])

        node1.flush()

        output = node1.nodetool('cfstats', True)[0]
        initialValue = 0
        if output.find("Standard1") != -1:
            output = output[output.find("Standard1"):]
            output = output[output.find("Space used (live):"):]
            initialValue = output[output.find(":")+1:output.find("\n")].strip()
        else:
            debug("datasize not found")

        node1.nodetool('compact')

        output = node1.nodetool('cfstats', True)[0]
        finalValue = 0
        if output.find("Standard1") != -1:
            output = output[output.find("Standard1"):]
            output = output[output.find("Space used (live):"):]
            finalValue = output[output.find(":")+1:output.find("\n")].strip()
        else:
            debug("datasize not found")

        self.assertFalse(finalValue >= initialValue)

    def sstable_deletion_test(self):
        """Test that sstables are deleted properly when able to be.
        Insert data setting gc_grace_seconds to 0, and determine sstable
        is deleted upon data deletion.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        cursor.execute("create table cf (key int PRIMARY KEY, val int) with gc_grace = 0 and compaction= {'class':" +self.strategy+"}")

        for x in range(0, 100):
            cursor.execute('insert into cf (key, c1) values (' + str(x) + ',1)')
        node1.flush()
        for x in range(0, 100):
            cursor.execute('delete from cf where key = ' + str(x))
        node1.flush()
        node1.nodetool('compact')

        try:
            cfs = os.listdir(node1.get_path() + "/data/ks")
        except OSError:
            self.fail("Path to sstables not valid.")

        self.assertEqual(len(cfs), 1)

    def compaction_throughput_test(self):
        """Test setting compaction throughput.
        Set throughput, insert data and ensure compaction performance corresponds.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        node1.stress(['write', "n=100000"])
        node1.flush()

        node1.nodetool('setcompactionthroughput -- 100')
        node1.nodetool('compact')

        node1.watch_log_for("100.00MB/s")

    def compaction_strategy_switiching_test(self):
        """Ensure that switching strategies does not result in problems.
        Insert data, switch strategies, then check against data loss.
        """
        strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']

        if self.strategy in strategies: strategies.remove(self.strategy):
            for strat in strategies:
                cluster = self.cluster
                cluster.populate(3).start()
                [node1] = cluster.nodelist()
                cursor = self.patient_cql_connection(node1)

                self.create_ks(cursor, 'ks', 1)
                cursor.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace = 0 and compaction= {'class':" + self.strategy + "};")

                for x in range(0, 100):
                    cursor.execute('insert into ks.cf (key, val) values (' + str(x) + ',1)')

                node1.flush()

                for x in range(0, 10):
                    cursor.execute('delete from cf where key = ' + str(x))

                cursor.execute("alter table ks.cf with compaction = {'class':" + strat + "};")

                for x in range(11,100):
                    assert_one(cursor.execute("select * from ks.cf where key =" + str(x))

                for x in range(0, 10):
                    assert_none(cursor.execute('select * from cf where key = ' + str(x)))

strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']
for strategy in strategies:
    cls_name = ('TestCompaction_with_' + strategy)
    vars()[cls_name] = type(cls_name, (TestCompaction,), {'strategy': strategy, '__test__':True})