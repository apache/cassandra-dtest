from dtest import Tester, debug, DISABLE_VNODES
import unittest
from ccmlib.cluster import Cluster
from ccmlib.node import Node, NodeError, TimeoutError
import time, re
from assertions import assert_invalid, assert_all, assert_none, assert_one
import tempfile
import os

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


        cursor.execute("create table ks.cf (key int PRIMARY KEY, val int) with compaction = {'class':'" + self.strategy + "'} and gc_grace_seconds = 30;")

        for x in range(0, 100):
            cursor.execute('insert into cf (key, val) values (' + str(x) + ',1)')

        node1.flush()
        for x in range(0, 10):
            cursor.execute('delete from cf where key = ' + str(x))

        node1.flush()
        for x in range(0, 10):
            assert_none(cursor, 'select * from cf where key = ' + str(x))

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("markedForDeleteAt")

        self.assertEqual(numfound, 10)

    def data_size_test(self):
        """Ensure that data size does not have unwarranted increases after compaction.
        Insert data and check data size before and after a compaction.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        node1.stress(['write', 'n=100000'])

        node1.flush()

        output = node1.nodetool('cfstats', True)[0]
        initialValue = 0
        if output.find("standard1") != -1:
            output = output[output.find("standard1"):]
            output = output[output.find("Space used (live):"):]
            initialValue = output[output.find(":")+1:output.find("\n")].strip()
        else:
            debug("datasize not found")
            debug(output)

        node1.nodetool('compact')

        output = node1.nodetool('cfstats', True)[0]
        finalValue = 0
        if output.find("standard1") != -1:
            output = output[output.find("standard1"):]
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
        cursor.execute("create table cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and compaction= {'class':'" +self.strategy+"'}")

        for x in range(0, 100):
            cursor.execute('insert into cf (key, val) values (' + str(x) + ',1)')
        node1.flush()
        for x in range(0, 100):
            cursor.execute('delete from cf where key = ' + str(x))
        node1.flush()
        node1.nodetool('compact')

        try:
            cfs = os.listdir(node1.get_path() + "/data/ks")
            ssdir = os.listdir(node1.get_path() + "/data/ks/" + cfs[0])
            for afile in ssdir:
                self.assertFalse("Data" in afile)            

        except OSError:
            self.fail("Path to sstables not valid.")

    def dtcs_deletion_test(self):
        """Test that sstables are deleted properly when able to be.
        Insert data setting max_sstable_age_days low, and determine sstable
        is deleted upon data deletion past max_sstable_age_days.
        """
        if not hasattr(self, 'strategy'):
            self.strategy = 'DateTieredCompactionStrategy'
        elif self.strategy != 'DateTieredCompactionStrategy':
            self.skipTest('Not implemented unless DateTieredCompactionStrategy is used')
            
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        # max sstable age is 0.5 minute:
        cursor.execute("create table cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and compaction= {'class':'DateTieredCompactionStrategy', 'max_sstable_age_days':0.00035, 'min_threshold':2}")

        #insert data
        for x in range(0, 300):
            cursor.execute('insert into cf (key, val) values (' + str(x) + ',1) USING TTL 35')
        node1.flush()
        time.sleep(40)
        expired_sstables = node1.get_sstables('ks', 'cf')
        assert len(expired_sstables) == 1
        expired_sstable = expired_sstables[0]
        # write a new sstable to make DTCS check for expired sstables:
        for x in range(0, 100):
            cursor.execute('insert into cf (key, val) values (%d, %d)'%(x,x))
        node1.flush()
        time.sleep(5)
        assert expired_sstable not in node1.get_sstables('ks','cf')

    def compaction_throughput_test(self):
        """Test setting compaction throughput.
        Set throughput, insert data and ensure compaction performance corresponds.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        node1.stress(['write', "n=100000"])
        node1.flush()

        threshold = "10"

        node1.nodetool('setcompactionthroughput -- ' + threshold)
        node1.nodetool('compact')

        matches = node1.watch_log_for("Compacted")

        stringline = matches[0]
        avgthroughput = stringline[stringline.find('=')+1:stringline.find("MB/s")]
        debug(avgthroughput)

        self.assertGreaterEqual(threshold, avgthroughput)

    def compaction_strategy_switching_test(self):
        """Ensure that switching strategies does not result in problems.
        Insert data, switch strategies, then check against data loss.
        """
        strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']

        if self.strategy in strategies:
            strategies.remove(self.strategy)
            cluster = self.cluster
            cluster.populate(1).start()
            [node1] = cluster.nodelist()


            for strat in strategies:
                cursor = self.patient_cql_connection(node1)
                self.create_ks(cursor, 'ks', 1)

                cursor.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and compaction= {'class':'" + self.strategy + "'};")

                for x in range(0, 100):
                    cursor.execute('insert into ks.cf (key, val) values (' + str(x) + ',1)')

                node1.flush()

                for x in range(0, 10):
                    cursor.execute('delete from cf where key = ' + str(x))

                cursor.execute("alter table ks.cf with compaction = {'class':'" + strat + "'};")

                for x in range(11,100):
                    assert_one(cursor, "select * from ks.cf where key =" + str(x),[x, 1])

                for x in range(0, 10):
                    assert_none(cursor, 'select * from cf where key = ' + str(x))

                node1.flush()
                cluster.clear()
                time.sleep(5)
                cluster.start()

strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']
for strategy in strategies:
    cls_name = ('TestCompaction_with_' + strategy)
    vars()[cls_name] = type(cls_name, (TestCompaction,), {'strategy': strategy, '__test__':True})

