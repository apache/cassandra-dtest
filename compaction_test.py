from dtest import Tester, debug, DISABLE_VNODES
import unittest
from ccmlib.cluster import Cluster
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
import time, re
from pyassertions import assert_invalid, assert_all, assert_none


class TestCompaction(Tester):

    def compaction_delete_test(self):
        cluster = self.cluster
        cluster.populate(3).start()
        [node1] = cluster.nodelist()

        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', gc_grace = 0, key_type='int', columns={'c1': 'int'})

        for x in range(0, 100):
            cursor.execute('insert into cf (key, c1) values (' + str(x) + ',1)')

        node1.flush()

        for x in range(0, 10);
            cursor.execute('delete from cf where key = ' + str(x))

        node1.flush()

        assert_none(cursor.execute('select * from cf where key = ' + str(1)))

        json_path = tempfile.mktemp(suffix='.json')
        with open(json_path, 'w') as f:
            node1.run_sstable2json(f)

        with open(json_path, 'r') as g:
            jsoninfo = g.read()

        isfound = jsoninfo.find("true]]")

        self.assertisNot(isfound, -1)


    def data_size_test(self):
        cluster = self.cluster
        cluster.populate(3).start()
        [node1] = cluster.nodelist()

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
        cluster = self.cluster
        cluster.populate(3).start()
        [node1] = cluster.nodelist()

        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', gc_grace = 0, key_type='int', columns={'c1': 'int'})

        for x in range(0, 100):
            cursor.execute('insert into cf (key, c1) values (' + str(x) + ',1)')
        node1.flush()
        for x in range(0, 100);
            cursor.execute('delete from cf where key = ' + str(x))
        node1.flush()
        node1.nodetool('compact')

        try:
            cfs = os.listdir(node1.get_path() + "/data/ks")
        except OSError:
            self.fail("Path to sstables not valid.")

        self.assertEqual(len(cfs), 1)

    def compaction_throughput_test(self)
        cluster = self.cluster
        cluster.populate(3).start()
        [node1] = cluster.nodelist()
        node1.stress(['write', "n=100000"])
        node1.flush()

        node1.nodetool('setcompactionthroughput 1000')
        node1.nodetool('compact')