from dtest import Tester
from assertions import *
from tools import *

import os, sys, time, tools
from ccmlib.cluster import Cluster

class TestPutGet(Tester):

    def putget_test(self):
        """ Simple put/get on a single row, hitting multiple sstables """
        self._putget()

    def putget_snappy_test(self):
        """ Simple put/get on a single row, but hitting multiple sstables (with snappy compression) """
        self._putget(compression="Snappy")

    def putget_deflate_test(self):
        """ Simple put/get on a single row, but hitting multiple sstables (with deflate compression) """
        self._putget(compression="Deflate")

    # Simple queries, but with flushes in between inserts to make sure we hit
    # sstables (and more than one) on reads
    def _putget(self, compression=None):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', compression=compression)

        tools.putget(cluster, cursor)

    def non_local_read_test(self):
        """ This test reads from a coordinator we know has no copy of the data """
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 2)
        self.create_cf(cursor, 'cf')

        # insert and get at CL.QUORUM (since RF=2, node1 won't have all key locally)
        for n in xrange(0, 1000):
            tools.insert_c1c2(cursor, n, "QUORUM")
            tools.query_c1c2(cursor, n, "QUORUM")

    def rangeputget_test(self):
        """ Simple put/get on ranges of rows, hitting multiple sstables """

        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 2)
        self.create_cf(cursor, 'cf')

        tools.range_putget(cluster, cursor)
