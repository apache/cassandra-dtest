from dtest import Tester
from tools import *
from assertions import *

import os, sys, time
from ccmlib.cluster import Cluster

class TestRangeGhosts(Tester):

    def ghosts_test(self):
        """ Check range ghost are correctly removed by the system """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        time.sleep(.5)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', gc_grace=0)

        rows = 1000

        for i in xrange(0, rows):
            cursor.execute("UPDATE cf SET c = 'value' WHERE key = k%i" % i)

        cursor.execute("SELECT * FROM cf LIMIT 10000")
        res = cursor.fetchall()
        assert len(res) == rows, res

        node1.flush()

        for i in xrange(0, rows/2):
            cursor.execute("DELETE FROM cf WHERE key = k%i" % i)

        cursor.execute("SELECT * FROM cf LIMIT 10000")
        res = cursor.fetchall()
        assert len(res) == rows, res

        node1.flush()
        time.sleep(1) # make sure tombstones are collected
        node1.compact()

        cursor.execute("SELECT * FROM cf LIMIT 10000")
        res = cursor.fetchall()
        assert len(res) == rows/2, res
