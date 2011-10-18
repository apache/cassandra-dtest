from dtest import Tester
from tools import *
from assertions import *

import os, sys, time
from ccmlib.cluster import Cluster

class TestPutGet(Tester):

    # Simple queries, but with flushes in between inserts to make sure we hit
    # sstables (and more than one) on reads
    def putget_test(self):
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf')
        kvs = [ "c%d=value%d" % (i, i) for i in xrange(0, 10) ]
        cursor.execute('UPDATE cf SET %s WHERE key=k0' % (','.join(kvs)))
        cluster.flush()
        kvs = [ "c%d=value%d" % (i*2, i*4) for i in xrange(0, 5) ]
        cursor.execute('UPDATE cf SET %s WHERE key=k0' % (','.join(kvs)))
        cluster.flush()
        kvs = [ "c%d=value%d" % (i*5, i*10) for i in xrange(0, 2) ]
        cursor.execute('UPDATE cf SET %s WHERE key=k0' % (','.join(kvs)))
        cluster.flush()

        # reads by name
        ks = [ "c%d" % i for i in xrange(0, 10) ]
        cursor.execute('SELECT %s FROM cf WHERE key=k0' % (','.join(ks)))
        assert cursor.rowcount == 1
        res = cursor.fetchone()
        assert len(res) == 10
        for i in [0, 1, 3, 7, 9]:
            assert res[i] == 'value%d' % i
        for i in [2, 4, 6, 8]:
            assert res[i] == 'value%d' % (i*2)
        assert res[5] == 'value10'

        # slice reads
        cursor.execute('SELECT * FROM cf WHERE key=k0')
        assert cursor.rowcount == 1
        res = cursor.fetchone()[1:] # removing key
        assert len(res) == 10
        for i in [0, 1, 3, 7, 9]:
            assert res[i] == 'value%d' % i
        for i in [2, 4, 6, 8]:
            assert res[i] == 'value%d' % (i*2)
        assert res[5] == 'value10'
