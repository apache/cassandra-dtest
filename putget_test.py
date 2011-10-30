from dtest import Tester
from tools import *
from assertions import *

import os, sys, time
from ccmlib.cluster import Cluster

class TestPutGet(Tester):

    def putget_test(self):
        self._putget()

    def putget_snappy_test(self):
        self._putget(compression="Snappy")

    def putget_deflate_test(self):
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

        #cursor.close()
        #time.sleep(.5)
        #cli = node1.cli()
        #cli.do("use ks")
        #cli.do("create column family cf with comparator=UTF8Type and key_validation_class=UTF8Type and default_validation_class=UTF8Type and compression_options={sstable_compression:%sCompressor}" % compression)
        #cli.close()
        #time.sleep(.5)
        #cursor = self.cql_connection(node1, 'ks').cursor()

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

    def gc_test(self):
        cluster = self.cluster

        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        time.sleep(.5)
        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', gc_grace=0, key_type='int', columns={'c1': 'int'})

        cursor.execute('insert into cf (key, c1) values (1,1)')
        cursor.execute('insert into cf (key, c1) values (2,1)')
        node1.flush()

        cursor.execute('select * from cf;')
        result = cursor.fetchall()
        assert len(result) == 2 and len(result[0]) == 2 and len(result[1]) == 2, result

        cursor.execute('delete from cf where key=1')
        cursor.execute('select * from cf;')
        result = cursor.fetchall()
        assert len(result) == 2 and len(result[0]) == 1 and len(result[1]) == 2, result

        node1.flush()
        node1.compact()

        cursor.execute('select * from cf;')
        result = cursor.fetchall()
        assert len(result) == 1 and len(result[0]) == 2, result

    def non_local_read_test(self):
        """ This test reads where we know the coordinator has no copy of the data """
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 2)
        self.create_cf(cursor, 'cf')

        # insert and get at CL.QUORUM (since RF=2, node1 won't have all key locally)
        for n in xrange(0, 1000):
            insert_c1c2(cursor, n, "QUORUM")
            query_c1c2(cursor, n, "QUORUM")
