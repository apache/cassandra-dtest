from dtest import Tester

import os, sys, time
from ccmlib.cluster import Cluster
from tools import require, since
from jmxutils import make_mbean, JolokiaAgent


class TestDeletion(Tester):

    def gc_test(self):
        """ Test that tombstone are fully purge after gc_grace """
        cluster = self.cluster

        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        time.sleep(.5)
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf', gc_grace=0, key_type='int', columns={'c1': 'int'})

        cursor.execute('insert into cf (key, c1) values (1,1)')
        cursor.execute('insert into cf (key, c1) values (2,1)')
        node1.flush()

        result = cursor.execute('select * from cf;')
        assert len(result) == 2 and len(result[0]) == 2 and len(result[1]) == 2, result

        cursor.execute('delete from cf where key=1')
        result = cursor.execute('select * from cf;')
        if cluster.version() < '1.2': # > 1.2 doesn't show tombstones
            assert len(result) == 2 and len(result[0]) == 1 and len(result[1]) == 1, result

        node1.flush()
        time.sleep(.5)
        node1.compact()
        time.sleep(.5)

        result = cursor.execute('select * from cf;')
        assert len(result) == 1 and len(result[0]) == 2, result

    @require(9194)
    def tombstone_size_test(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = self.cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('CREATE TABLE test (i int PRIMARY KEY)')

        stmt = cursor.prepare('DELETE FROM test where i = ?')
        for i in range(100):
            cursor.execute(stmt, [i])

        self.assertEqual(memtable_count(node1, 'ks', 'test'), 100)
        self.assertGreater(memtable_size(node1, 'ks', 'test'), 0)


def memtable_size(node, keyspace, table):
    new_name = node.get_cassandra_version() >= '2.1'
    name = 'MemtableLiveDataSize' if new_name else 'MemtableDataSize'
    return columnfamily_metric(node, keyspace, table, name)


def memtable_count(node, keyspace, table):
    return columnfamily_metric(node, keyspace, table, 'MemtableColumnsCount')


def columnfamily_metric(node, keyspace, table, name):
    with JolokiaAgent(node) as jmx:
        mbean = make_mbean('metrics', type='ColumnFamily',
                           name=name, keyspace=keyspace, scope=table)
        value = jmx.read_attribute(mbean, 'Value')

    return value
