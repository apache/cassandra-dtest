from dtest import Tester

import time
from jmxutils import make_mbean, JolokiaAgent, remove_perf_disable_shared_mem


class TestDeletion(Tester):

    def gc_test(self):
        """ Test that tombstone are fully purge after gc_grace """
        cluster = self.cluster

        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        time.sleep(.5)
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        self.create_cf(session, 'cf', gc_grace=0, key_type='int', columns={'c1': 'int'})

        session.execute('insert into cf (key, c1) values (1,1)')
        session.execute('insert into cf (key, c1) values (2,1)')
        node1.flush()

        result = session.execute('select * from cf;')
        assert len(result) == 2 and len(result[0]) == 2 and len(result[1]) == 2, result

        session.execute('delete from cf where key=1')
        result = session.execute('select * from cf;')
        if cluster.version() < '1.2':  # > 1.2 doesn't show tombstones
            assert len(result) == 2 and len(result[0]) == 1 and len(result[1]) == 1, result

        node1.flush()
        time.sleep(.5)
        node1.compact()
        time.sleep(.5)

        result = session.execute('select * from cf;')
        assert len(result) == 1 and len(result[0]) == 2, result

    def tombstone_size_test(self):
        self.cluster.populate(1)
        node1 = self.cluster.nodelist()[0]

        remove_perf_disable_shared_mem(node1)

        self.cluster.start(wait_for_binary_proto=True)
        [node1] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE test (i int PRIMARY KEY)')

        stmt = session.prepare('DELETE FROM test where i = ?')
        for i in range(100):
            session.execute(stmt, [i])

        self.assertEqual(memtable_count(node1, 'ks', 'test'), 100)
        self.assertGreater(memtable_size(node1, 'ks', 'test'), 0)


def memtable_size(node, keyspace, table):
    version = node.get_cassandra_version()
    name = 'MemtableOnHeapSize' if version >= '2.1' else 'MemtableDataSize'
    return columnfamily_metric(node, keyspace, table, name)


def memtable_count(node, keyspace, table):
    return columnfamily_metric(node, keyspace, table, 'MemtableColumnsCount')


def columnfamily_metric(node, keyspace, table, name):
    with JolokiaAgent(node) as jmx:
        mbean = make_mbean('metrics', type='ColumnFamily',
                           name=name, keyspace=keyspace, scope=table)
        value = jmx.read_attribute(mbean, 'Value')

    return value
