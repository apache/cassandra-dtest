import time

from dtest import Tester, debug

import cql
from cql.cassandra.ttypes import CfDef, ColumnParent, CounterColumn, \
        ConsistencyLevel, ColumnPath

class TestSuperCounterClusterRestart(Tester):
    """
    This test is part of this issue:
    https://issues.apache.org/jira/browse/CASSANDRA-3821
    """

    def functional_test(self):
        NUM_SUBCOLS = 100
        NUM_ADDS = 100

        cluster = self.cluster
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        time.sleep(.5)
        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 3)
        time.sleep(1) # wait for propagation

        # create the columnfamily using thrift
        host, port = node1.network_interfaces['thrift']
        thrift_conn = cql.connect(host, port, keyspace='ks')
        cf_def = CfDef(keyspace='ks', name='cf', column_type='Super',
                default_validation_class='CounterColumnType')
        thrift_conn.client.system_add_column_family(cf_def)

        # let the sediment settle to to the bottom before drinking...
        time.sleep(2)

        for subcol in xrange(NUM_SUBCOLS):
            for add in xrange(NUM_ADDS):
                column_parent = ColumnParent(column_family='cf',
                        super_column='subcol_%d' % subcol)
                counter_column = CounterColumn('col_0', 1)
                thrift_conn.client.add('row_0', column_parent, counter_column,
                        ConsistencyLevel.QUORUM)
        time.sleep(1)

        # flush everything and the problem will be mostly corrected.
#        for node in cluster.nodelist():
#            node.flush()

        debug("Stopping cluster")
        cluster.stop()
        time.sleep(5)
        debug("Starting cluster")
        cluster.start()
        time.sleep(5)

        thrift_conn = cql.connect(host, port, keyspace='ks')

        from_db = []

        for i in xrange(NUM_SUBCOLS):
            column_path = ColumnPath(column_family='cf', column='col_0',
                    super_column='subcol_%d'%i)
            column_or_super_column = thrift_conn.client.get('row_0', column_path,
                    ConsistencyLevel.QUORUM)
            val = column_or_super_column.counter_column.value
            debug(str(val)),
            from_db.append(val)
        debug("")

        expected = [NUM_ADDS for i in xrange(NUM_SUBCOLS)]

        if from_db != expected:
            raise Exception("Expected a bunch of the same values out of the db. Got this: " + str(from_db))
