from dtest import Tester
import pytools as tools
from pytools import no_vnodes, create_c1c2_table, retry_till_success
from cassandra import ConsistencyLevel

import time, re

from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

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

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf', compression=compression)

        tools.putget(cluster, cursor)

    def non_local_read_test(self):
        """ This test reads from a coordinator we know has no copy of the data """
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 2)
        create_c1c2_table(self, cursor)

        # insert and get at CL.QUORUM (since RF=2, node1 won't have all key locally)
        for n in xrange(0, 1000):
            tools.insert_c1c2(cursor, n, ConsistencyLevel.QUORUM)
            tools.query_c1c2(cursor, n, ConsistencyLevel.QUORUM)

    def rangeputget_test(self):
        """ Simple put/get on ranges of rows, hitting multiple sstables """

        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 2)
        self.create_cf(cursor, 'cf')

        tools.range_putget(cluster, cursor)

    def wide_row_test(self):
        """ Test wide row slices """
        cluster = self.cluster

        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        self.create_cf(cursor, 'cf')

        key = 'wide'

        for x in xrange(1, 5001):
            tools.insert_columns(self, cursor, key, 100, offset=x-1)

        for size in (10, 100, 1000):
            for x in xrange(1, (50001 - size) / size):
                tools.query_columns(self, cursor, key, size, offset=x*size-1)

    @no_vnodes()
    def wide_slice_test(self):
        """
        Check slicing a wide row.
        See https://issues.apache.org/jira/browse/CASSANDRA-4919

        From Sylvain about duplicating:

        Ok, so now that I think about it, you can't reproduce that with CQL currently.
        You'll have to use the thrift get_paged_slice call as it's the only way to
        trigger this.

        Then, I think you'll be able to reproduce with the following steps:
        1) you'd want to use 2 nodes with RF=1 and with ByteOrderedPartitioner (it's
        possible to reproduce with a random partitioner but a tad more painful)
        2) picks token for the nodes so that you know what goes on which node. For
        example you may want that any row key starting with 'a' goes on node1, and
        anything starting with a 'b' goes on node 2.
        3) insers data that span the two nodes. Say inserts 20 rows 'a0' ... 'a9' and
        'b0' ...'b9' (so 10 rows on each node) with say 10 columns on row.
        4) then do a get_paged_slice for keys 'a5' to 'b4' and for the column filter, a
        slice filter that picks the fifth last columns.
        5) the get_paged_slice is supposed to return 95 columns (it should return the 5
        last columns of a5 and then all 10 columns for 'a6' to 'b4'), but without
        CASSANDRA-4919 it will return 90 columns only (it will only return the 5 last
        columns of 'b0').
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'partitioner': 'org.apache.cassandra.dht.ByteOrderedPartitioner'})
        cluster.populate(2)
        [node1, node2] = cluster.nodelist()
        node1.set_configuration_options(values={'initial_token': "a".encode('hex')  })
        node1.set_configuration_options(values={'initial_token': "b".encode('hex')  })
        cluster.start()
        time.sleep(.5)
        cursor = self.patient_cql_connection(node1, version="2.0.0")
        self.create_ks(cursor, 'ks', 1)

        query = """
            CREATE TABLE test (
                k text PRIMARY KEY,
                col0 int,
                col1 int,
                col2 int,
                col3 int,
                col4 int,
                col5 int,
                col6 int,
                col7 int,
                col8 int,
                col9 int
            );
        """
        cursor.execute(query)
        time.sleep(.5)

        for i in xrange(10):
            key_num = str(i).zfill(2)
            query1 = "INSERT INTO test (k, col0, col1, col2, col3, col4, col5, col6, col7, col8, col9) VALUES ('a%s', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)" % (key_num)
            query2 = "INSERT INTO test (k, col0, col1, col2, col3, col4, col5, col6, col7, col8, col9) VALUES ('b%s', 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)" % (key_num)
            cursor.execute(query1)
            cursor.execute(query2)

        cursor.shutdown()

        tc = ThriftConnection(node1, ks_name='ks', cf_name='test')
        tc.use_ks()

        # Slice on the keys
        rnge = tc.Cassandra.KeyRange(
            start_key="a%s" % ('5'.zfill(2)),
            end_key="b%s" % ('4'.zfill(2)),
            count=9999,
        )
        rows = tc.client.get_paged_slice(
            column_family='test',
            range=rnge,
            start_column='col5',
            consistency_level=tc.Cassandra.ConsistencyLevel.ONE,
        )
        keys = [fd.key for fd in rows]
        columns = []
        for row in rows:
            cols = [col.column.name for col in row.columns]
            columns.extend(cols)
            #print row.key
            #print cols

        assert len(columns) == 95, "Regression in cassandra-4919. Expected 95 columns, got %d." % len(columns)

class ThriftConnection(object):
    """
    A thrift connection. For when CQL doesn't do what we need.
    """

    def __init__(self, node=None, host=None, port=None, ks_name='ks', cf_name='cf',
            cassandra_interface='11'):
        """
        initializes the connection.
         - node: a ccm node. If supplied, the host and port, and cassandra_interface
           will be pulled from the node.
         - host, port: overwritten if node is supplied
         - ks_name, cf_name: all operations are done on the supplied ks and cf
         - cassandra_interface: '07' and '11' are currently supported. This is the
           thrift interface to cassandra. '11' suffices for now except when creating
           keyspaces against cassandra0.7, in which case 07 must be used.
        """
        if node:
            host, port = node.network_interfaces['thrift']
        self.node = node
        self.host = host
        self.port = port
        self.cassandra_interface = cassandra_interface

        # import the correct version of the cassandra thrift interface
        # and set self.Cassandra as the imported module
        module_name = 'cassandra-thrift.v%s' % cassandra_interface
        imp = __import__(module_name, globals(), locals(), ['Cassandra'])
        self.Cassandra = imp.Cassandra

        socket = TSocket.TSocket(host, port)
        self.transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
        self.client = self.Cassandra.Client(protocol)

        socket.open()
        self.open_socket = True

        self.ks_name = ks_name
        self.cf_name = cf_name

    def create_ks(self, replication_factor=1):
        if self.cassandra_interface == '07':
            ks_def = self.Cassandra.KsDef(name=self.ks_name,
                    strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                    replication_factor=int(replication_factor),
                    cf_defs=[])
        else:
            ks_def = self.Cassandra.KsDef(name=self.ks_name,
                    strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                    strategy_options={'replication_factor': str(replication_factor)},
                    cf_defs=[])
        retry_till_success(self.client.system_add_keyspace, ks_def, timeout=30)
        time.sleep(0.5)
        retry_till_success(self.wait_for_agreement, timeout=10)
        time.sleep(0.5)
        self.use_ks()
        return self

    def use_ks(self):
        retry_till_success(self.client.set_keyspace, self.ks_name, timeout=30)
        return self


    def create_cf(self):
        cf_def = self.Cassandra.CfDef(name=self.cf_name, keyspace=self.ks_name)
        retry_till_success(self.client.system_add_column_family, cf_def, timeout=30)
        time.sleep(0.5)
        retry_till_success(self.wait_for_agreement, timeout=10)
        time.sleep(0.5)
        return self

    def wait_for_agreement(self):
        schemas = self.client.describe_schema_versions()
        if len([ss for ss in schemas.keys() if ss != 'UNREACHABLE']) > 1:
            raise Exception("schema agreement not reached")

    def _translate_cl(self, cl):
        return self.Cassandra.ConsistencyLevel._NAMES_TO_VALUES[cl]


    def insert_columns(self, num_rows=10, consistency_level='QUORUM'):
        """ Insert some basic values """
        cf_parent = self.Cassandra.ColumnParent(column_family=self.cf_name)

        for row_key in ('row_%d'%i for i in xrange(num_rows)):
            col = self.Cassandra.Column(name='col_0', value='val_0',
                    timestamp=int(time.time()*1000))
            retry_till_success(self.client.insert,
                    key=row_key, column_parent=cf_parent, column=col,
                    consistency_level=self._translate_cl(consistency_level),
                    timeout=30)
        return self


    def query_columns(self, num_rows=10, consistency_level='QUORUM'):
        """ Check that the values inserted in insert_columns() are present """
        for row_key in ('row_%d'%i for i in xrange(num_rows)):
            cpath = self.Cassandra.ColumnPath(column_family=self.cf_name,
                    column='col_0')
            cosc = retry_till_success(self.client.get, key=row_key, column_path=cpath,
                    consistency_level=self._translate_cl(consistency_level),
                    timeout=30)
            col = cosc.column
            value = col.value
            assert value == 'val_0', "column did not have the same value that was inserted!"
        return self