import time
from ccmlib.node import Node
from decorator  import decorator
import cql
import re

from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

def retry_till_success(fun, *args, **kwargs):
    if 'timeout' in kwargs:
        timeout = kwargs['timeout']
        del(kwargs['timeout']) # don't pass timeout to the function
    else:
        timeout = 60

    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except:
            if time.time() > deadline:
                raise

def insert_c1c2(cursor, key, consistency="QUORUM"):
    cursor.execute('UPDATE cf USING CONSISTENCY %s SET c1=value1, c2=value2 WHERE key=k%d' % (consistency, key))

def insert_columns(cursor, key, columns_count, consistency="QUORUM"):
    kvs = [ "c%d=value%d" % (i, i) for i in xrange(0, columns_count)]
    query = 'UPDATE cf USING CONSISTENCY %s SET %s WHERE key=k%d' % (consistency, ', '.join(kvs), key)
    cursor.execute(query)

def query_c1c2(cursor, key, consistency="QUORUM"):
    cursor.execute('SELECT c1, c2 FROM cf USING CONSISTENCY %s WHERE key=k%d' % (consistency, key))
    assert cursor.rowcount == 1
    res = cursor.fetchone()
    assert len(res) == 2 and res[0] == 'value1' and res[1] == 'value2'

def query_columns(cursor, key, columns_count, consistency="QUORUM"):
    cursor.execute('SELECT c0..c%d FROM cf USING CONSISTENCY %s WHERE key=k%d' % (columns_count, consistency, key))
    assert cursor.rowcount == 1
    res = cursor.fetchone()
    assert len(res) == columns_count
    for i in xrange(0, columns_count):
        assert res[i] == 'value%d' % i

def remove_c1c2(cursor, key, consistency="QUORUM"):
    cursor.execute('DELETE c1, c2 FROM cf USING CONSISTENCY %s WHERE key=k%d' % (consistency, key))

# work for cluster started by populate
def new_node(cluster, bootstrap=True, token=None):
    i = len(cluster.nodes) + 1
    node = Node('node%s' % i,
                cluster,
                bootstrap,
                ('127.0.0.%s' % i, 9160),
                ('127.0.0.%s' % i, 7000),
                str(7000 + i * 100),
                token)
    cluster.add(node, not bootstrap)
    return node

def _put_with_overwrite(cluster, cursor, nb_keys, cl="QUORUM"):
    for k in xrange(0, nb_keys):
        kvs = [ "c%02d=value%d" % (i, i) for i in xrange(0, 100) ]
        cursor.execute('UPDATE cf USING CONSISTENCY %s SET %s WHERE key=k%s' % (cl, ','.join(kvs), k))
        time.sleep(.01)
    cluster.flush()
    for k in xrange(0, nb_keys):
        kvs = [ "c%02d=value%d" % (i*2, i*4) for i in xrange(0, 50) ]
        cursor.execute('UPDATE cf USING CONSISTENCY %s SET %s WHERE key=k%d' % (cl, ','.join(kvs), k))
        time.sleep(.01)
    cluster.flush()
    for k in xrange(0, nb_keys):
        kvs = [ "c%02d=value%d" % (i*5, i*20) for i in xrange(0, 20) ]
        cursor.execute('UPDATE cf USING CONSISTENCY %s SET %s WHERE key=k%d' % (cl, ','.join(kvs), k))
        time.sleep(.01)
    cluster.flush()

def _validate_row(res):
    assert len(res) == 100
    for i in xrange(0, 100):
        if i % 5 == 0:
            assert res[i] == 'value%d' % (i*4), 'for %d, expecting value%d, got %s' % (i, i*4, res[i])
        elif i % 2 == 0:
            assert res[i] == 'value%d' % (i*2), 'for %d, expecting value%d, got %s' % (i, i*2, res[i])
        else:
            assert res[i] == 'value%d' % i, 'for %d, expecting value%d, got %s' % (i, i, res[i])

# Simple puts and get (on one row), testing both reads by names and by slice,
# with overwrites and flushes between inserts to make sure we hit multiple
# sstables on reads
def putget(cluster, cursor, cl="QUORUM"):

    _put_with_overwrite(cluster, cursor, 1, cl)

    # reads by name
    ks = [ "c%02d" % i for i in xrange(0, 100) ]
    cursor.execute('SELECT %s FROM cf USING CONSISTENCY %s WHERE key=k0' % (','.join(ks), cl))
    assert cursor.rowcount == 1
    res = cursor.fetchone()
    _validate_row(res)

    # slice reads
    cursor.execute('SELECT * FROM cf USING CONSISTENCY %s WHERE key=k0' % cl)
    assert cursor.rowcount == 1
    res = cursor.fetchone()[1:] # removing key
    _validate_row(res)

# Simple puts and range gets, with overwrites and flushes between inserts to
# make sure we hit multiple sstables on reads
def range_putget(cluster, cursor, cl="QUORUM"):
    keys = 100

    _put_with_overwrite(cluster, cursor, keys, cl)

    cursor.execute('SELECT * FROM cf USING CONSISTENCY %s WHERE key >= \'\'' % cl)
    assert cursor.rowcount == 100
    for res in cursor:
        res = res[1:] # removing key
        _validate_row(res)

class since(object):
    def __init__(self, cass_version):
        self.cass_version = cass_version

    def __call__(self, f):
        def wrapped(obj):
            if obj.cluster.version() < self.cass_version:
                obj.skip("%s < %s" % (obj.cluster.version(), self.cass_version))
            f(obj)
        wrapped.__name__ = f.__name__
        wrapped.__doc__ = f.__doc__
        return wrapped

class require(object):
    def __init__(self, msg):
        self.msg = msg

    def __call__(self, f):
        def wrapped(obj):
            obj.skip("require " + self.msg)
            f(obj)
        wrapped.__name__ = f.__name__
        wrapped.__doc__ = f.__doc__
        return wrapped

def not_implemented(f):
    def wrapped(obj):
        obj.skip("this test not implemented")
        f(obj)
    wrapped.__name__ = f.__name__
    wrapped.__doc__ = f.__doc__
    return wrapped


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
            if re.findall('0\.7\.\d+', node.get_cassandra_dir()):
                cassandra_interface='07'
        self.node = node; self.host = host; self.port = port; self.cassandra_interface = cassandra_interface

        # import the correct version of the cassandra thrift interface
        # and set self.Cassandra as the imported module
        module_name = 'cassandra.v%s' % cassandra_interface
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
        self.use_ks()
        time.sleep(.5)
        return self


    def use_ks(self):
        retry_till_success(self.client.set_keyspace, self.ks_name, timeout=30)
        return self
        
    
    def create_cf(self):
        cf_def = self.Cassandra.CfDef(name=self.cf_name, keyspace=self.ks_name)
        retry_till_success(self.client.system_add_column_family, cf_def, timeout=30)
        return self


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


