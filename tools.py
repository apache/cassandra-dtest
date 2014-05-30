import time
from ccmlib.node import Node
from decorator  import decorator
from distutils.version import LooseVersion
import cql
import re
import os
import sys
import fileinput

from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol

def retry_till_success(fun, *args, **kwargs):
    timeout = kwargs.pop('timeout', 60)
    bypassed_exception = kwargs.pop('bypassed_exception', Exception)

    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.time() > deadline:
                raise
            else:
                # brief pause before next attempt
                time.sleep(0.25)

def create_c1c2_table(tester, cursor, read_repair=None):
    tester.create_cf(cursor, 'cf', columns={ 'c1' : 'text', 'c2' : 'text' }, read_repair=read_repair)

def insert_c1c2(cursor, key, consistency="QUORUM"):
    if cursor.cql_major_version >= 3:
        cursor.execute('UPDATE cf SET c1=\'value1\', c2=\'value2\' WHERE key=\'k%d\'' % key, consistency_level=consistency)
    else:
        cursor.execute('UPDATE cf USING CONSISTENCY %s SET c1=\'value1\', c2=\'value2\' WHERE key=\'k%d\'' % (consistency, key))

def insert_columns(tester, cursor, key, columns_count, consistency="QUORUM", offset=0):
    if tester.cluster.version() >= "1.2":
        upds = [ "UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%06d\'" % (i, key, i) for i in xrange(offset*columns_count, columns_count*(offset+1))]
        query = 'BEGIN BATCH %s; APPLY BATCH' % '; '.join(upds)
        cursor.execute(query, consistency_level=consistency)
    else:
        kvs = [ "c%06d=value%d" % (i, i) for i in xrange(offset*columns_count, columns_count*(offset+1))]
        query = 'UPDATE cf USING CONSISTENCY %s SET %s WHERE key=k%s' % (consistency, ', '.join(kvs), key)
        cursor.execute(query)

def query_c1c2(cursor, key, consistency="QUORUM"):
    if cursor.cql_major_version >= 3:
        cursor.execute('SELECT c1, c2 FROM cf WHERE key=\'k%d\'' % key, consistency_level=consistency)
    else:
        cursor.execute('SELECT c1, c2 FROM cf USING CONSISTENCY %s WHERE key=\'k%d\'' % (consistency, key))
    assert cursor.rowcount == 1
    res = cursor.fetchone()
    assert len(res) == 2 and res[0] == 'value1' and res[1] == 'value2', res

def query_columns(tester, cursor, key, columns_count, consistency="QUORUM", offset=0):
    if tester.cluster.version() >= "1.2":
        cursor.execute('SELECT c, v FROM cf WHERE key=\'k%s\' AND c >= \'c%06d\' AND c <= \'c%06d\'' % (key, offset, columns_count+offset-1), consistency_level=consistency)
        res = cursor.fetchall()
        assert len(res) == columns_count, "%s != %s (%s-%s)" % (len(res), columns_count, offset, columns_count+offset-1)
        for i in xrange(0, columns_count):
            assert res[i][1] == 'value%d' % (i+offset)
    else:
        cursor.execute('SELECT c%06d..c%06d FROM cf USING CONSISTENCY %s WHERE key=k%s' % (offset, columns_count+offset-1, consistency, key))
        assert cursor.rowcount == 1
        res = cursor.fetchone()
        assert len(res) == columns_count, "%s != %s (%s-%s)" % (len(res), columns_count, offset, columns_count+offset-1)
        for i in xrange(0, columns_count):
            assert res[i] == 'value%d' % (i+offset)

def remove_c1c2(cursor, key, consistency="QUORUM"):
    if cursor.cql_major_version >= 3:
        cursor.execute('DELETE c1, c2 FROM cf WHERE key=k%d' % key, consistency_level=consistency)
    else:
        cursor.execute('DELETE c1, c2 FROM cf USING CONSISTENCY %s WHERE key=k%d' % (consistency, key))

# work for cluster started by populate
def new_node(cluster, bootstrap=True, token=None, remote_debug_port='2000'):
    i = len(cluster.nodes) + 1
    node = Node('node%s' % i,
                cluster,
                bootstrap,
                ('127.0.0.%s' % i, 9160),
                ('127.0.0.%s' % i, 7000),
                str(7000 + i * 100),
                remote_debug_port,
                token)
    cluster.add(node, not bootstrap)
    return node

def _put_with_overwrite(cluster, cursor, nb_keys, cl="QUORUM"):
    if cluster.version() >= "1.2":
        for k in xrange(0, nb_keys):
            kvs = [ "UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i, k, i) for i in xrange(0, 100) ]
            cursor.execute('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
            time.sleep(.01)
        cluster.flush()
        for k in xrange(0, nb_keys):
            kvs = [ "UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i*4, k, i*2) for i in xrange(0, 50) ]
            cursor.execute('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
            time.sleep(.01)
        cluster.flush()
        for k in xrange(0, nb_keys):
            kvs = [ "UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i*20, k, i*5) for i in xrange(0, 20) ]
            cursor.execute('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
            time.sleep(.01)
        cluster.flush()
    else:
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

def _validate_row(cluster, res):
    if cluster.version() >= "1.2":
        assert len(res) == 100, len(res)
        for i in xrange(0, 100):
            if i % 5 == 0:
                assert res[i][2] == 'value%d' % (i*4), 'for %d, expecting value%d, got %s' % (i, i*4, res[i][2])
            elif i % 2 == 0:
                assert res[i][2] == 'value%d' % (i*2), 'for %d, expecting value%d, got %s' % (i, i*2, res[i][2])
            else:
                assert res[i][2] == 'value%d' % i, 'for %d, expecting value%d, got %s' % (i, i, res[i][2])
    else:
        assert len(res) == 100, len(res)
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
    ks = [ "\'c%02d\'" % i for i in xrange(0, 100) ]
    # We do not support proper IN queries yet
    #if cluster.version() >= "1.2":
    #    cursor.execute('SELECT * FROM cf USING CONSISTENCY %s WHERE key=\'k0\' AND c IN (%s)' % (cl, ','.join(ks)))
    #else:
    #    cursor.execute('SELECT %s FROM cf USING CONSISTENCY %s WHERE key=\'k0\'' % (','.join(ks), cl))
    #_validate_row(cluster, cursor)
    if cluster.version() < "1.2":
        cursor.execute('SELECT %s FROM cf USING CONSISTENCY %s WHERE key=\'k0\'' % (','.join(ks), cl))
        assert cursor.rowcount == 1
        res = cursor.fetchone() #[1:] # removing key
        _validate_row(cluster, res)

    # slice reads
    if cluster.version() >= "1.2":
        cursor.execute('SELECT * FROM cf WHERE key=\'k0\'', consistency_level=cl)
        _validate_row(cluster, cursor.fetchall())
    else:
        cursor.execute('SELECT * FROM cf USING CONSISTENCY %s WHERE key=\'k0\'' % cl)
        _validate_row(cluster, cursor.fetchone()[1:])

# Simple puts and range gets, with overwrites and flushes between inserts to
# make sure we hit multiple sstables on reads
def range_putget(cluster, cursor, cl="QUORUM"):
    keys = 100

    _put_with_overwrite(cluster, cursor, keys, cl)

    if cluster.version() >= "1.2":
        cursor.execute('SELECT * FROM cf LIMIT 10000000')
    else:
        cursor.execute('SELECT * FROM cf USING CONSISTENCY %s LIMIT 10000000' % cl)
    if cluster.version() >= "1.2":
        assert cursor.rowcount == keys * 100, cursor.rowcount
        for k in xrange(0, keys):
            res = cursor.fetchmany(100)
            _validate_row(cluster, res)
    else:
        assert cursor.rowcount == keys
        for res in cursor:
            res = res[1:] # removing key
            _validate_row(cluster, res)

class since(object):
    def __init__(self, cass_version, max_version=None):
        self.cass_version = LooseVersion(cass_version)
        self.max_version = max_version
        if self.max_version is not None:
            self.max_version = LooseVersion(self.max_version)

    def __call__(self, f):
        def wrapped(obj):
            cluster_version = LooseVersion(obj.cluster.version())
            if cluster_version < self.cass_version:
                obj.skip("%s < %s" % (cluster_version, self.cass_version))
            if self.max_version and \
                    cluster_version[:len(self.max_version)] > self.max_version:
                obj.skip("%s > %s" %(cluster_version, self.max_version)) 
            f(obj)
        wrapped.__name__ = f.__name__
        wrapped.__doc__ = f.__doc__
        return wrapped

from dtest import DISABLE_VNODES
# Use this decorator to skip a test when vnodes are enabled.
class no_vnodes(object):
    def __call__(self, f):
        def wrapped(obj):
            if not DISABLE_VNODES:
                obj.skip("Test disabled for vnodes")
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


def replace_in_file(filepath, search_replacements):
    """In-place file search and replace.
    
    filepath - The path of the file to edit 
    search_replacements - a list of tuples (regex, replacement) that
    represent however many search and replace operations you wish to
    perform.

    Note: This does not work with multi-line regexes.
    """
    for line in fileinput.input(filepath, inplace=True):
        for regex, replacement in search_replacements:
            line = re.sub(regex, replacement, line)
        sys.stdout.write(line)

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
        self.node = node
        self.host = host
        self.port = port
        self.cassandra_interface = cassandra_interface

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


