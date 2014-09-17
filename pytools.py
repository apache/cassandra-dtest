from ccmlib.node import Node
from decorator  import decorator
from distutils.version import LooseVersion
import re, os, sys, fileinput, time

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

def rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list

def create_c1c2_table(tester, session, read_repair=None):
    tester.create_cf(session, 'cf', columns={ 'c1' : 'text', 'c2' : 'text' }, read_repair=read_repair)

def insert_c1c2(session, key, consistency=ConsistencyLevel.QUORUM):
    query = SimpleStatement('UPDATE cf SET c1=\'value1\', c2=\'value2\' WHERE key=\'k%d\'' % key, consistency_level=consistency)
    session.execute(query)

def query_c1c2(session, key, consistency=ConsistencyLevel.QUORUM):
    query = SimpleStatement('SELECT c1, c2 FROM cf WHERE key=\'k%d\'' % key, consistency_level=consistency)
    rows = session.execute(query)
    assert len(rows) == 1
    res = rows[0]
    assert len(res) == 2 and res[0] == 'value1' and res[1] == 'value2', res

# work for cluster started by populate
def new_node(cluster, bootstrap=True, token=None, remote_debug_port='2000', data_center=None):
    i = len(cluster.nodes) + 1
    node = Node('node%s' % i,
                cluster,
                bootstrap,
                ('127.0.0.%s' % i, 9160),
                ('127.0.0.%s' % i, 7000),
                str(7000 + i * 100),
                remote_debug_port,
                token,
                binary_interface=('127.0.0.%s' % i, 9042))
    cluster.add(node, not bootstrap, data_center=data_center)
    return node

def insert_columns(tester, session, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    upds = [ "UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%06d\'" % (i, key, i) for i in xrange(offset*columns_count, columns_count*(offset+1))]
    query = 'BEGIN BATCH %s; APPLY BATCH' % '; '.join(upds)
    simple_query = SimpleStatement(query, consistency_level=consistency)
    session.execute(simple_query)

def query_columns(tester, cursor, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    query = SimpleStatement('SELECT c, v FROM cf WHERE key=\'k%s\' AND c >= \'c%06d\' AND c <= \'c%06d\'' % (key, offset, columns_count+offset-1), consistency_level=consistency)
    res = cursor.execute(query)
    assert len(res) == columns_count, "%s != %s (%s-%s)" % (len(res), columns_count, offset, columns_count+offset-1)
    for i in xrange(0, columns_count):
        assert res[i][1] == 'value%d' % (i+offset)

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

# Simple puts and get (on one row), testing both reads by names and by slice,
# with overwrites and flushes between inserts to make sure we hit multiple
# sstables on reads
def putget(cluster, cursor, cl=ConsistencyLevel.QUORUM):

    _put_with_overwrite(cluster, cursor, 1, cl)

    # reads by name
    ks = [ "\'c%02d\'" % i for i in xrange(0, 100) ]
    # We do not support proper IN queries yet
    #if cluster.version() >= "1.2":
    #    cursor.execute('SELECT * FROM cf USING CONSISTENCY %s WHERE key=\'k0\' AND c IN (%s)' % (cl, ','.join(ks)))
    #else:
    #    cursor.execute('SELECT %s FROM cf USING CONSISTENCY %s WHERE key=\'k0\'' % (','.join(ks), cl))
    #_validate_row(cluster, cursor)
    # slice reads
    query = SimpleStatement('SELECT * FROM cf WHERE key=\'k0\'', consistency_level=cl)
    rows = cursor.execute(query)
    _validate_row(cluster, rows)

def _put_with_overwrite(cluster, cursor, nb_keys, cl=ConsistencyLevel.QUORUM):
    for k in xrange(0, nb_keys):
        kvs = [ "UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i, k, i) for i in xrange(0, 100) ]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        cursor.execute(query)
        time.sleep(.01)
    cluster.flush()
    for k in xrange(0, nb_keys):
        kvs = [ "UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i*4, k, i*2) for i in xrange(0, 50) ]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        cursor.execute(query)
        time.sleep(.01)
    cluster.flush()
    for k in xrange(0, nb_keys):
        kvs = [ "UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i*20, k, i*5) for i in xrange(0, 20) ]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        cursor.execute(query)
        time.sleep(.01)
    cluster.flush()

def _validate_row(cluster, res):
    assert len(res) == 100, len(res)
    for i in xrange(0, 100):
        if i % 5 == 0:
            assert res[i][2] == 'value%d' % (i*4), 'for %d, expecting value%d, got %s' % (i, i*4, res[i][2])
        elif i % 2 == 0:
            assert res[i][2] == 'value%d' % (i*2), 'for %d, expecting value%d, got %s' % (i, i*2, res[i][2])
        else:
            assert res[i][2] == 'value%d' % i, 'for %d, expecting value%d, got %s' % (i, i, res[i][2])

# Simple puts and range gets, with overwrites and flushes between inserts to
# make sure we hit multiple sstables on reads
def range_putget(cluster, cursor, cl=ConsistencyLevel.QUORUM):
    keys = 100

    _put_with_overwrite(cluster, cursor, keys, cl)

    paged_results = cursor.execute('SELECT * FROM cf LIMIT 10000000')
    rows = [result for result in paged_results]

    assert len(rows) == keys * 100, len(rows)
    for k in xrange(0, keys):
        res = rows[:100]
        del rows[:100]
        _validate_row(cluster, res)

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

def not_implemented(f):
    def wrapped(obj):
        obj.skip("this test not implemented")
        f(obj)
    wrapped.__name__ = f.__name__
    wrapped.__doc__ = f.__doc__
    return wrapped

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