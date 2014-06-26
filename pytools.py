import time
from ccmlib.node import Node
from decorator  import decorator
from distutils.version import LooseVersion
import cql
import re
import os
import sys
import fileinput

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

def rows_to_list(rows):
    new_list = []
    for row in rows:
        new_list.append(list(row))
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