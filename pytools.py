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
import pdb

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