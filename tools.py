import time
from ccmlib.node import Node

def retry_till_success(fun, *args, **kwargs):
    timeout = kwargs["timeout"] or 60
    deadline = time.time() + timeout
    exception = None
    while time.time() < deadline:
        try:
            if len(args) == 0:
                fun(None)
            else:
                fun(*args)
            return
        except Exception as e:
            exception = e
    raise exception

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
