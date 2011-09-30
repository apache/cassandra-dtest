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

def insert_c1c2(cursor, key, consistency):
    cursor.execute('UPDATE cf USING CONSISTENCY %s SET c1=value1, c2=value2 WHERE key=k%d' % (consistency, key))

def query_c1c2(cursor, key, consistency):
    cursor.execute('SELECT c1, c2 FROM cf USING CONSISTENCY %s WHERE key=k%d' % (consistency, key))
    assert cursor.rowcount == 1
    res = cursor.fetchone()
    assert len(res) == 2 and res[0] == 'value1' and res[1] == 'value2'

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
