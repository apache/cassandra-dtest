from __future__ import with_statement
import os, tempfile, sys, shutil, types, time

from ccmlib.cluster import Cluster
from ccmlib.node import Node

LOG_SAVED_DIR="logs"
LAST_LOG = os.path.join(LOG_SAVED_DIR, "last")

LAST_TEST_DIR='last_test_dir'

class Tester(object):
    def __get_cluster(self, name='test'):
        self.test_path = tempfile.mkdtemp(prefix='dtest-')
        try:
            version = os.environ['CASSANDRA_VERSION']
            return Cluster(self.test_path, name, cassandra_version=version)
        except KeyError:
            try:
                cdir = os.environ['CASSANDRA_DIR']
            except KeyError:
                cdir = './'
            return Cluster(self.test_path, name, cassandra_dir=cdir)

    def __cleanup_cluster(self):
        self.cluster.remove()
        os.rmdir(self.test_path)
        os.remove(LAST_TEST_DIR)

    def setUp(self):
        # cleaning up if a previous execution didn't trigger tearDown (which
        # can happen if it is interrupted by KeyboardInterrupt)
        # TODO: move that part to a generic fixture
        if os.path.exists(LAST_TEST_DIR):
            with open(LAST_TEST_DIR) as f:
                self.test_path = f.readline().strip('\n')
                name = f.readline()
                self.cluster = Cluster.load(self.test_path, name)
                self.__cleanup_cluster()

        self.cluster = self.__get_cluster()
        with open(LAST_TEST_DIR, 'w') as f:
            f.write(self.test_path + '\n')
            f.write(self.cluster.name)
        self.connections = []

    def tearDown(self):
        for con in self.connections:
            con.close()

        try:
            if sys.exc_info() != (None, None, None):
                # means the test failed. Save the logs for inspection.
                if not os.path.exists(LOG_SAVED_DIR):
                    os.mkdir(LOG_SAVED_DIR)
                logs = [ (node.name, node.logfilename()) for node in self.cluster.nodes.values() ]
                if len(logs) is not 0:
                    basedir = str(int(time.time() * 1000))
                    dir = os.path.join(LOG_SAVED_DIR, basedir)
                    os.mkdir(dir)
                    for name, log in logs:
                        shutil.copyfile(log, os.path.join(dir, name + ".log"))
                    if os.path.exists(LAST_LOG):
                        os.unlink(LAST_LOG)
                    os.symlink(basedir, LAST_LOG)
        except Exception as e:
            print "Error saving log:", str(e)
        finally:
            self.__cleanup_cluster()

    def cql_connection(self, node, keyspace=None):
        import cql
        host, port = node.network_interfaces['thrift']
        con = cql.connect(host, port, keyspace)
        self.connections.append(con)
        return con

    def create_ks(self, cursor, name, rf):
        query = 'CREATE KEYSPACE %s WITH strategy_class=%s AND %s'
        if isinstance(rf, types.IntType):
            # we assume simpleStrategy
            cursor.execute(query % (name, 'SimpleStrategy', 'strategy_options:replication_factor=%d' % rf))
        else:
            assert len(name) != 0, "At least one datacenter/rf pair is needed"
            # we assume networkTopolyStrategy
            options = [ 'strategy_options:%s=%d' % (dc, rf[dc]) for dc in rf ].join(' AND ')
            cursor.execute(query % (name, 'NetworkTopologyStrategy', options))
        #cursor.execute('USE :name;', {"name": name})
        cursor.execute('USE %s' % name)

    # We default to UTF8Type because it's simpler to use in tests
    def create_cf(self, cursor, name, key_type="varchar", comparator="UTF8Type", validation="UTF8Type"):
        cursor.execute('CREATE COLUMNFAMILY %s (key %s PRIMARY KEY) WITH comparator=%s AND default_validation=%s' % (name, key_type, comparator, validation))
