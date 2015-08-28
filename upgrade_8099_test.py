from dtest import Tester
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
import random
from struct import *

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException
from thrift_bindings.v30 import Cassandra
from thrift_bindings.v30.Cassandra import *

class TestUpgrade8099(Tester):
    def _setup_cluster(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="2.1.5")
        cluster.populate(1).start()

        [node1] = cluster.nodelist()

        return self.patient_cql_connection(node1)

    def _do_upgrade(self):

        [node1] = self.cluster.nodelist()

        node1.flush()
        time.sleep(.5)
        node1.stop(wait_other_notice=True)

        print("Node stopped...")
        node1.set_install_dir(install_dir="/home/pcmanus/Git/cassandra") # TODO: change
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        print("Node restarted...")

        return self.patient_cql_connection(node1)

    def upgrade_with_clustered_CQL_table_test(self):
        self.upgrade_with_clustered_table("")

    def upgrade_with_clustered_compact_table_test(self):
        self.upgrade_with_clustered_table(" WITH COMPACT STORAGE")

    def upgrade_with_unclustered_CQL_table_test(self):
        self.upgrade_with_unclustered_table("")

    def upgrade_with_unclustered_compact_table_test(self):
        self.upgrade_with_unclustered_table(" WITH COMPACT STORAGE")

    def upgrade_with_clustered_table(self, table_options):
        PARTITIONS = 2
        ROWS = 1000

        cursor = self._setup_cluster()

        print("Creating schema...")
        cursor.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        cursor.execute('USE ks')
        cursor.execute('CREATE TABLE t (k int, t int, v int, PRIMARY KEY (k, t))' + table_options)

        print("Inserting...")
        for n in xrange(0, PARTITIONS):
            for r in xrange(0, ROWS):
                cursor.execute("INSERT INTO t(k, t, v) VALUES (%d, %d, %d)" % (n, r, r))

        print("Upgrading node...")
        cursor = self._do_upgrade()

        cursor.execute('USE ks')

        print("Querying...")
        for n in xrange(0, PARTITIONS):
            assert_all(cursor, "SELECT * FROM t WHERE k = %d" % (n), [ [n, v, v] for v in xrange (0, ROWS)])
            assert_all(cursor, "SELECT * FROM t WHERE k = %d ORDER BY t DESC" % (n), [ [n, v, v] for v in xrange (ROWS-1, -1, -1)])

            # Querying a "large" slice
            start = ROWS / 10
            end = ROWS - 1 - (ROWS / 10)
            assert_all(cursor, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d" % (n, start, end), [ [n, v, v] for v in xrange (start, end)])
            assert_all(cursor, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d ORDER BY t DESC" % (n, start, end), [ [n, v, v] for v in xrange (end -1, start-1, -1)])

            # Querying a "small" slice
            start = ROWS / 2
            end = ROWS / 2 + 5
            assert_all(cursor, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d" % (n, start, end), [ [n, v, v] for v in xrange (start, end)])
            assert_all(cursor, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d ORDER BY t DESC" % (n, start, end), [ [n, v, v] for v in xrange (end -1, start-1, -1)])

        self.cluster.compact()

        print("Post-compaction Querying...")
        for n in xrange(0, PARTITIONS):
            assert_all(cursor, "SELECT * FROM t WHERE k = %d" % (n), [ [n, v, v] for v in xrange (0, ROWS)])
            assert_all(cursor, "SELECT * FROM t WHERE k = %d ORDER BY t DESC" % (n), [ [n, v, v] for v in xrange (ROWS-1, -1, -1)])

            # Querying a "large" slice
            start = ROWS / 10
            end = ROWS - 1 - (ROWS / 10)
            assert_all(cursor, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d" % (n, start, end), [ [n, v, v] for v in xrange (start, end)])
            assert_all(cursor, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d ORDER BY t DESC" % (n, start, end), [ [n, v, v] for v in xrange (end -1, start-1, -1)])

            # Querying a "small" slice
            start = ROWS / 2
            end = ROWS / 2 + 5
            assert_all(cursor, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d" % (n, start, end), [ [n, v, v] for v in xrange (start, end)])
            assert_all(cursor, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d ORDER BY t DESC" % (n, start, end), [ [n, v, v] for v in xrange (end -1, start-1, -1)])

    def upgrade_with_unclustered_table(self, table_options):
        PARTITIONS = 5

        cursor = self._setup_cluster()

        print("Creating schema...")
        cursor.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        cursor.execute('USE ks')
        cursor.execute('CREATE TABLE t (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)' + table_options)

        print("Inserting...")
        for n in xrange(0, PARTITIONS):
                cursor.execute("INSERT INTO t(k, v1, v2, v3, v4) VALUES (%d, %d, %d, %d, %d)" % (n, n+1, n+2, n+3, n+4))

        print("Upgrading node...")
        cursor = self._do_upgrade()

        cursor.execute('USE ks')

        print("Querying...")
        for n in xrange(0, PARTITIONS):
            assert_one(cursor, "SELECT * FROM t WHERE k = %d" % (n), [n, n+1, n+2, n+3, n+4])

        self.cluster.compact()

        print("Post-compaction Querying...")
        for n in xrange(0, PARTITIONS):
            assert_one(cursor, "SELECT * FROM t WHERE k = %d" % (n), [n, n+1, n+2, n+3, n+4])

    def upgrade_with_statics_test(self):
        PARTITIONS = 1
        ROWS = 10

        cursor = self._setup_cluster()

        print("Creating schema...")
        cursor.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        cursor.execute('USE ks')
        cursor.execute('CREATE TABLE t (k int, s1 int static, s2 int static, t int, v1 int, v2 int, PRIMARY KEY (k, t))')

        print("Inserting...")
        for n in xrange(0, PARTITIONS):
            for r in xrange(0, ROWS):
                cursor.execute("INSERT INTO t(k, s1, s2, t, v1, v2) VALUES (%d, %d, %d, %d, %d, %d)" % (n, r, r+1, r, r, r+1))

        print("Upgrading node...")
        cursor = self._do_upgrade()

        cursor.execute('USE ks')

        print("Querying...")
        for n in xrange(0, PARTITIONS):
            assert_all(cursor, "SELECT * FROM t WHERE k = %d" % (n), [ [n, v, ROWS-1, ROWS, v, v+1] for v in xrange (0, ROWS)])
            assert_all(cursor, "SELECT * FROM t WHERE k = %d ORDER BY t DESC" % (n), [ [n, v, ROWS-1, ROWS, v, v+1] for v in xrange (ROWS-1, -1, -1)])

        self.cluster.compact()

        print("Post-compaction Querying...")
        for n in xrange(0, PARTITIONS):
            assert_all(cursor, "SELECT * FROM t WHERE k = %d" % (n), [ [n, v, ROWS-1, ROWS, v, v+1] for v in xrange (0, ROWS)])
            assert_all(cursor, "SELECT * FROM t WHERE k = %d ORDER BY t DESC" % (n), [ [n, v, ROWS-1, ROWS, v, v+1] for v in xrange (ROWS-1, -1, -1)])

    def upgrade_with_wide_partition_test(self):
        self.upgrade_with_wide_partition()

    def upgrade_with_wide_partition_reversed_test(self):
        self.upgrade_with_wide_partition(query_modifier=" ORDER BY t DESC")

    def upgrade_with_wide_partition(self, query_modifier=""):
        ROWS = 100

        cursor = self._setup_cluster()

        print("Creating schema...")
        cursor.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        cursor.execute('USE ks')
        cursor.execute('CREATE TABLE t (k int, t int, v1 int, v2 blob, v3 set<int>, PRIMARY KEY (k, t))')

        # the blob is only here to make the row bigger internally so it sometimes span multiple index blocks
        bigish_blob = "0x";
        for i in xrange(0, 1000):
            bigish_blob = bigish_blob + "0000"

        print("Inserting...")
        for r in xrange(0, ROWS):
            cursor.execute("INSERT INTO t(k, t, v1, v2, v3) VALUES (%d, %d, %d, %s, {%d, %d})" % (0, r, r, bigish_blob, r*2, r*3))

        self.cluster.flush()

        # delete every other row
        for r in xrange(0, ROWS, 2):
            cursor.execute("DELETE FROM t WHERE k=0 AND t=%d" % (r))

        # delete the set from every other remaining row
        for r in xrange(1, ROWS, 4):
            cursor.execute("UPDATE t SET v3={} WHERE k=0 AND t=%d" % (r))

        print("Upgrading node...")
        cursor = self._do_upgrade()

        cursor.execute('USE ks')

        print("Querying...")
        for r in xrange(0, ROWS):
            query = "SELECT t, v1, v3 FROM t WHERE k = 0 AND t=%d%s" % (r, query_modifier)
            if (r-1) % 4 == 0:
                assert_one(cursor, query, [r, r, None])
            elif (r+1) % 2 == 0:
                assert_one(cursor, query, [r, r, set([r*2, r*3])])
            else:
                assert_none(cursor, query)

        self.cluster.compact()

        print("Post-compaction Querying...")
        for r in xrange(0, ROWS):
            query = "SELECT t, v1, v3 FROM t WHERE k = 0 AND t=%d%s" % (r, query_modifier)
            if (r-1) % 4 == 0:
                assert_one(cursor, query, [r, r, None])
            elif (r+1) % 2 == 0:
                assert_one(cursor, query, [r, r, set([r*2, r*3])])
            else:
                assert_none(cursor, query)

    def upgrade_with_index_test(self):
        PARTITIONS = 2
        ROWS = 4

        cursor = self._setup_cluster()

        print("Creating schema...")
        cursor.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        cursor.execute('USE ks')
        cursor.execute('CREATE TABLE t (k int, t int, v1 int, v2 int, PRIMARY KEY (k, t))')

        cursor.execute('CREATE INDEX ON t(v1)')

        print("Inserting...")
        for p in xrange(0, PARTITIONS):
            for r in xrange(0, ROWS):
                cursor.execute("INSERT INTO t(k, t, v1, v2) VALUES (%d, %d, %d, %d)" % (p, r, r % 2, r * 2))

        self.cluster.flush()

        print("Querying...")
        assert_all(cursor, "SELECT * FROM t WHERE v1 = 0", [[p, r, 0, r *2] for p in xrange(0, PARTITIONS) for r in xrange(0, ROWS) if r%2 == 0], ignore_order=True)

        print("Upgrading node...")
        cursor = self._do_upgrade()

        cursor.execute('USE ks')

        print("Querying...")
        assert_all(cursor, "SELECT * FROM t WHERE v1 = 0", [[p, r, 0, r *2] for p in xrange(0, PARTITIONS) for r in xrange(0, ROWS) if r%2 == 0], ignore_order=True)

        self.cluster.compact()

        print("Querying...")
        assert_all(cursor, "SELECT * FROM t WHERE v1 = 0", [[p, r, 0, r *2] for p in xrange(0, PARTITIONS) for r in xrange(0, ROWS) if r%2 == 0], ignore_order=True)
