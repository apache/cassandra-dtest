from dtest import Tester, debug
import time
from tools import since
from assertions import assert_all, assert_one, assert_none


@since('3.0')
class TestStorageEngineUpgrade(Tester):
    def setUp(self):
        super(TestStorageEngineUpgrade, self).setUp()
        self.default_install_dir = self.cluster.get_install_dir()

    def _setup_cluster(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="2.1.9")
        cluster.populate(1).start()

        node1 = cluster.nodelist()[0]

        return self.patient_cql_connection(node1)

    def _do_upgrade(self):

        node1 = self.cluster.nodelist()[0]

        node1.flush()
        time.sleep(.5)
        node1.stop(wait_other_notice=True)

        debug("Node stopped...")
        node1.set_install_dir(install_dir=self.default_install_dir)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        debug("Node restarted...")

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

        session = self._setup_cluster()

        debug("Creating schema...")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        session.execute('USE ks')
        session.execute('CREATE TABLE t (k int, t int, v int, PRIMARY KEY (k, t))' + table_options)

        debug("Inserting...")
        for n in xrange(0, PARTITIONS):
            for r in xrange(0, ROWS):
                session.execute("INSERT INTO t(k, t, v) VALUES (%d, %d, %d)" % (n, r, r))

        debug("Upgrading node...")
        session = self._do_upgrade()

        session.execute('USE ks')

        debug("Querying...")
        for n in xrange(0, PARTITIONS):
            assert_all(session, "SELECT * FROM t WHERE k = %d" % (n), [[n, v, v] for v in xrange(0, ROWS)])
            assert_all(session, "SELECT * FROM t WHERE k = %d ORDER BY t DESC" % (n), [[n, v, v] for v in xrange(ROWS - 1, -1, -1)])

            # Querying a "large" slice
            start = ROWS / 10
            end = ROWS - 1 - (ROWS / 10)
            assert_all(session, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d" % (n, start, end), [[n, v, v] for v in xrange(start, end)])
            assert_all(session, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d ORDER BY t DESC" % (n, start, end), [[n, v, v] for v in xrange(end - 1, start - 1, -1)])

            # Querying a "small" slice
            start = ROWS / 2
            end = ROWS / 2 + 5
            assert_all(session, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d" % (n, start, end), [[n, v, v] for v in xrange(start, end)])
            assert_all(session, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d ORDER BY t DESC" % (n, start, end), [[n, v, v] for v in xrange(end - 1, start - 1, -1)])

        self.cluster.compact()

        debug("Post-compaction Querying...")
        for n in xrange(0, PARTITIONS):
            assert_all(session, "SELECT * FROM t WHERE k = %d" % (n), [[n, v, v] for v in xrange(0, ROWS)])
            assert_all(session, "SELECT * FROM t WHERE k = %d ORDER BY t DESC" % (n), [[n, v, v] for v in xrange(ROWS - 1, -1, -1)])

            # Querying a "large" slice
            start = ROWS / 10
            end = ROWS - 1 - (ROWS / 10)
            assert_all(session, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d" % (n, start, end), [[n, v, v] for v in xrange(start, end)])
            assert_all(session, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d ORDER BY t DESC" % (n, start, end), [[n, v, v] for v in xrange(end - 1, start - 1, -1)])

            # Querying a "small" slice
            start = ROWS / 2
            end = ROWS / 2 + 5
            assert_all(session, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d" % (n, start, end), [[n, v, v] for v in xrange(start, end)])
            assert_all(session, "SELECT * FROM t WHERE k = %d AND t >= %d AND t < %d ORDER BY t DESC" % (n, start, end), [[n, v, v] for v in xrange(end - 1, start - 1, -1)])

    def upgrade_with_unclustered_table(self, table_options):
        PARTITIONS = 5

        session = self._setup_cluster()

        debug("Creating schema...")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        session.execute('USE ks')
        session.execute('CREATE TABLE t (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)' + table_options)

        debug("Inserting...")
        for n in xrange(0, PARTITIONS):
                session.execute("INSERT INTO t(k, v1, v2, v3, v4) VALUES (%d, %d, %d, %d, %d)" % (n, n + 1, n + 2, n + 3, n + 4))

        debug("Upgrading node...")
        session = self._do_upgrade()

        session.execute('USE ks')

        debug("Querying...")
        for n in xrange(0, PARTITIONS):
            assert_one(session, "SELECT * FROM t WHERE k = %d" % (n), [n, n + 1, n + 2, n + 3, n + 4])

        self.cluster.compact()

        debug("Post-compaction Querying...")
        for n in xrange(0, PARTITIONS):
            assert_one(session, "SELECT * FROM t WHERE k = %d" % (n), [n, n + 1, n + 2, n + 3, n + 4])

    def upgrade_with_statics_test(self):
        PARTITIONS = 1
        ROWS = 10

        session = self._setup_cluster()

        debug("Creating schema...")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        session.execute('USE ks')
        session.execute('CREATE TABLE t (k int, s1 int static, s2 int static, t int, v1 int, v2 int, PRIMARY KEY (k, t))')

        debug("Inserting...")
        for n in xrange(0, PARTITIONS):
            for r in xrange(0, ROWS):
                session.execute("INSERT INTO t(k, s1, s2, t, v1, v2) VALUES (%d, %d, %d, %d, %d, %d)" % (n, r, r + 1, r, r, r + 1))

        debug("Upgrading node...")
        session = self._do_upgrade()

        session.execute('USE ks')

        debug("Querying...")
        for n in xrange(0, PARTITIONS):
            assert_all(session, "SELECT * FROM t WHERE k = %d" % (n), [[n, v, ROWS - 1, ROWS, v, v + 1] for v in xrange(0, ROWS)])
            assert_all(session, "SELECT * FROM t WHERE k = %d ORDER BY t DESC" % (n), [[n, v, ROWS - 1, ROWS, v, v + 1] for v in xrange(ROWS - 1, -1, -1)])

        self.cluster.compact()

        debug("Post-compaction Querying...")
        for n in xrange(0, PARTITIONS):
            assert_all(session, "SELECT * FROM t WHERE k = %d" % (n), [[n, v, ROWS - 1, ROWS, v, v + 1] for v in xrange(0, ROWS)])
            assert_all(session, "SELECT * FROM t WHERE k = %d ORDER BY t DESC" % (n), [[n, v, ROWS - 1, ROWS, v, v + 1] for v in xrange(ROWS - 1, -1, -1)])

    def upgrade_with_wide_partition_test(self):
        self.upgrade_with_wide_partition()

    def upgrade_with_wide_partition_reversed_test(self):
        self.upgrade_with_wide_partition(query_modifier=" ORDER BY t DESC")

    def upgrade_with_wide_partition(self, query_modifier=""):
        ROWS = 100

        session = self._setup_cluster()

        debug("Creating schema...")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        session.execute('USE ks')
        session.execute('CREATE TABLE t (k int, t int, v1 int, v2 blob, v3 set<int>, PRIMARY KEY (k, t))')

        # the blob is only here to make the row bigger internally so it sometimes span multiple index blocks
        bigish_blob = "0x"
        for i in xrange(0, 1000):
            bigish_blob = bigish_blob + "0000"

        debug("Inserting...")
        for r in xrange(0, ROWS):
            session.execute("INSERT INTO t(k, t, v1, v2, v3) VALUES (%d, %d, %d, %s, {%d, %d})" % (0, r, r, bigish_blob, r * 2, r * 3))

        self.cluster.flush()

        # delete every other row
        for r in xrange(0, ROWS, 2):
            session.execute("DELETE FROM t WHERE k=0 AND t=%d" % (r))

        # delete the set from every other remaining row
        for r in xrange(1, ROWS, 4):
            session.execute("UPDATE t SET v3={} WHERE k=0 AND t=%d" % (r))

        debug("Upgrading node...")
        session = self._do_upgrade()

        session.execute('USE ks')

        debug("Querying...")
        for r in xrange(0, ROWS):
            query = "SELECT t, v1, v3 FROM t WHERE k = 0 AND t=%d%s" % (r, query_modifier)
            if (r - 1) % 4 == 0:
                assert_one(session, query, [r, r, None])
            elif (r + 1) % 2 == 0:
                assert_one(session, query, [r, r, set([r * 2, r * 3])])
            else:
                assert_none(session, query)

        self.cluster.compact()

        debug("Post-compaction Querying...")
        for r in xrange(0, ROWS):
            query = "SELECT t, v1, v3 FROM t WHERE k = 0 AND t=%d%s" % (r, query_modifier)
            if (r - 1) % 4 == 0:
                assert_one(session, query, [r, r, None])
            elif (r + 1) % 2 == 0:
                assert_one(session, query, [r, r, set([r * 2, r * 3])])
            else:
                assert_none(session, query)

    def upgrade_with_index_test(self):
        PARTITIONS = 2
        ROWS = 4

        session = self._setup_cluster()

        debug("Creating schema...")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        session.execute('USE ks')
        session.execute('CREATE TABLE t (k int, t int, v1 int, v2 int, PRIMARY KEY (k, t))')

        session.execute('CREATE INDEX ON t(v1)')

        debug("Inserting...")
        for p in xrange(0, PARTITIONS):
            for r in xrange(0, ROWS):
                session.execute("INSERT INTO t(k, t, v1, v2) VALUES (%d, %d, %d, %d)" % (p, r, r % 2, r * 2))

        self.cluster.flush()

        debug("Querying...")
        assert_all(session, "SELECT * FROM t WHERE v1 = 0", [[p, r, 0, r * 2] for p in xrange(0, PARTITIONS) for r in xrange(0, ROWS) if r % 2 == 0], ignore_order=True)

        debug("Upgrading node...")
        session = self._do_upgrade()

        session.execute('USE ks')

        debug("Querying...")
        assert_all(session, "SELECT * FROM t WHERE v1 = 0", [[p, r, 0, r * 2] for p in xrange(0, PARTITIONS) for r in xrange(0, ROWS) if r % 2 == 0], ignore_order=True)

        self.cluster.compact()

        debug("Querying...")
        assert_all(session, "SELECT * FROM t WHERE v1 = 0", [[p, r, 0, r * 2] for p in xrange(0, PARTITIONS) for r in xrange(0, ROWS) if r % 2 == 0], ignore_order=True)

    def upgrade_with_range_tombstones(self):
        ROWS = 100

        session = self._setup_cluster()

        debug("Creating schema...")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        session.execute('USE ks')
        session.execute('CREATE TABLE t (k int, t1 int, t2 int, PRIMARY KEY (k, t1, t2))')

        debug("Inserting...")
        for n in xrange(0, ROWS):
            session.execute("INSERT INTO t(k, t1, t2) VALUES (0, 0, %d)" % n)

        session.execute("DELETE FROM t WHERE k=0 AND t1=0")

        for n in xrange(0, ROWS, 2):
            session.execute("INSERT INTO t(k, t1, t2) VALUES (0, 0, %d)" % n)

        debug("Upgrading node...")
        session = self._do_upgrade()

        session.execute('USE ks')

        debug("Querying...")
        assert_all(session, "SELECT * FROM t WHERE k = 0", [[0, 0, n] for n in xrange(0, ROWS, 2)])

        self.cluster.compact()

    def upgrade_with_range_and_collection_tombstones(self):
        session = self._setup_cluster()

        debug("Creating schema...")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
        session.execute('USE ks')
        session.execute('CREATE TABLE t (k text, t int, c list<int>, PRIMARY KEY (k, t))')

        debug("Inserting...")
        session.execute("INSERT INTO t(k, t, c) VALUES ('some_key', 0, %s)" % str([i for i in xrange(0, 10000)]))

        debug("Upgrading node...")
        session = self._do_upgrade()

        self.cluster.compact()

        session.execute('USE ks')

        debug("Querying...")
        assert_one(session, "SELECT k FROM t", ['some_key'])
