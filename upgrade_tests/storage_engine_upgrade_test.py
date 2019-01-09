import os
import time
import pytest
import logging

from dtest import Tester, MAJOR_VERSION_4
from sstable_generation_loading_test import TestBaseSStableLoader
from thrift_bindings.thrift010.Cassandra import (ConsistencyLevel, Deletion,
                                           Mutation, SlicePredicate,
                                           SliceRange)
from thrift_test import composite, get_thrift_client, i32
from tools.assertions import (assert_all, assert_length_equal, assert_none,
                              assert_one)
from tools.misc import new_node

since = pytest.mark.since
logger = logging.getLogger(__name__)

LEGACY_SSTABLES_JVM_ARGS = ["-Dcassandra.streamdes.initial_mem_buffer_size=1",
                            "-Dcassandra.streamdes.max_mem_buffer_size=5",
                            "-Dcassandra.streamdes.max_spill_file_size=128"]


@pytest.mark.upgrade_test
@since('3.0')
class TestStorageEngineUpgrade(Tester):

    @pytest.fixture(autouse=True)
    def storage_engine_upgrade_setup(self, fixture_dtest_setup):
        self.fixture_dtest_setup.default_install_dir = fixture_dtest_setup.cluster.get_install_dir()
        self.fixture_dtest_setup.bootstrap = False
        return ()

    def _setup_cluster(self, create_keyspace=True, cluster_options=None):
        cluster = self.cluster

        if cluster_options:
            cluster.set_configuration_options(cluster_options)

        # Forcing cluster version on purpose
        if self.dtest_config.cassandra_version_from_build >= MAJOR_VERSION_4:
            cluster.set_install_dir(version="git:cassandra-3.0")
        else:
            cluster.set_install_dir(version="git:cassandra-2.1")
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()
        cluster.populate(1).start()

        node1 = cluster.nodelist()[0]

        cursor = self.patient_cql_connection(node1)
        if create_keyspace:
            cursor.execute("CREATE KEYSPACE ks WITH replication = {'class':'SimpleStrategy', 'replication_factor': 1};")
            cursor.execute('USE ks')
        return cursor

    def _do_upgrade(self, login_keyspace=True):
        cluster = self.cluster
        node1 = cluster.nodelist()[0]

        node1.flush()
        time.sleep(.5)
        node1.stop(wait_other_notice=True)

        node1.set_install_dir(install_dir=self.fixture_dtest_setup.default_install_dir)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        if self.fixture_dtest_setup.bootstrap:
            cluster.set_install_dir(install_dir=self.fixture_dtest_setup.default_install_dir)
            # Add a new node, bootstrap=True ensures that it is not a seed
            node2 = new_node(cluster, bootstrap=True)
            node2.start(wait_for_binary_proto=True, jvm_args=self.fixture_dtest_setup.jvm_args)

            temp_files = self.glob_data_dirs(os.path.join('*', "tmp", "*.dat"))
            logger.debug("temp files: " + str(temp_files))
            assert 0 == len(temp_files), "Temporary files were not cleaned up."

        cursor = self.patient_cql_connection(node1)
        if login_keyspace:
            cursor.execute('USE ks')
        return cursor

    def test_update_and_drop_column(self):
        """
        Checks that dropped columns are properly handled in legacy sstables

        @jira_ticket CASSANDRA-11018
        """
        cursor = self._setup_cluster()

        cursor.execute('CREATE TABLE t (k text PRIMARY KEY, a int, b int)')

        cursor.execute("INSERT INTO t(k, a, b) VALUES ('some_key', 0, 0)")

        cursor = self._do_upgrade()

        cursor.execute("ALTER TABLE t DROP b")

        self.cluster.compact()

        assert_one(cursor, "SELECT * FROM t", ['some_key', 0])

    def test_upgrade_with_clustered_CQL_table(self):
        """
        Validates we can do basic slice queries (forward and reverse ones) on legacy sstables for a CQL table
        with a clustering column.
        """
        self.upgrade_with_clustered_table()

    def test_upgrade_with_clustered_compact_table(self):
        """
        Validates we can do basic slice queries (forward and reverse ones) on legacy sstables for a COMPACT table
        with a clustering column.
        """
        self.upgrade_with_clustered_table(compact_storage=True)

    def test_upgrade_with_unclustered_CQL_table(self):
        """
        Validates we can do basic name queries on legacy sstables for a CQL table without clustering.
        """
        self.upgrade_with_unclustered_table()

    def test_upgrade_with_unclustered_compact_table(self):
        """
        Validates we can do basic name queries on legacy sstables for a COMPACT table without clustering.
        """
        self.upgrade_with_unclustered_table(compact_storage=True)

    def upgrade_with_clustered_table(self, compact_storage=False):
        PARTITIONS = 2
        ROWS = 1000

        session = self._setup_cluster()

        session.execute(
            'CREATE TABLE t (k int, t int, v int, PRIMARY KEY (k, t))' +
            (' WITH COMPACT STORAGE' if compact_storage else ''))

        for n in range(PARTITIONS):
            for r in range(ROWS):
                session.execute("INSERT INTO t(k, t, v) VALUES ({n}, {r}, {r})".format(n=n, r=r))

        #4.0 doesn't support compact storage
        if compact_storage and self.dtest_config.cassandra_version_from_build >= MAJOR_VERSION_4:
            session.execute("ALTER TABLE t DROP COMPACT STORAGE;")

        session = self._do_upgrade()

        for n in range(PARTITIONS):
            assert_all(session,
                       "SELECT * FROM t WHERE k = {}".format(n),
                       [[n, v, v] for v in range(ROWS)])
            assert_all(session,
                       "SELECT * FROM t WHERE k = {} ORDER BY t DESC".format(n),
                       [[n, v, v] for v in range(ROWS - 1, -1, -1)])

            # Querying a "large" slice
            start = ROWS // 10
            end = ROWS - 1 - (ROWS // 10)
            assert_all(session,
                       "SELECT * FROM t WHERE k = {n} AND t >= {start} AND t < {end}".format(n=n, start=start, end=end),
                       [[n, v, v] for v in range(start, end)])
            assert_all(session,
                       "SELECT * FROM t WHERE k = {n} AND t >= {start} AND t < {end} ORDER BY t DESC".format(n=n, start=start, end=end),
                       [[n, v, v] for v in range(end - 1, start - 1, -1)])

            # Querying a "small" slice
            start = ROWS // 2
            end = ROWS // 2 + 5
            assert_all(session,
                       "SELECT * FROM t WHERE k = {n} AND t >= {start} AND t < {end}".format(n=n, start=start, end=end),
                       [[n, v, v] for v in range(start, end)])
            assert_all(session,
                       "SELECT * FROM t WHERE k = {n} AND t >= {start} AND t < {end} ORDER BY t DESC".format(n=n, start=start, end=end),
                       [[n, v, v] for v in range(end - 1, start - 1, -1)])

        self.cluster.compact()

        for n in range(PARTITIONS):
            assert_all(session, "SELECT * FROM t WHERE k = {}".format(n), [[n, v, v] for v in range(ROWS)])
            assert_all(session,
                       "SELECT * FROM t WHERE k = {} ORDER BY t DESC".format(n),
                       [[n, v, v] for v in range(ROWS - 1, -1, -1)])

            # Querying a "large" slice
            start = ROWS // 10
            end = ROWS - 1 - (ROWS // 10)
            assert_all(session,
                       "SELECT * FROM t WHERE k = {n} AND t >= {start} AND t < {end}".format(n=n, start=start, end=end),
                       [[n, v, v] for v in range(start, end)])
            assert_all(session,
                       "SELECT * FROM t WHERE k = {n} AND t >= {start} AND t < {end} ORDER BY t DESC".format(n=n, start=start, end=end),
                       [[n, v, v] for v in range(end - 1, start - 1, -1)])

            # Querying a "small" slice
            start = ROWS // 2
            end = ROWS // 2 + 5
            assert_all(session,
                       "SELECT * FROM t WHERE k = {n} AND t >= {start} AND t < {end}".format(n=n, start=start, end=end),
                       [[n, v, v] for v in range(start, end)])
            assert_all(session,
                       "SELECT * FROM t WHERE k = {n} AND t >= {start} AND t < {end} ORDER BY t DESC".format(n=n, start=start, end=end),
                       [[n, v, v] for v in range(end - 1, start - 1, -1)])

    def upgrade_with_unclustered_table(self, compact_storage=False):
        PARTITIONS = 5

        session = self._setup_cluster()

        session.execute('CREATE TABLE t (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)' +
                        (' WITH COMPACT STORAGE' if compact_storage else ''))

        for n in range(PARTITIONS):
            session.execute("INSERT INTO t(k, v1, v2, v3, v4) VALUES ({}, {}, {}, {}, {})".format(n, n + 1, n + 2, n + 3, n + 4))

        is40 = self.dtest_config.cassandra_version_from_build >= MAJOR_VERSION_4
        if compact_storage and is40:
            session.execute("ALTER TABLE t DROP COMPACT STORAGE;")

        session = self._do_upgrade()

        def maybe_add_compact_columns(expected):
            if is40 and compact_storage:
                expected.insert(1, None)
                expected.append(None)
            return expected

        for n in range(PARTITIONS):
            assert_one(session, "SELECT * FROM t WHERE k = {}".format(n), maybe_add_compact_columns([n, n + 1, n + 2, n + 3, n + 4]))

        self.cluster.compact()

        for n in range(PARTITIONS):
            assert_one(session, "SELECT * FROM t WHERE k = {}".format(n), maybe_add_compact_columns([n, n + 1, n + 2, n + 3, n + 4]))

    def test_upgrade_with_statics(self):
        self.upgrade_with_statics(rows=10)

    def test_upgrade_with_wide_partition_and_statics(self):
        """ Checks we read old indexed sstables with statics by creating partitions larger than a single index block"""
        self.upgrade_with_statics(rows=1000)

    def upgrade_with_statics(self, rows):
        """
        Validates we can read legacy sstables with static columns.
        """
        PARTITIONS = 1
        ROWS = rows
        session = self._setup_cluster()

        session.execute('CREATE TABLE t (k int, s1 int static, s2 int static, t int, v1 int, v2 int, PRIMARY KEY (k, t))')

        for n in range(PARTITIONS):
            for r in range(ROWS):
                session.execute("INSERT INTO t(k, s1, s2, t, v1, v2) VALUES ({}, {}, {}, {}, {}, {})".format(n, r, r + 1, r, r, r + 1))

        session = self._do_upgrade()

        for n in range(PARTITIONS):
            assert_all(session,
                       "SELECT * FROM t WHERE k = {}".format(n),
                       [[n, v, ROWS - 1, ROWS, v, v + 1] for v in range(ROWS)])
            assert_all(session,
                       "SELECT * FROM t WHERE k = {} ORDER BY t DESC".format(n),
                       [[n, v, ROWS - 1, ROWS, v, v + 1] for v in range(ROWS - 1, -1, -1)])

        self.cluster.compact()

        for n in range(PARTITIONS):
            assert_all(session,
                       "SELECT * FROM t WHERE k = {}".format(n),
                       [[n, v, ROWS - 1, ROWS, v, v + 1] for v in range(ROWS)])
            assert_all(session,
                       "SELECT * FROM t WHERE k = {} ORDER BY t DESC".format(n),
                       [[n, v, ROWS - 1, ROWS, v, v + 1] for v in range(ROWS - 1, -1, -1)])

    def test_upgrade_with_wide_partition(self):
        """
        Checks we can read old indexed sstable by creating large partitions (larger than the index block used by sstables).
        """
        self.upgrade_with_wide_partition()

    def test_upgrade_with_wide_partition_reversed(self):
        """
        Checks we can read old indexed sstable by creating large partitions (larger than the index block used by sstables). This test
        validates reverse queries.
        """
        self.upgrade_with_wide_partition(query_modifier=" ORDER BY t DESC")

    def upgrade_with_wide_partition(self, query_modifier=""):
        ROWS = 100

        session = self._setup_cluster()

        session.execute('CREATE TABLE t (k int, t int, v1 int, v2 blob, v3 set<int>, PRIMARY KEY (k, t))')

        # the blob is only here to make the row bigger internally so it sometimes span multiple index blocks
        bigish_blob = "0x"
        for i in range(1000):
            bigish_blob = bigish_blob + "0000"

        for r in range(ROWS):
            session.execute("INSERT INTO t(k, t, v1, v2, v3) VALUES ({}, {}, {}, {}, {{{}, {}}})".format(0, r, r, bigish_blob, r * 2, r * 3))

        self.cluster.flush()

        # delete every other row
        for r in range(0, ROWS, 2):
            session.execute("DELETE FROM t WHERE k=0 AND t={}".format(r))

        # delete the set from every other remaining row
        for r in range(1, ROWS, 4):
            session.execute("UPDATE t SET v3={{}} WHERE k=0 AND t={}".format(r))

        session = self._do_upgrade()

        for r in range(0, ROWS):
            query = "SELECT t, v1, v3 FROM t WHERE k = 0 AND t={}{}".format(r, query_modifier)
            if (r - 1) % 4 == 0:
                assert_one(session, query, [r, r, None])
            elif (r + 1) % 2 == 0:
                assert_one(session, query, [r, r, set([r * 2, r * 3])])
            else:
                assert_none(session, query)

        self.cluster.compact()

        for r in range(ROWS):
            query = "SELECT t, v1, v3 FROM t WHERE k = 0 AND t={}{}".format(r, query_modifier)
            if (r - 1) % 4 == 0:
                assert_one(session, query, [r, r, None])
            elif (r + 1) % 2 == 0:
                assert_one(session, query, [r, r, set([r * 2, r * 3])])
            else:
                assert_none(session, query)

    def test_upgrade_with_index(self):
        """
        Checks a simple index can still be read after upgrade.
        """
        PARTITIONS = 2
        ROWS = 4

        session = self._setup_cluster()

        session.execute('CREATE TABLE t (k int, t int, v1 int, v2 int, PRIMARY KEY (k, t))')

        session.execute('CREATE INDEX ON t(v1)')

        for p in range(PARTITIONS):
            for r in range(ROWS):
                session.execute("INSERT INTO t(k, t, v1, v2) VALUES ({}, {}, {}, {})".format(p, r, r % 2, r * 2))

        self.cluster.flush()

        assert_all(session,
                   "SELECT * FROM t WHERE v1 = 0",
                   [[p, r, 0, r * 2] for p in range(PARTITIONS) for r in range(ROWS) if r % 2 == 0],
                   ignore_order=True)

        session = self._do_upgrade()

        assert_all(session,
                   "SELECT * FROM t WHERE v1 = 0",
                   [[p, r, 0, r * 2] for p in range(PARTITIONS) for r in range(ROWS) if r % 2 == 0],
                   ignore_order=True)

        self.cluster.compact()

        assert_all(session,
                   "SELECT * FROM t WHERE v1 = 0",
                   [[p, r, 0, r * 2] for p in range(PARTITIONS) for r in range(ROWS) if r % 2 == 0],
                   ignore_order=True)

    def test_upgrade_with_range_tombstones(self):
        """
        Checks sstable including range tombstone can be read after upgrade.

        @jira_ticket CASSANDRA-10360
        """
        ROWS = 100

        session = self._setup_cluster()

        session.execute('CREATE TABLE t (k int, t1 int, t2 int, PRIMARY KEY (k, t1, t2))')

        for n in range(ROWS):
            session.execute("INSERT INTO t(k, t1, t2) VALUES (0, 0, {})".format(n))

        session.execute("DELETE FROM t WHERE k=0 AND t1=0")

        for n in range(0, ROWS, 2):
            session.execute("INSERT INTO t(k, t1, t2) VALUES (0, 0, {})".format(n))

        session = self._do_upgrade()

        assert_all(session, "SELECT * FROM t WHERE k = 0", [[0, 0, n] for n in range(0, ROWS, 2)])

        self.cluster.compact()

    def test_upgrade_with_range_and_collection_tombstones(self):
        """
        Check sstable including collection tombstone (inserted through adding a collection) can be read after upgrade.

        @jira_ticket CASSANDRA-10743
        """
        session = self._setup_cluster()

        session.execute('CREATE TABLE t (k text, t int, c list<int>, PRIMARY KEY (k, t))')

        session.execute("INSERT INTO t(k, t, c) VALUES ('some_key', 0, %s)" % str([i for i in range(10000)]))

        session = self._do_upgrade()

        self.cluster.compact()

        assert_one(session, "SELECT k FROM t", ['some_key'])

    @since('3.0', max_version='3.99')
    def test_upgrade_with_range_tombstone_eoc_0(self):
        """
        Check sstable upgrading when the sstable contains a range tombstone with EOC=0.

        @jira_ticket CASSANDRA-12423
        """
        session = self._setup_cluster(cluster_options={'start_rpc': 'true'})

        session.execute("CREATE TABLE rt (id INT, c1 TEXT, c2 TEXT, v INT, PRIMARY KEY (id, c1, c2)) "
                        "with compact storage and compression = {'sstable_compression': ''};")

        range_delete = {
            i32(1): {
                'rt': [Mutation(deletion=Deletion(2470761440040513,
                                                  predicate=SlicePredicate(slice_range=SliceRange(
                                                      start=composite('a', eoc='\x00'),
                                                      finish=composite('asd', eoc='\x00')))))]
            }
        }

        client = get_thrift_client()
        client.transport.open()
        client.set_keyspace('ks')
        client.batch_mutate(range_delete, ConsistencyLevel.ONE)
        client.transport.close()

        session.execute("INSERT INTO rt (id, c1, c2, v) VALUES (1, 'asd', '', 0) USING TIMESTAMP 1470761451368658")
        session.execute("INSERT INTO rt (id, c1, c2, v) VALUES (1, 'asd', 'asd', 0) USING TIMESTAMP 1470761449416613")

        session = self._do_upgrade()

        ret = list(session.execute('SELECT * FROM rt'))
        assert_length_equal(ret, 2)

    @since('3.0')
    def test_upgrade_with_range_tombstone_ae(self):
        """
        Certain range tombstone pattern causes AssertionError when upgrade.
        This test makes sure it won't happeen.

        @jira_ticket CASSANDRA-12203
        """
        session = self._setup_cluster()
        session.execute('CREATE TABLE test (k ascii, c1 ascii, c2 int, c3 int, val text, PRIMARY KEY (k, c1, c2, c3))')
        session.execute("DELETE FROM ks.test WHERE k = 'a' AND c1 = 'a'")
        session.execute("DELETE FROM ks.test WHERE k = 'a' AND c1 = 'a' AND c2 = 1")
        session = self._do_upgrade()
        assert_none(session, "SELECT k FROM test")


@pytest.mark.upgrade_test
@since('3.0')
class TestBootstrapAfterUpgrade(TestStorageEngineUpgrade):

    @pytest.fixture(autouse=True)
    def set_up(self, storage_engine_upgrade_setup):
        self.fixture_dtest_setup.bootstrap=True
        self.fixture_dtest_setup.jvm_args=LEGACY_SSTABLES_JVM_ARGS

@pytest.mark.upgrade_test
@since('3.0', max_version='3.99')
class TestLoadKaSStables(TestBaseSStableLoader):
    upgrade_test = True
    upgrade_from = '2.1.20'
    jvm_args = LEGACY_SSTABLES_JVM_ARGS


@pytest.mark.upgrade_test
@since('3.0', max_version='3.99')
class TestLoadKaCompactSStables(TestBaseSStableLoader):
    upgrade_test = True
    upgrade_from = '2.1.20'
    jvm_args = LEGACY_SSTABLES_JVM_ARGS
    test_compact = True


@pytest.mark.upgrade_test
@since('3.0', max_version='3.99')
class TestLoadLaSStables(TestBaseSStableLoader):
    upgrade_test = True
    upgrade_from = '2.2.13'
    jvm_args = LEGACY_SSTABLES_JVM_ARGS


@pytest.mark.upgrade_test
@since('3.0', max_version='3.99')
class TestLoadLaCompactSStables(TestBaseSStableLoader):
    upgrade_test = True
    upgrade_from = '2.2.13'
    jvm_args = LEGACY_SSTABLES_JVM_ARGS
    test_compact = True


@pytest.mark.upgrade_test
@since('4.0', max_version='4.99')
class TestLoadMdSStables(TestBaseSStableLoader):
    upgrade_from = '3.0.17'


@pytest.mark.upgrade_test
@pytest.mark.skip("4.0 sstableloader can't handle formerly compact tables even after drop compact storage, rebuild, cleanup")
@since('4.0', max_version='4.99')
class TestLoadMdCompactSStables(TestBaseSStableLoader):
    upgrade_from = '3.0.17'
    test_compact = True


@pytest.mark.upgrade_test
@since('4.0', max_version='4.99')
class TestLoadMdThreeOneOneSStables(TestBaseSStableLoader):
    upgrade_from = '3.11.3'


@pytest.mark.upgrade_test
@pytest.mark.skip("4.0 sstableloader can't handle formerly compact tables even after drop compact storage, rebuild, cleanup")
@since('4.0', max_version='4.99')
class TestLoadMdThreeOneOneCompactSStables(TestBaseSStableLoader):
    upgrade_from = '3.11.3'
    test_compact = True
