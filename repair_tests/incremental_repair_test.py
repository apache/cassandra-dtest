import time
import pytest
import re
import logging

from datetime import datetime
from collections import Counter, namedtuple
from re import findall, compile
from uuid import UUID, uuid1

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.metadata import Murmur3Token
from ccmlib.common import is_win
from ccmlib.node import Node, ToolError

from dtest import Tester, create_ks, create_cf
from tools.assertions import assert_almost_equal, assert_one
from tools.data import create_c1c2_table, insert_c1c2
from tools.misc import new_node, ImmutableMapping
from tools.jmxutils import make_mbean, JolokiaAgent

since = pytest.mark.since
logger = logging.getLogger(__name__)


class ConsistentState(object):
    PREPARING = 0
    PREPARED = 1
    REPAIRING = 2
    FINALIZE_PROMISED = 3
    FINALIZED = 4
    FAILED = 5


def assert_parent_repair_session_count(nodes, expected):
    for node in nodes:
        with JolokiaAgent(node) as jmx:
            result = jmx.execute_method("org.apache.cassandra.db:type=RepairService",
                                        "parentRepairSessionsCount")
            assert expected == result, "The number of cached ParentRepairSessions should be {} but was {}. " \
                                       "This may mean that PRS objects are leaking on node {}. Check " \
                                       "ActiveRepairService for PRS clean up code.".format(expected, result, node.name)


class TestIncRepair(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            r'Can\'t send migration request: node.*is down'
        )

    @classmethod
    def _get_repaired_data(cls, node, keyspace):
        _sstable_name = compile('SSTable: (.+)')
        _repaired_at = compile('Repaired at: (\d+)')
        _pending_repair = compile('Pending repair: (\-\-|null|[a-f0-9\-]+)')
        _sstable_data = namedtuple('_sstabledata', ('name', 'repaired', 'pending_id'))

        out = node.run_sstablemetadata(keyspace=keyspace).stdout

        def matches(pattern):
            return filter(None, [pattern.match(l) for l in out.split('\n')])
        names = [m.group(1) for m in matches(_sstable_name)]
        repaired_times = [int(m.group(1)) for m in matches(_repaired_at)]

        def uuid_or_none(s):
            return None if s == 'null' or s == '--' else UUID(s)
        pending_repairs = [uuid_or_none(m.group(1)) for m in matches(_pending_repair)]
        assert names
        assert repaired_times
        assert pending_repairs
        assert len(names) == len(repaired_times) == len(pending_repairs)
        return [_sstable_data(*a) for a in zip(names, repaired_times, pending_repairs)]

    def assertNoRepairedSSTables(self, node, keyspace):
        """ Checks that no sstables are marked repaired, and none are marked pending repair """
        data = self._get_repaired_data(node, keyspace)
        assert all([t.repaired == 0 for t in data]), '{}'.format(data)
        assert all([t.pending_id is None for t in data])

    def assertAllPendingRepairSSTables(self, node, keyspace, pending_id=None):
        """ Checks that no sstables are marked repaired, and all are marked pending repair """
        data = self._get_repaired_data(node, keyspace)
        assert all([t.repaired == 0 for t in data]), '{}'.format(data)
        if pending_id:
            assert all([t.pending_id == pending_id for t in data])
        else:
            assert all([t.pending_id is not None for t in data])

    def assertAllRepairedSSTables(self, node, keyspace):
        """ Checks that all sstables are marked repaired, and none are marked pending repair """
        data = self._get_repaired_data(node, keyspace)
        assert all([t.repaired > 0 for t in data]), '{}'.format(data)
        assert all([t.pending_id is None for t in data]), '{}'.format(data)

    def assertRepairedAndUnrepaired(self, node, keyspace):
        """ Checks that a node has both repaired and unrepaired sstables for a given keyspace """
        data = self._get_repaired_data(node, keyspace)
        assert any([t.repaired > 0 for t in data]), '{}'.format(data)
        assert any([t.repaired == 0 for t in data]), '{}'.format(data)
        assert all([t.pending_id is None for t in data]), '{}'.format(data)

    @since('4.0')
    def test_consistent_repair(self):
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                     'num_tokens': 1,
                                                                                     'commitlog_sync_period_in_ms': 500})
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()

        # make data inconsistent between nodes
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))
        node3.flush()
        time.sleep(1)
        node3.stop(gently=False)
        stmt.consistency_level = ConsistencyLevel.QUORUM

        session = self.exclusive_cql_connection(node1)
        for i in range(10):
            session.execute(stmt, (i + 10, i + 10))
        node1.flush()
        time.sleep(1)
        node1.stop(gently=False)
        node3.start(wait_for_binary_proto=True)
        session = self.exclusive_cql_connection(node2)
        for i in range(10):
            session.execute(stmt, (i + 20, i + 20))
        node1.start(wait_for_binary_proto=True)

        # flush and check that no sstables are marked repaired
        for node in self.cluster.nodelist():
            node.flush()
            self.assertNoRepairedSSTables(node, 'ks')
            session = self.patient_exclusive_cql_connection(node)
            results = list(session.execute("SELECT * FROM system.repairs"))
            assert len(results) == 0, str(results)

        # disable compaction so we can verify sstables are marked pending repair
        for node in self.cluster.nodelist():
            node.nodetool('disableautocompaction ks tbl')

        node1.repair(options=['ks'])

        # check that all participating nodes have the repair recorded in their system
        # table, that all nodes are listed as participants, and that all sstables are
        # (still) marked pending repair
        expected_participants = {n.address() for n in self.cluster.nodelist()}
        expected_participants_wp = {n.address_and_port() for n in self.cluster.nodelist()}
        recorded_pending_ids = set()
        for node in self.cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node)
            results = list(session.execute("SELECT * FROM system.repairs"))
            assert len(results) == 1
            result = results[0]
            assert set(result.participants) == expected_participants
            if hasattr(result, "participants_wp"):
                assert set(result.participants_wp) == expected_participants_wp
            assert result.state, ConsistentState.FINALIZED == "4=FINALIZED"
            pending_id = result.parent_id
            self.assertAllPendingRepairSSTables(node, 'ks', pending_id)
            recorded_pending_ids.add(pending_id)

        assert len(recorded_pending_ids) == 1

        # sstables are compacted out of pending repair by a compaction
        # task, we disabled compaction earlier in the test, so here we
        # force the compaction and check that all sstables are promoted
        for node in self.cluster.nodelist():
            node.nodetool('compact ks tbl')
            self.assertAllRepairedSSTables(node, 'ks')

    def test_sstable_marking(self):
        """
        * Launch a three node cluster
        * Stop node3
        * Write 10K rows with stress
        * Start node3
        * Issue an incremental repair, and wait for it to finish
        * Run sstablemetadata on every node, assert that all sstables are marked as repaired
        """
        # hinted handoff can create SSTable that we don't need after node3 restarted
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false'})
        self.init_default_config()
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()

        node3.stop(gently=True)

        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=3)'])
        node1.flush()
        node2.flush()

        node3.start()
        if node3.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        node3.watch_log_for("Initializing keyspace1.standard1", filename=log_file)
        # wait for things to settle before starting repair
        time.sleep(1)
        if self.cluster.version() >= "2.2":
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")

        if self.cluster.version() >= '4.0':
            # sstables are compacted out of pending repair by a compaction
            for node in self.cluster.nodelist():
                node.nodetool('compact keyspace1 standard1')

        for out in (node.run_sstablemetadata(keyspace='keyspace1').stdout for node in self.cluster.nodelist()):
            assert 'Repaired at: 0' not in out

    def test_multiple_repair(self):
        """
        * Launch a three node cluster
        * Create a keyspace with RF 3 and a table
        * Insert 49 rows
        * Stop node3
        * Insert 50 more rows
        * Restart node3
        * Issue an incremental repair on node3
        * Stop node2
        * Insert a final50 rows
        * Restart node2
        * Issue an incremental repair on node2
        * Replace node3 with a new node
        * Verify data integrity
        # TODO: Several more verifications of data need to be interspersed throughout the test. The final assertion is insufficient.
        @jira_ticket CASSANDRA-10644
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        if cluster.version() < '4.0':
            create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})
        else:
            create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})

        logger.debug("insert data")

        insert_c1c2(session, keys=list(range(1, 50)), consistency=ConsistencyLevel.ALL)
        node1.flush()

        logger.debug("bringing down node 3")
        node3.flush()
        node3.stop(gently=False)

        logger.debug("inserting additional data into node 1 and 2")
        insert_c1c2(session, keys=list(range(50, 100)), consistency=ConsistencyLevel.TWO)
        node1.flush()
        node2.flush()

        logger.debug("restarting and repairing node 3")
        node3.start(wait_for_binary_proto=True)

        if cluster.version() >= "2.2":
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")

        # wait stream handlers to be closed on windows
        # after session is finished (See CASSANDRA-10644)
        if is_win:
            time.sleep(2)

        logger.debug("stopping node 2")
        node2.stop(gently=False)

        logger.debug("inserting data in nodes 1 and 3")
        insert_c1c2(session, keys=list(range(100, 150)), consistency=ConsistencyLevel.TWO)
        node1.flush()
        node3.flush()

        logger.debug("start and repair node 2")
        node2.start(wait_for_binary_proto=True)

        if cluster.version() >= "2.2":
            node2.repair()
        else:
            node2.nodetool("repair -par -inc")

        logger.debug("replace node and check data integrity")
        node3.stop(gently=False)
        node5 = Node('node5', cluster, True, ('127.0.0.5', 9160), ('127.0.0.5', 7000), '7500', '0', None, binary_interface=('127.0.0.5', 9042))
        cluster.add(node5, False, data_center="dc1")
        node5.start(replace_address='127.0.0.3', wait_for_binary_proto=120)

        assert_one(session, "SELECT COUNT(*) FROM ks.cf LIMIT 200", [149])

    def test_sstable_repairedset(self):
        """
        * Launch a two node cluster
        * Insert data with stress
        * Stop node2
        * Run sstablerepairedset against node2
        * Start node2
        * Run sstablemetadata on both nodes, pipe to a file
        * Verify the output of sstablemetadata shows no repairs have occurred
        * Stop node1
        * Insert more data with stress
        * Start node1
        * Issue an incremental repair
        * Run sstablemetadata on both nodes again, pipe to a new file
        * Verify repairs occurred and repairedAt was updated
        """
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false'})
        self.init_default_config()
        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=2)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)', '-rate', 'threads=50'])

        node1.flush()
        node2.flush()

        node2.stop(gently=False)

        node2.run_sstablerepairedset(keyspace='keyspace1')
        node2.start(wait_for_binary_proto=True)

        initialOut1 = node1.run_sstablemetadata(keyspace='keyspace1').stdout
        initialOut2 = node2.run_sstablemetadata(keyspace='keyspace1').stdout

        matches = findall('(?<=Repaired at:).*', '\n'.join([initialOut1, initialOut2]))
        logger.debug("Repair timestamps are: {}".format(matches))

        uniquematches = set(matches)
        matchcount = Counter(matches)

        assert len(uniquematches) >= 2, uniquematches

        assert len(max(matchcount)) >= 1, matchcount

        assert re.search('Repaired at: 0', '\n'.join([initialOut1, initialOut2]))

        node1.stop()
        node2.stress(['write', 'n=15K', 'no-warmup', '-schema', 'replication(factor=2)'])
        node2.flush()
        node1.start(wait_for_binary_proto=True)

        if self.cluster.version() >= "2.2":
            node1.repair()
        else:
            node1.nodetool("repair -par -inc")

        if self.cluster.version() >= '4.0':
            # sstables are compacted out of pending repair by a compaction
            for node in self.cluster.nodelist():
                node.nodetool('compact keyspace1 standard1')

        finalOut1 = node1.run_sstablemetadata(keyspace='keyspace1').stdout
        if not isinstance(finalOut1, str):
            finalOut1 = finalOut1
        finalOut2 = node2.run_sstablemetadata(keyspace='keyspace1').stdout
        if not isinstance(finalOut2, str):
            finalOut2 = finalOut2

        matches = findall('(?<=Repaired at:).*', '\n'.join([finalOut1, finalOut2]))

        logger.debug(matches)

        uniquematches = set(matches)
        matchcount = Counter(matches)

        assert len(uniquematches) >= 2

        assert len(max(matchcount)) >= 2

        assert not re.search('Repaired at: 0', '\n'.join([finalOut1, finalOut2]))

    def test_compaction(self):
        """
        Test we can major compact after an incremental repair
        * Launch a three node cluster
        * Create a keyspace with RF 3 and a table
        * Stop node3
        * Insert 100 rows
        * Restart node3
        * Issue an incremental repair
        * Insert 50 more rows
        * Perform a major compaction on node3
        * Verify all data is present
        # TODO: I have no idea what this is testing. The assertions do not verify anything meaningful.
        # TODO: Fix all the string formatting
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        session.execute("create table tab(key int PRIMARY KEY, val int);")

        node3.stop()

        for x in range(0, 100):
            session.execute("insert into tab(key,val) values(" + str(x) + ",0)")
        node1.flush()

        node3.start(wait_for_binary_proto=True)

        if cluster.version() >= "2.2":
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")
        for x in range(0, 150):
            session.execute("insert into tab(key,val) values(" + str(x) + ",1)")

        cluster.flush()

        node3.nodetool('compact')

        for x in range(0, 150):
            assert_one(session, "select val from tab where key =" + str(x), [1])

    @since("2.2")
    def test_multiple_full_repairs_lcs(self):
        """
        @jira_ticket CASSANDRA-11172 - repeated full repairs should not cause infinite loop in getNextBackgroundTask
        """
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        for x in range(0, 10):
            node1.stress(['write', 'n=100k', 'no-warmup', '-rate', 'threads=10', '-schema', 'compaction(strategy=LeveledCompactionStrategy,sstable_size_in_mb=10)', 'replication(factor=2)'])
            cluster.flush()
            cluster.wait_for_compactions()
            node1.nodetool("repair -full keyspace1 standard1")

    @pytest.mark.env("long")
    @pytest.mark.skip(reason='hangs CI')
    def test_multiple_subsequent_repair(self):
        """
        @jira_ticket CASSANDRA-8366

        There is an issue with subsequent inc repairs increasing load size.
        So we perform several repairs and check that the expected amount of data exists.
        * Launch a three node cluster
        * Write 5M rows with stress
        * Wait for minor compactions to finish
        * Issue an incremental repair on each node, sequentially
        * Issue major compactions on each node
        * Sleep for a while so load size can be propagated between nodes
        * Verify the correct amount of data is on each node
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        logger.debug("Inserting data with stress")
        node1.stress(['write', 'n=5M', 'no-warmup', '-rate', 'threads=10', '-schema', 'replication(factor=3)'])

        logger.debug("Flushing nodes")
        cluster.flush()

        logger.debug("Waiting compactions to finish")
        cluster.wait_for_compactions()

        if self.cluster.version() >= '2.2':
            logger.debug("Repairing node1")
            node1.nodetool("repair")
            logger.debug("Repairing node2")
            node2.nodetool("repair")
            logger.debug("Repairing node3")
            node3.nodetool("repair")
        else:
            logger.debug("Repairing node1")
            node1.nodetool("repair -par -inc")
            logger.debug("Repairing node2")
            node2.nodetool("repair -par -inc")
            logger.debug("Repairing node3")
            node3.nodetool("repair -par -inc")

        # Using "print" instead of logger.debug() here is on purpose.  The compactions
        # take a long time and don't print anything by default, which can result
        # in the test being timed out after 20 minutes.  These print statements
        # prevent it from being timed out.
        print("compacting node1")
        node1.compact()
        print("compacting node2")
        node2.compact()
        print("compacting node3")
        node3.compact()

        # wait some time to be sure the load size is propagated between nodes
        logger.debug("Waiting for load size info to be propagated between nodes")
        time.sleep(45)

        load_size_in_kb = float(sum([n.data_size() for n in [node1, node2, node3]]))
        load_size = load_size_in_kb / 1024 / 1024
        logger.debug("Total Load size: {}GB".format(load_size))

        # There is still some overhead, but it's lot better. We tolerate 25%.
        expected_load_size = 4.5  # In GB
        assert_almost_equal(load_size, expected_load_size, error=0.25)

    @pytest.mark.resource_intensive
    def test_sstable_marking_not_intersecting_all_ranges(self):
        """
        @jira_ticket CASSANDRA-10299
        * Launch a four node cluster
        * Insert data with stress
        * Issue an incremental repair on each node sequentially
        * Assert no extra, unrepaired sstables are generated
        """
        cluster = self.cluster
        cluster.populate(4).start()
        node1, node2, node3, node4 = cluster.nodelist()

        logger.debug("Inserting data with stress")
        node1.stress(['write', 'n=3', 'no-warmup', '-rate', 'threads=1', '-schema', 'replication(factor=3)'])

        logger.debug("Flushing nodes")
        cluster.flush()

        repair_options = '' if self.cluster.version() >= '2.2' else '-inc -par'

        logger.debug("Repairing node 1")
        node1.nodetool("repair {}".format(repair_options))
        logger.debug("Repairing node 2")
        node2.nodetool("repair {}".format(repair_options))
        logger.debug("Repairing node 3")
        node3.nodetool("repair {}".format(repair_options))
        logger.debug("Repairing node 4")
        node4.nodetool("repair {}".format(repair_options))

        if cluster.version() >= '4.0':
            # sstables are compacted out of pending repair by a compaction
            for node in cluster.nodelist():
                node.nodetool('compact keyspace1 standard1')

        for out in (node.run_sstablemetadata(keyspace='keyspace1').stdout for node in cluster.nodelist() if len(node.get_sstables('keyspace1', 'standard1')) > 0):
            assert 'Repaired at: 0' not in out

    @pytest.mark.no_vnodes
    @since('4.0')
    def test_move(self):
        """ Test repaired data remains in sync after a move """
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                     'commitlog_sync_period_in_ms': 500})
        self.init_default_config()
        self.cluster.populate(4, tokens=[0, 2**32, 2**48, -(2**32)]).start()
        node1, node2, node3, node4 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        # insert some data
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        for i in range(1000):
            session.execute(stmt, (i, i))

        node1.repair(options=['ks'])

        for i in range(1000):
            v = i + 1000
            session.execute(stmt, (v, v))

        # everything should be in sync
        for node in self.cluster.nodelist():
            result = node.repair(options=['ks', '--validate'])
            assert "Repaired data is in sync" in result.stdout

        node2.nodetool('move {}'.format(2**16))

        # everything should still be in sync
        for node in self.cluster.nodelist():
            result = node.repair(options=['ks', '--validate'])
            assert "Repaired data is in sync" in result.stdout

    @pytest.mark.no_vnodes
    @since('4.0')
    def test_decommission(self):
        """ Test repaired data remains in sync after a decommission """
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                     'commitlog_sync_period_in_ms': 500})
        self.init_default_config()
        self.cluster.populate(4).start()
        node1, node2, node3, node4 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        # insert some data
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        for i in range(1000):
            session.execute(stmt, (i, i))

        node1.repair(options=['ks'])

        for i in range(1000):
            v = i + 1000
            session.execute(stmt, (v, v))

        # everything should be in sync
        for node in self.cluster.nodelist():
            result = node.repair(options=['ks', '--validate'])
            assert "Repaired data is in sync" in result.stdout

        node2.nodetool('decommission')

        # everything should still be in sync
        for node in [node1, node3, node4]:
            result = node.repair(options=['ks', '--validate'])
            assert "Repaired data is in sync" in result.stdout

    @pytest.mark.no_vnodes
    @since('4.0')
    def test_bootstrap(self):
        """ Test repaired data remains in sync after a bootstrap """
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                     'commitlog_sync_period_in_ms': 500})
        self.init_default_config()
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        # insert some data
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        for i in range(1000):
            session.execute(stmt, (i, i))

        node1.repair(options=['ks'])

        for i in range(1000):
            v = i + 1000
            session.execute(stmt, (v, v))

        # everything should be in sync
        for node in [node1, node2, node3]:
            result = node.repair(options=['ks', '--validate'])
            assert "Repaired data is in sync" in result.stdout

        node4 = new_node(self.cluster)
        node4.start(wait_for_binary_proto=True)

        assert len(self.cluster.nodelist()) == 4
        # everything should still be in sync
        for node in self.cluster.nodelist():
            result = node.repair(options=['ks', '--validate'])
            assert "Repaired data is in sync" in result.stdout

    @since('4.0')
    def test_force(self):
        """
        forcing an incremental repair should incrementally repair any nodes
        that are up, but should not promote the sstables to repaired
        """
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                     'num_tokens': 1,
                                                                                     'commitlog_sync_period_in_ms': 500})
        self.init_default_config()
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))

        node2.stop(wait_other_notice=True)

        # repair should fail because node2 is down
        with pytest.raises(ToolError):
            node1.repair(options=['ks'])

        # run with force flag
        node1.repair(options=['ks', '--force'])

        # ... and verify nothing was promoted to repaired
        self.assertNoRepairedSSTables(node1, 'ks')
        self.assertNoRepairedSSTables(node2, 'ks')

    @since('4.0')
    def test_force_with_none_down(self):
        """
        if we force an incremental repair, but all the involved nodes are up, 
        we should run normally and promote sstables afterwards
        """
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                     'num_tokens': 1,
                                                                                     'commitlog_sync_period_in_ms': 500})
        self.init_default_config()
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))

        # run with force flag
        node1.repair(options=['ks', '--force'])

        # ... and verify everything was still promoted
        self.assertAllRepairedSSTables(node1, 'ks')
        self.assertAllRepairedSSTables(node2, 'ks')
        self.assertAllRepairedSSTables(node3, 'ks')

    @since('4.0')
    def test_hosts(self):
        """
        running an incremental repair with hosts specified should incrementally repair
        the given nodes, but should not promote the sstables to repaired
        """
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                     'num_tokens': 1,
                                                                                     'commitlog_sync_period_in_ms': 500})
        self.init_default_config()
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))

        # run with force flag
        node1.repair(options=['ks', '-hosts', ','.join([node1.address(), node2.address()])])

        # ... and verify nothing was promoted to repaired
        self.assertNoRepairedSSTables(node1, 'ks')
        self.assertNoRepairedSSTables(node2, 'ks')

    @since('4.0')
    def test_subrange(self):
        """
        running an incremental repair with hosts specified should incrementally repair
        the given nodes, but should not promote the sstables to repaired
        """
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                     'num_tokens': 1,
                                                                                     'commitlog_sync_period_in_ms': 500,
                                                                                     'partitioner': 'org.apache.cassandra.dht.Murmur3Partitioner'})
        self.init_default_config()
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))

        for node in self.cluster.nodelist():
            node.flush()
            self.assertNoRepairedSSTables(node, 'ks')

        # only repair the partition k=0
        token = Murmur3Token.from_key(bytes([0, 0, 0, 0]))
        # import ipdb; ipdb.set_trace()
        # run with force flag
        node1.repair(options=['ks', '-st', str(token.value - 1), '-et', str(token.value)])

        # verify we have a mix of repaired and unrepaired sstables
        self.assertRepairedAndUnrepaired(node1, 'ks')
        self.assertRepairedAndUnrepaired(node2, 'ks')
        self.assertRepairedAndUnrepaired(node3, 'ks')

    @since('4.0')
    def test_repaired_tracking_with_partition_deletes(self):
        """
        check that when an tracking repaired data status following a digest mismatch,
        repaired data mismatches are marked as unconfirmed as we may skip sstables
        after the partition delete are encountered.
        @jira_ticket CASSANDRA-14145
        """
        session, node1, node2 = self.setup_for_repaired_data_tracking()
        stmt = SimpleStatement("INSERT INTO ks.tbl (k, c, v) VALUES (%s, %s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i, i))

        for node in self.cluster.nodelist():
            node.flush()
            self.assertNoRepairedSSTables(node, 'ks')

        node1.repair(options=['ks'])
        node2.stop(wait_other_notice=True)

        session.execute("delete from ks.tbl where k = 5")

        node1.flush()
        node2.start()

        # expect unconfirmed inconsistencies as the partition deletes cause some sstables to be skipped
        with JolokiaAgent(node1) as jmx:
            self.query_and_check_repaired_mismatches(jmx, session, "SELECT * FROM ks.tbl WHERE k = 5",
                                                     expect_unconfirmed_inconsistencies=True)
            self.query_and_check_repaired_mismatches(jmx, session, "SELECT * FROM ks.tbl WHERE k = 5 AND c = 5",
                                                     expect_unconfirmed_inconsistencies=True)
            # no digest reads for range queries so blocking read repair metric isn't incremented
            # *all* sstables are read for partition ranges too, and as the repaired set is still in sync there should
            # be no inconsistencies
            self.query_and_check_repaired_mismatches(jmx, session, "SELECT * FROM ks.tbl", expect_read_repair=False)

    @since('4.0')
    def test_repaired_tracking_with_varying_sstable_sets(self):
        """
        verify that repaired data digests are computed over the merged data for each replica
        and that the particular number of sstables on each doesn't affect the comparisons
        both replicas start with the same repaired set, comprising 2 sstables. node1's is
        then compacted and additional unrepaired data added (which overwrites some in the
        repaired set). We expect the repaired digests to still match as the tracking will
        force all sstables containing the partitions to be read
        there are two variants of this, for single partition slice & names reads and range reads
        @jira_ticket CASSANDRA-14145
        """
        session, node1, node2 = self.setup_for_repaired_data_tracking()
        stmt = SimpleStatement("INSERT INTO ks.tbl (k, c, v) VALUES (%s, %s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i, i))

        for node in self.cluster.nodelist():
            node.flush()

        for i in range(10,20):
            session.execute(stmt, (i, i, i))

        for node in self.cluster.nodelist():
            node.flush()
            self.assertNoRepairedSSTables(node, 'ks')

        node1.repair(options=['ks'])
        node2.stop(wait_other_notice=True)

        session.execute("insert into ks.tbl (k, c, v) values (5, 5, 55)")
        session.execute("insert into ks.tbl (k, c, v) values (15, 15, 155)")
        node1.flush()
        node1.compact()
        node1.compact()
        node2.start()

        # we don't expect any inconsistencies as all repaired data is read on both replicas
        with JolokiaAgent(node1) as jmx:
            self.query_and_check_repaired_mismatches(jmx, session, "SELECT * FROM ks.tbl WHERE k = 5")
            self.query_and_check_repaired_mismatches(jmx, session, "SELECT * FROM ks.tbl WHERE k = 5 AND c = 5")
            # no digest reads for range queries so read repair metric isn't incremented
            self.query_and_check_repaired_mismatches(jmx, session, "SELECT * FROM ks.tbl", expect_read_repair=False)

    @since('4.0')
    def test_repaired_tracking_with_mismatching_replicas(self):
        """
        verify that when replicas have different repaired sets, this can be detected via the digests
        computed at read time. All nodes have start with the same data, but only 1 replica's sstables
        are marked repaired. Then a divergence is introduced by overwriting on 1 replica only, which
        is required to trigger a digest mismatch & full data read (for single partition reads).
        As the repaired sets are different between the replicas, but no other shortcutting occurs
        (no partition tombstones or sstable skipping) and no sstables are involved in pending repair
        session, we expect confirmed inconsistencies to be reported.
        there are two variants of this, for single partition slice & names reads and range reads
        @jira_ticket CASSANDRA-14145
        """
        session, node1, node2 = self.setup_for_repaired_data_tracking()
        stmt = SimpleStatement("INSERT INTO ks.tbl (k, c, v) VALUES (%s, %s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i, i))

        for node in self.cluster.nodelist():
            node.flush()

        for i in range(10,20):
            session.execute(stmt, (i, i, i))

        for node in self.cluster.nodelist():
            node.flush()
            self.assertNoRepairedSSTables(node, 'ks')

        # stop node 2 and mark its sstables repaired
        node2.stop(wait_other_notice=True)
        node2.run_sstablerepairedset(keyspace='ks')
        # before restarting node2 overwrite some data on node1 to trigger digest mismatches
        session.execute("insert into ks.tbl (k, c, v) values (5, 5, 55)")
        node2.start(wait_for_binary_proto=True)

        out1 = node1.run_sstablemetadata(keyspace='ks').stdout
        out2 = node2.run_sstablemetadata(keyspace='ks').stdout

        # verify the repaired at times for the sstables on node1/node2
        assert all(t == 0 for t in [int(x) for x in [y.split(' ')[0] for y in findall('(?<=Repaired at: ).*', out1)]])
        assert all(t > 0 for t in [int(x) for x in [y.split(' ')[0] for y in findall('(?<=Repaired at: ).*', out2)]])

        # we expect inconsistencies due to sstables being marked repaired on one replica only
        # these are marked confirmed because no sessions are pending & all sstables are
        # skipped due to partition deletes
        with JolokiaAgent(node1) as jmx:
            self.query_and_check_repaired_mismatches(jmx, session, "SELECT * FROM ks.tbl WHERE k = 5",
                                                     expect_confirmed_inconsistencies=True)
            self.query_and_check_repaired_mismatches(jmx, session, "SELECT * FROM ks.tbl WHERE k = 5 AND c = 5",
                                                     expect_confirmed_inconsistencies=True)
            # no digest reads for range queries so read repair metric isn't incremented
            self.query_and_check_repaired_mismatches(jmx, session, "SELECT * FROM ks.tbl",
                                                     expect_confirmed_inconsistencies=True,
                                                     expect_read_repair=False)

    @since('4.0')
    def test_parent_repair_session_cleanup(self):
        """
        Calls incremental repair on 3 node cluster and verifies if all ParentRepairSession objects are cleaned
        @jira_ticket CASSANDRA-16446
        """
        self.cluster.populate(3).start()
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)

        for node in self.cluster.nodelist():
            node.repair(options=['ks'])

        assert_parent_repair_session_count(self.cluster.nodelist(), 0)

    def setup_for_repaired_data_tracking(self):
        self.fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                     'num_tokens': 1,
                                                                                     'commitlog_sync_period_in_ms': 500})
        self.init_default_config()
        self.cluster.populate(2)
        node1, node2 = self.cluster.nodelist()
        self.cluster.start()

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 2}")
        session.execute("CREATE TABLE ks.tbl (k INT, c INT, v INT, PRIMARY KEY (k,c)) with read_repair='NONE'")
        return session, node1, node2

    def query_and_check_repaired_mismatches(self, jmx, session, query,
                                            expect_read_repair=True,
                                            expect_unconfirmed_inconsistencies=False,
                                            expect_confirmed_inconsistencies=False):

        rr_count = make_mbean('metrics', type='ReadRepair', name='ReconcileRead')
        unconfirmed_count = make_mbean('metrics', type='Table,keyspace=ks', name='RepairedDataInconsistenciesUnconfirmed,scope=tbl')
        confirmed_count = make_mbean('metrics', type='Table,keyspace=ks', name='RepairedDataInconsistenciesConfirmed,scope=tbl')

        rr_before = self.get_attribute_count(jmx, rr_count)
        uc_before = self.get_attribute_count(jmx, unconfirmed_count)
        cc_before = self.get_attribute_count(jmx, confirmed_count)

        stmt = SimpleStatement(query)
        stmt.consistency_level = ConsistencyLevel.ALL
        session.execute(stmt)

        rr_after = self.get_attribute_count(jmx, rr_count)
        uc_after = self.get_attribute_count(jmx, unconfirmed_count)
        cc_after = self.get_attribute_count(jmx, confirmed_count)

        logger.debug("Read Repair Count: {before}, {after}".format(before=rr_before, after=rr_after))
        logger.debug("Unconfirmed Inconsistency Count: {before}, {after}".format(before=uc_before, after=uc_after))
        logger.debug("Confirmed Inconsistency Count: {before}, {after}".format(before=cc_before, after=cc_after))

        if expect_read_repair:
            assert rr_after > rr_before
        else:
            assert rr_after == rr_before

        if expect_unconfirmed_inconsistencies:
            assert uc_after > uc_before
        else:
            assert uc_after == uc_before

        if expect_confirmed_inconsistencies:
            assert cc_after > cc_before
        else:
            assert cc_after == cc_before

    def get_attribute_count(self, jmx, bean):
        # the MBean may not have been initialized, in which case Jolokia agent will return
        # a HTTP 404 response. If we receive such, we know that the count can only be 0
        if jmx.has_mbean(bean):
            return jmx.read_attribute(bean, 'Count')
        else:
            return 0
