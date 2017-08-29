import time
from datetime import datetime
from collections import Counter, namedtuple
from re import findall, compile
from unittest import skip
from uuid import UUID, uuid1

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from cassandra.metadata import Murmur3Token
from ccmlib.common import is_win
from ccmlib.node import Node, ToolError
from nose.plugins.attrib import attr

from dtest import Tester, debug, create_ks, create_cf
from tools.assertions import assert_almost_equal, assert_one
from tools.data import insert_c1c2
from tools.decorators import since, no_vnodes
from tools.misc import new_node


class ConsistentState(object):
    PREPARING = 0
    PREPARED = 1
    REPAIRING = 2
    FINALIZE_PROMISED = 3
    FINALIZED = 4
    FAILED = 5


class TestIncRepair(Tester):
    ignore_log_patterns = (r'Can\'t send migration request: node.*is down',)

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
        self.assertTrue(all([t.repaired == 0 for t in data]), '{}'.format(data))
        self.assertTrue(all([t.pending_id is None for t in data]))

    def assertAllPendingRepairSSTables(self, node, keyspace, pending_id=None):
        """ Checks that no sstables are marked repaired, and all are marked pending repair """
        data = self._get_repaired_data(node, keyspace)
        self.assertTrue(all([t.repaired == 0 for t in data]), '{}'.format(data))
        if pending_id:
            self.assertTrue(all([t.pending_id == pending_id for t in data]))
        else:
            self.assertTrue(all([t.pending_id is not None for t in data]))

    def assertAllRepairedSSTables(self, node, keyspace):
        """ Checks that all sstables are marked repaired, and none are marked pending repair """
        data = self._get_repaired_data(node, keyspace)
        self.assertTrue(all([t.repaired > 0 for t in data]), '{}'.format(data))
        self.assertTrue(all([t.pending_id is None for t in data]), '{}'.format(data))

    def assertRepairedAndUnrepaired(self, node, keyspace):
        """ Checks that a node has both repaired and unrepaired sstables for a given keyspace """
        data = self._get_repaired_data(node, keyspace)
        self.assertTrue(any([t.repaired > 0 for t in data]), '{}'.format(data))
        self.assertTrue(any([t.repaired == 0 for t in data]), '{}'.format(data))
        self.assertTrue(all([t.pending_id is None for t in data]), '{}'.format(data))

    @since('4.0')
    def consistent_repair_test(self):
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

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
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        session = self.exclusive_cql_connection(node2)
        for i in range(10):
            session.execute(stmt, (i + 20, i + 20))
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)

        # flush and check that no sstables are marked repaired
        for node in cluster.nodelist():
            node.flush()
            self.assertNoRepairedSSTables(node, 'ks')
            session = self.patient_exclusive_cql_connection(node)
            results = list(session.execute("SELECT * FROM system.repairs"))
            self.assertEqual(len(results), 0, str(results))

        # disable compaction so we can verify sstables are marked pending repair
        for node in cluster.nodelist():
            node.nodetool('disableautocompaction ks tbl')

        node1.repair(options=['ks'])

        # check that all participating nodes have the repair recorded in their system
        # table, that all nodes are listed as participants, and that all sstables are
        # (still) marked pending repair
        expected_participants = {n.address() for n in cluster.nodelist()}
        recorded_pending_ids = set()
        for node in cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node)
            results = list(session.execute("SELECT * FROM system.repairs"))
            self.assertEqual(len(results), 1)
            result = results[0]
            self.assertEqual(set(result.participants), expected_participants)
            self.assertEqual(result.state, ConsistentState.FINALIZED, "4=FINALIZED")
            pending_id = result.parent_id
            self.assertAllPendingRepairSSTables(node, 'ks', pending_id)
            recorded_pending_ids.add(pending_id)

        self.assertEqual(len(recorded_pending_ids), 1)

        # sstables are compacted out of pending repair by a compaction
        # task, we disabled compaction earlier in the test, so here we
        # force the compaction and check that all sstables are promoted
        for node in cluster.nodelist():
            node.nodetool('compact ks tbl')
            self.assertAllRepairedSSTables(node, 'ks')

    def _make_fake_session(self, keyspace, table):
        node1 = self.cluster.nodelist()[0]
        session = self.patient_exclusive_cql_connection(node1)
        session_id = uuid1()
        cfid = list(session.execute("SELECT * FROM system_schema.tables WHERE keyspace_name='{}' AND table_name='{}'".format(keyspace, table)))[0].id
        now = datetime.now()
        # pulled from a repairs table
        ranges = {'\x00\x00\x00\x08K\xc2\xed\\<\xd3{X\x00\x00\x00\x08r\x04\x89[j\x81\xc4\xe6',
                  '\x00\x00\x00\x08r\x04\x89[j\x81\xc4\xe6\x00\x00\x00\x08\xd8\xcdo\x9e\xcbl\x83\xd4',
                  '\x00\x00\x00\x08\xd8\xcdo\x9e\xcbl\x83\xd4\x00\x00\x00\x08K\xc2\xed\\<\xd3{X'}
        ranges = {buffer(b) for b in ranges}

        for node in self.cluster.nodelist():
            session = self.patient_exclusive_cql_connection(node)
            session.execute("INSERT INTO system.repairs "
                            "(parent_id, cfids, coordinator, last_update, participants, ranges, repaired_at, started_at, state) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            [session_id, {cfid}, node1.address(), now, {n.address() for n in self.cluster.nodelist()},
                             ranges, now, now, ConsistentState.REPAIRING])  # 2=REPAIRING

        time.sleep(1)
        for node in self.cluster.nodelist():
            node.stop(gently=False)

        for node in self.cluster.nodelist():
            node.start()

        return session_id

    @since('4.0')
    def manual_session_fail_test(self):
        """ check manual failing of repair sessions via nodetool works properly """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # make data inconsistent between nodes
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            self.assertIn("no sessions", out.stdout)

        session_id = self._make_fake_session('ks', 'tbl')

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("REPAIRING", line)

        node1.nodetool("repair_admin --cancel {}".format(session_id))

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin --all')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("FAILED", line)

    @since('4.0')
    def manual_session_cancel_non_coordinator_failure_test(self):
        """ check manual failing of repair sessions via a node other than the coordinator fails """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # make data inconsistent between nodes
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            self.assertIn("no sessions", out.stdout)

        session_id = self._make_fake_session('ks', 'tbl')

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("REPAIRING", line)

        try:
            node2.nodetool("repair_admin --cancel {}".format(session_id))
            self.fail("cancel from a non coordinator should fail")
        except ToolError:
            pass  # expected

        # nothing should have changed
        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("REPAIRING", line)

    @since('4.0')
    def manual_session_force_cancel_test(self):
        """ check manual failing of repair sessions via a non-coordinator works if the --force flag is set """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # make data inconsistent between nodes
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            self.assertIn("no sessions", out.stdout)

        session_id = self._make_fake_session('ks', 'tbl')

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("REPAIRING", line)

        node2.nodetool("repair_admin --cancel {} --force".format(session_id))

        for node in self.cluster.nodelist():
            out = node.nodetool('repair_admin --all')
            lines = out.stdout.split('\n')
            self.assertGreater(len(lines), 1)
            line = lines[1]
            self.assertIn(str(session_id), line)
            self.assertIn("FAILED", line)

    def sstable_marking_test(self):
        """
        * Launch a three node cluster
        * Stop node3
        * Write 10K rows with stress
        * Start node3
        * Issue an incremental repair, and wait for it to finish
        * Run sstablemetadata on every node, assert that all sstables are marked as repaired
        """
        cluster = self.cluster
        # hinted handoff can create SSTable that we don't need after node3 restarted
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node3.stop(gently=True)

        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=3)'])
        node1.flush()
        node2.flush()

        node3.start(wait_other_notice=True)
        if node3.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        node3.watch_log_for("Initializing keyspace1.standard1", filename=log_file)
        # wait for things to settle before starting repair
        time.sleep(1)
        if cluster.version() >= "2.2":
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")

        if cluster.version() >= '4.0':
            # sstables are compacted out of pending repair by a compaction
            for node in cluster.nodelist():
                node.nodetool('compact keyspace1 standard1')

        for out in (node.run_sstablemetadata(keyspace='keyspace1').stdout for node in cluster.nodelist()):
            self.assertNotIn('Repaired at: 0', out)

    def multiple_repair_test(self):
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
        create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        debug("insert data")

        insert_c1c2(session, keys=range(1, 50), consistency=ConsistencyLevel.ALL)
        node1.flush()

        debug("bringing down node 3")
        node3.flush()
        node3.stop(gently=False)

        debug("inserting additional data into node 1 and 2")
        insert_c1c2(session, keys=range(50, 100), consistency=ConsistencyLevel.TWO)
        node1.flush()
        node2.flush()

        debug("restarting and repairing node 3")
        node3.start(wait_for_binary_proto=True)

        if cluster.version() >= "2.2":
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")

        # wait stream handlers to be closed on windows
        # after session is finished (See CASSANDRA-10644)
        if is_win:
            time.sleep(2)

        debug("stopping node 2")
        node2.stop(gently=False)

        debug("inserting data in nodes 1 and 3")
        insert_c1c2(session, keys=range(100, 150), consistency=ConsistencyLevel.TWO)
        node1.flush()
        node3.flush()

        debug("start and repair node 2")
        node2.start(wait_for_binary_proto=True)

        if cluster.version() >= "2.2":
            node2.repair()
        else:
            node2.nodetool("repair -par -inc")

        debug("replace node and check data integrity")
        node3.stop(gently=False)
        node5 = Node('node5', cluster, True, ('127.0.0.5', 9160), ('127.0.0.5', 7000), '7500', '0', None, ('127.0.0.5', 9042))
        cluster.add(node5, False)
        node5.start(replace_address='127.0.0.3', wait_other_notice=True)

        assert_one(session, "SELECT COUNT(*) FROM ks.cf LIMIT 200", [149])

    def sstable_repairedset_test(self):
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
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False})
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        node1.stress(['write', 'n=10K', 'no-warmup', '-schema', 'replication(factor=2)', 'compaction(strategy=SizeTieredCompactionStrategy,enabled=false)', '-rate', 'threads=50'])

        node1.flush()
        node2.flush()

        node2.stop(gently=False)

        node2.run_sstablerepairedset(keyspace='keyspace1')
        node2.start(wait_for_binary_proto=True)

        initialOut1 = node1.run_sstablemetadata(keyspace='keyspace1').stdout
        initialOut2 = node2.run_sstablemetadata(keyspace='keyspace1').stdout

        matches = findall('(?<=Repaired at:).*', '\n'.join([initialOut1, initialOut2]))
        debug("Repair timestamps are: {}".format(matches))

        uniquematches = set(matches)
        matchcount = Counter(matches)

        self.assertGreaterEqual(len(uniquematches), 2, uniquematches)

        self.assertGreaterEqual(max(matchcount), 1, matchcount)

        self.assertIn('Repaired at: 0', '\n'.join([initialOut1, initialOut2]))

        node1.stop()
        node2.stress(['write', 'n=15K', 'no-warmup', '-schema', 'replication(factor=2)'])
        node2.flush()
        node1.start(wait_for_binary_proto=True)

        if cluster.version() >= "2.2":
            node1.repair()
        else:
            node1.nodetool("repair -par -inc")

        if cluster.version() >= '4.0':
            # sstables are compacted out of pending repair by a compaction
            for node in cluster.nodelist():
                node.nodetool('compact keyspace1 standard1')

        finalOut1 = node1.run_sstablemetadata(keyspace='keyspace1').stdout
        finalOut2 = node2.run_sstablemetadata(keyspace='keyspace1').stdout

        matches = findall('(?<=Repaired at:).*', '\n'.join([finalOut1, finalOut2]))

        debug(matches)

        uniquematches = set(matches)
        matchcount = Counter(matches)

        self.assertGreaterEqual(len(uniquematches), 2)

        self.assertGreaterEqual(max(matchcount), 2)

        self.assertNotIn('Repaired at: 0', '\n'.join([finalOut1, finalOut2]))

    def compaction_test(self):
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
    def multiple_full_repairs_lcs_test(self):
        """
        @jira_ticket CASSANDRA-11172 - repeated full repairs should not cause infinite loop in getNextBackgroundTask
        """
        cluster = self.cluster
        cluster.populate(2).start(wait_for_binary_proto=True)
        node1, node2 = cluster.nodelist()
        for x in xrange(0, 10):
            node1.stress(['write', 'n=100k', 'no-warmup', '-rate', 'threads=10', '-schema', 'compaction(strategy=LeveledCompactionStrategy,sstable_size_in_mb=10)', 'replication(factor=2)'])
            cluster.flush()
            cluster.wait_for_compactions()
            node1.nodetool("repair -full keyspace1 standard1")

    @attr('long')
    @skip('hangs CI')
    def multiple_subsequent_repair_test(self):
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

        debug("Inserting data with stress")
        node1.stress(['write', 'n=5M', 'no-warmup', '-rate', 'threads=10', '-schema', 'replication(factor=3)'])

        debug("Flushing nodes")
        cluster.flush()

        debug("Waiting compactions to finish")
        cluster.wait_for_compactions()

        if self.cluster.version() >= '2.2':
            debug("Repairing node1")
            node1.nodetool("repair")
            debug("Repairing node2")
            node2.nodetool("repair")
            debug("Repairing node3")
            node3.nodetool("repair")
        else:
            debug("Repairing node1")
            node1.nodetool("repair -par -inc")
            debug("Repairing node2")
            node2.nodetool("repair -par -inc")
            debug("Repairing node3")
            node3.nodetool("repair -par -inc")

        # Using "print" instead of debug() here is on purpose.  The compactions
        # take a long time and don't print anything by default, which can result
        # in the test being timed out after 20 minutes.  These print statements
        # prevent it from being timed out.
        print "compacting node1"
        node1.compact()
        print "compacting node2"
        node2.compact()
        print "compacting node3"
        node3.compact()

        # wait some time to be sure the load size is propagated between nodes
        debug("Waiting for load size info to be propagated between nodes")
        time.sleep(45)

        load_size_in_kb = float(sum(map(lambda n: n.data_size(), [node1, node2, node3])))
        load_size = load_size_in_kb / 1024 / 1024
        debug("Total Load size: {}GB".format(load_size))

        # There is still some overhead, but it's lot better. We tolerate 25%.
        expected_load_size = 4.5  # In GB
        assert_almost_equal(load_size, expected_load_size, error=0.25)

    @attr('resource-intensive')
    def sstable_marking_test_not_intersecting_all_ranges(self):
        """
        @jira_ticket CASSANDRA-10299
        * Launch a four node cluster
        * Insert data with stress
        * Issue an incremental repair on each node sequentially
        * Assert no extra, unrepaired sstables are generated
        """
        cluster = self.cluster
        cluster.populate(4).start(wait_for_binary_proto=True)
        node1, node2, node3, node4 = cluster.nodelist()

        debug("Inserting data with stress")
        node1.stress(['write', 'n=3', 'no-warmup', '-rate', 'threads=1', '-schema', 'replication(factor=3)'])

        debug("Flushing nodes")
        cluster.flush()

        repair_options = '' if self.cluster.version() >= '2.2' else '-inc -par'

        debug("Repairing node 1")
        node1.nodetool("repair {}".format(repair_options))
        debug("Repairing node 2")
        node2.nodetool("repair {}".format(repair_options))
        debug("Repairing node 3")
        node3.nodetool("repair {}".format(repair_options))
        debug("Repairing node 4")
        node4.nodetool("repair {}".format(repair_options))

        if cluster.version() >= '4.0':
            # sstables are compacted out of pending repair by a compaction
            for node in cluster.nodelist():
                node.nodetool('compact keyspace1 standard1')

        for out in (node.run_sstablemetadata(keyspace='keyspace1').stdout for node in cluster.nodelist() if len(node.get_sstables('keyspace1', 'standard1')) > 0):
            self.assertNotIn('Repaired at: 0', out)

    @no_vnodes()
    @since('4.0')
    def move_test(self):
        """ Test repaired data remains in sync after a move """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(4, tokens=[0, 2**32, 2**48, -(2**32)]).start()
        node1, node2, node3, node4 = cluster.nodelist()

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
        for node in cluster.nodelist():
            result = node.repair(options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

        node2.nodetool('move {}'.format(2**16))

        # everything should still be in sync
        for node in cluster.nodelist():
            result = node.repair(options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

    @no_vnodes()
    @since('4.0')
    def decommission_test(self):
        """ Test repaired data remains in sync after a decommission """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(4).start()
        node1, node2, node3, node4 = cluster.nodelist()

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
        for node in cluster.nodelist():
            result = node.repair(options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

        node2.nodetool('decommission')

        # everything should still be in sync
        for node in [node1, node3, node4]:
            result = node.repair(options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

    @no_vnodes()
    @since('4.0')
    def bootstrap_test(self):
        """ Test repaired data remains in sync after a bootstrap """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

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
            self.assertIn("Repaired data is in sync", result.stdout)

        node4 = new_node(self.cluster)
        node4.start(wait_for_binary_proto=True)

        self.assertEqual(len(self.cluster.nodelist()), 4)
        # everything should still be in sync
        for node in self.cluster.nodelist():
            result = node.repair(options=['ks', '--validate'])
            self.assertIn("Repaired data is in sync", result.stdout)

    @since('4.0')
    def force_test(self):
        """ 
        forcing an incremental repair should incrementally repair any nodes 
        that are up, but should not promote the sstables to repaired 
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))

        node2.stop()

        # repair should fail because node2 is down
        with self.assertRaises(ToolError):
            node1.repair(options=['ks'])

        # run with force flag
        node1.repair(options=['ks', '--force'])

        # ... and verify nothing was promoted to repaired
        self.assertNoRepairedSSTables(node1, 'ks')
        self.assertNoRepairedSSTables(node2, 'ks')

    @since('4.0')
    def hosts_test(self):
        """ 
        running an incremental repair with hosts specified should incrementally repair 
        the given nodes, but should not promote the sstables to repaired 
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False, 'num_tokens': 1, 'commitlog_sync_period_in_ms': 500})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

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
    def subrange_test(self):
        """ 
        running an incremental repair with hosts specified should incrementally repair 
        the given nodes, but should not promote the sstables to repaired 
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'hinted_handoff_enabled': False,
                                                  'num_tokens': 1,
                                                  'commitlog_sync_period_in_ms': 500,
                                                  'partitioner': 'org.apache.cassandra.dht.Murmur3Partitioner'})
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_exclusive_cql_connection(node3)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION={'class':'SimpleStrategy', 'replication_factor': 3}")
        session.execute("CREATE TABLE ks.tbl (k INT PRIMARY KEY, v INT)")
        stmt = SimpleStatement("INSERT INTO ks.tbl (k,v) VALUES (%s, %s)")
        stmt.consistency_level = ConsistencyLevel.ALL
        for i in range(10):
            session.execute(stmt, (i, i))

        for node in cluster.nodelist():
            node.flush()
            self.assertNoRepairedSSTables(node, 'ks')

        # only repair the partition k=0
        token = Murmur3Token.from_key(str(bytearray([0,0,0,0])))
        # import ipdb; ipdb.set_trace()
        # run with force flag
        node1.repair(options=['ks', '-st', str(token.value - 1), '-et', str(token.value)])

        # verify we have a mix of repaired and unrepaired sstables
        self.assertRepairedAndUnrepaired(node1, 'ks')
        self.assertRepairedAndUnrepaired(node2, 'ks')
        self.assertRepairedAndUnrepaired(node3, 'ks')
