import os
import time
from collections import Counter
from re import findall
from unittest import skip

from cassandra import ConsistencyLevel
from ccmlib.common import is_win
from ccmlib.node import Node
from nose.plugins.attrib import attr

from assertions import assert_almost_equal, assert_one
from dtest import Tester, debug
from tools import insert_c1c2, known_failure, since


class TestIncRepair(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

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

        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=3)'])
        node1.flush()
        node2.flush()

        node3.start(wait_other_notice=True)
        time.sleep(3)

        if cluster.version() >= "2.2":
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")

        with open('sstables.txt', 'w') as f:
            node1.run_sstablemetadata(output_file=f, keyspace='keyspace1')
            node2.run_sstablemetadata(output_file=f, keyspace='keyspace1')
            node3.run_sstablemetadata(output_file=f, keyspace='keyspace1')

        with open("sstables.txt", 'r') as r:
            output = r.read().replace('\n', '')

        self.assertNotIn('Repaired at: 0', output)

        os.remove('sstables.txt')

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11268',
                   flaky=True,
                   notes='windows')
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
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

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
        node1.stress(['write', 'n=10K', '-schema', 'replication(factor=2)', '-rate', 'threads=50'])

        node1.flush()
        node2.flush()

        node2.stop(gently=False)

        node2.run_sstablerepairedset(keyspace='keyspace1')
        node2.start(wait_for_binary_proto=True)

        with open('initial.txt', 'w') as f:
            node2.run_sstablemetadata(output_file=f, keyspace='keyspace1')
            node1.run_sstablemetadata(output_file=f, keyspace='keyspace1')

        with open('initial.txt', 'r') as r:
            initialoutput = r.read()

        matches = findall('(?<=Repaired at:).*', initialoutput)
        debug("Repair timestamps are: {}".format(matches))

        uniquematches = set(matches)
        matchcount = Counter(matches)

        self.assertGreaterEqual(len(uniquematches), 2, uniquematches)

        self.assertGreaterEqual(max(matchcount), 1, matchcount)

        self.assertIn('Repaired at: 0', initialoutput)

        node1.stop()
        node2.stress(['write', 'n=15K', '-schema', 'replication(factor=2)'])
        node2.flush()
        node1.start(wait_for_binary_proto=True)

        if cluster.version() >= "2.2":
            node1.repair()
        else:
            node1.nodetool("repair -par -inc")

        with open('final.txt', 'w') as h:
            node1.run_sstablemetadata(output_file=h, keyspace='keyspace1')
            node2.run_sstablemetadata(output_file=h, keyspace='keyspace1')

        with open('final.txt', 'r') as r:
            finaloutput = r.read()

        matches = findall('(?<=Repaired at:).*', finaloutput)

        debug(matches)

        uniquematches = set(matches)
        matchcount = Counter(matches)

        self.assertGreaterEqual(len(uniquematches), 2)

        self.assertGreaterEqual(max(matchcount), 2)

        self.assertNotIn('Repaired at: 0', finaloutput)

        os.remove('initial.txt')
        os.remove('final.txt')

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
        self.create_ks(session, 'ks', 3)
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
            node1.stress(['write', 'n=100k', '-rate', 'threads=10', '-schema', 'compaction(strategy=LeveledCompactionStrategy,sstable_size_in_mb=10)', 'replication(factor=2)'])
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
        node1.stress(['write', 'n=5M', '-rate', 'threads=10', '-schema', 'replication(factor=3)'])

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
        node1.stress(['write', 'n=3', '-rate', 'threads=1', '-schema', 'replication(factor=3)'])

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

        with open("final.txt", "w") as h:
            node1.run_sstablemetadata(output_file=h, keyspace='keyspace1')
            node2.run_sstablemetadata(output_file=h, keyspace='keyspace1')
            node3.run_sstablemetadata(output_file=h, keyspace='keyspace1')
            node4.run_sstablemetadata(output_file=h, keyspace='keyspace1')

        with open("final.txt", "r") as r:
            output = r.read()

        self.assertNotIn('Repaired at: 0', output)

        os.remove('final.txt')
