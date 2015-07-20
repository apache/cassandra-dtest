import os
import time
from re import findall

from cassandra import ConsistencyLevel
from ccmlib.node import Node
from nose.plugins.attrib import attr

from assertions import assert_almost_equal, assert_one
from dtest import Tester, debug
from flaky import flaky
from tools import insert_c1c2, since


@since('2.1')
class TestIncRepair(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            r'Can\'t send migration request: node.*is down',
        ]
        Tester.__init__(self, *args, **kwargs)

    def sstable_marking_test(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node3.stop(gently=True)

        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])
        node1.flush()
        node2.flush()

        node3.start(wait_other_notice=True)
        time.sleep(3)

        if cluster.version() >= "2.2":
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")

        output = ""
        with open('sstables.txt', 'w') as f:
            node1.run_sstablemetadata(output_file=f, keyspace='keyspace1')
            node2.run_sstablemetadata(output_file=f, keyspace='keyspace1')
            node3.run_sstablemetadata(output_file=f, keyspace='keyspace1')

        with open("sstables.txt", 'r') as r:
            output = r.read().replace('\n', '')

        self.assertNotIn('repairedAt: 0', output)

        os.remove('sstables.txt')

    def multiple_repair_test(self):
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', read_repair=0.0, columns={'c1': 'text', 'c2': 'text'})

        debug("insert data")

        for x in range(1, 50):
            insert_c1c2(session, x, ConsistencyLevel.ALL)
        node1.flush()

        debug("bringing down node 3")
        node3.flush()
        node3.stop(gently=False)

        debug("inserting additional data into node 1 and 2")
        for y in range(50, 100):
            insert_c1c2(session, y, ConsistencyLevel.TWO)
        node1.flush()
        node2.flush()

        debug("restarting and repairing node 3")
        node3.start(wait_for_binary_proto=True)

        if cluster.version() >= "2.2":
            node3.repair()
        else:
            node3.nodetool("repair -par -inc")

        debug("stopping node 2")
        node2.stop(gently=False)

        debug("inserting data in nodes 1 and 3")
        for z in range(100, 150):
            insert_c1c2(session, z, ConsistencyLevel.TWO)
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
        cluster = self.cluster
        cluster.populate(2).start()
        node1, node2 = cluster.nodelist()
        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=2)'])

        node1.flush()
        node2.flush()

        node2.stop(gently=False)

        node2.run_sstablerepairedset(keyspace='keyspace1')
        node2.start(wait_for_binary_proto=True)

        with open('initial.txt', 'w') as f:
            node2.run_sstablemetadata(output_file=f, keyspace='keyspace1')
            node1.run_sstablemetadata(output_file=f, keyspace='keyspace1')

        node1.stop()
        node2.stress(['write', 'n=15000', '-schema', 'replication(factor=2)'])
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

        uniquematches = []
        matchcount = []
        for value in matches:
            if value not in uniquematches:
                uniquematches.append(value)
                matchcount.append(1)
            else:
                index = uniquematches.index(value)
                matchcount[index] = matchcount[index] + 1

        self.assertGreaterEqual(len(uniquematches), 2)

        self.assertGreaterEqual(max(matchcount), 2)

        self.assertNotIn('repairedAt: 0', finaloutput)

        os.remove('initial.txt')
        os.remove('final.txt')

    @since('2.1')
    def compaction_test(self):
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
        node1.flush()
        node2.flush()
        node3.flush()

        node3.nodetool('compact')

        for x in range(0, 150):
            assert_one(session, "select val from tab where key =" + str(x), [1])

    @since('2.1')
    @attr('long')
    @flaky  # see CASSANDRA-9752
    def multiple_subsequent_repair_test(self):
        """
        Covers CASSANDRA-8366

        There is an issue with subsequent inc repairs.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            'compaction_throughput_mb_per_sec': 0
        })
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()

        debug("Inserting data with stress")
        expected_load_size = 4.5  # In GB
        node1.stress(['write', 'n=5M', '-rate', 'threads=50', '-schema', 'replication(factor=3)'])

        debug("Flushing nodes")
        node1.flush()
        node2.flush()
        node3.flush()

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
        assert_almost_equal(load_size, expected_load_size, error=0.25)
