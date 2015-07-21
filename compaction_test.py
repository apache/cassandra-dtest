import os
import re
import tempfile
import time

from assertions import assert_almost_equal, assert_none, assert_one
from dtest import Tester, debug
from tools import require, since


class TestCompaction(Tester):

    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    @since('0', '2.2.X')
    def compaction_delete_test(self):
        """
        Test that executing a delete properly tombstones a row.
        Insert data, delete a partition of data and check that the requesite rows are tombstoned.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)

        session.execute("create table ks.cf (key int PRIMARY KEY, val int) with compaction = {'class':'" + self.strategy + "'} and gc_grace_seconds = 30;")

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')

        node1.flush()
        for x in range(0, 10):
            session.execute('delete from cf where key = ' + str(x))

        node1.flush()
        for x in range(0, 10):
            assert_none(session, 'select * from cf where key = ' + str(x))

        json_path = tempfile.mkstemp(suffix='.json')
        jname = json_path[1]
        with open(jname, 'w') as f:
            node1.run_sstable2json(f)

        with open(jname, 'r') as g:
            jsoninfo = g.read()

        numfound = jsoninfo.count("markedForDeleteAt")

        self.assertEqual(numfound, 10)

    def data_size_test(self):
        """
        Ensure that data size does not have unwarranted increases after compaction.
        Insert data and check data size before and after a compaction.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        stress_write(node1)

        node1.flush()

        table_name = 'Standard1' if node1.get_cassandra_version() < '2.1' else 'standard1'
        output = node1.nodetool('cfstats', True)[0]
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            initialValue = int(output[output.find(":")+1:output.find("\n")].strip())
        else:
            debug("datasize not found")
            debug(output)

        block_on_compaction_log(node1)

        output = node1.nodetool('cfstats', True)[0]
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            finalValue = int(output[output.find(":")+1:output.find("\n")].strip())
        else:
            debug("datasize not found")

        self.assertLess(finalValue, initialValue)

    def sstable_deletion_test(self):
        """
        Test that sstables are deleted properly when able after compaction.
        Insert data setting gc_grace_seconds to 0, and determine sstable
        is deleted upon data deletion.
        """
        self.skip_if_no_major_compaction()
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute("create table cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and compaction= {'class':'" + self.strategy + "'}")

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')
        node1.flush()
        for x in range(0, 100):
            session.execute('delete from cf where key = ' + str(x))

        block_on_compaction_log(node1, ks='ks', table='cf')

        try:
            cfs = os.listdir(node1.get_path() + "/data/ks")
            ssdir = os.listdir(node1.get_path() + "/data/ks/" + cfs[0])
            for afile in ssdir:
                self.assertFalse("Data" in afile)

        except OSError:
            self.fail("Path to sstables not valid.")

    def dtcs_deletion_test(self):
        """
        Test that sstables are deleted properly when able after compaction with
        DateTieredCompactionStrategy.
        Insert data setting max_sstable_age_days low, and determine sstable
        is deleted upon data deletion past max_sstable_age_days.
        """
        if not hasattr(self, 'strategy'):
            self.strategy = 'DateTieredCompactionStrategy'
        elif self.strategy != 'DateTieredCompactionStrategy':
            self.skipTest('Not implemented unless DateTieredCompactionStrategy is used')

        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        # max sstable age is 0.5 minute:
        session.execute("""create table cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0
            and compaction= {'class':'DateTieredCompactionStrategy', 'max_sstable_age_days':0.00035, 'min_threshold':2}""")

        # insert data
        for x in range(0, 300):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1) USING TTL 35')
        node1.flush()
        time.sleep(40)
        expired_sstables = node1.get_sstables('ks', 'cf')
        self.assertEqual(len(expired_sstables), 1)
        expired_sstable = expired_sstables[0]
        # write a new sstable to make DTCS check for expired sstables:
        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (%d, %d)' % (x, x))
        node1.flush()
        time.sleep(5)
        assert expired_sstable not in node1.get_sstables('ks', 'cf')

    @require('dtest PR #373', broken_in='3.0')
    def compaction_throughput_test(self):
        """
        Test setting compaction throughput.
        Set throughput, insert data and ensure compaction performance corresponds.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        # disableautocompaction only disables compaction for existing tables,
        # so initialize stress tables with stress first
        stress_write(node1, keycount=1)
        node1.nodetool('disableautocompaction')

        stress_write(node1, keycount=200000)

        threshold = "5"
        node1.nodetool('setcompactionthroughput -- ' + threshold)

        matches = block_on_compaction_log(node1)
        stringline = matches[0]
        throughput_pattern = re.compile('''.*          # it doesn't matter what the line starts with
                                           =           # wait for an equals sign
                                           ([\s\d\.]*) # capture a decimal number, possibly surrounded by whitespace
                                           MB/s.*      # followed by 'MB/s'
                                        ''', re.X)

        avgthroughput = re.match(throughput_pattern, stringline).group(1).strip()
        debug(avgthroughput)

        assert_almost_equal(float(threshold), float(avgthroughput), error=0.1)

    def compaction_strategy_switching_test(self):
        """Ensure that switching strategies does not result in problems.
        Insert data, switch strategies, then check against data loss.
        """
        strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']

        if self.strategy in strategies:
            strategies.remove(self.strategy)
            cluster = self.cluster
            cluster.populate(1).start(wait_for_binary_proto=True)
            [node1] = cluster.nodelist()

            for strat in strategies:
                session = self.patient_cql_connection(node1)
                self.create_ks(session, 'ks', 1)

                session.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and compaction= {'class':'" + self.strategy + "'};")

                for x in range(0, 100):
                    session.execute('insert into ks.cf (key, val) values (' + str(x) + ',1)')

                node1.flush()

                for x in range(0, 10):
                    session.execute('delete from cf where key = ' + str(x))

                session.execute("alter table ks.cf with compaction = {'class':'" + strat + "'};")

                for x in range(11, 100):
                    assert_one(session, "select * from ks.cf where key =" + str(x), [x, 1])

                for x in range(0, 10):
                    assert_none(session, 'select * from cf where key = ' + str(x))

                node1.flush()
                cluster.clear()
                time.sleep(5)
                cluster.start(wait_for_binary_proto=True)

    @since("2.1")
    def large_compaction_warning_test(self):
        """
        @jira_ticket CASSANDRA-9643
        Check that we log a warning when the partition size is bigger than compaction_large_partition_warning_threshold_mb
        """
        cluster = self.cluster
        cluster.set_configuration_options({'compaction_large_partition_warning_threshold_mb': 1})
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()

        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)

        mark = node.mark_log()
        session.execute("CREATE TABLE large(userid text PRIMARY KEY, properties map<int, text>)")
        for i in range(50000):  # ensures partition size larger than compaction_large_partition_warning_threshold_mb
            session.execute("UPDATE ks.large SET properties[%i] = 'x' WHERE userid = 'user'" % i)

        node.flush()

        node.nodetool('compact ks large')
        node.watch_log_for('Compacting large partition ks/large:user \(\d+ bytes\)', from_mark=mark, timeout=180)

    def skip_if_no_major_compaction(self):
        if self.cluster.version() < '2.2' and self.strategy == 'LeveledCompactionStrategy':
            self.skipTest('major compaction not implemented for LCS in this version of Cassandra')


def block_on_compaction_log(node, ks=None, table=None):
    """
    @param node the node on which to trigger and block on compaction
    @param ks the keyspace to compact
    @param table the table to compact

    Helper method for testing compaction. This triggers compactions by
    calling flush and compact on node. In situations where major
    compaction won't apply to a table, such as in pre-2.2 LCS tables, the
    flush will trigger minor compactions.

    This method uses log-watching to block until compaction is completed.

    By default, this method uses the keyspace and table names generated by
    cassandra-stress. These will not be used if ks and table names parameters
    are passed in.

    Calling flush before calling this method may cause it to hang; if
    compaction completes before the method starts, it may not occur again
    during this method.
    """
    mark = node.mark_log()
    node.flush()

    # on newer C* versions, default stress names are titlecased
    stress_name_upper = node.get_cassandra_version() < '2.1'
    stress_keyspace, stress_table = (('Keyspace1', 'Standard1') if stress_name_upper else
                                     ('keyspace1', 'standard1'))

    ks = ks or stress_keyspace
    table = table or stress_table

    node.nodetool('compact {ks} {table}'.format(ks=ks, table=table))

    return node.watch_log_for('Compacted', from_mark=mark)


def stress_write(node, keycount=100000):
    if node.get_cassandra_version() < '2.1':
        node.stress(['--num-keys={keycount}'.format(keycount=keycount)])
    else:
        node.stress(['write', 'n={keycount}'.format(keycount=keycount)])


strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']
for strategy in strategies:
    cls_name = ('TestCompaction_with_' + strategy)
    vars()[cls_name] = type(cls_name, (TestCompaction,), {'strategy': strategy, '__test__': True})
