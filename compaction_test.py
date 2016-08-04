import os
import random
import re
import string
import subprocess
import tempfile
import time

from ccmlib import common
from assertions import assert_none, assert_one, assert_length_equal
from dtest import Tester, debug
from distutils.version import LooseVersion
from nose.tools import assert_equal
from tools import known_failure, since


class TestCompaction(Tester):

    __test__ = False

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def setUp(self):
        Tester.setUp(self)
        # compaction test for version 2.2.2 and above relies on DEBUG log in debug.log
        self.cluster.set_log_level("DEBUG")

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

        table_name = 'standard1'
        output = node1.nodetool('cfstats').stdout
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            initialValue = int(output[output.find(":") + 1:output.find("\n")].strip())
        else:
            debug("datasize not found")
            debug(output)

        block_on_compaction_log(node1)

        output = node1.nodetool('cfstats').stdout
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            finalValue = int(output[output.find(":") + 1:output.find("\n")].strip())
        else:
            debug("datasize not found")
        # allow 5% size increase - if we have few sstables it is not impossible that live size increases *slightly* after compaction
        self.assertLess(finalValue, initialValue * 1.05)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12323',
                   flaky=True)
    def bloomfilter_size_test(self):
        """
        @jira_ticket CASSANDRA-11344
        Check that bloom filter size is between 50KB and 100KB for 100K keys
        """
        if not hasattr(self, 'strategy') or self.strategy == "LeveledCompactionStrategy":
            strategy_string = 'strategy=LeveledCompactionStrategy,sstable_size_in_mb=1'
            min_bf_size = 40000
            max_bf_size = 100000
        else:
            if self.strategy == "DateTieredCompactionStrategy":
                strategy_string = "strategy=DateTieredCompactionStrategy,base_time_seconds=86400"  # we want a single sstable, so make sure we don't have a tiny first window
            else:
                strategy_string = "strategy={}".format(self.strategy)
            min_bf_size = 100000
            max_bf_size = 150000
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        for x in xrange(0, 5):
            node1.stress(['write', 'n=100K', "no-warmup", "cl=ONE", "-rate",
                          "threads=300", "-schema", "replication(factor=1)",
                          "compaction({},enabled=false)".format(strategy_string)])
            node1.flush()

        node1.nodetool('enableautocompaction')
        node1.wait_for_compactions()

        table_name = 'standard1'
        output = node1.nodetool('cfstats').stdout
        output = output[output.find(table_name):]
        output = output[output.find("Bloom filter space used"):]
        bfSize = int(output[output.find(":") + 1:output.find("\n")].strip())

        debug("bloom filter size is: {}".format(bfSize))

        self.assertGreaterEqual(bfSize, min_bf_size)
        self.assertLessEqual(bfSize, max_bf_size)

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
        time.sleep(1)
        try:
            for data_dir in node1.data_directories():
                cfs = os.listdir(os.path.join(data_dir, "ks"))
                ssdir = os.listdir(os.path.join(data_dir, "ks", cfs[0]))
                for afile in ssdir:
                    self.assertFalse("Data" in afile, afile)

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
        expected_sstable_count = 1
        if LooseVersion(self.cluster.version()) > LooseVersion('3.1'):
            expected_sstable_count = cluster.data_dir_count
        self.assertEqual(len(expired_sstables), expected_sstable_count)
        # write a new sstable to make DTCS check for expired sstables:
        for x in range(0, 100):
            session.execute('insert into cf (key, val) values ({}, {})'.format(x, x))
        node1.flush()
        time.sleep(5)
        # we only check every 10 minutes - sstable should still be there:
        for expired_sstable in expired_sstables:
            self.assertIn(expired_sstable, node1.get_sstables('ks', 'cf'))

        session.execute("alter table cf with compaction =  {'class':'DateTieredCompactionStrategy', 'max_sstable_age_days':0.00035, 'min_threshold':2, 'expired_sstable_check_frequency_seconds':0}")
        time.sleep(1)
        for x in range(0, 100):
            session.execute('insert into cf (key, val) values ({}, {})'.format(x, x))
        node1.flush()
        time.sleep(5)
        for expired_sstable in expired_sstables:
            self.assertNotIn(expired_sstable, node1.get_sstables('ks', 'cf'))

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

        stress_write(node1, keycount=200000 * cluster.data_dir_count)

        threshold = "5"
        node1.nodetool('setcompactionthroughput -- ' + threshold)

        matches = block_on_compaction_log(node1)
        stringline = matches[0]
        units = 'MB/s' if LooseVersion(cluster.version()) < LooseVersion('3.6') else '(K|M|G)iB/s'
        throughput_pattern = re.compile('''.*           # it doesn't matter what the line starts with
                                           =            # wait for an equals sign
                                           ([\s\d\.]*)  # capture a decimal number, possibly surrounded by whitespace
                                           {}.*         # followed by units
                                        '''.format(units), re.X)

        avgthroughput = re.match(throughput_pattern, stringline).group(1).strip()
        debug(avgthroughput)

        # The throughput in the log is computed independantly from the throttling and on the output files while
        # throttling is on the input files, so while that throughput shouldn't be higher than the one set in
        # principle, a bit of wiggle room is expected
        self.assertGreaterEqual(float(threshold) + 0.5, float(avgthroughput))

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11281',
                   flaky=True,
                   notes='windows')
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
        strlen = (1024 * 1024) / 100
        session.execute("CREATE TABLE large(userid text PRIMARY KEY, properties map<int, text>) with compression = {}")
        for i in range(200):  # ensures partition size larger than compaction_large_partition_warning_threshold_mb
            session.execute("UPDATE ks.large SET properties[%i] = '%s' WHERE userid = 'user'" % (i, get_random_word(strlen)))

        ret = list(session.execute("SELECT properties from ks.large where userid = 'user'"))
        assert_length_equal(ret, 1)
        self.assertEqual(200, len(ret[0][0].keys()))

        node.flush()

        node.nodetool('compact ks large')
        verb = 'Writing' if self.cluster.version() > '2.2' else 'Compacting'
        sizematcher = '\d+ bytes' if LooseVersion(self.cluster.version()) < LooseVersion('3.6') else '\d+\.\d{3}(K|M|G)iB'
        node.watch_log_for('{} large partition ks/large:user \({}\)'.format(verb, sizematcher), from_mark=mark, timeout=180)

        ret = list(session.execute("SELECT properties from ks.large where userid = 'user'"))
        assert_length_equal(ret, 1)
        self.assertEqual(200, len(ret[0][0].keys()))

    def disable_autocompaction_nodetool_test(self):
        """
        Make sure we can enable/disable compaction using nodetool
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE to_disable (id int PRIMARY KEY, d TEXT) WITH compaction = {{\'class\':\'{0}\'}}'.format(self.strategy))
        node.nodetool('disableautocompaction ks to_disable')
        for i in range(1000):
            session.execute('insert into to_disable (id, d) values ({0}, \'{1}\')'.format(i, 'hello' * 100))
            if i % 100 == 0:
                node.flush()
        if node.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(self.strategy))
        node.nodetool('enableautocompaction ks to_disable')
        # sleep to allow compactions to start
        time.sleep(2)
        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) > 0, 'Found no log items for {0}'.format(self.strategy))

    def disable_autocompaction_schema_test(self):
        """
        Make sure we can disable compaction via the schema compaction parameter 'enabled' = false
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE to_disable (id int PRIMARY KEY, d TEXT) WITH compaction = {{\'class\':\'{0}\', \'enabled\':\'false\'}}'.format(self.strategy))
        for i in range(1000):
            session.execute('insert into to_disable (id, d) values ({0}, \'{1}\')'.format(i, 'hello' * 100))
            if i % 100 == 0:
                node.flush()
        if node.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'

        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(self.strategy))
        # should still be disabled after restart:
        node.stop()
        node.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        session.execute("use ks")
        # sleep to make sure we dont start any logs
        time.sleep(2)
        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(self.strategy))
        node.nodetool('enableautocompaction ks to_disable')
        # sleep to allow compactions to start
        time.sleep(2)
        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) > 0, 'Found no log items for {0}'.format(self.strategy))

    def disable_autocompaction_alter_test(self):
        """
        Make sure we can enable compaction using an alter-statement
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE to_disable (id int PRIMARY KEY, d TEXT) WITH compaction = {{\'class\':\'{0}\'}}'.format(self.strategy))
        session.execute('ALTER TABLE to_disable WITH compaction = {{\'class\':\'{0}\', \'enabled\':\'false\'}}'.format(self.strategy))
        for i in range(1000):
            session.execute('insert into to_disable (id, d) values ({0}, \'{1}\')'.format(i, 'hello' * 100))
            if i % 100 == 0:
                node.flush()
        if node.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(self.strategy))
        session.execute('ALTER TABLE to_disable WITH compaction = {{\'class\':\'{0}\', \'enabled\':\'true\'}}'.format(self.strategy))
        # we need to flush atleast once when altering to enable:
        session.execute('insert into to_disable (id, d) values (99, \'hello\')')
        node.flush()
        # sleep to allow compactions to start
        time.sleep(2)
        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) > 0, 'Found no log items for {0}'.format(self.strategy))

    def disable_autocompaction_alter_and_nodetool_test(self):
        """
        Make sure compaction stays disabled after an alter statement where we have disabled using nodetool first
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = cluster.nodelist()
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE to_disable (id int PRIMARY KEY, d TEXT) WITH compaction = {{\'class\':\'{0}\'}}'.format(self.strategy))
        node.nodetool('disableautocompaction ks to_disable')
        for i in range(1000):
            session.execute('insert into to_disable (id, d) values ({0}, \'{1}\')'.format(i, 'hello' * 100))
            if i % 100 == 0:
                node.flush()
        if node.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(self.strategy))
        session.execute('ALTER TABLE to_disable WITH compaction = {{\'class\':\'{0}\', \'tombstone_threshold\':0.9}}'.format(self.strategy))
        session.execute('insert into to_disable (id, d) values (99, \'hello\')')
        node.flush()
        time.sleep(2)
        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found log items for {0}'.format(self.strategy))
        node.nodetool('enableautocompaction ks to_disable')
        # sleep to allow compactions to start
        time.sleep(2)
        self.assertTrue(len(node.grep_log('Compacting.+to_disable', filename=log_file)) > 0, 'Found no log items for {0}'.format(self.strategy))

    @since('3.7')
    def user_defined_compaction_test(self):
        """
        Test a user defined compaction task by generating a few sstables with cassandra stress
        and autocompaction disabled, and then passing a list of sstable data files directly to nodetool compact.
        Check that after compaction there is only one sstable per node directory. Make sure to use sstableutil
        to list only final files because after a compaction some temporary files may still not have been deleted.

        @jira_ticket CASSANDRA-11765
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()

        # disableautocompaction only disables compaction for existing tables,
        # so initialize stress tables with stress first
        stress_write(node1, keycount=1)
        node1.nodetool('disableautocompaction')

        stress_write(node1, keycount=500000)
        node1.nodetool('flush keyspace1 standard1')

        sstable_files = ' '.join(get_sstable_data_files(node1, 'keyspace1', 'standard1'))
        debug('Compacting {}'.format(sstable_files))
        node1.nodetool('compact --user-defined {}'.format(sstable_files))

        sstable_files = get_sstable_data_files(node1, 'keyspace1', 'standard1')
        self.assertEquals(len(node1.data_directories()), len(sstable_files),
                          'Expected one sstable data file per node directory but got {}'.format(sstable_files))

    def skip_if_no_major_compaction(self):
        if self.cluster.version() < '2.2' and self.strategy == 'LeveledCompactionStrategy':
            self.skipTest('major compaction not implemented for LCS in this version of Cassandra')


def get_sstable_data_files(node, ks, table):
    """
    Read sstable data files by using sstableutil, so we ignore temporary files
    """
    env = common.make_cassandra_env(node.get_install_cassandra_root(), node.get_node_cassandra_root())
    args = [node.get_tool('sstableutil'), '--type', 'final', ks, table]

    p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    assert_equal(p.returncode, 0, "Error invoking sstableutil; returned {code}; {err}"
                 .format(code=p.returncode, err=stderr))

    ret = sorted(filter(lambda s: s.endswith('-Data.db'), stdout.splitlines()))
    return ret


def get_random_word(wordLen, population=string.ascii_letters + string.digits):
    return ''.join([random.choice(population) for _ in range(wordLen)])


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
    if node.get_cassandra_version() < '2.2':
        log_file = 'system.log'
    else:
        log_file = 'debug.log'
    mark = node.mark_log(filename=log_file)
    node.flush()

    # on newer C* versions, default stress names are titlecased
    stress_keyspace, stress_table = ('keyspace1', 'standard1')

    ks = ks or stress_keyspace
    table = table or stress_table

    node.nodetool('compact {ks} {table}'.format(ks=ks, table=table))

    return node.watch_log_for('Compacted', from_mark=mark, filename=log_file)


def stress_write(node, keycount=100000):
    node.stress(['write', 'n={keycount}'.format(keycount=keycount)])


strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']
for strategy in strategies:
    cls_name = ('TestCompaction_with_' + strategy)
    vars()[cls_name] = type(cls_name, (TestCompaction,), {'strategy': strategy, '__test__': True})
