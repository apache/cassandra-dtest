import os
import random
import re
import string
import tempfile
import time
from distutils.version import LooseVersion
import pytest
import parse
import logging

from dtest import Tester, create_ks
from tools.assertions import assert_length_equal, assert_none, assert_one

since = pytest.mark.since
ported_to_in_jvm = pytest.mark.ported_to_in_jvm
logger = logging.getLogger(__name__)

strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy', 'UnifiedCompactionStrategy']


class TestCompaction(Tester):

    @pytest.fixture(scope='function', autouse=True)
    def fixture_set_cluster_log_level(self, fixture_dtest_setup):
        # compaction test for version 2.2.2 and above relies on DEBUG log in debug.log
        fixture_dtest_setup.cluster.set_log_level("DEBUG")

    @pytest.mark.parametrize("strategy", strategies)
    @since('0', max_version='2.2.X')
    def test_compaction_delete(self, strategy):
        """
        Test that executing a delete properly tombstones a row.
        Insert data, delete a partition of data and check that the requesite rows are tombstoned.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)

        session.execute("create table ks.cf (key int PRIMARY KEY, val int) with compaction = {'class':'" + strategy + "'} and gc_grace_seconds = 30;")

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

        assert numfound == 10

    def test_data_size(self):
        """
        Ensure that data size does not have unwarranted increases after compaction.
        Insert data and check data size before and after a compaction.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        stress_write(node1)

        node1.flush()

        table_name = 'standard1'
        output = node1.nodetool('tablestats').stdout
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            initialValue = int(output[output.find(":") + 1:output.find("\n")].strip())
        else:
            logger.debug("datasize not found")
            logger.debug(output)

        node1.flush()
        node1.compact()
        node1.wait_for_compactions()

        output = node1.nodetool('tablestats').stdout
        if output.find(table_name) != -1:
            output = output[output.find(table_name):]
            output = output[output.find("Space used (live)"):]
            finalValue = int(output[output.find(":") + 1:output.find("\n")].strip())
        else:
            logger.debug("datasize not found")
        # allow 5% size increase - if we have few sstables it is not impossible that live size increases *slightly* after compaction
        assert finalValue < initialValue * 1.05

    @pytest.mark.parametrize("strategy", strategies)
    def test_bloomfilter_size(self, strategy):
        """
        @jira_ticket CASSANDRA-11344
        Check that bloom filter size is between 50KB and 100KB for 100K keys
        """

        self.skip_if_not_supported(strategy)

        if not hasattr(self, 'strategy') or strategy == "LeveledCompactionStrategy":
            strategy_string = 'strategy=LeveledCompactionStrategy,sstable_size_in_mb=1'
            min_bf_size = 40000
            max_bf_size = 100000
        else:
            if strategy == "DateTieredCompactionStrategy":
                strategy_string = "strategy=DateTieredCompactionStrategy,base_time_seconds=86400"  # we want a single sstable, so make sure we don't have a tiny first window
            else:
                strategy_string = "strategy={}".format(strategy)
            min_bf_size = 100000
            max_bf_size = 150000
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        for x in range(0, 5):
            node1.stress(['write', 'n=100K', "no-warmup", "cl=ONE", "-rate",
                          "threads=300", "-schema", "replication(factor=1)",
                          "compaction({},enabled=false)".format(strategy_string)])
            node1.flush()

        node1.nodetool('enableautocompaction')
        node1.wait_for_compactions()

        table_name = 'standard1'
        output = node1.nodetool('tablestats').stdout
        output = output[output.find(table_name):]
        output = output[output.find("Bloom filter space used"):]
        bfSize = int(output[output.find(":") + 1:output.find("\n")].strip())

        # in some rare cases we can end up with more than one sstable per data directory with
        # non-lcs strategies (see CASSANDRA-12323)
        if not hasattr(self, 'strategy') or strategy == "LeveledCompactionStrategy":
            size_factor = 1
        else:
            sstable_count = len(node1.get_sstables('keyspace1', 'standard1'))
            dir_count = len(node1.data_directories())
            logger.debug("sstable_count is: {}".format(sstable_count))
            logger.debug("dir_count is: {}".format(dir_count))
            if node1.get_cassandra_version() < LooseVersion('3.2'):
                size_factor = sstable_count
            else:
                size_factor = sstable_count / float(dir_count)

        logger.debug("bloom filter size is: {}".format(bfSize))
        logger.debug("size factor = {}".format(size_factor))
        assert bfSize >= size_factor * min_bf_size
        assert bfSize <= size_factor * max_bf_size

    @pytest.mark.parametrize("strategy", strategies)
    def test_sstable_deletion(self, strategy):
        """
        Test that sstables are deleted properly when able after compaction.
        Insert data setting gc_grace_seconds to 0, and determine sstable
        is deleted upon data deletion.
        """
        self.skip_if_no_major_compaction(strategy)
        self.skip_if_not_supported(strategy)
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute("create table cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and compaction= {'class':'" + strategy + "'}")

        for x in range(0, 100):
            session.execute('insert into cf (key, val) values (' + str(x) + ',1)')
        node1.flush()
        for x in range(0, 100):
            session.execute('delete from cf where key = ' + str(x))

        node1.flush()
        node1.nodetool("compact ks cf")
        node1.wait_for_compactions()
        time.sleep(1)
        try:
            for data_dir in node1.data_directories():
                cfs = os.listdir(os.path.join(data_dir, "ks"))
                ssdir = os.listdir(os.path.join(data_dir, "ks", cfs[0]))
                for afile in ssdir:
                    assert not "Data" in afile, afile

        except OSError:
            pytest.fail("Path to sstables not valid.")

    @pytest.mark.parametrize("strategy", ['DateTieredCompactionStrategy'])
    def test_dtcs_deletion(self, strategy):
        """
        Test that sstables are deleted properly when able after compaction with
        DateTieredCompactionStrategy.
        Insert data setting max_sstable_age_days low, and determine sstable
        is deleted upon data deletion past max_sstable_age_days.
        """

        self.skip_if_not_supported(strategy)

        if strategy != 'DateTieredCompactionStrategy':
            pytest.skip('Not implemented unless DateTieredCompactionStrategy is used')

        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
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
        if self.cluster.version() > LooseVersion('3.1'):
            expected_sstable_count = cluster.data_dir_count
        assert len(expired_sstables) == expected_sstable_count
        # write a new sstable to make DTCS check for expired sstables:
        for x in range(0, 100):
            session.execute('insert into cf (key, val) values ({}, {})'.format(x, x))
        node1.flush()
        time.sleep(5)
        # we only check every 10 minutes - sstable should still be there:
        for expired_sstable in expired_sstables:
            assert expired_sstable, node1.get_sstables('ks' in 'cf')

        session.execute("alter table cf with compaction =  {'class':'DateTieredCompactionStrategy', 'max_sstable_age_days':0.00035, 'min_threshold':2, 'expired_sstable_check_frequency_seconds':0}")
        time.sleep(1)
        for x in range(0, 100):
            session.execute('insert into cf (key, val) values ({}, {})'.format(x, x))
        node1.flush()
        time.sleep(5)
        for expired_sstable in expired_sstables:
            assert expired_sstable, node1.get_sstables('ks' not in 'cf')

    def test_compaction_throughput(self):
        """
        Test setting compaction throughput.
        Set throughput, insert data and ensure compaction performance corresponds.
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        # disableautocompaction only disables compaction for existing tables,
        # so initialize stress tables with stress first
        stress_write(node1, keycount=1)
        node1.nodetool('disableautocompaction')

        stress_write(node1, keycount=200000 * cluster.data_dir_count)
        node1.flush()

        threshold = "5"
        node1.nodetool('setcompactionthroughput -- ' + threshold)

        if node1.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        mark = node1.mark_log(filename=log_file)
        node1.nodetool('compact keyspace1 standard1')
        matches = node1.watch_log_for('Compacted', from_mark=mark, filename=log_file)

        stringline = matches[0]

        throughput_pattern = '{}={avgthroughput:f}{units}/s'
        m = parse.search(throughput_pattern, stringline)
        avgthroughput = m.named['avgthroughput']
        found_units = m.named['units']

        unit_conversion_dct = {
            "MB": 1,
            "MiB": 1,
            "KiB": 1. / 1024,
            "GiB": 1024,
            "B": 1. / (1024 * 1024),
        }

        units = ['MB'] if cluster.version() < LooseVersion('3.6') else ['B', 'KiB', 'MiB', 'GiB']
        assert found_units in units

        logger.debug(avgthroughput)
        avgthroughput_mb = unit_conversion_dct[found_units] * float(avgthroughput)

        # The rate limiter accumulates a second of burst time before starting to throttle, so we should expect
        # the duration of the first operation to be one second shorter than what the limit sets. In this case it's
        # a 40 MiB file, so we need to accept the difference between 40MiB/7s and 40MiB/8s
        lenience = 8.0 / 7 * 1.05

        assert float(threshold) * lenience >= avgthroughput_mb

    @pytest.mark.parametrize("strategy", strategies)
    def test_compaction_strategy_switching(self, strategy):
        """
        Ensure that switching strategies does not result in problems.
        Insert data, switch strategies, then check against data loss.
        """

        self.skip_if_not_supported(strategy)

        if self.cluster.version() >= '5.0':
            strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy']
        else:
            strategies = ['LeveledCompactionStrategy', 'SizeTieredCompactionStrategy', 'DateTieredCompactionStrategy']

        if strategy in strategies:
            strategies.remove(strategy)
            cluster = self.cluster
            cluster.populate(1).start()
            [node1] = cluster.nodelist()

            for strat in strategies:
                session = self.patient_cql_connection(node1)
                create_ks(session, 'ks', 1)

                session.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds = 0 and compaction= {'class':'" + strategy + "'};")

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
                cluster.start()

    @ported_to_in_jvm('5.0')  # org.apache.cassandra.distributed.test.guardrails.GuardrailPartitionSizeTest
    def test_large_compaction_warning(self):
        """
        @jira_ticket CASSANDRA-9643
        Check that we log a warning when the partition size is bigger than compaction_large_partition_warning_threshold_mb
        """
        cluster = self.cluster
        cluster.set_configuration_options({'compaction_large_partition_warning_threshold_mb': 1})
        cluster.populate(1).start()
        [node] = cluster.nodelist()

        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        mark = node.mark_log()
        strlen = (1024 * 1024) / 100
        session.execute("CREATE TABLE large(userid text PRIMARY KEY, properties map<int, text>) with compression = {}")
        for i in range(200):  # ensures partition size larger than compaction_large_partition_warning_threshold_mb
            session.execute("UPDATE ks.large SET properties[%i] = '%s' WHERE userid = 'user'" % (i, get_random_word(strlen)))

        ret = list(session.execute("SELECT properties from ks.large where userid = 'user'"))
        assert_length_equal(ret, 1)
        assert 200 == len(list(ret[0][0].keys()))

        node.flush()

        node.nodetool('compact ks large')
        verb = 'Writing' if self.cluster.version() > '2.2' else 'Compacting'
        sizematcher = '\d+ bytes' if self.cluster.version() < LooseVersion('3.6') else '\d+\.\d{3}(K|M|G)iB'
        node.watch_log_for('{} large partition ks/large:user \({}'.format(verb, sizematcher), from_mark=mark, timeout=180)

        ret = list(session.execute("SELECT properties from ks.large where userid = 'user'"))
        assert_length_equal(ret, 1)
        assert 200 == len(list(ret[0][0].keys()))

    @pytest.mark.parametrize("strategy", strategies)
    def test_disable_autocompaction_nodetool(self, strategy):
        """
        Make sure we can enable/disable compaction using nodetool
        """
        self.skip_if_not_supported(strategy)
        cluster = self.cluster
        cluster.populate(1).start()
        [node] = cluster.nodelist()
        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE to_disable (id int PRIMARY KEY, d TEXT) WITH compaction = {{\'class\':\'{0}\'}}'.format(strategy))
        node.nodetool('disableautocompaction ks to_disable')
        for i in range(1000):
            session.execute('insert into to_disable (id, d) values ({0}, \'{1}\')'.format(i, 'hello' * 100))
            if i % 100 == 0:
                node.flush()
        if node.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(strategy)
        node.nodetool('enableautocompaction ks to_disable')
        # sleep to allow compactions to start
        time.sleep(2)
        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) > 0, 'Found no log items for {0}'.format(strategy)

    @pytest.mark.parametrize("strategy", strategies)
    def test_disable_autocompaction_schema(self, strategy):
        """
        Make sure we can disable compaction via the schema compaction parameter 'enabled' = false
        """
        self.skip_if_not_supported(strategy)
        cluster = self.cluster
        cluster.populate(1).start()
        [node] = cluster.nodelist()
        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE to_disable (id int PRIMARY KEY, d TEXT) WITH compaction = {{\'class\':\'{0}\', \'enabled\':\'false\'}}'.format(strategy))
        for i in range(1000):
            session.execute('insert into to_disable (id, d) values ({0}, \'{1}\')'.format(i, 'hello' * 100))
            if i % 100 == 0:
                node.flush()
        if node.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'

        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(strategy)
        # should still be disabled after restart:
        node.stop()
        node.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        session.execute("use ks")
        # sleep to make sure we dont start any logs
        time.sleep(2)
        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(strategy)
        node.nodetool('enableautocompaction ks to_disable')
        # sleep to allow compactions to start
        time.sleep(2)
        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) > 0, 'Found no log items for {0}'.format(strategy)

    @pytest.mark.parametrize("strategy", strategies)
    def test_disable_autocompaction_alter(self, strategy):
        """
        Make sure we can enable compaction using an alter-statement
        """
        self.skip_if_not_supported(strategy)
        cluster = self.cluster
        cluster.populate(1).start()
        [node] = cluster.nodelist()
        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE to_disable (id int PRIMARY KEY, d TEXT) WITH compaction = {{\'class\':\'{0}\'}}'.format(strategy))
        session.execute('ALTER TABLE to_disable WITH compaction = {{\'class\':\'{0}\', \'enabled\':\'false\'}}'.format(strategy))
        for i in range(1000):
            session.execute('insert into to_disable (id, d) values ({0}, \'{1}\')'.format(i, 'hello' * 100))
            if i % 100 == 0:
                node.flush()
        if node.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(strategy)
        session.execute('ALTER TABLE to_disable WITH compaction = {{\'class\':\'{0}\', \'enabled\':\'true\'}}'.format(strategy))
        # we need to flush atleast once when altering to enable:
        session.execute('insert into to_disable (id, d) values (99, \'hello\')')
        node.flush()
        # sleep to allow compactions to start
        time.sleep(2)
        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) > 0, 'Found no log items for {0}'.format(strategy)

    @pytest.mark.parametrize("strategy", strategies)
    def test_disable_autocompaction_alter_and_nodetool(self, strategy):
        """
        Make sure compaction stays disabled after an alter statement where we have disabled using nodetool first
        """
        self.skip_if_not_supported(strategy)
        cluster = self.cluster
        cluster.populate(1).start()
        [node] = cluster.nodelist()
        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)
        session.execute('CREATE TABLE to_disable (id int PRIMARY KEY, d TEXT) WITH compaction = {{\'class\':\'{0}\'}}'.format(strategy))
        node.nodetool('disableautocompaction ks to_disable')
        for i in range(1000):
            session.execute('insert into to_disable (id, d) values ({0}, \'{1}\')'.format(i, 'hello' * 100))
            if i % 100 == 0:
                node.flush()
        if node.get_cassandra_version() < '2.2':
            log_file = 'system.log'
        else:
            log_file = 'debug.log'
        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found compaction log items for {0}'.format(strategy)
        session.execute('ALTER TABLE to_disable WITH compaction = {{\'class\':\'{0}\', \'tombstone_threshold\':0.9}}'.format(strategy))
        session.execute('insert into to_disable (id, d) values (99, \'hello\')')
        node.flush()
        time.sleep(2)
        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) == 0, 'Found log items for {0}'.format(strategy)
        node.nodetool('enableautocompaction ks to_disable')
        # sleep to allow compactions to start
        time.sleep(2)
        assert len(node.grep_log('Compacting.+to_disable', filename=log_file)) > 0, 'Found no log items for {0}'.format(strategy)

    @since('3.7')
    def test_user_defined_compaction(self):
        """
        Test a user defined compaction task by generating a few sstables with cassandra stress
        and autocompaction disabled, and then passing a list of sstable data files directly to nodetool compact.
        Check that after compaction there is only one sstable per node directory. Make sure to use sstableutil
        to list only final files because after a compaction some temporary files may still not have been deleted.

        @jira_ticket CASSANDRA-11765
        """
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        # disableautocompaction only disables compaction for existing tables,
        # so initialize stress tables with stress first
        stress_write(node1, keycount=1)
        node1.nodetool('disableautocompaction')

        stress_write(node1, keycount=500000)
        node1.nodetool('flush keyspace1 standard1')

        sstable_files = ' '.join(node1.get_sstable_data_files('keyspace1', 'standard1'))
        logger.debug('Compacting {}'.format(sstable_files))
        node1.nodetool('compact --user-defined {}'.format(sstable_files))

        sstable_files = node1.get_sstable_data_files('keyspace1', 'standard1')
        assert len(node1.data_directories()) == len(sstable_files), \
            'Expected one sstable data file per node directory but got {}'.format(sstable_files)

    @pytest.mark.parametrize("strategy", ['LeveledCompactionStrategy'])
    @since('3.10')
    def test_fanout_size(self, strategy):
        """
        @jira_ticket CASSANDRA-11550
        """
        if not hasattr(self, 'strategy') or strategy != 'LeveledCompactionStrategy':
            pytest.skip('Not implemented unless LeveledCompactionStrategy is used')

        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()

        stress_write(node1, keycount=1)
        node1.nodetool('disableautocompaction')

        session = self.patient_cql_connection(node1)
        logger.debug("Altering compaction strategy to LCS")
        session.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1, 'fanout_size':10};")

        stress_write(node1, keycount=1000000)
        node1.nodetool('flush keyspace1 standard1')

        # trigger the compaction
        node1.compact()

        # check the sstable count in each level
        table_name = 'standard1'
        output = grep_sstables_in_each_level(node1, table_name)

        # [0, ?/10, ?, 0, 0, 0...]
        p = re.compile(r'0,\s(\d+)/10,.*')
        m = p.search(output)
        assert 10 * len(node1.data_directories()) == int(m.group(1))

        logger.debug("Altering the fanout_size")
        session.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1, 'fanout_size':5};")

        # trigger the compaction
        node1.compact()
        # check the sstable count in each level again
        output = grep_sstables_in_each_level(node1, table_name)

        # [0, ?/5, ?/25, ?, 0, 0...]
        p = re.compile(r'0,\s(\d+)/5,\s(\d+)/25,.*')
        m = p.search(output)
        assert 5 * len(node1.data_directories()) == int(m.group(1))
        assert 25 * len(node1.data_directories()) == int(m.group(2))

    def skip_if_no_major_compaction(self, strategy):
        if self.cluster.version() < '2.2' and strategy == 'LeveledCompactionStrategy':
            pytest.skip('major compaction not implemented for LCS in this version of Cassandra')

    def skip_if_not_supported(self, strategy):
        if self.cluster.version() >= '5.0' and strategy == 'DateTieredCompactionStrategy':
            pytest.skip('DateTieredCompactionStrategy is not supported in Cassandra 5.0 and later')
        if self.cluster.version() < '5.0' and strategy == 'UnifiedCompactionStrategy':
            pytest.skip('UnifiedCompactionStrategy is only supported in Cassandra 5.0 and later')

def grep_sstables_in_each_level(node, table_name):
    output = node.nodetool('tablestats').stdout
    output = output[output.find(table_name):]
    output = output[output.find("SSTables in each level"):]
    return output[output.find(":") + 1:output.find("\n")].strip()


def get_random_word(wordLen, population=string.ascii_letters + string.digits):
    return ''.join([random.choice(population) for _ in range(int(wordLen))])


def stress_write(node, keycount=100000):
    node.stress(['write', 'n={keycount}'.format(keycount=keycount)])
