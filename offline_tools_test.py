import json
import os
import random
import re
import subprocess
import pytest
import logging

from ccmlib import common
from ccmlib.node import ToolError

from dtest import Tester, create_ks

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestOfflineTools(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            # In 2.0, we will get this error log message due to jamm not being
            # in the classpath
            "Unable to initialize MemoryMeter"
        )

    def test_sstablelevelreset(self):
        """
        Insert data and call sstablelevelreset on a series of
        tables. Confirm level is reset to 0 using its output.
        Test a variety of possible errors and ensure response is resonable.
        @since 2.1.5
        @jira_ticket CASSANDRA-7614
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        # test by trying to run on nonexistent keyspace
        cluster.stop(gently=False)
        try:
            node1.run_sstablelevelreset("keyspace1", "standard1")
        except ToolError as e:
            assert re.search("ColumnFamily not found: keyspace1/standard1", str(e))
            # this should return exit code 1
            assert e.exit_status == 1, "Expected sstablelevelreset to have a return code of 1 == but instead return code was {}".format(
                e.exit_status)

        # now test by generating keyspace but not flushing sstables
        cluster.start()
        node1.stress(['write', 'n=100', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=8'])
        cluster.stop(gently=False)

        output, error, rc = node1.run_sstablelevelreset("keyspace1", "standard1")
        self._check_stderr_error(error)
        assert re.search("Found no sstables, did you give the correct keyspace", output)
        assert rc == 0, str(rc)

        # test by writing small amount of data and flushing (all sstables should be level 0)
        cluster.start()
        session = self.patient_cql_connection(node1)
        session.execute(
            "ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1};")
        node1.stress(['write', 'n=1K', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=8'])
        node1.flush()
        cluster.stop(gently=False)

        output, error, rc = node1.run_sstablelevelreset("keyspace1", "standard1")
        self._check_stderr_error(error)
        assert re.search("since it is already on level 0", output)
        assert rc == 0, str(rc)

        # test by loading large amount data so we have multiple levels and checking all levels are 0 at end
        cluster.start()
        node1.stress(['write', 'n=50K', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=8'])
        cluster.flush()
        self.wait_for_compactions(node1)
        cluster.stop()

        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        _, error, rc = node1.run_sstablelevelreset("keyspace1", "standard1")
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        self._check_stderr_error(error)
        assert rc == 0, str(rc)

        logger.debug(initial_levels)
        logger.debug(final_levels)

        # let's make sure there was at least L1 before resetting levels
        assert max(initial_levels) > 0

        # let's check all sstables are on L0 after sstablelevelreset
        assert max(final_levels) == 0

        # verify that the cluster can still start after messing with the sstables
        cluster.start()

    def get_levels(self, data):
        (out, err, rc) = data
        return list(map(int, re.findall("SSTable Level: ([0-9])", out)))

    def wait_for_compactions(self, node):
        pattern = re.compile("pending tasks: 0")
        while True:
            output, err, _ = node.nodetool("compactionstats")
            if pattern.search(output):
                break

    def test_sstableofflinerelevel(self):
        """
        Generate sstables of varying levels.
        Reset sstables to L0 with sstablelevelreset
        Run sstableofflinerelevel and ensure tables are promoted correctly
        Also test a variety of bad inputs including nonexistent keyspace and sstables
        @since 2.1.5
        @jira_ticket CASSANDRA-8031
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'compaction_throughput_mb_per_sec': 0})
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        # NOTE - As of now this does not return when it encounters Exception and causes test to hang, temporarily commented out
        # test by trying to run on nonexistent keyspace
        # cluster.stop(gently=False)
        # output, error, rc = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        # assert "java.lang.IllegalArgumentException: Unknown keyspace/columnFamily keyspace1.standard1" in error
        # # this should return exit code 1
        # assert rc, 1 == msg=str(rc)
        # cluster.start()

        # now test by generating keyspace but not flushing sstables

        node1.stress(['write', 'n=1', 'no-warmup',
                      '-schema', 'replication(factor=1)',
                      '-col', 'n=FIXED(10)', 'SIZE=FIXED(1024)',
                      '-rate', 'threads=8'])

        cluster.stop(gently=False)
        try:
            output, error, _ = node1.run_sstableofflinerelevel("keyspace1", "standard1")
        except ToolError as e:
            assert re.search("No sstables to relevel for keyspace1.standard1", e.stdout)
            assert e.exit_status == 1, str(e.exit_status)

        # test by flushing (sstable should be level 0)
        cluster.start()
        session = self.patient_cql_connection(node1)
        logger.debug("Altering compaction strategy to LCS")
        session.execute(
            "ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1, 'enabled':'false'};")

        node1.stress(['write', 'n=1K', 'no-warmup',
                      '-schema', 'replication(factor=1)',
                      '-col', 'n=FIXED(10)', 'SIZE=FIXED(1024)',
                      '-rate', 'threads=8'])

        node1.flush()
        cluster.stop()

        output, _, rc = node1.run_sstableofflinerelevel("keyspace1", "standard1")
        assert re.search("L0=1", output)
        assert rc == 0, str(rc)

        cluster.start()
        node1.nodetool('enableautocompaction keyspace1 standard1')
        # test by loading large amount data so we have multiple sstables
        # must write enough to create more than just L1 sstables
        keys = 8 * cluster.data_dir_count
        node1.stress(['write', 'n={0}K'.format(keys), 'no-warmup',
                      '-schema', 'replication(factor=1)',
                      '-col', 'n=FIXED(10)', 'SIZE=FIXED(1200)',
                      '-rate', 'threads=8'])

        node1.flush()
        logger.debug("Waiting for compactions to finish")
        self.wait_for_compactions(node1)
        logger.debug("Stopping node")
        cluster.stop()
        logger.debug("Done stopping node")

        # Let's reset all sstables to L0
        logger.debug("Getting initial levels")
        initial_levels = list(
            self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"])))
        assert [] != initial_levels
        logger.debug('initial_levels:')
        logger.debug(initial_levels)
        logger.debug("Running sstablelevelreset")
        node1.run_sstablelevelreset("keyspace1", "standard1")
        logger.debug("Getting final levels")
        final_levels = list(
            self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"])))
        assert [] != final_levels
        logger.debug('final levels:')
        logger.debug(final_levels)

        # let's make sure there was at least 3 levels (L0, L1 and L2)
        assert max(initial_levels) > 1
        # let's check all sstables are on L0 after sstablelevelreset
        assert max(final_levels) == 0

        # time to relevel sstables
        logger.debug("Getting initial levels")
        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        logger.debug("Running sstableofflinerelevel")
        output, error, _ = node1.run_sstableofflinerelevel("keyspace1", "standard1")
        logger.debug("Getting final levels")
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))

        logger.debug(output)
        logger.debug(error)

        logger.debug(initial_levels)
        logger.debug(final_levels)

        # let's check sstables were promoted after releveling
        assert max(final_levels) > 1

        # verify that the cluster can still start after messing with the sstables
        cluster.start()

    @since('2.2')
    def test_sstableverify(self):
        """
        Generate sstables and test offline verification works correctly
        Test on bad input: nonexistent keyspace and sstables
        Test on potential situations: deleted sstables, corrupted sstables
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        options = []
        # In versions >= 4.1, we need to explicitly enable force on verify as it's guarded by default
        # see CASSANDRA-17017
        if cluster.version() >= '4.1':
            options.append('-f')

        # test on nonexistent keyspace
        try:
            (out, err, rc) = node1.run_sstableverify("keyspace1", "standard1", options=options)
        except ToolError as e:
            assert "Unknown keyspace/table keyspace1.standard1" in repr(e)
            assert e.exit_status == 1, str(e.exit_status)

        # test on nonexistent sstables:
        node1.stress(['write', 'n=100', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=8'])
        (out, err, rc) = node1.run_sstableverify("keyspace1", "standard1", options=options)
        assert rc == 0, str(rc)

        # only works on existing ks/cf, but we just created them
        node1.nodetool("disableautocompaction")
        # Generate multiple sstables and test works properly in the simple case
        node1.stress(['write', 'n=100K', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=8'])
        node1.flush()
        node1.stress(['write', 'n=100K', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=8'])
        node1.flush()
        cluster.stop()

        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", options=options)

        assert rc == 0, str(rc)

        # STDOUT of the sstableverify command consists of multiple lines which may contain
        # Java-normalized paths. To later compare these with Python-normalized paths, we
        # map over each line of out and replace Java-normalized paths with Python equivalents.
        outlines = [re.sub("(?<=path=').*(?=')",
                           lambda match: os.path.normcase(match.group(0)),
                           line) for line in out.splitlines()]

        # check output is correct for each sstable
        sstables = self._get_final_sstables(node1, "keyspace1", "standard1")

        for sstable in sstables:
            verified = False
            hashcomputed = False
            for line in outlines:
                if sstable in line:
                    if "Verifying BigTableReader" in line:
                        verified = True
                    elif "Checking computed hash of BigTableReader" in line:
                        hashcomputed = True
                    else:
                        logger.debug(line)

            logger.debug(verified)
            logger.debug(hashcomputed)
            logger.debug(sstable)
            assert verified and hashcomputed

        # now try intentionally corrupting an sstable to see if hash computed is different and error recognized
        sstable1 = sstables[1]
        with open(sstable1, 'rb') as f:
            sstabledata = bytearray(f.read())
        with open(sstable1, 'wb') as out:
            position = random.randrange(0, len(sstabledata))
            sstabledata[position] = (sstabledata[position] + 1) % 256
            out.write(sstabledata)

        # use verbose to get some coverage on it
        try:
            options.append('-v')
            (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", options=options)
            assert False, "sstable verify did not fail; rc={}\nout={}\nerr={}".format(str(rc), out, error)
        except ToolError as e:
            m = re.match("(?ms).*Corrupted SSTable : (?P<sstable>\S+)", str(e))
            assert m is not None, str(e)
            # MacOS might use the "private" prefix.
            assert os.path.normcase(m.group('sstable')).replace("/private/var/folders", "/var/folders") == sstable1
            assert e.exit_status == 1, str(e.exit_status)

    def test_sstableexpiredblockers(self):
        cluster = self.cluster
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds=0")
        # create a blocker:
        session.execute("insert into ks.cf (key, val) values (1,1)")
        node1.flush()
        session.execute("delete from ks.cf where key = 2")
        node1.flush()
        session.execute("delete from ks.cf where key = 3")
        node1.flush()
        out, error, _ = node1.run_sstableexpiredblockers(keyspace="ks", column_family="cf")
        assert "blocks 2 expired sstables from getting dropped" in out

    # 4.0 removes back compatibility with pre-3.0 versions, so testing upgradesstables for
    # paths from those versions to 4.0 is invalid (and can only fail). There isn't currently
    # any difference between the 3.0 and 4.0 sstable format though, but when the version is
    # bumped for 4.0, remove the max_version & add a case for testing a 3.0 -> 4.0 upgrade
    @since('2.2', max_version='3.X')
    def test_sstableupgrade(self):
        """
        Test that sstableupgrade functions properly offline on a same-version Cassandra sstable, a
        stdout message of "Found 0 sstables that need upgrading." should be returned.
        """
        # Set up original node version to test for upgrade
        cluster = self.cluster
        testversion = cluster.version()
        original_install_dir = cluster.get_install_dir()
        logger.debug('Original install dir: {}'.format(original_install_dir))

        # Set up last major version to upgrade from, assuming 2.1 branch is the oldest tested version
        if testversion < '2.2':
            # Upgrading from 2.0->2.1 fails due to the jamm 0.2.5->0.3.0 jar update.
            #   ** This will happen again next time jamm version is upgraded.
            # CCM doesn't handle this upgrade correctly and results in an error when flushing 2.1:
            #   Error opening zip file or JAR manifest missing : /home/mshuler/git/cassandra/lib/jamm-0.2.5.jar
            # The 2.1 installed jamm version is 0.3.0, but bin/cassandra.in.sh used by nodetool still has 0.2.5
            # (when this is fixed in CCM issue #463, install version='github:apache/cassandra-2.0' as below)
            pytest.skip('Skipping 2.1 test due to jamm.jar version upgrade problem in CCM node configuration.')
        elif testversion < '3.0':
            logger.debug('Test version: {} - installing github:apache/cassandra-2.1'.format(testversion))
            cluster.set_install_dir(version='github:apache/cassandra-2.1')
        # As of 3.5, sstable format 'ma' from 3.0 is still the latest - install 2.2 to upgrade from
        elif testversion < '4.0':
            logger.debug('Test version: {} - installing github:apache/cassandra-2.2'.format(testversion))
            cluster.set_install_dir(version='github:apache/cassandra-2.2')
        # From 4.0, one can only upgrade from 3.0
        else:
            logger.debug('Test version: {} - installing github:apache/cassandra-3.0'.format(testversion))
            cluster.set_install_dir(version='github:apache/cassandra-3.0')

        # Start up last major version, write out an sstable to upgrade, and stop node
        cluster.populate(1).start()
        self.install_nodetool_legacy_parsing()
        [node1] = cluster.nodelist()
        # Check that node1 is actually what we expect
        logger.debug('Downgraded install dir: {}'.format(node1.get_install_dir()))
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute('create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds=0')
        session.execute('insert into ks.cf (key, val) values (1,1)')
        node1.flush()
        cluster.stop()
        logger.debug('Beginning ks.cf sstable: {}'.format(node1.get_sstables(keyspace='ks', column_family='cf')))

        # Upgrade Cassandra to original testversion and run sstableupgrade
        cluster.set_install_dir(original_install_dir)
        # Check that node1 is actually upgraded
        logger.debug('Upgraded to original install dir: {}'.format(node1.get_install_dir()))
        # Perform a node start/stop so system tables get internally updated, otherwise we may get "Unknown keyspace/table ks.cf"
        cluster.start()
        self.install_nodetool_legacy_parsing()
        node1.flush()
        cluster.stop()

        # A bit hacky, but we can only upgrade to 4.0 from 3.0, but both use the
        # same sstable major format currently, so there is no upgrading to do.
        # So on 4.0, we only test that sstable upgrade detect there is no
        # upgrade. We'll removed that test if 4.0 introduce a major sstable
        # change before it's release.
        if testversion < '4.0':
            (out, error, rc) = node1.run_sstableupgrade(keyspace='ks', column_family='cf')
            out = str(out)
            error = str(error)
            logger.debug(out)
            logger.debug(error)
            logger.debug('Upgraded ks.cf sstable: {}'.format(node1.get_sstables(keyspace='ks', column_family='cf')))
            assert 'Found 1 sstables that need upgrading.' in str(out)

        # Check that sstableupgrade finds no upgrade needed on current version.
        (out, error, rc) = node1.run_sstableupgrade(keyspace='ks', column_family='cf')
        out = str(out)
        error = str(error)
        logger.debug(out)
        logger.debug(error)
        assert 'Found 0 sstables that need upgrading.' in out

    @since('3.0')
    def test_sstabledump(self):
        """
        Test that sstabledump functions properly offline to output the contents of a table.
        """
        cluster = self.cluster
        # disable JBOD conf since the test expects exactly one SSTable to be written.
        cluster.set_datadir_count(1)
        cluster.populate(1).start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 1)
        session.execute('create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds=0')
        session.execute('insert into ks.cf (key, val) values (1,1)')

        # delete a partition and then insert a row to test CASSANDRA-13177
        session.execute('DELETE FROM ks.cf WHERE key = 2')
        session.execute('INSERT INTO ks.cf (key, val) VALUES (2, 2)')

        node1.flush()
        cluster.stop()
        [(out, error, rc)] = node1.run_sstabledump(keyspace='ks', column_families=['cf'])
        logger.debug(out)
        logger.debug(error)

        # Load the json output and check that it contains the inserted key=1
        s = json.loads(out)
        logger.debug(s)
        assert len(s) == 2

        # order the rows so that we have key=1 first, then key=2
        row0, row1 = s
        (row0, row1) = (row0, row1) if row0['partition']['key'] == ['1'] else (row1, row0)

        assert row0['partition']['key'] == ['1']

        assert row1['partition']['key'] == ['2']
        assert row1['partition'].get('deletion_info') is not None
        assert row1.get('rows') is not None

        # Check that we only get the key back using the enumerate option
        [(out, error, rc)] = node1.run_sstabledump(keyspace='ks', column_families=['cf'], enumerate_keys=True)
        logger.debug(out)
        logger.debug(error)
        s = json.loads(out)
        logger.debug(s)
        assert len(s) == 2
        dumped_keys = set(row[0] for row in s)
        assert {'1', '2'} == dumped_keys

    def _check_stderr_error(self, error):
        acceptable = ["Max sstable size of",
                      "Consider adding more capacity",
                      "JNA link failure",
                      "Class JavaLaunchHelper is implemented in both",
                      "Picked up JAVA_TOOL_OPTIONS:",
                      # Warnings for backward compatibility should be logged CASSANDRA-15234
                      "parameters have been deprecated. They have new names and/or value format"]

        if len(error) > 0:
            for line in error.splitlines():
                assert any([msg in line for msg in acceptable]), \
                    'Found line \n\n"{line}"\n\n in error\n\n{error}'.format(line=line, error=error)

    def _get_final_sstables(self, node, ks, table):
        """
        Return the node final sstable data files, excluding the temporary tables.
        If sstableutil exists (>= 3.0) then we rely on this tool since the table
        file names no longer contain tmp in their names (CASSANDRA-7066).
        """
        # Get all sstable data files
        allsstables = list(map(os.path.normcase, node.get_sstables(ks, table)))

        # Remove any temporary files
        tool_bin = node.get_tool('sstableutil')
        if os.path.isfile(tool_bin):
            args = [tool_bin, '--type', 'tmp', ks, table]
            env = common.make_cassandra_env(node.get_install_cassandra_root(), node.get_node_cassandra_root())
            p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (stdout, stderr) = p.communicate()
            tmpsstables = list(map(os.path.normcase, stdout.splitlines()))

            ret = list(set(allsstables) - set(tmpsstables))
        else:
            ret = [sstable for sstable in allsstables if "tmp" not in sstable[50:]]

        return ret
