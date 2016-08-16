import json
import os
import random
import re
import subprocess

from ccmlib import common
from ccmlib.node import ToolError

from dtest import Tester, debug
from tools import known_failure, since


class TestOfflineTools(Tester):

    # In 2.0, we will get this error log message due to jamm not being
    # in the classpath
    ignore_log_patterns = ["Unable to initialize MemoryMeter"]

    def sstablelevelreset_test(self):
        """
        Insert data and call sstablelevelreset on a series of
        tables. Confirm level is reset to 0 using its output.
        Test a variety of possible errors and ensure response is resonable.
        @since 2.1.5
        @jira_ticket CASSANDRA-7614
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # test by trying to run on nonexistent keyspace
        cluster.stop(gently=False)
        try:
            node1.run_sstablelevelreset("keyspace1", "standard1")
        except ToolError as e:
            self.assertIn("ColumnFamily not found: keyspace1/standard1", e.message)
            # this should return exit code 1
            self.assertEqual(e.exit_status, 1, "Expected sstablelevelreset to have a return code of 1, but instead return code was {}".format(e.exit_status))

        # now test by generating keyspace but not flushing sstables
        cluster.start(wait_for_binary_proto=True)
        node1.stress(['write', 'n=100', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=8'])
        cluster.stop(gently=False)

        output, error, rc = node1.run_sstablelevelreset("keyspace1", "standard1")
        self._check_stderr_error(error)
        self.assertIn("Found no sstables, did you give the correct keyspace", output)
        self.assertEqual(rc, 0, msg=str(rc))

        # test by writing small amount of data and flushing (all sstables should be level 0)
        cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1};")
        node1.stress(['write', 'n=1K', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=8'])
        node1.flush()
        cluster.stop(gently=False)

        output, error, rc = node1.run_sstablelevelreset("keyspace1", "standard1")
        self._check_stderr_error(error)
        self.assertIn("since it is already on level 0", output)
        self.assertEqual(rc, 0, msg=str(rc))

        # test by loading large amount data so we have multiple levels and checking all levels are 0 at end
        cluster.start(wait_for_binary_proto=True)
        node1.stress(['write', 'n=50K', 'no-warmup', '-schema', 'replication(factor=1)',
                      '-rate', 'threads=8'])
        cluster.flush()
        self.wait_for_compactions(node1)
        cluster.stop()

        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        _, error, rc = node1.run_sstablelevelreset("keyspace1", "standard1")
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        self._check_stderr_error(error)
        self.assertEqual(rc, 0, msg=str(rc))

        debug(initial_levels)
        debug(final_levels)

        # let's make sure there was at least L1 beforing resetting levels
        self.assertTrue(max(initial_levels) > 0)

        # let's check all sstables are on L0 after sstablelevelreset
        self.assertTrue(max(final_levels) == 0)

    def get_levels(self, data):
        (out, err, rc) = data
        return map(int, re.findall("SSTable Level: ([0-9])", out))

    def wait_for_compactions(self, node):
        pattern = re.compile("pending tasks: 0")
        while True:
            output, err, _ = node.nodetool("compactionstats")
            if pattern.search(output):
                break

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12275',
                   flaky=False,
                   notes='windows')
    def sstableofflinerelevel_test(self):
        """
        Generate sstables of varying levels.
        Reset sstables to L0 with sstablelevelreset
        Run sstableofflinerelevel and ensure tables are promoted correctly
        Also test a variety of bad inputs including nonexistent keyspace and sstables
        @since 2.1.5
        @jira_ticket CASSANRDA-8031
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={'compaction_throughput_mb_per_sec': 0})
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # NOTE - As of now this does not return when it encounters Exception and causes test to hang, temporarily commented out
        # test by trying to run on nonexistent keyspace
        # cluster.stop(gently=False)
        # output, error, rc = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        # self.assertTrue("java.lang.IllegalArgumentException: Unknown keyspace/columnFamily keyspace1.standard1" in error)
        # # this should return exit code 1
        # self.assertEqual(rc, 1, msg=str(rc))
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
            self.assertIn("No sstables to relevel for keyspace1.standard1", e.stdout)
            self.assertEqual(e.exit_status, 1, msg=str(e.exit_status))

        # test by flushing (sstable should be level 0)
        cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        debug("Altering compaction strategy to LCS")
        session.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1};")

        node1.stress(['write', 'n=1K', 'no-warmup',
                      '-schema', 'replication(factor=1)',
                      '-col', 'n=FIXED(10)', 'SIZE=FIXED(1024)',
                      '-rate', 'threads=8'])

        node1.flush()
        cluster.stop()

        output, _, rc = node1.run_sstableofflinerelevel("keyspace1", "standard1")
        self.assertIn("L0=1", output)
        self.assertEqual(rc, 0, msg=str(rc))

        cluster.start(wait_for_binary_proto=True)
        # test by loading large amount data so we have multiple sstables
        # must write enough to create more than just L1 sstables
        keys = 8 * cluster.data_dir_count
        node1.stress(['write', 'n={0}K'.format(keys), 'no-warmup',
                      '-schema', 'replication(factor=1)',
                      '-col', 'n=FIXED(10)', 'SIZE=FIXED(1024)',
                      '-rate', 'threads=8'])

        node1.flush()
        debug("Waiting for compactions to finish")
        self.wait_for_compactions(node1)
        debug("Stopping node")
        cluster.stop()
        debug("Done stopping node")

        # Let's reset all sstables to L0
        debug("Getting initial levels")
        initial_levels = list(self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"])))
        self.assertNotEqual([], initial_levels)
        debug('initial_levels:')
        debug(initial_levels)
        debug("Running sstablelevelreset")
        node1.run_sstablelevelreset("keyspace1", "standard1")
        debug("Getting final levels")
        final_levels = list(self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"])))
        self.assertNotEqual([], final_levels)
        debug('final levels:')
        debug(final_levels)

        # let's make sure there was at least 3 levels (L0, L1 and L2)
        self.assertGreater(max(initial_levels), 1)
        # let's check all sstables are on L0 after sstablelevelreset
        self.assertEqual(max(final_levels), 0)

        # time to relevel sstables
        debug("Getting initial levels")
        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        debug("Running sstableofflinerelevel")
        output, error, _ = node1.run_sstableofflinerelevel("keyspace1", "standard1")
        debug("Getting final levels")
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))

        debug(output)
        debug(error)

        debug(initial_levels)
        debug(final_levels)

        # let's check sstables were promoted after releveling
        self.assertGreater(max(final_levels), 1)

    @since('2.2')
    def sstableverify_test(self):
        """
        Generate sstables and test offline verification works correctly
        Test on bad input: nonexistent keyspace and sstables
        Test on potential situations: deleted sstables, corrupted sstables
        """

        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # test on nonexistent keyspace
        try:
            (out, err, rc) = node1.run_sstableverify("keyspace1", "standard1")
        except ToolError as e:
            self.assertIn("Unknown keyspace/table keyspace1.standard1", e.message)
            self.assertEqual(e.exit_status, 1, msg=str(e.exit_status))

        # test on nonexistent sstables:
        node1.stress(['write', 'n=100', 'no-warmup', '-schema', 'replication(factor=3)',
                      '-rate', 'threads=8'])
        (out, err, rc) = node1.run_sstableverify("keyspace1", "standard1")
        self.assertEqual(rc, 0, msg=str(rc))

        # Generate multiple sstables and test works properly in the simple case
        node1.stress(['write', 'n=100K', 'no-warmup', '-schema', 'replication(factor=3)',
                      '-rate', 'threads=8'])
        node1.flush()
        node1.stress(['write', 'n=100K', 'no-warmup', '-schema', 'replication(factor=3)',
                      '-rate', 'threads=8'])
        node1.flush()
        cluster.stop()

        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1")

        self.assertEqual(rc, 0, msg=str(rc))

        # STDOUT of the sstableverify command consists of multiple lines which may contain
        # Java-normalized paths. To later compare these with Python-normalized paths, we
        # map over each line of out and replace Java-normalized paths with Python equivalents.
        outlines = map(lambda line: re.sub("(?<=path=').*(?=')",
                                           lambda match: os.path.normcase(match.group(0)),
                                           line),
                       out.splitlines())

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
                        debug(line)

            debug(verified)
            debug(hashcomputed)
            debug(sstable)
            self.assertTrue(verified and hashcomputed)

        # now try intentionally corrupting an sstable to see if hash computed is different and error recognized
        sstable1 = sstables[1]
        with open(sstable1, 'r') as f:
            sstabledata = bytearray(f.read())
        with open(sstable1, 'w') as out:
            position = random.randrange(0, len(sstabledata))
            sstabledata[position] = (sstabledata[position] + 1) % 256
            out.write(sstabledata)

        # use verbose to get some coverage on it
        try:
            (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", options=['-v'])
        except ToolError as e:
            # Process sstableverify output to normalize paths in string to Python casing as above
            error = re.sub("(?<=Corrupted: ).*", lambda match: os.path.normcase(match.group(0)), e.message)

            self.assertIn("Corrupted: " + sstable1, error)
            self.assertEqual(e.exit_status, 1, msg=str(e.exit_status))

    def sstableexpiredblockers_test(self):
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute("create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds=0")
        # create a blocker:
        session.execute("insert into ks.cf (key, val) values (1,1)")
        node1.flush()
        session.execute("delete from ks.cf where key = 2")
        node1.flush()
        session.execute("delete from ks.cf where key = 3")
        node1.flush()
        out, error, _ = node1.run_sstableexpiredblockers(keyspace="ks", column_family="cf")
        self.assertIn("blocks 2 expired sstables from getting dropped", out)

    def sstableupgrade_test(self):
        """
        Test that sstableupgrade functions properly offline on a same-version Cassandra sstable, a
        stdout message of "Found 0 sstables that need upgrading." should be returned.
        """
        # Set up original node version to test for upgrade
        cluster = self.cluster
        testversion = cluster.version()
        original_install_dir = cluster.get_install_dir()
        debug('Original install dir: {}'.format(original_install_dir))

        # Set up last major version to upgrade from, assuming 2.1 branch is the oldest tested version
        if testversion < '2.2':
            # Upgrading from 2.0->2.1 fails due to the jamm 0.2.5->0.3.0 jar update.
            #   ** This will happen again next time jamm version is upgraded.
            # CCM doesn't handle this upgrade correctly and results in an error when flushing 2.1:
            #   Error opening zip file or JAR manifest missing : /home/mshuler/git/cassandra/lib/jamm-0.2.5.jar
            # The 2.1 installed jamm version is 0.3.0, but bin/cassandra.in.sh used by nodetool still has 0.2.5
            # (when this is fixed in CCM issue #463, install version='git:cassandra-2.0' as below)
            self.skipTest('Skipping 2.1 test due to jamm.jar version upgrade problem in CCM node configuration.')
        elif testversion < '3.0':
            debug('Test version: {} - installing git:cassandra-2.1'.format(testversion))
            cluster.set_install_dir(version='git:cassandra-2.1')
        # As of 3.5, sstable format 'ma' from 3.0 is still the latest - install 2.2 to upgrade from
        else:
            debug('Test version: {} - installing git:cassandra-2.2'.format(testversion))
            cluster.set_install_dir(version='git:cassandra-2.2')

        # Start up last major version, write out an sstable to upgrade, and stop node
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        # Check that node1 is actually what we expect
        debug('Downgraded install dir: {}'.format(node1.get_install_dir()))
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds=0')
        session.execute('insert into ks.cf (key, val) values (1,1)')
        node1.flush()
        cluster.stop()
        debug('Beginning ks.cf sstable: {}'.format(node1.get_sstables(keyspace='ks', column_family='cf')))

        # Upgrade Cassandra to original testversion and run sstableupgrade
        cluster.set_install_dir(original_install_dir)
        # Check that node1 is actually upgraded
        debug('Upgraded to original install dir: {}'.format(node1.get_install_dir()))
        # Perform a node start/stop so system tables get internally updated, otherwise we may get "Unknown keyspace/table ks.cf"
        cluster.start(wait_for_binary_proto=True)
        node1.flush()
        cluster.stop()
        (out, error, rc) = node1.run_sstableupgrade(keyspace='ks', column_family='cf')
        debug(out)
        debug(error)
        debug('Upgraded ks.cf sstable: {}'.format(node1.get_sstables(keyspace='ks', column_family='cf')))
        self.assertIn('Found 1 sstables that need upgrading.', out)

        # Check that sstableupgrade finds no upgrade needed on current version.
        (out, error, rc) = node1.run_sstableupgrade(keyspace='ks', column_family='cf')
        debug(out)
        debug(error)
        self.assertIn('Found 0 sstables that need upgrading.', out)

    @since('3.0')
    def sstabledump_test(self):
        """
        Test that sstabledump functions properly offline to output the contents of a table.
        """
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute('create table ks.cf (key int PRIMARY KEY, val int) with gc_grace_seconds=0')
        session.execute('insert into ks.cf (key, val) values (1,1)')
        node1.flush()
        cluster.stop()
        [(out, error, rc)] = node1.run_sstabledump(keyspace='ks', column_families=['cf'])
        debug(out)
        debug(error)

        # Load the json output and check that it contains the inserted key=1
        s = json.loads(out)
        debug(s)
        self.assertEqual(len(s), 1)
        dumped_row = s[0]
        self.assertEqual(dumped_row['partition']['key'], ['1'])

        # Check that we only get the key back using the enumerate option
        [(out, error, rc)] = node1.run_sstabledump(keyspace='ks', column_families=['cf'], enumerate_keys=True)
        debug(out)
        debug(error)
        s = json.loads(out)
        debug(s)
        self.assertEqual(len(s), 1)
        dumped_row = s[0][0]
        self.assertEqual(dumped_row, '1')

    def _check_stderr_error(self, error):
        acceptable = ["Max sstable size of", "Consider adding more capacity", "JNA link failure", "Class JavaLaunchHelper is implemented in both"]

        if len(error) > 0:
            for line in error.splitlines():
                self.assertTrue(any([msg in line for msg in acceptable]),
                                'Found line \n\n"{line}"\n\n in error\n\n{error}'.format(line=line, error=error))

    def _get_final_sstables(self, node, ks, table):
        """
        Return the node final sstable data files, excluding the temporary tables.
        If sstableutil exists (>= 3.0) then we rely on this tool since the table
        file names no longer contain tmp in their names (CASSANDRA-7066).
        """
        # Get all sstable data files
        allsstables = map(os.path.normcase, node.get_sstables(ks, table))

        # Remove any temporary files
        tool_bin = node.get_tool('sstableutil')
        if os.path.isfile(tool_bin):
            args = [tool_bin, '--type', 'tmp', ks, table]
            env = common.make_cassandra_env(node.get_install_cassandra_root(), node.get_node_cassandra_root())
            p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (stdout, stderr) = p.communicate()
            tmpsstables = map(os.path.normcase, stdout.splitlines())

            ret = list(set(allsstables) - set(tmpsstables))
        else:
            ret = [sstable for sstable in allsstables if "tmp" not in sstable[50:]]

        return ret
