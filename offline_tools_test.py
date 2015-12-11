import os
import random
import re
import subprocess

from ccmlib import common
from dtest import Tester, debug
from tools import since


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
        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self.assertIn("ColumnFamily not found: keyspace1/standard1", error)
        # this should return exit code 1
        self.assertEqual(rc, 1, msg=str(rc))

        # now test by generating keyspace but not flushing sstables
        cluster.start(wait_for_binary_proto=True)
        node1.stress(['write', 'n=100', '-schema', 'replication(factor=1)'])
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self._check_stderr_error(error)
        self.assertIn("Found no sstables, did you give the correct keyspace", output)
        self.assertEqual(rc, 0, msg=str(rc))

        # test by writing small amount of data and flushing (all sstables should be level 0)
        cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1};")
        node1.stress(['write', 'n=1K', '-schema', 'replication(factor=1)'])
        node1.flush()
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self._check_stderr_error(error)
        self.assertIn("since it is already on level 0", output)
        self.assertEqual(rc, 0, msg=str(rc))

        # test by loading large amount data so we have multiple levels and checking all levels are 0 at end
        cluster.start(wait_for_binary_proto=True)
        node1.stress(['write', 'n=50K', '-rate', 'threads=20', '-schema', 'replication(factor=1)'])
        cluster.flush()
        self.wait_for_compactions(node1)
        cluster.stop()

        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
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
        levels = []
        for sstable in data:
            (metadata, error, rc) = sstable
            level = int(re.findall("SSTable Level: [0-9]", metadata)[0][-1])
            levels.append(level)
        return levels

    def wait_for_compactions(self, node):
        pattern = re.compile("pending tasks: 0")
        while True:
            output, err = node.nodetool("compactionstats", capture_output=True)
            if pattern.search(output):
                break

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
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        # NOTE - As of now this does not return when it encounters Exception and causes test to hang, temporarily commented out
        # test by trying to run on nonexistent keyspace
        # cluster.stop(gently=False)
        # (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        # self.assertTrue("java.lang.IllegalArgumentException: Unknown keyspace/columnFamily keyspace1.standard1" in error)
        # # this should return exit code 1
        # self.assertEqual(rc, 1, msg=str(rc))
        # cluster.start()

        # now test by generating keyspace but not flushing sstables

        node1.stress(['write', 'n=1', '-schema', 'replication(factor=1)'])
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)

        self.assertIn("No sstables to relevel for keyspace1.standard1", output)
        self.assertEqual(rc, 1, msg=str(rc))

        # test by flushing (sstable should be level 0)
        cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':1};")

        node1.stress(['write', 'n=1K', '-schema', 'replication(factor=1)'])

        node1.flush()
        cluster.stop()

        (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        self.assertIn("L0=1", output)
        self.assertEqual(rc, 0, msg=str(rc))

        # test by loading large amount data so we have multiple sstables
        cluster.start(wait_for_binary_proto=True)
        node1.stress(['write', 'n=100K', '-schema', 'replication(factor=1)'])
        node1.flush()
        self.wait_for_compactions(node1)
        cluster.stop()

        # Let's reset all sstables to L0
        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))

        # let's make sure there was at least 3 levels (L0, L1 and L2)
        self.assertGreater(max(initial_levels), 1)
        # let's check all sstables are on L0 after sstablelevelreset
        self.assertEqual(max(final_levels), 0)

        # time to relevel sstables
        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))

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
        (out, err, rc) = node1.run_sstableverify("keyspace1", "standard1", output=True)
        self.assertIn("Unknown keyspace/table keyspace1.standard1", err)
        self.assertEqual(rc, 1, msg=str(rc))

        # test on nonexistent sstables:
        node1.stress(['write', 'n=100', '-schema', 'replication(factor=3)'])
        (out, err, rc) = node1.run_sstableverify("keyspace1", "standard1", output=True)
        self.assertEqual(rc, 0, msg=str(rc))

        # Generate multiple sstables and test works properly in the simple case
        node1.stress(['write', 'n=100K', '-schema', 'replication(factor=3)'])
        node1.flush()
        node1.stress(['write', 'n=100K', '-schema', 'replication(factor=3)'])
        node1.flush()
        cluster.stop()

        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", output=True)

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
        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", options=['-v'], output=True)

        # Process sstableverify output to normalize paths in string to Python casing as above
        error = re.sub("(?<=Corrupted: ).*", lambda match: os.path.normcase(match.group(0)), error)

        self.assertIn("Corrupted: " + sstable1, error)
        self.assertEqual(rc, 1, msg=str(rc))

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
        [(out, error, rc)] = node1.run_sstableexpiredblockers(keyspace="ks", column_family="cf")
        self.assertIn("blocks 2 expired sstables from getting dropped", out)

    def _check_stderr_error(self, error):
        if len(error) > 0:
            for line in error.splitlines():
                self.assertTrue("Max sstable size of" in line or "Consider adding more capacity" in line,
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
