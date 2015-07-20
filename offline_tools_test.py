import os
import re
import subprocess

from ccmlib import common
from dtest import Tester, debug, require
from tools import since


class TestOfflineTools(Tester):

    # In 2.0, we will get this error log message due to jamm not being
    # in the classpath
    ignore_log_patterns = ["Unable to initialize MemoryMeter"]

    @since('2.1')
    def sstablelevelreset_test(self):
        """
        Insert data and call sstablelevelreset on a series of
        tables. Confirm level is reset to 0 using its output.
        Test a variety of possible errors and ensure response is resonable.
        @since 2.1.5
        @jira_ticket CASSANDRA-7614
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        #test by trying to run on nonexistent keyspace
        cluster.stop(gently=False)
        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self.assertIn("ColumnFamily not found: keyspace1/standard1", error)
        # this should return exit code 1
        self.assertEqual(rc, 1, msg=str(rc))

        #now test by generating keyspace but not flushing sstables
        cluster.start()
        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=100', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=100', '-schema', 'replication(factor=3)'])
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        debug(error)
        self.assertIn("Found no sstables, did you give the correct keyspace", output)
        self.assertEqual(rc, 0, msg=str(rc))

        #test by writing small amount of data and flushing (all sstables should be level 0)
        cluster.start()
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':3};")
        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=10000', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])
        node1.flush()
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self.assertIn("since it is already on level 0", output)
        self.assertEqual(rc, 0, msg=str(rc))

        #test by loading large amount data so we have multiple levels and checking all levels are 0 at end
        cluster.start()

        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=1000000', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=1M', '-schema', 'replication(factor=3)'])
        self.wait_for_compactions(node1)
        cluster.stop()

        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))

        debug(initial_levels)
        debug(final_levels)

        for x in range(0, len(final_levels)):
            self.assertEqual(final_levels[x], 0)

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

    @since('2.1')
    def sstableofflinerelevel_test(self):
        """
        Generate sstables of varying levels.
        Run sstableofflinerelevel and ensure tables are promoted correctly
        Also test a variety of bad inputs including nonexistent keyspace and sstables
        @since 2.1.5
        @jira_ticket CASSANRDA-8031
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        # NOTE - As of now this does not return when it encounters Exception and causes test to hang, temporarily commented out
        # test by trying to run on nonexistent keyspace
        # cluster.stop(gently=False)
        # (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        # self.assertTrue("java.lang.IllegalArgumentException: Unknown keyspace/columnFamily keyspace1.standard1" in error)
        # # this should return exit code 1
        # self.assertEqual(rc, 1, msg=str(rc))
        #cluster.start()

        #now test by generating keyspace but not flushing sstables
        node1.stress(['write', 'n=100', '-schema', 'replication(factor=3)'])
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)

        self.assertIn("No sstables to relevel for keyspace1.standard1", output)
        self.assertEqual(rc, 1, msg=str(rc))

        #test by flushing (sstable should be level 0)
        cluster.start()
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':3};")

        node1.stress(['write', 'n=1000', '-schema', 'replication(factor=3)'])

        node1.flush()
        cluster.stop()

        (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        self.assertIn("L0=1", output)
        self.assertEqual(rc, 0, msg=str(rc))

        #test by loading large amount data so we have multiple sstables
        cluster.start()
        node1.stress(['write', 'n=1M', '-schema', 'replication(factor=3)'])
        node1.flush()
        node1.stress(['write', 'n=5M', '-schema', 'replication(factor=3)'])
        node1.flush()
        cluster.stop()

        initial_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))
        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        final_levels = self.get_levels(node1.run_sstablemetadata(keyspace="keyspace1", column_families=["standard1"]))

        debug(initial_levels)
        debug(final_levels)

        for x in range(0, len(final_levels)):
            initial = "intial level: " + str(initial_levels[x])
            self.assertEqual(final_levels[x], 0, msg=initial)

    @since('2.2')
    @require(9774, broken_in='3.0')
    def sstableverify_test(self):
        """
        Generate sstables and test offline verification works correctly
        Test on bad input: nonexistent keyspace and sstables
        Test on potential situations: deleted sstables, corrupted sstables
        """

        cluster = self.cluster
        cluster.populate(3).start()
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
        node1.stress(['write', 'n=1M', '-schema', 'replication(factor=3)'])
        node1.flush()
        node1.stress(['write', 'n=1M', '-schema', 'replication(factor=3)'])
        node1.flush()
        cluster.stop(gently=False)
        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", output=True)

        self.assertEqual(rc, 0, msg=str(rc))

        outlines = out.split("\n")

        #check output is correct for each sstable
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

        # try removing an sstable and running verify with extended option to ensure missing table is found
        os.remove(sstables[0])
        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", options=['-e'], output=True)

        self.assertEqual(rc, 0, msg=str(rc))
        self.assertIn("was not released before the reference was garbage collected", out)

        #now try intentionally corrupting an sstable to see if hash computed is different and error recognized
        with open(sstables[1], 'r') as f:
            sstabledata = f.read().splitlines(True)
        with open(sstables[1], 'w') as out:
            out.writelines(sstabledata[2:])

        #use verbose to get some coverage on it
        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", options=['-v'], output=True)

        self.assertIn("java.lang.Exception: Invalid SSTable", error)
        self.assertEqual(rc, 1, msg=str(rc))

    def _get_final_sstables(self, node, ks, table):
        """
        Return the node final sstable data files, excluding the temporary tables.
        If sstablelister exists (>= 3.0) then we rely on this tool since the table
        file names no longer contain tmp in their names (CASSANDRA-7066).
        """
        # Get all sstable data files
        allsstables = node.get_sstables(ks, table)

        # Remove any temporary files
        tool_bin = node.get_tool('sstablelister')
        if os.path.isfile(tool_bin):
            args = [ tool_bin, '--type', 'tmp', ks, table]
            env = common.make_cassandra_env(node.get_install_cassandra_root(), node.get_node_cassandra_root())
            p = subprocess.Popen(args, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (stdin, stderr) = p.communicate()
            tmpsstables = stdin.split('\n')
            ret = list(set(allsstables) - set(tmpsstables))
        else:
            ret = [sstable for sstable in allsstables if "tmp" not in sstable[50:]]

        return ret
