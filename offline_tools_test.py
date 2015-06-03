from dtest import Tester, debug
import os
import re
from tools import since


class TestOfflineTools(Tester):

    # In 2.0, we will get this error log message due to jamm not being
    # in the classpath
    ignore_log_patterns = ["Unable to initialize MemoryMeter"]

    def sstablelevelreset_test(self):
        """
        Insert data and call sstablelevelreset on a series of
        tables. Confirm level is reset to 0 using sstable2json to read tables.
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
        self.assertTrue("ColumnFamily not found: keyspace1/standard1" in error, msg=error)
        # this should return exit code 1
        self.assertEqual(rc, 1, msg=str(rc))

        #now test by generating keyspace but not flushing sstables
        cluster.start()
        node1.stress(['write', 'n=100', '-schema', 'replication(factor=3)'])
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        debug(error)
        self.assertTrue("Found no sstables, did you give the correct keyspace" in output, msg=output)
        self.assertEqual(rc, 0, msg=str(rc))

        #test by writing small amount of data and flushing (all sstables should be level 0)
        cluster.start()
        cursor = self.patient_cql_connection(node1)
        cursor.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':3};")
        node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])
        node1.flush()
        cluster.stop(gently=False)

        (output, error, rc) = node1.run_sstablelevelreset("keyspace1", "standard1", output=True)
        self.assertTrue("since it is already on level 0" in output, msg=debug(output))
        self.assertEqual(rc, 0, msg=str(rc))

        #test by loading large amount data so we have multiple levels and checking all levels are 0 at end
        cluster.start()
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

        self.assertTrue("No sstables to relevel for keyspace1.standard1" in output, msg="Improper error msg with no sstables")
        self.assertEqual(rc, 1, msg=str(rc))

        #test by flushing (sstable should be level 0)
        cluster.start()
        cursor = self.patient_cql_connection(node1)
        cursor.execute("ALTER TABLE keyspace1.standard1 with compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb':3};")
        node1.flush()
        cluster.stop()

        (output, error, rc) = node1.run_sstableofflinerelevel("keyspace1", "standard1", output=True)
        self.assertTrue("L0=1" in output)
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
        self.assertTrue("Unknown keyspace/table keyspace1.standard1" in err)
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
        allsstables = node1.get_sstables("keyspace1", "standard1")
        sstables = [sstable for sstable in allsstables if "tmp" not in sstable[50:]]

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
                        self.fail("Unexpected line in output")

            debug(verified)
            debug(hashcomputed)
            debug(sstable)
            self.assertTrue(verified and hashcomputed)

        # try removing an sstable and running verify with extended option to ensure missing table is found
        os.remove(sstables[0])
        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", options=['-e'], output=True)

        self.assertEqual(rc, 0, msg=str(rc))
        self.assertTrue("was not released before the reference was garbage collected" in out)

        #now try intentionally corrupting an sstable to see if hash computed is different and error recognized
        with open(sstables[1], 'r') as f:
            sstabledata = f.read().splitlines(True)
        with open(sstables[1], 'w') as out:
            out.writelines(sstabledata[2:])

        #use verbose to get some coverage on it
        (out, error, rc) = node1.run_sstableverify("keyspace1", "standard1", options=['-v'], output=True)

        self.assertTrue("java.lang.Exception: Invalid SSTable" in error)
        self.assertEqual(rc, 1, msg=str(rc))
