from dtest import Tester, debug
from tools import since

from cassandra.concurrent import execute_concurrent_with_args

from jmxutils import JolokiaAgent, make_mbean


class TestUpgradeIndexSummary(Tester):

    @since('2.1')
    def test_upgrade_index_summary(self):
        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        original_install_dir = node.get_install_dir()

        # start out with a 2.0 version
        node.set_install_dir(version='2.0.12')
        node.set_log_level("INFO")
        node.stop()
        cluster.start()

        # Insert enough partitions to fill a full sample's worth of entries
        # in the index summary.  The default index_interval is 128, so every
        # 128th partition will get an entry in the summary.  The minimal downsampling
        # operation will remove every 128th entry in the summary.  So, we need
        # to have 128 entries in the summary, which means 128 * 128 partitions.
        session = self.patient_cql_connection(node, protocol_version=2)
        session.execute("CREATE KEYSPACE testindexsummary WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.set_keyspace("testindexsummary")
        session.execute("CREATE TABLE test (k int PRIMARY KEY, v int)")

        insert_statement = session.prepare("INSERT INTO test (k, v) VALUES (? , ?)")
        execute_concurrent_with_args(session, insert_statement, [(i, i) for i in range(128 * 128)])

        # upgrade to 2.1.3
        session.cluster.shutdown()
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop()
        node.set_install_dir(version='2.1.3')  # 2.1.3 is affected by CASSANDRA-8993
        debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))

        # setup log4j / logback again (necessary moving from 2.0 -> 2.1)
        node.set_log_level("INFO")
        node.start()

        session = self.patient_cql_connection(node)

        mbean = make_mbean('db', 'IndexSummaries')
        with JolokiaAgent(node) as jmx:
            avg_interval = jmx.read_attribute(mbean, 'AverageIndexInterval')
            self.assertEqual(128.0, avg_interval)

            # force downsampling of the index summary (if it were allowed)
            jmx.write_attribute(mbean, 'MemoryPoolCapacityInMB', 0)
            jmx.execute_method(mbean, 'redistributeSummaries')

            avg_interval = jmx.read_attribute(mbean, 'AverageIndexInterval')

            # after downsampling, the average interval goes up
            self.assertGreater(avg_interval, 128.0)

        # upgrade to the latest 2.1+ by using the original install dir
        session.cluster.shutdown()
        node.drain()
        node.watch_log_for("DRAINED")
        node.stop()
        node.set_install_dir(original_install_dir)
        debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))

        node.set_log_level("INFO")
        node.start()

        # on startup, it should detect that the old-format sstable had its
        # index summary downsampled (forcing it to be rebuilt)
        node.watch_log_for("Detected erroneously downsampled index summary")

        session = self.patient_cql_connection(node)

        mbean = make_mbean('db', 'IndexSummaries')
        with JolokiaAgent(node) as jmx:
            avg_interval = jmx.read_attribute(mbean, 'AverageIndexInterval')
            self.assertEqual(128.0, avg_interval)

            # force downsampling of the index summary (if it were allowed)
            jmx.write_attribute(mbean, 'MemoryPoolCapacityInMB', 0)
            jmx.execute_method(mbean, 'redistributeSummaries')

            avg_interval = jmx.read_attribute(mbean, 'AverageIndexInterval')

            # post-8993, it shouldn't allow downsampling of old-format sstables
            self.assertEqual(128.0, avg_interval)
