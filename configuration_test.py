import os

import parse
from cassandra.concurrent import execute_concurrent_with_args

from dtest import Tester, debug, create_ks
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)


class TestConfiguration(Tester):

    def compression_chunk_length_test(self):
        """ Verify the setting of compression chunk_length [#3558]"""
        cluster = self.cluster

        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        create_table_query = "CREATE TABLE test_table (row varchar, name varchar, value int, PRIMARY KEY (row, name));"
        alter_chunk_len_query = "ALTER TABLE test_table WITH compression = {{'sstable_compression' : 'SnappyCompressor', 'chunk_length_kb' : {chunk_length}}};"

        session.execute(create_table_query)

        session.execute(alter_chunk_len_query.format(chunk_length=32))
        self._check_chunk_length(session, 32)

        session.execute(alter_chunk_len_query.format(chunk_length=64))
        self._check_chunk_length(session, 64)

    def change_durable_writes_test(self):
        """
        @jira_ticket CASSANDRA-9560

        Test that changes to the DURABLE_WRITES option on keyspaces is
        respected in subsequent writes.

        This test starts by writing a dataset to a cluster and asserting that
        the commitlogs have been written to. The subsequent test depends on
        the assumption that this dataset triggers an fsync.

        After checking this assumption, the test destroys the cluster and
        creates a fresh one. Then it tests that DURABLE_WRITES is respected by:

        - creating a keyspace with DURABLE_WRITES set to false,
        - using ALTER KEYSPACE to set its DURABLE_WRITES option to true,
        - writing a dataset to this keyspace that is known to trigger a commitlog fsync,
        - asserting that the commitlog has grown in size since the data was written.
        """
        def new_commitlog_cluster_node():
            # writes should block on commitlog fsync
            self.cluster.populate(1)
            node = self.cluster.nodelist()[0]
            self.cluster.set_configuration_options(values={'commitlog_segment_size_in_mb': 1})
            self.cluster.set_batch_commitlog(enabled=True)

            # disable JVM option so we can use Jolokia
            # this has to happen after .set_configuration_options because of implmentation details
            remove_perf_disable_shared_mem(node)
            self.cluster.start(wait_for_binary_proto=True)
            return node

        durable_node = new_commitlog_cluster_node()
        durable_init_size = commitlog_size(durable_node)
        durable_session = self.patient_exclusive_cql_connection(durable_node)

        # test assumption that write_to_trigger_fsync actually triggers a commitlog fsync
        durable_session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1} "
                                "AND DURABLE_WRITES = true")
        durable_session.execute('CREATE TABLE ks.tab (key int PRIMARY KEY, a int, b int, c int)')
        debug('commitlog size diff = ' + str(commitlog_size(durable_node) - durable_init_size))
        write_to_trigger_fsync(durable_session, 'ks', 'tab')

        self.assertGreater(commitlog_size(durable_node), durable_init_size,
                           msg='This test will not work in this environment; '
                               'write_to_trigger_fsync does not trigger fsync.')

        # get a fresh cluster to work on
        self.tearDown()
        self.setUp()

        node = new_commitlog_cluster_node()
        init_size = commitlog_size(node)
        session = self.patient_exclusive_cql_connection(node)

        # set up a keyspace without durable writes, then alter it to use them
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1} "
                        "AND DURABLE_WRITES = false")
        session.execute('CREATE TABLE ks.tab (key int PRIMARY KEY, a int, b int, c int)')
        session.execute('ALTER KEYSPACE ks WITH DURABLE_WRITES=true')
        write_to_trigger_fsync(session, 'ks', 'tab')
        self.assertGreater(commitlog_size(node), init_size,
                           msg='ALTER KEYSPACE was not respected')

    def overlapping_data_folders(self):
        """
        @jira_ticket CASSANDRA-10902
        """
        self.cluster.populate(1)
        node1 = self.cluster.nodelist()[0]
        default_path = node1.data_directories()[0]
        node1.set_configuration_options({'saved_caches_directory': os.path.join(default_path, 'saved_caches')})
        remove_perf_disable_shared_mem(node1)
        self.cluster.start(wait_for_binary_proto=True)

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
        session.execute("CREATE TABLE ks.tab (key int PRIMARY KEY, a int)")
        session.execute("INSERT INTO ks.tab (key, a) VALUES (%s, %s)", [0, 0])
        session.execute("SELECT * FROM ks.tab WHERE key = %s", [0])

        cache_service = make_mbean('db', type="Caches")
        with JolokiaAgent(node1) as jmx:
            jmx.execute_method(cache_service, 'saveCaches')

        self.cluster.stop()
        self.cluster.start(wait_for_binary_proto=True)

    def _check_chunk_length(self, session, value):
        result = session.cluster.metadata.keyspaces['ks'].tables['test_table'].as_cql_query()
        # Now extract the param list
        params = ''

        if self.cluster.version() < '3.0':
            if 'sstable_compression' in result:
                params = result
        else:
            if 'compression' in result:
                params = result

        self.assertNotEqual(params, '', "Looking for the string 'sstable_compression', but could not find it in {str}".format(str=result))

        chunk_string = "chunk_length_kb" if self.cluster.version() < '3.0' else "chunk_length_in_kb"
        chunk_length = parse.search("'" + chunk_string + "': '{chunk_length:d}'", result).named['chunk_length']

        self.assertEqual(chunk_length, value, "Expected chunk_length: {}.  We got: {}".format(value, chunk_length))


def write_to_trigger_fsync(session, ks, table):
    """
    Given a session, a keyspace name, and a table name, inserts enough values
    to trigger an fsync to the commitlog, assuming the cluster's
    commitlog_segment_size_in_mb is 1. Assumes the table's columns are
    (key int, a int, b int, c int).
    """
    execute_concurrent_with_args(session,
                                 session.prepare('INSERT INTO "{ks}"."{table}" (key, a, b, c) VALUES (?, ?, ?, ?)'.format(ks=ks, table=table)),
                                 ((x, x + 1, x + 2, x + 3) for x in range(50000)))


def commitlog_size(node):
    commitlog_size_mbean = make_mbean('metrics', type='CommitLog', name='TotalCommitLogSize')
    with JolokiaAgent(node) as jmx:
        return jmx.read_attribute(commitlog_size_mbean, 'Value')
