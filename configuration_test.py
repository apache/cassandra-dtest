import os
import logging
import parse
import pytest

from cassandra.concurrent import execute_concurrent_with_args

from tools.misc import ImmutableMapping
from dtest_setup_overrides import DTestSetupOverrides
from dtest import Tester, create_ks
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)

logger = logging.getLogger(__name__)


@pytest.fixture()
def fixture_dtest_setup_overrides(request, dtest_config):
    dtest_setup_overrides = DTestSetupOverrides()
    if request.node.name == "test_change_durable_writes":
        dtest_setup_overrides.cluster_options = ImmutableMapping({'commitlog_segment_size_in_mb': 1})
    return dtest_setup_overrides


class TestConfiguration(Tester):

    def test_compression_chunk_length(self):
        """ Verify the setting of compression chunk_length [#3558]"""
        cluster = self.cluster

        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        create_ks(session, 'ks', 1)

        create_table_query = "CREATE TABLE test_table (row varchar, name varchar, value int, PRIMARY KEY (row, name));"
        alter_chunk_len_query = "ALTER TABLE test_table WITH " \
                                "compression = {{'sstable_compression' : 'SnappyCompressor', " \
                                "'chunk_length_kb' : {chunk_length}}};"

        session.execute(create_table_query)

        session.execute(alter_chunk_len_query.format(chunk_length=32))
        self._check_chunk_length(session, 32)

        session.execute(alter_chunk_len_query.format(chunk_length=64))
        self._check_chunk_length(session, 64)

    @pytest.mark.timeout(60*30)
    def test_change_durable_writes(self):
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
            self.fixture_dtest_setup.cluster.populate(1)
            node = self.fixture_dtest_setup.cluster.nodelist()[0]
            self.fixture_dtest_setup.cluster.set_batch_commitlog(enabled=True)

            # disable JVM option so we can use Jolokia
            # this has to happen after .set_configuration_options because of implementation details
            remove_perf_disable_shared_mem(node)
            self.fixture_dtest_setup.cluster.start(wait_for_binary_proto=True)
            return node

        durable_node = new_commitlog_cluster_node()
        durable_init_size = commitlog_size(durable_node)
        durable_session = self.patient_exclusive_cql_connection(durable_node)

        # test assumption that write_to_trigger_fsync actually triggers a commitlog fsync
        durable_session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1} "
                                "AND DURABLE_WRITES = true")
        durable_session.execute('CREATE TABLE ks.tab (key int PRIMARY KEY, a int, b int, c int)')
        logger.debug('commitlog size diff = ' + str(commitlog_size(durable_node) - durable_init_size))
        write_to_trigger_fsync(durable_session, 'ks', 'tab')

        assert commitlog_size(durable_node) > durable_init_size, \
            "This test will not work in this environment; write_to_trigger_fsync does not trigger fsync."

        # get a fresh cluster to work on
        durable_session.shutdown()
        self.fixture_dtest_setup.cleanup_and_replace_cluster()

        node = new_commitlog_cluster_node()
        init_size = commitlog_size(node)
        session = self.patient_exclusive_cql_connection(node)

        # set up a keyspace without durable writes, then alter it to use them
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1} "
                        "AND DURABLE_WRITES = false")
        session.execute('CREATE TABLE ks.tab (key int PRIMARY KEY, a int, b int, c int)')
        session.execute('ALTER KEYSPACE ks WITH DURABLE_WRITES=true')
        write_to_trigger_fsync(session, 'ks', 'tab')
        assert commitlog_size(node) > init_size, "ALTER KEYSPACE was not respected"

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

        assert not params == '', "Looking for the string 'sstable_compression', but could not find " \
                                 "it in {str}".format(str=result)

        chunk_string = "chunk_length_kb" if self.cluster.version() < '3.0' else "chunk_length_in_kb"
        chunk_length = parse.search("'" + chunk_string + "': '{chunk_length:d}'", result).named['chunk_length']

        assert chunk_length == value, "Expected chunk_length: {}.  We got: {}".format(value, chunk_length)


def write_to_trigger_fsync(session, ks, table):
    """
    Given a session, a keyspace name, and a table name, inserts enough values
    to trigger an fsync to the commitlog, assuming the cluster's
    commitlog_segment_size_in_mb is 1. Assumes the table's columns are
    (key int, a int, b int, c int).
    """
    """
    From https://github.com/datastax/python-driver/pull/877/files
      "Note: in the case that `generators` are used, it is important to ensure the consumers do not
       block or attempt further synchronous requests, because no further IO will be processed until
       the consumer returns. This may also produce a deadlock in the IO event thread."
    """
    execute_concurrent_with_args(session,
                                 session.prepare('INSERT INTO "{ks}"."{table}" (key, a, b, c) '
                                                 'VALUES (?, ?, ?, ?)'.format(ks=ks, table=table)),
                                 ((x, x + 1, x + 2, x + 3)
                                 for x in range(50000)), concurrency=5)


def commitlog_size(node):
    commitlog_size_mbean = make_mbean('metrics', type='CommitLog', name='TotalCommitLogSize')
    with JolokiaAgent(node) as jmx:
        return jmx.read_attribute(commitlog_size_mbean, 'Value')
