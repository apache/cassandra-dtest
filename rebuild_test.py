import pytest
import time
import logging

from flaky import flaky

from threading import Thread

from cassandra import ConsistencyLevel
from ccmlib.node import ToolError

from dtest import Tester, create_ks, create_cf, mk_bman_path
from tools.data import insert_c1c2, query_c1c2

since = pytest.mark.since
logger = logging.getLogger(__name__)


class TestRebuild(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # ignore streaming error during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred'
        )

    def test_simple_rebuild(self):
        """
        @jira_ticket CASSANDRA-9119

        Test rebuild from other dc works as expected.
        """

        keys = 1000

        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        node1 = cluster.create_node('node1', False,
                                    None,
                                    ('127.0.0.1', 7000),
                                    '7100', '2000', None,
                                    binary_interface=('127.0.0.1', 9042))
        cluster.add(node1, True, data_center='dc1')

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', {'dc1': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.LOCAL_ONE)

        # check data
        for i in range(0, keys):
            query_c1c2(session, i, ConsistencyLevel.LOCAL_ONE)
        session.shutdown()

        # Bootstrapping a new node in dc2 with auto_bootstrap: false
        node2 = cluster.create_node('node2', False,
                                    ('127.0.0.2', 9160),
                                    ('127.0.0.2', 7000),
                                    '7200', '2001', None,
                                    binary_interface=('127.0.0.2', 9042))
        cluster.add(node2, False, data_center='dc2')
        node2.start(wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        if self.cluster.version() >= '2.2':
            # alter system_auth -- rebuilding it no longer possible after
            # CASSANDRA-11848 prevented local node from being considered a source
            # Only do this on 2.2+, because on 2.1, this keyspace only
            # exists if auth is enabled, which it isn't in this test
            session.execute("ALTER KEYSPACE system_auth WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute('USE ks')

        self.rebuild_errors = 0

        # rebuild dc2 from dc1
        def rebuild():
            try:
                node2.nodetool('rebuild dc1')
            except ToolError as e:
                if 'Node is still rebuilding' in e.stdout:
                    self.rebuild_errors += 1
                else:
                    raise e

        class Runner(Thread):
            def __init__(self, func):
                Thread.__init__(self)
                self.func = func
                self.thread_exc_info = None

            def run(self):
                """
                Closes over self to catch any exceptions raised by func and
                register them at self.thread_exc_info
                Based on http://stackoverflow.com/a/1854263
                """
                try:
                    self.func()
                except Exception:
                    import sys
                    self.thread_exc_info = sys.exc_info()

        cmd1 = Runner(rebuild)
        cmd1.start()

        # concurrent rebuild should not be allowed (CASSANDRA-9119)
        # (following sleep is needed to avoid conflict in 'nodetool()' method setting up env.)
        time.sleep(.1)
        # we don't need to manually raise exeptions here -- already handled
        rebuild()

        cmd1.join()

        # manually raise exception from cmd1 thread
        # see http://stackoverflow.com/a/1854263
        if cmd1.thread_exc_info is not None:
            raise cmd1.thread_exc_info[1].with_traceback(cmd1.thread_exc_info[2])

        # exactly 1 of the two nodetool calls should fail
        # usually it will be the one in the main thread,
        # but occasionally it wins the race with the one in the secondary thread,
        # so we check that one succeeded and the other failed
        assert self.rebuild_errors == 1, \
            'rebuild errors should be 1, but found {}. Concurrent rebuild should not be allowed, but one rebuild command should have succeeded.'.format(self.rebuild_errors)

        # check data
        for i in range(0, keys):
            query_c1c2(session, i, ConsistencyLevel.LOCAL_ONE)

    @since('2.2')
    def test_resumable_rebuild(self):
        """
        @jira_ticket CASSANDRA-10810

        Test rebuild operation is resumable
        """
        self.fixture_dtest_setup.ignore_log_patterns = list(self.fixture_dtest_setup.ignore_log_patterns) + [
            r'Error while rebuilding node',
            r'Streaming error occurred on session with peer 127.0.0.3',
            r'Remote peer 127.0.0.3 failed stream session',
            r'Streaming error occurred on session with peer 127.0.0.3:7000',
            r'Remote peer /?127.0.0.3:7000 failed stream session',
            r'Stream receive task .* already finished',
            r'stream operation from /?127.0.0.1:.* failed'
        ]

        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})

        # Create 2 nodes on dc1
        node1 = cluster.create_node('node1', False,
                                    ('127.0.0.1', 9160),
                                    ('127.0.0.1', 7000),
                                    '7100', '2000', None,
                                    binary_interface=('127.0.0.1', 9042))
        node2 = cluster.create_node('node2', False,
                                    ('127.0.0.2', 9160),
                                    ('127.0.0.2', 7000),
                                    '7200', '2001', None,
                                    binary_interface=('127.0.0.2', 9042))

        cluster.add(node1, True, data_center='dc1')
        cluster.add(node2, True, data_center='dc1')

        node1.start(wait_for_binary_proto=True, jvm_args=['-Dcassandra.reset_bootstrap_progress=false'])
        node2.start(wait_for_binary_proto=True, jvm_args=['-Dcassandra.reset_bootstrap_progress=false'])

        # Insert data into node1 and node2
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', {'dc1': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=10000, consistency=ConsistencyLevel.ALL)
        key = list(range(10000, 20000))
        session = self.patient_exclusive_cql_connection(node2)
        session.execute('USE ks')
        insert_c1c2(session, keys=key, consistency=ConsistencyLevel.ALL)
        session.shutdown()

        # Create a new node3 on dc2
        node3 = cluster.create_node('node3', False,
                                    ('127.0.0.3', 9160),
                                    ('127.0.0.3', 7000),
                                    '7300', '2002', None,
                                    binary_interface=('127.0.0.3', 9042),
                                    byteman_port='8300')

        cluster.add(node3, False, data_center='dc2')

        node3.start(wait_other_notice=False, wait_for_binary_proto=True, jvm_args=['-Dcassandra.reset_bootstrap_progress=false'])
        # Wait for snitch to be refreshed
        time.sleep(5)

        # Alter necessary keyspace for rebuild operation
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute("ALTER KEYSPACE system_auth WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")

        # Path to byteman script which makes the streaming to node2 throw an exception, making rebuild fail
        if cluster.version() < '4.0':
            script = [mk_bman_path('pre4.0/inject_failure_streaming_to_node2.btm')]
        else:
            script = [mk_bman_path('4.0/inject_failure_streaming_to_node2.btm')]
        node3.byteman_submit(script)

        # First rebuild must fail and data must be incomplete
        with pytest.raises(ToolError, message='Unexpected: SUCCEED'):
            logger.debug('Executing first rebuild -> '),
            node3.nodetool('rebuild dc1')
        logger.debug('Expected: FAILED')

        session.execute('USE ks')
        with pytest.raises(AssertionError, message='Unexpected: COMPLETE'):
            logger.debug('Checking data is complete -> '),
            for i in range(0, 20000):
                query_c1c2(session, i, ConsistencyLevel.LOCAL_ONE)
        logger.debug('Expected: INCOMPLETE')

        logger.debug('Executing second rebuild -> '),
        node3.nodetool('rebuild dc1')
        logger.debug('Expected: SUCCEED')

        # Check all streaming sessions completed, streamed ranges are skipped and verify streamed data
        node3.watch_log_for('All sessions completed')
        node3.watch_log_for('Skipping streaming those ranges.')
        logger.debug('Checking data is complete -> '),
        for i in range(0, 20000):
            query_c1c2(session, i, ConsistencyLevel.LOCAL_ONE)
        logger.debug('Expected: COMPLETE')

    @since('3.6')
    def test_rebuild_ranges(self):
        """
        @jira_ticket CASSANDRA-10406
        """
        keys = 1000

        cluster = self.cluster
        tokens = cluster.balanced_tokens_across_dcs(['dc1', 'dc2'])
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})
        node1 = cluster.create_node('node1', False,
                                    ('127.0.0.1', 9160),
                                    ('127.0.0.1', 7000),
                                    '7100', '2000', tokens[0],
                                    binary_interface=('127.0.0.1', 9042))
        node1.set_configuration_options(values={'initial_token': tokens[0]})
        cluster.add(node1, True, data_center='dc1')
        node1 = cluster.nodelist()[0]

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        # ks1 will be rebuilt in node2
        create_ks(session, 'ks1', {'dc1': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)
        # ks2 will not be rebuilt in node2
        create_ks(session, 'ks2', {'dc1': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)
        session.shutdown()

        # Bootstraping a new node in dc2 with auto_bootstrap: false
        node2 = cluster.create_node('node2', False,
                                    ('127.0.0.2', 9160),
                                    ('127.0.0.2', 7000),
                                    '7200', '2001', tokens[1],
                                    binary_interface=('127.0.0.2', 9042))
        node2.set_configuration_options(values={'initial_token': tokens[1]})
        cluster.add(node2, False, data_center='dc2')
        node2.start(wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        session.execute("ALTER KEYSPACE ks1 WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute("ALTER KEYSPACE ks2 WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute('USE ks1')

        # rebuild only ks1 with range that is node1's replica
        node2.nodetool('rebuild -ks ks1 -ts (%s,%s] dc1' % (tokens[1], str(pow(2, 63) - 1)))

        # check data is sent by stopping node1
        node1.stop()
        for i in range(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE)
        # ks2 should not be streamed
        session.execute('USE ks2')
        for i in range(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)

    @since('3.10')
    @pytest.mark.no_vnodes
    def test_disallow_rebuild_nonlocal_range(self):
        """
        @jira_ticket CASSANDRA-9875
        Verifies that nodetool rebuild throws an error when an operator
        attempts to rebuild a range that does not actually belong to the
        current node

        1. Set up a 3 node cluster
        2. Create a new keyspace with replication factor 2
        3. Run rebuild on node1 with a range that it does not own and assert that an error is raised
        """
        cluster = self.cluster
        tokens = cluster.balanced_tokens(3)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})

        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        node1_token, node2_token, node3_token = tokens[:3]
        node1.set_configuration_options(values={'initial_token': node1_token})
        node2.set_configuration_options(values={'initial_token': node2_token})
        node3.set_configuration_options(values={'initial_token': node3_token})
        cluster.start()

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};")

        with pytest.raises(ToolError, match='is not a range that is owned by this node'):
            node1.nodetool('rebuild -ks ks1 -ts (%s,%s]' % (node1_token, node2_token))

    @since('3.10')
    @pytest.mark.no_vnodes
    def test_disallow_rebuild_from_nonreplica(self):
        """
        @jira_ticket CASSANDRA-9875
        Verifies that nodetool rebuild throws an error when an operator
        attempts to rebuild a range and specifies sources that are not
        replicas of that range.

        1. Set up a 3 node cluster
        2. Create a new keyspace with replication factor 2
        3. Run rebuild on node1 with a specific range using a source that
           does not own the range and assert that an error is raised
        """
        cluster = self.cluster
        tokens = cluster.balanced_tokens(3)
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})

        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()

        node1_token, node2_token, node3_token = tokens[:3]
        node1.set_configuration_options(values={'initial_token': node1_token})
        node2.set_configuration_options(values={'initial_token': node2_token})
        node3.set_configuration_options(values={'initial_token': node3_token})
        cluster.start()

        node3_address = node3.network_interfaces['binary'][0]

        session = self.patient_exclusive_cql_connection(node1)
        session.execute("CREATE KEYSPACE ks1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':2};")

        with pytest.raises(ToolError, message='Unable to find sufficient sources for streaming range'):
            node1.nodetool('rebuild -ks ks1 -ts (%s,%s] -s %s' % (node3_token, node1_token, node3_address))

    @since('3.10')
    @pytest.mark.no_vnodes
    def test_rebuild_with_specific_sources(self):
        """
        @jira_ticket CASSANDRA-9875
        Verifies that an operator can specify specific sources to use
        when rebuilding.

        1. Set up a 2 node cluster across dc1 and dc2
        2. Create new keyspaces with replication factor 2 (one replica in each datacenter)
        4. Populate nodes with data
        5. Create a new node in dc3 and update the keyspace replication
        6. Run rebuild on the new node with a specific source in dc2
        7. Assert that streaming only occurred between the new node and the specified source
        8. Assert that the rebuild was successful by checking the data
        """
        keys = 1000

        cluster = self.cluster
        tokens = cluster.balanced_tokens_across_dcs(['dc1', 'dc2', 'dc3'])
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        cluster.set_configuration_options(values={'num_tokens': 1})

        cluster.populate([1, 1], tokens=tokens[:2])
        node1, node2 = cluster.nodelist()

        cluster.start()

        # populate data in dc1, dc2
        session = self.patient_exclusive_cql_connection(node1)
        # ks1 will be rebuilt in node3
        create_ks(session, 'ks1', {'dc1': 1, 'dc2': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)
        # ks2 will not be rebuilt in node3
        create_ks(session, 'ks2', {'dc1': 1, 'dc2': 1})
        create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)
        session.shutdown()

        # bootstrap a new node in dc3 with auto_bootstrap: false
        node3 = cluster.create_node('node3', False,
                                    ('127.0.0.3', 9160),
                                    ('127.0.0.3', 7000),
                                    '7300', '2002', tokens[2],
                                    binary_interface=('127.0.0.3', 9042))
        cluster.add(node3, False, data_center='dc3')
        node3.start(wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate to dc3
        session = self.patient_exclusive_cql_connection(node3)
        session.execute("ALTER KEYSPACE ks1 WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1, 'dc3':1};")
        session.execute("ALTER KEYSPACE ks2 WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1, 'dc3':1};")
        session.execute('USE ks1')

        node2_address = node2.network_interfaces['binary'][0]
        node3_address = node3.network_interfaces['binary'][0]

        # rebuild only ks1, restricting the source to node2
        node3.nodetool('rebuild -ks ks1 -ts (%s,%s] -s %s' % (tokens[2], str(pow(2, 63) - 1), node2_address))

        # verify that node2 streamed to node3
        log_matches = node2.grep_log('Session with %s is complete' % node3.address_for_current_version_slashy())
        assert len(log_matches) > 0

        # verify that node1 did not participate
        log_matches = node1.grep_log('streaming plan for Rebuild')
        assert len(log_matches) == 0

        # check data is sent by stopping node1, node2
        node1.stop()
        node2.stop()
        for i in range(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE)
        # ks2 should not be streamed
        session.execute('USE ks2')
        for i in range(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)
