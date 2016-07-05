import time
from threading import Thread

from cassandra import ConsistencyLevel
from ccmlib.node import NodetoolError

from dtest import Tester
from tools import insert_c1c2, known_failure, query_c1c2, since


class TestRebuild(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # ignore streaming error during bootstrap
            r'Exception encountered during startup',
            r'Streaming error occurred'
        ]
        Tester.__init__(self, *args, **kwargs)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-11687',
                   flaky=True)
    def simple_rebuild_test(self):
        """
        @jira_ticket CASSANDRA-9119

        Test rebuild from other dc works as expected.
        """

        keys = 1000

        cluster = self.cluster
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.PropertyFileSnitch'})
        node1 = cluster.create_node('node1', False,
                                    ('127.0.0.1', 9160),
                                    ('127.0.0.1', 7000),
                                    '7100', '2000', None,
                                    binary_interface=('127.0.0.1', 9042))
        cluster.add(node1, True, data_center='dc1')

        # start node in dc1
        node1.start(wait_for_binary_proto=True)

        # populate data in dc1
        session = self.patient_exclusive_cql_connection(node1)
        self.create_ks(session, 'ks', {'dc1': 1})
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)

        # check data
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)
        session.shutdown()

        # Bootstraping a new node in dc2 with auto_bootstrap: false
        node2 = cluster.create_node('node2', False,
                                    ('127.0.0.2', 9160),
                                    ('127.0.0.2', 7000),
                                    '7200', '2001', None,
                                    binary_interface=('127.0.0.2', 9042))
        cluster.add(node2, False, data_center='dc2')
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

        # wait for snitch to reload
        time.sleep(60)
        # alter keyspace to replicate to dc2
        session = self.patient_exclusive_cql_connection(node2)
        session.execute("ALTER KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'dc1':1, 'dc2':1};")
        session.execute('USE ks')

        self.rebuild_errors = 0

        # rebuild dc2 from dc1
        def rebuild():
            try:
                node2.nodetool('rebuild dc1')
            except NodetoolError as e:
                if 'Node is still rebuilding' in e.message:
                    self.rebuild_errors += 1

        cmd1 = Thread(target=rebuild)
        cmd1.start()

        # concurrent rebuild should not be allowed (CASSANDRA-9119)
        # (following sleep is needed to avoid conflict in 'nodetool()' method setting up env.)
        time.sleep(.1)
        try:
            node2.nodetool('rebuild dc1')
        except NodetoolError:
            self.rebuild_errors += 1

        cmd1.join()

        # exactly 1 of the two nodetool calls should fail
        # usually it will be the one in the main thread,
        # but occasionally it wins the race with the one in the secondary thread,
        # so we check that one succeeded and the other failed
        self.assertEqual(self.rebuild_errors, 1,
                         msg='concurrent rebuild should not be allowed, but one rebuild command should have succeeded.')

        # check data
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)

    @since('3.6')
    def rebuild_ranges_test(self):
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
        self.create_ks(session, 'ks1', {'dc1': 1})
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
        insert_c1c2(session, n=keys, consistency=ConsistencyLevel.ALL)
        # ks2 will not be rebuilt in node2
        self.create_ks(session, 'ks2', {'dc1': 1})
        self.create_cf(session, 'cf', columns={'c1': 'text', 'c2': 'text'})
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
        node2.start(wait_other_notice=True, wait_for_binary_proto=True)

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
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE)
        # ks2 should not be streamed
        session.execute('USE ks2')
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)
