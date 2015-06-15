import time
from threading import Thread

from cassandra import ConsistencyLevel

from ccmlib.node import NodetoolError
from dtest import Tester
from tools import insert_c1c2, query_c1c2, require


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

    @require(9119)
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
        for i in xrange(0, keys):
            insert_c1c2(session, i, ConsistencyLevel.ALL)

        # check data
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)
        session.shutdown()

        # Boostraping a new node in dc2 with auto_bootstrap: false
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

        # rebuild dc2 from dc1
        def rebuild():
            node2.nodetool('rebuild dc1')
        cmd1 = Thread(target=rebuild)
        cmd1.start()

        # concurrent rebuild should not be allowed (CASSANDRA-9119)
        # (following sleep is needed to avoid conflict in 'nodetool()' method setting up env.)
        time.sleep(.1)
        try:
            node2.nodetool('rebuild dc1')
            self.fail("concurrent rebuild should not be allowed")
        except NodetoolError:
            pass

        cmd1.join()

        # check data
        for i in xrange(0, keys):
            query_c1c2(session, i, ConsistencyLevel.ALL)
