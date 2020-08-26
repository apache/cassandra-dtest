import os
import socket
import time
import pytest
import logging

from cassandra import ConsistencyLevel
from dtest import Tester
from tools.jmxutils import (JolokiaAgent, make_mbean,
                            remove_perf_disable_shared_mem)

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('2.2.5')
class TestGossipingPropertyFileSnitch(Tester):

    # Throws connection refused if cannot connect
    def _test_connect(self, address, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.1)
        s.connect((address, port))
        s.close()


    def test_prefer_local_reconnect_on_listen_address(self):
        """
        @jira_ticket CASSANDRA-9748
        @jira_ticket CASSANDRA-8084

        Test that it's possible to connect over the broadcast_address when
        listen_on_broadcast_address=true and that GossipingPropertyFileSnitch
        reconnect via listen_address when prefer_local=true
        """

        NODE1_LISTEN_ADDRESS = '127.0.0.1'
        NODE1_BROADCAST_ADDRESS = '127.0.0.3'

        NODE1_LISTEN_FMT_ADDRESS = '/127.0.0.1'
        NODE1_BROADCAST_FMT_ADDRESS = '/127.0.0.3'

        NODE1_40_LISTEN_ADDRESS = '127.0.0.1:7000'
        NODE1_40_BROADCAST_ADDRESS = '127.0.0.3:7000'

        NODE1_40_LISTEN_FMT_ADDRESS = '/127.0.0.1:7000'
        NODE1_40_BROADCAST_FMT_ADDRESS = '/127.0.0.3:7000'

        NODE2_LISTEN_ADDRESS = '127.0.0.2'
        NODE2_BROADCAST_ADDRESS = '127.0.0.4'

        NODE2_LISTEN_FMT_ADDRESS = '/127.0.0.2'
        NODE2_BROADCAST_FMT_ADDRESS = '/127.0.0.4'

        NODE2_40_LISTEN_ADDRESS = '127.0.0.2:7000'
        NODE2_40_BROADCAST_ADDRESS = '127.0.0.4:7000'

        NODE2_40_LISTEN_FMT_ADDRESS = '/127.0.0.2:7000'
        NODE2_40_BROADCAST_FMT_ADDRESS = '/127.0.0.4:7000'

        STORAGE_PORT = 7000

        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

        running40 = node1.get_base_cassandra_version() >= 4.0

        cluster.seeds = [NODE1_BROADCAST_ADDRESS]
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch',
                                                  'listen_on_broadcast_address': 'true'})
        node1.set_configuration_options(values={'broadcast_address': NODE1_BROADCAST_ADDRESS})
        node2.auto_bootstrap = True
        node2.set_configuration_options(values={'broadcast_address': NODE2_BROADCAST_ADDRESS})

        for node in cluster.nodelist():
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as snitch_file:
                snitch_file.write("dc=dc1" + os.linesep)
                snitch_file.write("rack=rack1" + os.linesep)
                snitch_file.write("prefer_local=true" + os.linesep)

        node1.start(wait_for_binary_proto=True)
        if running40:
            node1.watch_log_for("Listening on address: \({}:{}\)".format(NODE1_40_LISTEN_FMT_ADDRESS[:-5], STORAGE_PORT), timeout=60)
            node1.watch_log_for("Listening on address: \({}:{}\)".format(NODE1_40_BROADCAST_FMT_ADDRESS[:-5], STORAGE_PORT), timeout=60)
        else:
            node1.watch_log_for("Starting Messaging Service on {}:{}".format(NODE1_LISTEN_FMT_ADDRESS, STORAGE_PORT), timeout=60)
            node1.watch_log_for("Starting Messaging Service on {}:{}".format(NODE1_BROADCAST_FMT_ADDRESS, STORAGE_PORT), timeout=60)

        self._test_connect(NODE1_LISTEN_ADDRESS, STORAGE_PORT)
        self._test_connect(NODE1_BROADCAST_ADDRESS, STORAGE_PORT)

        # write some data to node1
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table)))

        node2.start(wait_for_binary_proto=True, wait_other_notice=False)
        if running40:
            node2.watch_log_for("Listening on address: \({}:{}\)".format(NODE2_40_LISTEN_FMT_ADDRESS[:-5], STORAGE_PORT), timeout=60)
            node2.watch_log_for("Listening on address: \({}:{}\)".format(NODE2_40_BROADCAST_FMT_ADDRESS[:-5], STORAGE_PORT), timeout=60)
        else:
            node2.watch_log_for("Starting Messaging Service on {}:{}".format(NODE2_LISTEN_FMT_ADDRESS, STORAGE_PORT), timeout=60)
            node2.watch_log_for("Starting Messaging Service on {}:{}".format(NODE2_BROADCAST_FMT_ADDRESS, STORAGE_PORT), timeout=60)

        self._test_connect(NODE2_LISTEN_ADDRESS, STORAGE_PORT)
        self._test_connect(NODE2_BROADCAST_ADDRESS, STORAGE_PORT)

        # Intiated -> Initiated typo was fixed in 3.10
        reconnectFmtString = "Ini?tiated reconnect to an Internal IP {} for the {}"
        if node1.get_base_cassandra_version() >= 3.10:
            reconnectFmtString = "Initiated reconnect to an Internal IP {} for the {}"
        node1.watch_log_for(reconnectFmtString.format(NODE2_40_LISTEN_FMT_ADDRESS if running40 else NODE2_LISTEN_FMT_ADDRESS,
                                               NODE2_40_BROADCAST_FMT_ADDRESS if running40 else NODE2_BROADCAST_FMT_ADDRESS), filename='debug.log', timeout=60)
        node2.watch_log_for(reconnectFmtString.format(NODE1_40_LISTEN_FMT_ADDRESS if running40 else NODE1_LISTEN_FMT_ADDRESS,
                                               NODE1_40_BROADCAST_FMT_ADDRESS if running40 else NODE1_BROADCAST_FMT_ADDRESS), filename='debug.log', timeout=60)

        # read data from node2 just to make sure data and connectivity is OK
        session = self.patient_exclusive_cql_connection(node2)
        new_rows = list(session.execute("SELECT * FROM {}".format(stress_table)))
        assert original_rows == new_rows

        out, err, _ = node1.nodetool('gossipinfo')
        assert 0 == len(err), err
        logger.debug(out)

        assert "/{}".format(NODE1_BROADCAST_ADDRESS) in out
        assert "INTERNAL_IP:{}:{}".format('9' if running40 else '6', NODE1_LISTEN_ADDRESS) in out
        assert "/{}".format(NODE2_BROADCAST_ADDRESS) in out
        assert "INTERNAL_IP:{}:{}".format('9' if running40 else '6', NODE2_LISTEN_ADDRESS) in out
        if running40:
            assert "INTERNAL_ADDRESS_AND_PORT:7:{}".format(NODE1_40_LISTEN_ADDRESS) in out
            assert "INTERNAL_ADDRESS_AND_PORT:7:{}".format(NODE2_40_LISTEN_ADDRESS) in out

class TestDynamicEndpointSnitch(Tester):
    @pytest.mark.resource_intensive
    @since('3.10')
    def test_multidatacenter_local_quorum(self):
        '''
        @jira_ticket CASSANDRA-13074

        If we do only local datacenters reads in a multidatacenter DES setup,
        DES should take effect and route around a degraded node
        '''

        def no_cross_dc(scores, cross_dc_nodes):
            return all('/' + k.address() not in scores for k in cross_dc_nodes)

        def snitchable(scores_before, scores_after, needed_nodes):
            return all('/' + k.address() in scores_before and '/' + k.address()
                       in scores_after for k in needed_nodes)

        cluster = self.cluster
        cluster.populate([3, 3])
        coordinator_node, healthy_node, degraded_node, node4, node5, node6 = cluster.nodelist()
        # increase DES reset/update interval so we clear any cross-DC startup reads faster
        cluster.set_configuration_options(values={'dynamic_snitch_reset_interval_in_ms': 10000,
                                                  'dynamic_snitch_update_interval_in_ms': 50,
                                                  'phi_convict_threshold': 12})
        remove_perf_disable_shared_mem(coordinator_node)
        remove_perf_disable_shared_mem(degraded_node)
        # Delay reads on the degraded node by 50 milliseconds
        degraded_node.start(jvm_args=['-Dcassandra.test.read_iteration_delay_ms=50',
                                      '-Dcassandra.allow_unsafe_join=true'])
        cluster.start(wait_for_binary_proto=30, wait_other_notice=True)

        des = make_mbean('db', type='DynamicEndpointSnitch')
        read_stage = make_mbean('metrics', type='ThreadPools', path='request',
                                scope='ReadStage', name='CompletedTasks')
        session = self.patient_exclusive_cql_connection(coordinator_node)
        session.execute("CREATE KEYSPACE snitchtestks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}")
        session.execute("CREATE TABLE snitchtestks.tbl1 (key int PRIMARY KEY) WITH speculative_retry = 'NONE'")
        read_stmt = session.prepare("SELECT * FROM snitchtestks.tbl1 where key = ?")
        read_stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        insert_stmt = session.prepare("INSERT INTO snitchtestks.tbl1 (key) VALUES (?)")
        insert_stmt.consistency_level = ConsistencyLevel.ALL
        with JolokiaAgent(coordinator_node) as jmx:
            with JolokiaAgent(degraded_node) as bad_jmx:
                for x in range(0, 300):
                    session.execute(insert_stmt, [x])

                cleared = False
                # Wait for a snitch reset in case any earlier
                # startup process populated cross-DC read timings
                while not cleared:
                    scores = jmx.read_attribute(des, 'Scores')
                    cleared = ('/127.0.0.1' in scores and (len(scores) == 1)) or not scores

                snitchable_count = 0

                for x in range(0, 300):
                    degraded_reads_before = bad_jmx.read_attribute(read_stage, 'Value')
                    scores_before = jmx.read_attribute(des, 'Scores')
                    assert no_cross_dc(scores_before, [node4, node5, node6]), "Cross DC scores were present: " + str(scores_before)
                    future = session.execute_async(read_stmt, [x])
                    future.result()
                    scores_after = jmx.read_attribute(des, 'Scores')
                    assert no_cross_dc(scores_after, [node4, node5, node6]), "Cross DC scores were present: " + str(scores_after)

                    if snitchable(scores_before, scores_after,
                                  [coordinator_node, healthy_node, degraded_node]):
                        snitchable_count = snitchable_count + 1
                        # If the DES correctly routed the read around the degraded node,
                        # it shouldn't have another completed read request in metrics
                        assert (degraded_reads_before ==
                                     bad_jmx.read_attribute(read_stage, 'Value'))
                    else:
                        # sleep to give dynamic snitch time to recalculate scores
                        time.sleep(.1)

                # check that most reads were snitchable, with some
                # room allowed in case score recalculation is slow
                assert snitchable_count >= 250
