import os
import socket

from dtest import Tester, debug
from tools import since


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

        NODE2_LISTEN_ADDRESS = '127.0.0.2'
        NODE2_BROADCAST_ADDRESS = '127.0.0.4'

        STORAGE_PORT = 7000

        cluster = self.cluster
        cluster.populate(2)
        node1, node2 = cluster.nodelist()

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
        node1.watch_log_for("Starting Messaging Service on /{}:{}".format(NODE1_LISTEN_ADDRESS, STORAGE_PORT), timeout=60)
        node1.watch_log_for("Starting Messaging Service on /{}:{}".format(NODE1_BROADCAST_ADDRESS, STORAGE_PORT), timeout=60)
        self._test_connect(NODE1_LISTEN_ADDRESS, STORAGE_PORT)
        self._test_connect(NODE1_BROADCAST_ADDRESS, STORAGE_PORT)

        # write some data to node1
        node1.stress(['write', 'n=10K', 'no-warmup', '-rate', 'threads=8'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1'
        original_rows = list(session.execute("SELECT * FROM {}".format(stress_table)))

        node2.start(wait_for_binary_proto=True, wait_other_notice=False)
        node2.watch_log_for("Starting Messaging Service on /{}:{}".format(NODE2_LISTEN_ADDRESS, STORAGE_PORT), timeout=60)
        node2.watch_log_for("Starting Messaging Service on /{}:{}".format(NODE2_BROADCAST_ADDRESS, STORAGE_PORT), timeout=60)
        self._test_connect(NODE2_LISTEN_ADDRESS, STORAGE_PORT)
        self._test_connect(NODE2_BROADCAST_ADDRESS, STORAGE_PORT)

        node1.watch_log_for("Intiated reconnect to an Internal IP /{} for the /{}".format(NODE2_LISTEN_ADDRESS,
                                                                                          NODE2_BROADCAST_ADDRESS), filename='debug.log', timeout=60)
        node2.watch_log_for("Intiated reconnect to an Internal IP /{} for the /{}".format(NODE1_LISTEN_ADDRESS,
                                                                                          NODE1_BROADCAST_ADDRESS), filename='debug.log', timeout=60)

        # read data from node2 just to make sure data and connectivity is OK
        session = self.patient_exclusive_cql_connection(node2)
        new_rows = list(session.execute("SELECT * FROM {}".format(stress_table)))
        self.assertEquals(original_rows, new_rows)

        out, err, _ = node1.nodetool('gossipinfo')
        self.assertEqual(0, len(err), err)
        debug(out)

        self.assertIn("/{}".format(NODE1_BROADCAST_ADDRESS), out)
        self.assertIn("INTERNAL_IP:6:{}".format(NODE1_LISTEN_ADDRESS), out)
        self.assertIn("/{}".format(NODE2_BROADCAST_ADDRESS), out)
        self.assertIn("INTERNAL_IP:6:{}".format(NODE2_LISTEN_ADDRESS), out)
