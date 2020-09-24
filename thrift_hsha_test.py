import glob
import os
import shlex
import subprocess
import time
import unittest
import pytest
import logging

from dtest import DEFAULT_DIR, Tester, create_ks
from thrift_test import get_thrift_client
from tools.jmxutils import JolokiaAgent, make_mbean

since = pytest.mark.since
logger = logging.getLogger(__name__)

JNA_PATH = '/usr/share/java/jna.jar'
ATTACK_JAR = 'lib/cassandra-attack.jar'

# Use jna.jar in {CASSANDRA_DIR,DEFAULT_DIR}/lib/, since >=2.1 needs correct version
try:
    if glob.glob('%s/lib/jna-*.jar' % os.environ['CASSANDRA_DIR']):
        logger.debug('Using jna.jar in CASSANDRA_DIR/lib..')
        JNA_IN_LIB = glob.glob('%s/lib/jna-*.jar' % os.environ['CASSANDRA_DIR'])
        JNA_PATH = JNA_IN_LIB[0]
except KeyError:
    if glob.glob('%s/lib/jna-*.jar' % DEFAULT_DIR):
        print ('Using jna.jar in DEFAULT_DIR/lib/..')
        JNA_IN_LIB = glob.glob('%s/lib/jna-*.jar' % DEFAULT_DIR)
        JNA_PATH = JNA_IN_LIB[0]


@since('2.0', max_version='4')
class TestThriftHSHA(Tester):

    def test_closing_connections(self):
        """
        @jira_ticket CASSANDRA-6546

        Test CASSANDRA-6546 - do connections get closed when disabling / renabling thrift service?
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            'start_rpc': 'true',
            'rpc_server_type': 'hsha',
            'rpc_max_threads': 20
        })

        cluster.populate(1)
        (node1,) = cluster.nodelist()
        cluster.start()

        session = self.patient_cql_connection(node1)
        create_ks(session, 'test', 1)
        session.execute("CREATE TABLE \"CF\" (key text PRIMARY KEY, val text) WITH COMPACT STORAGE;")

        def make_connection():
            host, port = node1.network_interfaces['thrift']
            client = get_thrift_client(host, port)
            client.transport.open()
            return client

        pools = []
        connected_thrift_clients = make_mbean('metrics', type='Client', name='connectedThriftClients')
        for i in range(10):
            logger.debug("Creating connection pools..")
            for x in range(3):
                pools.append(make_connection())
            logger.debug("Disabling/Enabling thrift iteration #{i}".format(i=i))
            node1.nodetool('disablethrift')
            node1.nodetool('enablethrift')
            logger.debug("Closing connections from the client side..")
            for client in pools:
                client.transport.close()

            with JolokiaAgent(node1) as jmx:
                num_clients = jmx.read_attribute(connected_thrift_clients, "Value")
                assert int(num_clients) == 0, "There are still open Thrift connections after stopping service " + str(num_clients)

    @unittest.skipIf(not os.path.exists(ATTACK_JAR), "No attack jar found")
    @unittest.skipIf(not os.path.exists(JNA_PATH), "No JNA jar found")
    def test_6285(self):
        """
        @jira_ticket CASSANDRA-6285

        Test CASSANDRA-6285 with Viktor Kuzmin's  attack jar.

        This jar file is not a part of this repository, you can
        compile it yourself from sources found on CASSANDRA-6285. This
        test will be skipped if the jar file is not found.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            'start_rpc': 'true',
            'rpc_server_type': 'hsha',
            'rpc_max_threads': 20
        })

        # Enable JNA:
        with open(os.path.join(self.test_path, 'test', 'cassandra.in.sh'), 'w') as f:
            f.write('CLASSPATH={jna_path}:$CLASSPATH\n'.format(
                jna_path=JNA_PATH))

        cluster.populate(2)
        nodes = (node1, node2) = cluster.nodelist()
        [n.start(use_jna=True) for n in nodes]
        logger.debug("Cluster started.")

        session = self.patient_cql_connection(node1)
        create_ks(session, 'tmp', 2)

        session.execute("""CREATE TABLE "CF" (
  key blob,
  column1 timeuuid,
  value blob,
  PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE;
""")

        logger.debug("running attack jar...")
        p = subprocess.Popen(shlex.split("java -jar {attack_jar}".format(attack_jar=ATTACK_JAR)))
        p.communicate()

        logger.debug("Stopping cluster..")
        cluster.stop()
        logger.debug("Starting cluster..")
        cluster.start(no_wait=True)
        logger.debug("Waiting 10 seconds before we're done..")
        time.sleep(10)
