from dtest import Tester, debug
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib.node import TimeoutError
import random
import os

from tools import ThriftConnection

CASSANDRA_VERSION = os.environ.get('CASSANDRA_VERSION', '1.0.0')

# As of Sep 4, 2012 there is an error upgrading to trunk due to 
# https://issues.apache.org/jira/browse/CASSANDRA-4576
versions = (
    '0.7.10', '0.8.10', 'git:cassandra-1.0', 'git:cassandra-1.1', 'git:trunk',
)

class TestUpgradeThroughVersions(Tester):
    """
    upgrades a 3-node cluster through each of the above versions.
    """

    def upgrade_test(self):
        self.num_rows = 0
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_cassandra_dir(cassandra_version=versions[0])

        # Create a ring
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()
        self.node2 = node2

        node1.watch_log_for('Listening for thrift clients...')
        conn = ThriftConnection(node1)
        conn.create_ks()
        conn.create_cf()
        time.sleep(.5)
        self._write_values()

        self.upgrade_to_version(versions[1], from_version_07=True)
        # upgrade through the other versions
        for version in versions[2:]:
            self.upgrade_to_version(version, from_version_07=False)


    def upgrade_to_version(self, version, from_version_07=True):
        debug('Upgrading to ' + version)
        for node in self.cluster.nodelist():
            debug('upgrading node ' + node.name)
            self._check_values()
            node.flush()
            time.sleep(.5)
            # wait_other_notice doesn't work right on version 07
            if from_version_07:
                node.stop(wait_other_notice=False)
                time.sleep(30)
                node.set_cassandra_dir(cassandra_version=version)
                node.start(wait_other_notice=False)
                time.sleep(30)
                node.nodetool('scrub')
            else:
                node.stop(wait_other_notice=True)
                node.set_cassandra_dir(cassandra_version=version)
                node.start(wait_other_notice=True)
                time.sleep(.5)
                node.nodetool('upgradesstables')
            self._write_values()
            self._check_values()

        # Check we can bootstrap a new 1.0 node
        debug("Adding a node to the cluster")
        self.cluster.set_cassandra_dir(cassandra_version=version)
        nnode = new_node(self.cluster, remote_debug_port=str(2000+len(self.cluster.nodes)))
        if from_version_07:
            nnode.start(no_wait=True)
            time.sleep(200)
        else:
            nnode.start(no_wait=False)
            nnode.watch_log_for("Bootstrap/Replace/Move completed")
        self._check_values()


    def _write_values(self, consistency_level='ALL'):
        self.num_rows += 2
        conn = ThriftConnection(self.node2).use_ks()
        conn.insert_columns(self.num_rows, consistency_level)


    def _check_values(self, consistency_level='ALL'):
        for node in self.cluster.nodelist():
            conn = ThriftConnection(node).use_ks()
            conn.query_columns(self.num_rows, consistency_level)
            
