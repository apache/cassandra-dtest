from dtest import Tester, debug
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib.node import TimeoutError
import random
import os

from tools import ThriftConnection

versions = (
    '1.1.9', 'git:cassandra-1.2'
)

class TestUpgradeThroughVersions(Tester):
    """
    upgrades a 3-node cluster through each of the above versions.
    """

    def __init__(self, *args, **kwargs):
        # Forcing cluster version on purpose
        os.environ['CASSANDRA_VERSION'] = versions[0]
        # Force cluster options that are common among versions:
        kwargs['cluster_options'] = {'partitioner':'org.apache.cassandra.dht.RandomPartitioner'}
        Tester.__init__(self, *args, **kwargs)

    def upgrade_test(self):
        self.upgrade_scenario()

    def upgrade_test_mixed(self):
        """Only upgrade part of the cluster, so we have mixed versions when
        we're done."""
        self.upgrade_scenario(mixed_version=True)

    def upgrade_scenario(self, mixed_version=False):
        self.num_rows = 0
        cluster = self.cluster

        # Create a ring
        cluster.populate(3)
        cluster.start()
        node1, node2, node3 = cluster.nodelist()
        self.node2 = node2

        node1.watch_log_for('Listening for thrift clients...')
        conn = ThriftConnection(node1)
        conn.create_ks()
        conn.create_cf()
        time.sleep(.5)
        self._write_values()

        # upgrade through versions
        for version in versions[1:]:
            if mixed_version:
                self.upgrade_to_version(version, mixed_version=True, nodes=(node1,))
            else:
                self.upgrade_to_version(version)

    def upgrade_to_version(self, version, mixed_version=False, nodes=None):
        """Upgrade Nodes - if *mixed_version* is True, only upgrade those nodes
        that are specified by *nodes*, otherwise ignore *nodes* specified
        and upgrade all nodes.
        """
        debug('Upgrading to ' + version)
        if not mixed_version:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Prepping node for shutdown: ' + node.name)
            node.flush()
            self._check_values()
        
        for node in nodes:
            debug('Shutting down node: ' + node.name)
            time.sleep(.5)
            node.stop(wait_other_notice=False)

        for node in nodes:
            debug('Upgrading node: ' + node.name)
            node.set_cassandra_dir(cassandra_version=version)
            node.start(wait_other_notice=True)
            time.sleep(.5)
            if not mixed_version:
                node.nodetool('upgradesstables')

        for node in nodes:
            debug('Checking node: ' + node.name)
            if not mixed_version:
                self._write_values()
            self._check_values()

        if not mixed_version:
            # Check we can bootstrap a new node on the upgraded cluster:
            debug("Adding a node to the cluster")
            self.cluster.set_cassandra_dir(cassandra_version=version)
            nnode = new_node(self.cluster, remote_debug_port=str(2000+len(self.cluster.nodes)))
            nnode.start(no_wait=False)
            nnode.watch_log_for("Bootstrap completed!")
            self._check_values()


    def _write_values(self, consistency_level='ALL'):
        self.num_rows += 2
        conn = ThriftConnection(self.node2).use_ks()
        conn.insert_columns(self.num_rows, consistency_level)


    def _check_values(self, consistency_level='ALL'):
        for node in self.cluster.nodelist():
            conn = ThriftConnection(node).use_ks()
            conn.query_columns(self.num_rows, consistency_level)
            
