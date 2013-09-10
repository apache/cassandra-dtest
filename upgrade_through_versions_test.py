from dtest import Tester, debug
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib.node import TimeoutError
import random
import time
import os
from distutils.version import LooseVersion

from tools import ThriftConnection

versions = (
    'git:cassandra-1.1', 'git:cassandra-1.2', 'git:cassandra-2.0', 'git:trunk'
)

def get_version_from_build():
    cassandra_dir = os.environ["CASSANDRA_DIR"]
    build = os.path.join(cassandra_dir, 'build.xml')
    with open(build) as f:
        for line in f:
            match = re.search('name="base\.version" value="([0-9.]+)[^"]*"', line)
            if match:
                return 'git:cassandra-' + match.group(1)

try:
    current_version = get_version_from_build()
except KeyError:
    current_version = versions[-1]

class TestUpgradeThroughVersions(Tester):
    """
    Upgrades a 3-node cluster through each of the above versions.
    If the CASSANDRA_DIR variable is set then upgrade to that version,
    otherwise upgrade all the way to the trunk.
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
        """Only upgrade part of the cluster, so we have mixed versions part way through."""
        self.upgrade_scenario(mixed_version=True)

    def upgrade_scenario(self, mixed_version=False):
        self.num_rows = 2000
        self.counter_val = 0
        cluster = self.cluster

        # Create a ring
        debug('Creating cluster (%s)' % versions[0])
        cluster.populate(3)
        cluster.start()
        node1, node2, node3 = cluster.nodelist()
        self.node2 = node2

        node1.watch_log_for('Listening for thrift clients...')
        conn = ThriftConnection(node1)
        conn.create_ks()
        conn.create_cf()
        self._create_counter_cf()
        
        time.sleep(.5)
        self._write_values()
        self._increment_counter_value()

        test_versions = [v for v in versions if v <= current_version ]
        debug( str(test_versions) )
        # upgrade through versions
        for version in test_versions[1:]:
            if mixed_version:
                self.upgrade_to_version(version, mixed_version=True, nodes=(node1,))
                self.upgrade_to_version(version, mixed_version=True, nodes=(node2,node3)) 
                node1.nodetool('upgradesstables')
                node2.nodetool('upgradesstables')
                node3.nodetool('upgradesstables')
            else:
                self.upgrade_to_version(version)
        cluster.stop()

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
        self._check_counter_values()
        
        for node in nodes:
            debug('Shutting down node: ' + node.name)
            time.sleep(.5)
            node.stop(wait_other_notice=False)

        if ENABLE_VNODES and version >= "1.2":
            self.cluster.set_configuration_options(values={'initial_token': None, 'num_tokens': 256})

        for node in nodes:
            debug('Upgrading node: ' + node.name)
            node.set_cassandra_dir(cassandra_version=version)
            node.start(wait_other_notice=True)
            time.sleep(.5)
            if not mixed_version:
                node.nodetool('upgradesstables')

        if ENABLE_VNODES and version >= "1.2" and not mixed_version:
            debug("Running shuffle")
            self.node2.shuffle("create")
            self.node2.shuffle("en")

        for node in nodes:
            debug('Checking node: ' + node.name)
            if not mixed_version:
                self._write_values()
            self._check_values()

        self._increment_counter_value()
        time.sleep(0.5)
        self._check_counter_values()
        
        if not mixed_version:
            # Check we can bootstrap a new node on the upgraded cluster:
            debug("Adding a node to the cluster")
            self.cluster.set_cassandra_dir(cassandra_version=version)
            nnode = new_node(self.cluster, remote_debug_port=str(2000+len(self.cluster.nodes)))
            nnode.start(no_wait=False)
            nnode.watch_log_for("Bootstrap completed!")
            debug("node should be up, but sleeping a bit to ensure...")
            time.sleep(15)
            self._check_values()
            self._check_counter_values()
        
        if mixed_version:
            debug('Successfully upgraded part of the cluster to %s' % version) 
        else:
            debug('Successfully upgraded to %s' % version)

    def _write_values(self, consistency_level='ALL'):
        self.num_rows += 2
        conn = ThriftConnection(self.node2).use_ks()
        conn.insert_columns(self.num_rows, consistency_level)


    def _check_values(self, consistency_level='ALL'):
        for node in self.cluster.nodelist():
            conn = ThriftConnection(node).use_ks()
            conn.query_columns(self.num_rows, consistency_level)
            
    def _create_counter_cf(self):
        cursor = self.cql_connection(self.node2).cursor()
        cursor.execute("use ks;")
        create_counter_table_query = ("CREATE TABLE countertable ( "
                                      "k text PRIMARY KEY, "
                                      "c counter) WITH "
                                      "default_validation=CounterColumnType;")
        cursor.execute( create_counter_table_query )

    def _increment_counter_value(self):
        debug("incrementing counter...")
        cursor = self.cql_connection(self.node2).cursor()
        cursor.execute("use ks;")
        update_counter_query = ("UPDATE countertable SET c = c + 1 WHERE k='www.datastax.com'")
        cursor.execute( update_counter_query )
        self.counter_val += 1
        debug("OK...")

    def _check_counter_values(self):
        debug("Checking counter values...")
        cursor = self.cql_connection(self.node2).cursor()
        cursor.execute("use ks;")
        cursor.execute("SELECT c from countertable;")
        res = cursor.fetchall()[0][0]
        assert res == self.counter_val, "Counter not at expected value."
        debug("OK...")
