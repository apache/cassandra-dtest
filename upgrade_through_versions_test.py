from dtest import Tester, debug, DISABLE_VNODES
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib.node import TimeoutError
import time
import os
import re
from distutils.version import LooseVersion

from tools import ThriftConnection

TRUNK_VERSION = '2.1'

versions = ('git:cassandra-1.1', 'git:cassandra-1.2.15', 'git:cassandra-2.0.5', 'git:trunk')
semantic_versions = [LooseVersion(
    v.replace('git:cassandra-','').replace('git:','').replace("trunk",TRUNK_VERSION)) 
                     for v in versions]

def get_version_from_build():
    cassandra_dir = os.environ["CASSANDRA_DIR"]
    build = os.path.join(cassandra_dir, 'build.xml')
    with open(build) as f:
        for line in f:
            match = re.search('name="base\.version" value="([0-9.]+)[^"]*"', line)
            if match:
                return LooseVersion(match.group(1))

try:
    current_version = get_version_from_build()
except:
    current_version = versions[-1]

test_versions = [v for v,s in zip(versions, semantic_versions) if s <= current_version]
debug("Versions to test: %s" % str(test_versions))

class TestUpgradeThroughVersions(Tester):
    """
    Upgrades a 3-node cluster through each of the above versions.
    If the CASSANDRA_DIR variable is set then upgrade to that version,
    otherwise upgrade all the way to the trunk.
    """

    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        # Force cluster options that are common among versions:
        kwargs['cluster_options'] = {'partitioner':'org.apache.cassandra.dht.RandomPartitioner'}
        Tester.__init__(self, *args, **kwargs)

    def setUp(self):
        # Forcing cluster version on purpose
        os.environ['CASSANDRA_VERSION'] = test_versions[0]
        super(TestUpgradeThroughVersions, self).setUp()

    def upgrade_test(self):
        self.upgrade_scenario(check_counters=False)

    def upgrade_test_mixed(self):
        """Only upgrade part of the cluster, so we have mixed versions part way through."""
        self.upgrade_scenario(check_counters=False, mixed_version=True)

    def upgrade_scenario(self, mixed_version=False, check_counters=True, flush=True):
        # Record the rows we write as we go:
        self.row_values = set()
        self.counter_val = 0
        cluster = self.cluster

        # Start with 3 node cluster
        debug('Creating cluster (%s)' % test_versions[0])
        cluster.populate(3)
        cluster.start()
        node1, node2, node3 = cluster.nodelist()
        self.node2 = node2

        self._create_schema()

        # node1.stress(['--operation=INSERT','--family-type=Standard','--num-keys=10000','--create-index=KEYS','--compression=SnappyCompressor','--compaction-strategy=LeveledCompactionStrategy'])
        
        self._write_values()
        self._increment_counter_value()

        # upgrade through versions
        for version in test_versions[1:]:
            if mixed_version:
                for num, node in enumerate(self.cluster.nodelist()):
                    self.upgrade_to_version(version, mixed_version=True, nodes=(node,), 
                                            check_counters=check_counters, flush=flush)
                    node.nodetool('upgradesstables')
                    debug('Successfully upgraded %d of %d nodes to %s' % 
                          (num+1, len(self.cluster.nodelist()), version))
            else:
                self.upgrade_to_version(version, check_counters=check_counters, flush=flush)
            debug('All nodes successfully upgraded to %s' % version)

        cluster.stop()

    def upgrade_to_version(self, version, mixed_version=False, nodes=None, check_counters=True, flush=True):
        """Upgrade Nodes - if *mixed_version* is True, only upgrade those nodes
        that are specified by *nodes*, otherwise ignore *nodes* specified
        and upgrade all nodes.
        """
        debug('Upgrading to ' + version)
        if not mixed_version:
            nodes = self.cluster.nodelist()

        # Shutdown nodes
        for node in nodes:
            debug('Prepping node for shutdown: ' + node.name)
            if flush:
                node.flush()
        
        for node in nodes:
            debug('Shutting down node: ' + node.name)
            if flush:
                node.drain()
                node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        if not DISABLE_VNODES and version >= "1.2":
            self.cluster.set_configuration_options(values={
                'initial_token': None, 
                'num_tokens': 256})

        # Update Cassandra Directory
        for node in nodes:
            node.set_cassandra_dir(cassandra_version=version)
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_cassandra_dir()))
        self.cluster.set_cassandra_dir(cassandra_version=version)

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, version))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True)
            if not mixed_version:
                node.nodetool('upgradesstables')

        if not DISABLE_VNODES and version >= "1.2" and not mixed_version:
            debug("Running shuffle")
            self.node2.shuffle("create")
            self.node2.shuffle("en")

        
        for node in nodes:
            debug('Checking %s ...' % (node.name))
            if not mixed_version:
                self._write_values()
            self._check_values()

        debug('upgrade.cf should have %d total rows' % (len(self.row_values)))
            
        self._increment_counter_value()
        if check_counters:
            self._check_counter_values()
        
        if not mixed_version:
            # Check we can bootstrap a new node on the upgraded cluster:
            debug("Adding a node to the cluster")
            self.cluster.set_cassandra_dir(cassandra_version=version)
            nnode = new_node(self.cluster, remote_debug_port=str(2000+len(self.cluster.nodes)))
            nnode.start(no_wait=False)
            self._check_values()
            if check_counters:
                self._check_counter_values()
                    
    def _create_schema(self):
        cursor = self.patient_cql_connection(self.node2).cursor()
        
        # DDL for C* 1.1 :
        cursor.execute("""CREATE KEYSPACE upgrade WITH strategy_class = 'SimpleStrategy' 
        AND strategy_options:replication_factor = 2;""")

        cursor.execute('use upgrade')
        cursor.execute('CREATE TABLE cf ( k int PRIMARY KEY , v text )')
        cursor.execute('CREATE INDEX vals ON cf (v)')
        cursor.execute("CREATE TABLE countertable ( "
                                      "k text PRIMARY KEY, "
                                      "c counter) WITH "
                                      "default_validation=CounterColumnType;")

    def _write_values(self, num=100, consistency_level='ALL'):
        cursor = self.patient_cql_connection(self.node2).cursor()
        cursor.execute("use upgrade")
        for i in xrange(num):
            x = len(self.row_values) + 1
            cursor.execute("UPDATE cf SET v='%d' WHERE k=%d" % (x,x))
            self.row_values.add(x)

    def _check_values(self, consistency_level='ALL'):
        for node in self.cluster.nodelist():
            cursor = self.patient_cql_connection(node).cursor()
            cursor.execute("use upgrade")
            for x in self.row_values:
                cursor.execute("SELECT k,v FROM cf WHERE k=%d" % x, consistency_level=consistency_level)
                k,v = cursor.fetchone()
                self.assertEqual(x, k)
                self.assertEqual(str(x), v)

    def _increment_counter_value(self):
        debug("incrementing counter...")
        cursor = self.patient_cql_connection(self.node2).cursor()
        cursor.execute("use upgrade;")
        update_counter_query = ("UPDATE countertable SET c = c + 1 WHERE k='www.datastax.com'")
        cursor.execute( update_counter_query )
        self.counter_val += 1

    def _check_counter_values(self):
        debug("Checking counter values...")
        cursor = self.patient_cql_connection(self.node2).cursor()
        cursor.execute("use upgrade;")
        cursor.execute("SELECT c from countertable;")
        res = cursor.fetchall()[0][0]
        assert res == self.counter_val, "Counter not at expected value."

