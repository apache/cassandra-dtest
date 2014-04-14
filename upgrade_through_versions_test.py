from collections import OrderedDict
import bisect, os, re, subprocess
from distutils.version import LooseVersion
from dtest import Tester, debug, DISABLE_VNODES, DEFAULT_DIR
from tools import new_node

START_VER = '1.1' # earliest version considered but won't necessarily be used!
TRUNK_VER = '2.1'

class TagSemVer(object):
    """
    Wraps a git tag up with a semver (as LooseVersion)
    """
    tag = None
    semver = None
    maj_min = None
    
    def __init__(self, tag, semver_str):
        self.tag = 'git:' + tag
        self.semver = LooseVersion(semver_str)
        self.maj_min = str(self.semver.version[0]) + '.' + str(self.semver.version[1])
    
    def __cmp__(self, other):
        return cmp(self.semver, other.semver)
    
def get_upgrade_path(start_ver=START_VER, trunk_ver=TRUNK_VER, num_patches=2):
    """
    Runs "git tag -l *cassandra-*"
    And returns a list representing upgrade path. Each major.minor version tag may
    include (up to) num_patches patch versions to be tested. These will be from the highest
    patch versions available for that major.minor version.
    
    The list elements will be simple wrapper objects with the git tag, and a corresponding
    LooseVersion for utility comparing versions.
    """
    git_path = os.environ.get('CASSANDRA_DIR', DEFAULT_DIR)
    
    tags = subprocess.check_output(
        ["git", "tag", "-l", "*cassandra-*"], cwd=git_path)\
        .rstrip()\
        .split('\n')
    
    # earliest version considered but won't necessarily be used!
    start_ver = LooseVersion(start_ver)
    
    wrappers = []
    for t in tags:
        match = re.match('^cassandra-(\d+\.\d+\.\d+)$', t)
        if match:
            tsv = TagSemVer(t, match.group(1))
            if tsv.semver >= start_ver:
                bisect.insort(wrappers, tsv)
    
    # manually add trunk with expected trunk version
    wrappers.append(TagSemVer('trunk', TRUNK_VER))
    
    # group by maj.min version
    release_groups = OrderedDict()
    for w in wrappers:
        if release_groups.get(w.maj_min) is None:
            release_groups[w.maj_min] = []

        release_groups[w.maj_min].append(w)
    
    upgrade_path = []
    for release, versions in release_groups.items():
        upgrade_path.extend(
            [ v for v in versions[-num_patches:] ]
        )
    
    return upgrade_path
    
def get_version_from_build():
    path = os.environ.get('CASSANDRA_DIR', DEFAULT_DIR)
    
    build = os.path.join(path, 'build.xml')
    with open(build) as f:
        for line in f:
            match = re.search('name="base\.version" value="([0-9.]+)[^"]*"', line)
            if match:
                return LooseVersion(match.group(1))


class TestUpgradeThroughVersions(Tester):
    """
    Upgrades a 3-node cluster through each of the above versions.
    If the CASSANDRA_DIR variable is set then upgrade to that version,
    otherwise upgrade all the way to the trunk.
    """
    test_versions = None # set on init to know which versions to use
    
    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        self.test_versions = get_upgrade_path()
        
        # Force cluster options that are common among versions:
        kwargs['cluster_options'] = {'partitioner':'org.apache.cassandra.dht.RandomPartitioner'}
        Tester.__init__(self, *args, **kwargs)

    def setUp(self):
        # Forcing cluster version on purpose
        os.environ['CASSANDRA_VERSION'] = self.test_versions[0].tag
        debug("Versions to test (%s): %s" % (type(self), str([v.tag for v in self.test_versions])))
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
        debug('Creating cluster (%s)' % self.test_versions[0].tag)
        cluster.populate(3)
        cluster.start()
        node1, node2, node3 = cluster.nodelist()
        self.node2 = node2

        self._create_schema()
        
        self._write_values()
        self._increment_counter_value()

        # upgrade through versions
        for tag_semver in self.test_versions[1:]:
            tag = tag_semver.tag
            semver = tag_semver.semver
            
            if mixed_version:
                for num, node in enumerate(self.cluster.nodelist()):
                    self.upgrade_to_version(tag, semver, mixed_version=True, nodes=(node,),
                                            check_counters=check_counters, flush=flush)
                    node.nodetool('upgradesstables')
                    debug('Successfully upgraded %d of %d nodes to %s' % 
                          (num+1, len(self.cluster.nodelist()), tag))
            else:
                self.upgrade_to_version(tag, semver, check_counters=check_counters, flush=flush)
            debug('All nodes successfully upgraded to %s' % tag)

        cluster.stop()

    def upgrade_to_version(self, tag, semver, mixed_version=False, nodes=None, check_counters=True, flush=True):
        """Upgrade Nodes - if *mixed_version* is True, only upgrade those nodes
        that are specified by *nodes*, otherwise ignore *nodes* specified
        and upgrade all nodes.
        """
        # see TagSemVer class
        debug('Upgrading to ' + tag)
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

        # note that this is checking the version about to be upgraded-to
        # and making settings that will apply to the new version (not necessarily the current one)
        if not DISABLE_VNODES and semver >= LooseVersion('1.2'):
            self.cluster.set_configuration_options(values={
                'initial_token': None,
                'num_tokens': 256})

        # Update Cassandra Directory
        for node in nodes:
            node.set_cassandra_dir(cassandra_version=tag)
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_cassandra_dir()))
        self.cluster.set_cassandra_dir(cassandra_version=tag)

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True)
            if not mixed_version:
                node.nodetool('upgradesstables')

        if not DISABLE_VNODES and self.cluster.version() >= '1.2':
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
            self.cluster.set_cassandra_dir(cassandra_version=tag)
            nnode = new_node(self.cluster, remote_debug_port=str(2000+len(self.cluster.nodes)))
            nnode.start(no_wait=False)
            self._check_values()
            if check_counters:
                self._check_counter_values()
                    
    def _create_schema(self):
        cursor = self.patient_cql_connection(self.node2).cursor()
        
        if self.cluster.version() >= '1.2':
            #DDL for C* 1.2+
            cursor.execute("""CREATE KEYSPACE upgrade WITH replication = {'class':'SimpleStrategy', 
                'replication_factor':2};
                """)
        else:
            # DDL for C* 1.1
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


class TestMurmurUpgrade(TestUpgradeThroughVersions):
    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        # murmur3 only works on 1.2 and up, so we have to filter the upgrade path
        # before setting self.test_versions
        min_murmur_ver = LooseVersion('1.2')
        self.test_versions = [v for v in get_upgrade_path() if v.semver >= min_murmur_ver]
        debug("Murmur3 tests intentionally exclude cassandra versions prior to 1.2 (murmur not supported)")
        
        # Force cluster options that are common among versions:
        kwargs['cluster_options'] = {'partitioner':'org.apache.cassandra.dht.Murmur3Partitioner'}
        Tester.__init__(self, *args, **kwargs)
        