import bisect, os, re, subprocess
from distutils.version import LooseVersion
from dtest import Tester, debug, DISABLE_VNODES, DEFAULT_DIR
from tools import new_node

TRUNK_VER = '2.2'

# Used to build upgrade path(s) for tests. Some tests will go from start to finish,
# other tests will focus on single upgrades from UPGRADE_PATH[n] to UPGRADE_PATH[n+1]
# Note that these strings should match git branch names, and will be used to search for
# tags which are related to a particular branch as well.
UPGRADE_PATH = ['cassandra-1.1', 'cassandra-1.2', 'cassandra-2.0', 'cassandra-2.1', 'trunk']


class GitSemVer(object):
    """
    Wraps a git ref up with a semver (as LooseVersion)
    """
    git_ref = None
    semver = None
    
    def __init__(self, git_ref, semver_str):
        self.git_ref = 'git:' + git_ref
        self.semver = LooseVersion(semver_str)
        if semver_str == 'trunk':
            self.semver = LooseVersion(TRUNK_VER)
    
    def __cmp__(self, other):
        return cmp(self.semver, other.semver)

def latest_tag_matching(match_string='cassandra-1.1'):
    """
    Returns the latest tag matching match_string*
    """
    git_path = os.environ.get('CASSANDRA_DIR', DEFAULT_DIR)
    
    tags = subprocess.check_output(
        ["git", "tag", "-l", "{search}*".format(search=match_string)], cwd=git_path)\
        .rstrip()\
        .split('\n')
    
    wrappers = []
    for t in tags:
        match = re.match('^cassandra-(\d+\.\d+\.\d+(-+\w+)*)$', t)
        if match:
            gsv = GitSemVer(t, match.group(1))
            bisect.insort(wrappers, gsv)
            
    if wrappers:
        return wrappers.pop().git_ref
    return None

def get_version_from_tag(tag):
    if tag == 'trunk':
        return TRUNK_VER
    
    match = re.match('^(git:)*cassandra-(\d+\.\d+\.*\d*(-+\w+)*)$', tag)
    if match:
        return match.group(2)
    return None

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
    Upgrades a 3-node Murmur3Partitioner cluster through versions specified in test_versions.
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
        
        # Force cluster options that are common among versions:
        kwargs['cluster_options'] = {'partitioner':'org.apache.cassandra.dht.Murmur3Partitioner'}
        Tester.__init__(self, *args, **kwargs)

    @property
    def test_versions(self):
        # Murmur was not present until 1.2+
        return ['git:'+v for v in UPGRADE_PATH if get_version_from_tag(v) >= '1.2']

    def setUp(self):
        # Forcing cluster version on purpose
        os.environ['CASSANDRA_VERSION'] = self.test_versions[0]
        debug("Versions to test (%s): %s" % (type(self), str([v for v in self.test_versions])))
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
        debug('Creating cluster (%s)' % self.test_versions[0])
        cluster.populate(3)
        cluster.start()
        node1, node2, node3 = cluster.nodelist()
        self.node2 = node2

        self._create_schema()
        
        self._write_values()
        self._increment_counter_value()

        self._log_current_ver(self.test_versions[0])
        
        # upgrade through versions
        for tag in self.test_versions[1:]:
            if mixed_version:
                for num, node in enumerate(self.cluster.nodelist()):
                    self.upgrade_to_version(tag, mixed_version=True, nodes=(node,),
                                            check_counters=check_counters, flush=flush)
                    node.nodetool('upgradesstables')
                    debug('Successfully upgraded %d of %d nodes to %s' % 
                          (num+1, len(self.cluster.nodelist()), tag))
            else:
                self.upgrade_to_version(tag, check_counters=check_counters, flush=flush)
            debug('All nodes successfully upgraded to %s' % tag)
            self._log_current_ver(tag)

        cluster.stop()

    def upgrade_to_version(self, tag, mixed_version=False, nodes=None, check_counters=True, flush=True):
        """Upgrade Nodes - if *mixed_version* is True, only upgrade those nodes
        that are specified by *nodes*, otherwise ignore *nodes* specified
        and upgrade all nodes.
        """
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
        if not DISABLE_VNODES and get_version_from_tag(tag) >= '1.2':
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
    
    def _log_current_ver(self, current_tag):
        """
        Logs where we currently are in the upgrade path, surrounding the current branch/tag, like ***sometag***
        """
        vers = self.test_versions
        curr_index = vers.index(current_tag)
        debug(
            "Current upgrade path: {}".format(
                vers[:curr_index] + ['***'+current_tag+'***'] + vers[curr_index+1:]))
    
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
        
        if self.cluster.version() >= '1.2':
            cursor.execute("""
                CREATE TABLE countertable (k text PRIMARY KEY, c counter);""")
        else:
            cursor.execute("""
                CREATE TABLE countertable (k text PRIMARY KEY, c counter)
                WITH default_validation=CounterColumnType;""")

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


class TestRandomPartitionerUpgrade(TestUpgradeThroughVersions):
    """
    Upgrades a 3-node RandomPartitioner cluster through versions specified in test_versions.
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

    @property
    def test_versions(self):
        return ['git:'+v for v in UPGRADE_PATH]


# create test classes for upgrading from latest tag on branch to the head of that same branch
for from_ver in UPGRADE_PATH:
    # we only want to do single upgrade tests for 1.2+
    # and trunk is the final version, so there's no test where trunk is upgraded to something else
    if get_version_from_tag(from_ver) >= '1.2' and from_ver != 'trunk':
        cls_name = ('TestUpgrade_from_'+from_ver+'_latest_tag_to_'+from_ver+'_HEAD').replace('-', '_').replace('.', '_')
        debug('Creating test upgrade class: {}'.format(cls_name))
        vars()[cls_name] = type(
            cls_name,
            (TestUpgradeThroughVersions,),
            {'test_versions': (latest_tag_matching(from_ver), 'git:'+from_ver,)})

# build a list of tuples like so:
# [(A, B), (B, C) ... ]
# each pair in the list represents an upgrade test (A, B)
# where we will upgrade from the latest *tag* matching A, to the HEAD of branch B
POINT_UPGRADES = []
points = [v for v in UPGRADE_PATH if get_version_from_tag(v) >= '1.2']
for i, _ in enumerate(points):
    verslice = tuple(points[i:i+2])
    if len(verslice) == 2: # exclude dangling version at end
        POINT_UPGRADES.append( tuple(points[i:i+2]) )

# create test classes
for (from_ver, to_branch) in POINT_UPGRADES:
    cls_name = ('TestUpgrade_from_'+from_ver+'_latest_tag_to_'+to_branch+'_HEAD').replace('-', '_').replace('.', '_')
    debug('Creating test upgrade class: {}'.format(cls_name))
    vars()[cls_name] = type(
        cls_name,
        (TestUpgradeThroughVersions,),
        {'test_versions': (latest_tag_matching(from_ver), 'git:'+to_branch,)})
