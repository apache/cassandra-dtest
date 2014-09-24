import bisect
import os
import random
import re
import subprocess
import time
import uuid

from collections import defaultdict
from distutils.version import LooseVersion
from dtest import Tester, debug, DISABLE_VNODES, DEFAULT_DIR
from pytools import new_node
from ccmlib import common as ccmcommon
import tarfile
from cassandra import ConsistencyLevel, WriteTimeout
from cassandra.query import SimpleStatement

TRUNK_VER = '2.2'

# Used to build upgrade path(s) for tests. Some tests will go from start to finish,
# other tests will focus on single upgrades from UPGRADE_PATH[n] to UPGRADE_PATH[n+1]
# Note that these strings should match git branch names, and will be used to search for
# tags which are related to a particular branch as well.

DEFAULT_PATH = ['cassandra-1.2', 'cassandra-2.0', 'cassandra-2.1', 'trunk']
CUSTOM_PATH = os.environ.get('UPGRADE_PATH', None)
UPGRADE_PATH = CUSTOM_PATH and CUSTOM_PATH.split(':') or DEFAULT_PATH
LOCAL_MODE = os.environ.get('LOCAL_MODE', '').lower() in ('yes', 'true')


if os.environ.get('CASSANDRA_VERSION'):
    debug('CASSANDRA_VERSION is not used by upgrade tests!')


class GitSemVer(object):
    """
    Wraps a git ref up with a semver (as LooseVersion)
    """
    git_ref = None
    semver = None

    def __init__(self, git_ref, semver_str):
        self.git_ref = git_ref
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
    test_versions = None  # set on init to know which versions to use

    def __init__(self, *args, **kwargs):
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]

        # Force cluster options that are common among versions:
        kwargs['cluster_options'] = {'partitioner': 'org.apache.cassandra.dht.Murmur3Partitioner'}
        Tester.__init__(self, *args, **kwargs)

    @property
    def test_versions(self):
        # Murmur was not present until 1.2+
        return [v for v in UPGRADE_PATH if get_version_from_tag(v) >= '1.2']

    def _init_local(self, git_ref):
        cdir = os.environ.get('CASSANDRA_DIR', DEFAULT_DIR)

        subprocess.check_call(
            ["git", "checkout", "{git_ref}".format(git_ref=git_ref)], cwd=cdir)

        subprocess.check_call(
            ["ant", "-Dbase.version={}".format(git_ref), "clean", "jar"], cwd=cdir)

    def setUp(self):
        # Forcing cluster version on purpose
        if LOCAL_MODE:
            self._init_local(self.test_versions[0])
        else:
            os.environ['CASSANDRA_VERSION'] = 'git:' + self.test_versions[0]

        debug("Versions to test (%s): %s" % (type(self), str([v for v in self.test_versions])))
        super(TestUpgradeThroughVersions, self).setUp()

    def upgrade_test(self):
        self.upgrade_scenario()

    def upgrade_test_mixed(self):
        """Only upgrade part of the cluster, so we have mixed versions part way through."""
        self.upgrade_scenario(mixed_version=True)

    def upgrade_scenario(self, populate=True, create_schema=True, mixed_version=False, after_upgrade_call=()):
        # Record the rows we write as we go:
        self.row_values = set()
        cluster = self.cluster

        if populate:
            # Start with 3 node cluster
            debug('Creating cluster (%s)' % self.test_versions[0])
            cluster.populate(3)
            [node.start(use_jna=True) for node in cluster.nodelist()]
        else:
            debug("Skipping cluster creation (should already be built)")

        # add nodes to self for convenience
        for i, node in enumerate(cluster.nodelist(), 1):
            node_name = 'node' + str(i)
            setattr(self, node_name, node)

        if create_schema:
            self._create_schema()
        else:
            debug("Skipping schema creation (should already be built)")
        time.sleep(5)  # sigh...

        self._log_current_ver(self.test_versions[0])

        # upgrade through versions
        for tag in self.test_versions[1:]:
            if mixed_version:
                for num, node in enumerate(self.cluster.nodelist()):
                    # do a write and check for each new node as upgraded

                    self._write_values()
                    self._increment_counters()

                    self.upgrade_to_version(tag, mixed_version=True, nodes=(node,))

                    self._check_values()
                    self._check_counters()

                    debug('Successfully upgraded %d of %d nodes to %s' %
                          (num + 1, len(self.cluster.nodelist()), tag))

                self.cluster.set_install_dir(version='git:' + tag)
            else:
                self._write_values()
                self._increment_counters()

                self.upgrade_to_version(tag)
                self.cluster.set_install_dir(version='git:' + tag)

                self._check_values()
                self._check_counters()

            # run custom post-upgrade callables
            for call in after_upgrade_call:
                call()

            debug('All nodes successfully upgraded to %s' % tag)
            self._log_current_ver(tag)

        cluster.stop()

    def upgrade_to_version(self, tag, mixed_version=False, nodes=None):
        """Upgrade Nodes - if *mixed_version* is True, only upgrade those nodes
        that are specified by *nodes*, otherwise ignore *nodes* specified
        and upgrade all nodes.
        """
        debug('Upgrading {nodes} to {tag}'.format(nodes=[n.name for n in nodes] if nodes is not None else 'all nodes',tag=tag))
        if not mixed_version:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update source or get a new version
        if LOCAL_MODE:
            self._init_local(tag)
            cdir = os.environ.get('CASSANDRA_DIR', DEFAULT_DIR)

            # Although we're not changing dirs, the source has changed, so ccm probably needs to know
            for node in nodes:
                node.set_install_dir(install_dir=cdir)
                debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        else:
            for node in nodes:
                node.set_install_dir(version='git:' + tag)
                debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))

        # hacky? yes. We could probably extend ccm to allow this publicly.
        # the topology file needs to be written before any nodes are started
        # otherwise they won't be grouped into dc's properly for multi-dc tests
        self.cluster._Cluster__update_topology_files()

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True)
            node.nodetool('upgradesstables -a')

    def _log_current_ver(self, current_tag):
        """
        Logs where we currently are in the upgrade path, surrounding the current branch/tag, like ***sometag***
        """
        vers = self.test_versions
        curr_index = vers.index(current_tag)
        debug(
            "Current upgrade path: {}".format(
                vers[:curr_index] + ['***' + current_tag + '***'] + vers[curr_index + 1:]))

    def _create_schema(self):
        cursor = self.patient_cql_connection(self.node2, version="3.0.0", protocol_version=1)

        cursor.execute("""CREATE KEYSPACE upgrade WITH replication = {'class':'SimpleStrategy',
            'replication_factor':2};
            """)

        cursor.execute('use upgrade')
        cursor.execute('CREATE TABLE cf ( k int PRIMARY KEY , v text )')
        cursor.execute('CREATE INDEX vals ON cf (v)')

        cursor.execute("""
            CREATE TABLE countertable (
                k1 text,
                k2 int,
                c counter,
                PRIMARY KEY (k1, k2)
                );""")

    def _write_values(self, num=100):
        cursor = self.patient_cql_connection(self.node2, protocol_version=1)
        cursor.execute("use upgrade")
        for i in xrange(num):
            x = len(self.row_values) + 1
            cursor.execute("UPDATE cf SET v='%d' WHERE k=%d" % (x, x))
            self.row_values.add(x)

    def _check_values(self, consistency_level=ConsistencyLevel.ALL):
        for node in self.cluster.nodelist():
            cursor = self.patient_cql_connection(node, protocol_version=1)
            cursor.execute("use upgrade")
            for x in self.row_values:
                query = SimpleStatement("SELECT k,v FROM cf WHERE k=%d" % x, consistency_level=consistency_level)
                result = cursor.execute(query)
                k,v = result[0]
                self.assertEqual(x, k)
                self.assertEqual(str(x), v)

    def _increment_counters(self, opcount=25000):
        debug("performing {opcount} counter increments".format(opcount=opcount))
        cursor = self.patient_cql_connection(self.node2, version="3.0.0", protocol_version=1)
        cursor.execute("use upgrade;")

        update_counter_query = ("UPDATE countertable SET c = c + 1 WHERE k1='{key1}' and k2={key2}")

        self.expected_counts = {}
        for i in range(10):
            self.expected_counts[uuid.uuid4()] = defaultdict(int)

        fail_count = 0

        for i in range(opcount):
            key1 = random.choice(self.expected_counts.keys())
            key2 = random.randint(1, 10)
            try:
                query = SimpleStatement(update_counter_query.format(key1=key1, key2=key2), consistency_level=ConsistencyLevel.ALL)
                cursor.execute(query)
            except WriteTimeout:
                fail_count += 1
            else:
                self.expected_counts[key1][key2] += 1
            if fail_count > 100:
                break

        assert fail_count < 100, "Too many counter increment failures"

    def _check_counters(self):
        debug("Checking counter values...")
        cursor = self.patient_cql_connection(self.node2, version="3.0.0", protocol_version=1)
        cursor.execute("use upgrade;")

        for key1 in self.expected_counts.keys():
            for key2 in self.expected_counts[key1].keys():
                expected_value = self.expected_counts[key1][key2]

                query = SimpleStatement("SELECT c from countertable where k1='{key1}' and k2={key2};".format(key1=key1, key2=key2),
                    consistency_level=ConsistencyLevel.ONE)
                results = cursor.execute(query)

                if results is not None:
                    actual_value = results[0][0]
                else:
                    # counter wasn't found
                    actual_value = None

                assert actual_value == expected_value, "Counter not at expected value. Got %s, expected %s" % (actual_value, expected_value)


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
        kwargs['cluster_options'] = {'partitioner': 'org.apache.cassandra.dht.RandomPartitioner'}
        Tester.__init__(self, *args, **kwargs)

    @property
    def test_versions(self):
        return [v for v in UPGRADE_PATH]


class PointToPointUpgradeBase(TestUpgradeThroughVersions):
    """
    Base class for testing a single upgrade (ver1->ver2).

    We are dynamically creating subclasses of this for testing point upgrades, so this is a convenient
    place to add functionality/tests for those subclasses to run.

    __test__ is False for this class. Subclasses need to revert to True to run tests!
    """
    __test__ = False

    def setUp(self):
        if LOCAL_MODE:
            self._init_local(self.test_versions[0])
        else:
            # Forcing cluster version on purpose
            os.environ['CASSANDRA_VERSION'] = 'git:' + self.test_versions[0]

        debug("Versions to test (%s): %s" % (type(self), str([v for v in self.test_versions])))
        super(TestUpgradeThroughVersions, self).setUp()

    def _bootstrap_new_node(self):
        # Check we can bootstrap a new node on the upgraded cluster:
        debug("Adding a node to the cluster")
        nnode = new_node(self.cluster, remote_debug_port=str(2000 + len(self.cluster.nodes)))
        nnode.start(use_jna=True, wait_other_notice=True)
        self._write_values()
        self._increment_counters()
        self._check_values()
        self._check_counters()

    def _bootstrap_new_node_multidc(self):
        # Check we can bootstrap a new node on the upgraded cluster:
        debug("Adding a node to the cluster")
        nnode = new_node(self.cluster, remote_debug_port=str(2000 + len(self.cluster.nodes)), data_center='dc2')

        nnode.start(use_jna=True, wait_other_notice=True)
        self._write_values()
        self._increment_counters()
        self._check_values()
        self._check_counters()

    def bootstrap_test(self):
        # try and add a new node
        self.upgrade_scenario(after_upgrade_call=(self._bootstrap_new_node,))

    def bootstrap_multidc_test(self):
        # try and add a new node
        # multi dc, 2 nodes in each dc
        self.cluster.populate([2, 2])
        [node.start(use_jna=True) for node in self.cluster.nodelist()]
        self._multidc_schema_create()
        self.upgrade_scenario(populate=False, create_schema=False, after_upgrade_call=(self._bootstrap_new_node_multidc,))

    def _multidc_schema_create(self):
        cursor = self.patient_cql_connection(self.cluster.nodelist()[0], version="3.0.0", protocol_version=1)

        if self.cluster.version() >= '1.2':
            # DDL for C* 1.2+
            cursor.execute("""CREATE KEYSPACE upgrade WITH replication = {'class':'NetworkTopologyStrategy',
                'dc1':1, 'dc2':1};
                """)
        else:
            # DDL for C* 1.1
            cursor.execute("""CREATE KEYSPACE upgrade WITH strategy_class = 'NetworkTopologyStrategy'
            AND strategy_options:'dc1':1
            AND strategy_options:'dc2':1;
            """)

        cursor.execute('use upgrade')
        cursor.execute('CREATE TABLE cf ( k int PRIMARY KEY , v text )')
        cursor.execute('CREATE INDEX vals ON cf (v)')

        cursor.execute("""
            CREATE TABLE countertable (
                k1 text,
                k2 int,
                c counter,
                PRIMARY KEY (k1, k2)
                );""")


# create test classes for upgrading from latest tag on branch to the head of that same branch
for from_ver in UPGRADE_PATH:
    # we only want to do single upgrade tests for 1.2+
    # and trunk is the final version, so there's no test where trunk is upgraded to something else
    if get_version_from_tag(from_ver) >= '1.2' and from_ver != 'trunk':
        cls_name = ('TestUpgrade_from_' + from_ver + '_latest_tag_to_' + from_ver + '_HEAD').replace('-', '_').replace('.', '_')
        start_ver_latest_tag = latest_tag_matching(from_ver)
        debug('Creating test upgrade class: {} with start tag of: {}'.format(cls_name, start_ver_latest_tag))
        vars()[cls_name] = type(
            cls_name,
            (PointToPointUpgradeBase,),
            {'test_versions': [start_ver_latest_tag, from_ver], '__test__': True})

# build a list of tuples like so:
# [(A, B), (B, C) ... ]
# each pair in the list represents an upgrade test (A, B)
# where we will upgrade from the latest *tag* matching A, to the HEAD of branch B
POINT_UPGRADES = []
points = [v for v in UPGRADE_PATH if get_version_from_tag(v) >= '1.2']
for i, _ in enumerate(points):
    verslice = tuple(points[i:i + 2])
    if len(verslice) == 2:  # exclude dangling version at end
        POINT_UPGRADES.append(tuple(points[i:i + 2]))

# create test classes for upgrading from latest tag on one branch, to head of the next branch (see comment above)
for (from_ver, to_branch) in POINT_UPGRADES:
    cls_name = ('TestUpgrade_from_' + from_ver + '_latest_tag_to_' + to_branch + '_HEAD').replace('-', '_').replace('.', '_')
    from_ver_latest_tag = latest_tag_matching(from_ver)
    debug('Creating test upgrade class: {} with start tag of: {}'.format(cls_name, from_ver_latest_tag))
    vars()[cls_name] = type(
        cls_name,
        (PointToPointUpgradeBase,),
        {'test_versions': [from_ver_latest_tag, to_branch], '__test__': True})

# create test classes for upgrading from HEAD of one branch to HEAD of next.
for (from_branch, to_branch) in POINT_UPGRADES:
    cls_name = ('TestUpgrade_from_' + from_branch + '_HEAD_to_' + to_branch + '_HEAD').replace('-', '_').replace('.', '_')
    debug('Creating test upgrade class: {}'.format(cls_name))
    vars()[cls_name] = type(
        cls_name,
        (PointToPointUpgradeBase,),
        {'test_versions': [from_branch, to_branch], '__test__': True})

# create test classes for upgrading from HEAD of one branch, to latest tag of next branch
for (from_branch, to_branch) in POINT_UPGRADES:
    cls_name = ('TestUpgrade_from_' + from_branch + '_HEAD_to_' + to_branch + '_latest_tag').replace('-', '_').replace('.', '_')
    to_ver_latest_tag = latest_tag_matching(to_branch)
    debug('Creating test upgrade class: {} with end tag of: {}'.format(cls_name, to_ver_latest_tag))

    # in some cases we might not find a tag (like when the to_branch is trunk)
    # so these will be skipped.
    if to_ver_latest_tag is None:
        continue

    vars()[cls_name] = type(
        cls_name,
        (PointToPointUpgradeBase,),
        {'test_versions': [from_branch, to_ver_latest_tag], '__test__': True})
