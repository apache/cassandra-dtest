import os
import re
import sys
import time
from abc import ABCMeta
from distutils.version import LooseVersion
from unittest import skipIf

from ccmlib.common import get_version_from_build, is_win

from dtest import DEBUG, Tester, debug

UPGRADE_TEST_RUN = os.environ.get('UPGRADE_TEST_RUN', '').lower() in {'true', 'yes'}


def sanitize_version(version, allow_ambiguous=True):
    """
    Takes version of the form cassandra-1.2, 2.0.10, or trunk.
    Returns a LooseVersion(x.y.z)

    If allow_ambiguous is False, will raise RuntimeError if no version is found.
    """
    if (version == 'git:trunk') or (version == 'trunk'):
        return LooseVersion('git:trunk')

    match = re.match('^.*(\d+\.+\d+\.*\d*).*$', unicode(version))
    if match:
        return LooseVersion(match.groups()[0])

    if not allow_ambiguous:
        raise RuntimeError("Version could not be identified")


def switch_jdks(version):
    cleaned_version = sanitize_version(version)

    if cleaned_version is None:
        debug("Not switching jdk as cassandra version couldn't be identified from {}".format(version))
        return

    try:
        if version < LooseVersion('2.1'):
            os.environ['JAVA_HOME'] = os.environ['JAVA7_HOME']
        else:
            os.environ['JAVA_HOME'] = os.environ['JAVA8_HOME']
    except KeyError:
        raise RuntimeError("You need to set JAVA7_HOME and JAVA8_HOME to run these tests!")
    debug("Set JAVA_HOME: [{}] for cassandra version: [{}]".format(os.environ['JAVA_HOME'], version))


@skipIf(not UPGRADE_TEST_RUN, 'set UPGRADE_TEST_RUN=true to run upgrade tests')
@skipIf(sys.platform == 'win32', 'Skip upgrade tests on Windows')
class UpgradeTester(Tester):
    """
    When run in 'normal' upgrade mode without specifying any version to run,
    this will test different upgrade paths depending on what version of C* you
    are testing. When run on 2.1 or 2.2, this will test the upgrade to 3.0.
    When run on 3.0, this will test the upgrade path to trunk. When run on
    versions above 3.0, this will test the upgrade path from 3.0 to HEAD.
    """
    # make this an abc so we can get all subclasses with __subclasses__()
    __metaclass__ = ABCMeta
    NODES, RF, __test__, CL, UPGRADE_PATH = 2, 1, False, None, None

    def prepare(self, ordered=False, create_keyspace=True, use_cache=False,
                nodes=None, rf=None, protocol_version=None, cl=None, **kwargs):
        nodes = self.NODES if nodes is None else nodes
        rf = self.RF if rf is None else rf

        cl = self.CL if cl is None else cl
        self.CL = cl  # store for later use in do_upgrade

        assert nodes >= 2, "backwards compatibility tests require at least two nodes"
        assert not self._preserve_cluster, "preserve_cluster cannot be True for upgrade tests"

        self.protocol_version = protocol_version

        cluster = self.cluster

        if (ordered):
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if (use_cache):
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        start_rpc = kwargs.pop('start_rpc', False)
        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        cluster.set_configuration_options(values={'internode_compression': 'none'})

        cluster.populate(nodes)
        node1 = cluster.nodelist()[0]
        cluster.set_install_dir(version=self.UPGRADE_PATH.starting_version)
        cluster.start(wait_for_binary_proto=True)

        node1 = cluster.nodelist()[0]
        time.sleep(0.2)

        session = self.patient_cql_connection(node1, protocol_version=protocol_version)
        if create_keyspace:
            self.create_ks(session, 'ks', rf)

        if cl:
            session.default_consistency_level = cl

        return session

    def do_upgrade(self, session):
        """
        Upgrades the first node in the cluster and returns a list of
        (is_upgraded, Session) tuples.  If `is_upgraded` is true, the
        Session is connected to the upgraded node.
        """
        session.cluster.shutdown()
        node1 = self.cluster.nodelist()[0]
        node2 = self.cluster.nodelist()[1]

        # stop the nodes
        node1.drain()
        node1.stop(gently=True)

        # Ignore errors before upgrade on Windows
        # We ignore errors from 2.1, because windows 2.1
        # support is only beta. There are frequent log errors,
        # related to filesystem interactions that are a direct result
        # of the lack of full functionality on 2.1 Windows, and we dont
        # want these to pollute our results.
        if is_win() and self.cluster.version() <= '2.2':
            node1.mark_log_for_errors()

        debug('upgrading node1 to {}'.format(self.UPGRADE_PATH.upgrade_version))

        node1.set_install_dir(version=self.UPGRADE_PATH.upgrade_version)

        # this is a bandaid; after refactoring, upgrades should account for protocol version
        new_version_from_build = get_version_from_build(node1.get_install_dir())
        if (new_version_from_build >= '3' and self.protocol_version is not None and self.protocol_version < 3):
            self.skip('Protocol version {} incompatible '
                      'with Cassandra version {}'.format(self.protocol_version, new_version_from_build))
        node1.set_log_level("DEBUG" if DEBUG else "INFO")
        node1.set_configuration_options(values={'internode_compression': 'none'})
        node1.start(wait_for_binary_proto=True, wait_other_notice=True)

        sessions = []
        session = self.patient_exclusive_cql_connection(node1, protocol_version=self.protocol_version)
        session.set_keyspace('ks')
        sessions.append((True, session))

        # open a second session with the node on the old version
        session = self.patient_exclusive_cql_connection(node2, protocol_version=self.protocol_version)
        session.set_keyspace('ks')
        sessions.append((False, session))

        if self.CL:
            for is_upgraded, session in sessions:
                session.default_consistency_level = self.CL

        # Let the nodes settle briefly before yielding connections in turn (on the upgraded and non-upgraded alike)
        # CASSANDRA-11396 was the impetus for this change, wherein some apparent perf noise was preventing
        # CL.ALL from being reached. The newly upgraded node needs to settle because it has just barely started, and each
        # non-upgraded node needs a chance to settle as well, because the entire cluster (or isolated nodes) may have been doing resource intensive activities
        # immediately before.
        for s in sessions:
            time.sleep(5)
            yield s

    def get_version(self):
        node1 = self.cluster.nodelist()[0]
        return node1.version()

    def get_node_versions(self):
        return [n.get_cassandra_version() for n in self.cluster.nodelist()]

    def node_version_above(self, version):
        return min(self.get_node_versions()) >= version

    def get_node_version(self, is_upgraded):
        """
        Used in places where is_upgraded was used to determine if the node version was >=2.2.
        """
        node_versions = self.get_node_versions()
        self.assertLessEqual(len(set(node_versions)), 2)
        return max(node_versions) if is_upgraded else min(node_versions)

    def tearDown(self):
        # Ignore errors before upgrade on Windows
        # We ignore errors from 2.1, because windows 2.1
        # support is only beta. There are frequent log errors,
        # related to filesystem interactions that are a direct result
        # of the lack of full functionality on 2.1 Windows, and we dont
        # want these to pollute our results.
        if is_win() and self.cluster.version() <= '2.2':
            self.cluster.nodelist()[1].mark_log_for_errors()
        super(UpgradeTester, self).tearDown()
