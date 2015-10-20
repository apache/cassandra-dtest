import os
import sys
import time
from collections import namedtuple
from unittest import skipIf

from ccmlib.common import get_version_from_build, is_win
from dtest import DEBUG, Tester, debug
from tools import cassandra_git_branch, since

QUERY_UPGRADED = os.environ.get('QUERY_UPGRADED', 'true').lower() in ('yes', 'true')
QUERY_OLD = os.environ.get('QUERY_OLD', 'true').lower() in ('yes', 'true')
OLD_CASSANDRA_DIR = os.environ.get('OLD_CASSANDRA_DIR', None)

# This controls how many of the nodes are upgraded.  Accepted values are
# "normal", "none", and "all".
# The "normal" setting results in one of the two nodes being upgraded before
# queries are run.
# The "none" setting doesn't upgrade any nodes.  When combined with debug
# logging, this is useful for seeing exactly what commands and responses
# a 2.1 cluster will use.
# The "all" setting upgrades all nodes before querying.  When combined with debug
# logging, this is useful for seeing exactly what commands and responses
# a 3.0 cluster will use.
UPGRADE_MODE = os.environ.get('UPGRADE_MODE', 'normal').lower()

# Specify a branch to upgrade to
UPGRADE_TO = os.environ.get('UPGRADE_TO', None)


UpgradePath = namedtuple('UpgradePath', ('starting_version', 'upgrade_version'))


def get_default_upgrade_path(job_version, cdir=None):
    """
    Given a version (which should be specified as a LooseVersion,
    StrictVersion, or NormalizedVersion object), return a tuple (start, target)
    whose members indicate the git branch that should be downloaded for the
    starting or target versions for an upgrade test. One or both of these
    will be None, so this specifies at most one end of the upgrade path.

    We assume that the version being passed in is the version of C* being
    tested on a CassCI job, which means if the version is less than 3.0, we
    will be running on JDK 1.7. This means we can't run 3.0+ on this version.
    """
    start_version, upgrade_version = None, None
    debug('getting default job version for {}'.format(job_version))

    start_2_2_X_release = 'binary:2.2.3'

    if '2.1' <= job_version < '2.2':
        # If this is 2.1.X, we can upgrade to 2.2.
        # Skip 2.2.X->3.X because of JDK compatibility.
        upgrade_version = start_2_2_X_release
    elif '3.0' <= job_version < '3.1':
        try:
            branch = cassandra_git_branch(cdir=cdir)
        except:
            branch = None
        start_version = ('binary:3.0.0-rc1'
                         if branch == 'trunk'
                         else start_2_2_X_release)
    elif '3.1' <= job_version:
        # 2.2->3.X, where X > 0, isn't a supported upgrade path,
        # but 3.0->3.X is.
        start_version = 'git:cassandra-3.0'

    err = 'Expected one or two upgrade path endpoints to be None; found {}'.format((start_version, upgrade_version))
    assert [start_version, upgrade_version].count(None) >= 1, err
    upgrade_path = UpgradePath(start_version, upgrade_version)
    debug(upgrade_path)
    return upgrade_path


@since('3.0')
@skipIf(sys.platform == 'win32', 'Skip upgrade tests on Windows')
class UpgradeTester(Tester):
    """
    When run in 'normal' upgrade mode without specifying any version to run,
    this will test different upgrade paths depending on what version of C* you
    are testing. When run on 2.1 or 2.2, this will test the upgrade to 3.0.
    When run on 3.0, this will test the upgrade path to trunk. When run on
    versions above 3.0, this will test the upgrade path from 3.0 to HEAD.
    """
    NODES, RF, __test__, CL = 2, 1, False, None

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
        if not cluster.nodelist():
            cluster.populate(nodes)
            node1 = cluster.nodelist()[0]
            self.original_install_dir = node1.get_install_dir()
            self.original_version = get_version_from_build(node_path=node1.get_path())
            self.upgrade_path = get_default_upgrade_path(self.original_version, cdir=self.original_install_dir)
            if OLD_CASSANDRA_DIR:
                cluster.set_install_dir(install_dir=OLD_CASSANDRA_DIR)
            elif self.upgrade_path.starting_version:
                try:
                    cluster.set_install_dir(version=self.upgrade_path.starting_version)
                except:
                    if self.upgrade_path.starting_version.startswith('binary'):
                        debug('Exception while downloading {}; falling back to source'.format(
                            self.upgrade_path.starting_version))
                        version_number = self.upgrade_path.starting_version.split(':')[-1]
                        source_ccm_id = 'git:cassandra-' + version_number
                        debug('Source identifier: {}'.format(source_ccm_id))
                        cluster.set_install_dir(version=source_ccm_id)

            # in other cases, just use the existing install directory
            cluster.start(wait_for_binary_proto=True)
            debug('starting from {}'.format(get_version_from_build(node1.get_install_dir())))

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

        if UPGRADE_MODE not in ('normal', 'all', 'none'):
            raise Exception("UPGRADE_MODE should be one of 'normal', 'all', or 'none'")

        # stop the nodes
        if UPGRADE_MODE != "none":
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

        if UPGRADE_MODE == "all":
            node2.drain()
            node2.stop(gently=True)
            if is_win() and self.cluster.version() <= '2.2':
                node2.mark_log_for_errors()

        # choose version to upgrade to
        if UPGRADE_TO:
            install_kwargs = {'version': UPGRADE_TO}
        else:
            if self.upgrade_path.upgrade_version:
                install_kwargs = {'version': self.upgrade_path.upgrade_version}
            else:
                install_kwargs = {'install_dir': self.original_install_dir}

        debug('upgrading to {}'.format(install_kwargs))

        # start them again
        if UPGRADE_MODE != "none":
            node1.set_install_dir(**install_kwargs)
            # this is a bandaid; after refactoring, upgrades should account for protocol version
            new_version_from_build = get_version_from_build(node1.get_install_dir())
            if (new_version_from_build >= '3' and self.protocol_version is not None and self.protocol_version < 3):
                self.skip('Protocol version {} incompatible '
                          'with Cassandra version {}'.format(self.protocol_version, new_version_from_build))
            node1.set_log_level("DEBUG" if DEBUG else "INFO")
            node1.set_configuration_options(values={'internode_compression': 'none'})
            node1.start(wait_for_binary_proto=True)

        if UPGRADE_MODE == "all":
            node2.set_install_dir(**install_kwargs)
            # this is a bandaid; after refactoring, upgrades should account for protocol version
            new_version_from_build = get_version_from_build(node1.get_install_dir())
            if (new_version_from_build >= '3' and self.protocol_version is not None and self.protocol_version < 3):
                self.skip('Protocol version {} incompatible '
                          'with Cassandra version {}'.format(self.protocol_version, new_version_from_build))
            node2.set_log_level("DEBUG" if DEBUG else "INFO")
            node2.set_configuration_options(values={'internode_compression': 'none'})
            node2.start(wait_for_binary_proto=True)

        sessions = []
        if QUERY_UPGRADED:
            session = self.patient_exclusive_cql_connection(node1, protocol_version=self.protocol_version)
            session.set_keyspace('ks')
            sessions.append((True, session))
        if QUERY_OLD:
            # open a second session with the node on the old version
            session = self.patient_exclusive_cql_connection(node2, protocol_version=self.protocol_version)
            session.set_keyspace('ks')
            sessions.append((False, session))

        if self.CL:
            for is_upgraded, session in sessions:
                session.default_consistency_level = self.CL

        return sessions

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
        self.assertLessEqual(len(node_versions), 2)
        return max(node_versions) if is_upgraded else min(node_versions)

    def tearDown(self):
        # Ignore errors before upgrade on Windows
        # We ignore errors from 2.1, because windows 2.1
        # support is only beta. There are frequent log errors,
        # related to filesystem interactions that are a direct result
        # of the lack of full functionality on 2.1 Windows, and we dont
        # want these to pollute our results.
        if is_win() and UPGRADE_MODE != "all" and self.cluster.version() <= '2.2':
            self.cluster.nodelist()[1].mark_log_for_errors()
        super(UpgradeTester, self).tearDown()
