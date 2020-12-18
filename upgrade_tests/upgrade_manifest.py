import logging

from collections import namedtuple

from dtest import RUN_STATIC_UPGRADE_MATRIX
from conftest import cassandra_dir_and_version

import ccmlib.repository
from ccmlib.common import get_version_from_build

from enum import Enum

logger = logging.getLogger(__name__)

# UpgradePath's contain data about upgrade paths we wish to test
# They also contain VersionMeta's for each version the path is testing
UpgradePath = namedtuple('UpgradePath', ('name', 'starting_version', 'upgrade_version', 'starting_meta', 'upgrade_meta'))

VERSION_FAMILY = None
CONFIG = None


def is_same_family_current_to_indev(origin, destination):
    """
    Within a version family it is useful to test that a prior release can upgrade to the indev version
    """
    return origin.family == destination.family and origin.variant == "current" and destination.variant == "indev"


class VersionSelectionStrategies(Enum):
    """
    Allow all versions
    """
    ALL=(lambda origin, destination: True,)
    """
    Test upgrading from indev -> indev, current -> current across versions, and current -> indev within a version
    """
    BOTH=(lambda origin, destination: (origin.variant == destination.variant) or is_same_family_current_to_indev(origin,destination),)
    """
    Exclusively test in development branches so your bug fixes show up
    """
    INDEV=(lambda origin, destination: origin.variant == 'indev' and destination.variant == 'indev' or is_same_family_current_to_indev(origin, destination),)
    """
    Test upgrading from releases to the latest release as well as from the current release to the indev tip 
    within the same version.
    """
    RELEASES=(lambda origin, destination: not VersionSelectionStrategies.INDEV.value[0](origin, destination) or is_same_family_current_to_indev(origin, destination),)


def set_config(config):
    global CONFIG
    CONFIG = config
    set_version_family()


def set_version_family():
    """
    Detects the version family (line) using dtest.py:CASSANDRA_VERSION_FROM_BUILD
    """
    # todo CASSANDRA-14421
    # current_version = CASSANDRA_VERSION_FROM_BUILD
    # There are times when we want to know the C* version we're testing against
    # before we call Tester.setUp. In the general case, we can't know that -- the
    # test method could use any version it wants for self.cluster. However, we can
    # get the version from build.xml in the C* repository specified by
    # CASSANDRA_VERSION or CASSANDRA_DIR. This should use the same resolution
    # strategy as the actual checkout code in Tester.setUp; if it does not, that is
    # a bug.
    cassandra_version_slug = CONFIG.getoption("--cassandra-version")
    cassandra_dir = CONFIG.getoption("--cassandra-dir") or CONFIG.getini("cassandra_dir")
    # Prefer CASSANDRA_VERSION if it's set in the environment. If not, use CASSANDRA_DIR
    if cassandra_version_slug:
        # fetch but don't build the specified C* version
        ccm_repo_cache_dir, _ = ccmlib.repository.setup(cassandra_version_slug)
        current_version = get_version_from_build(ccm_repo_cache_dir)
    else:
        current_version = get_version_from_build(cassandra_dir)

    if current_version.vstring.startswith('2.0'):
        version_family = '2.0.x'
    elif current_version.vstring.startswith('2.1'):
        version_family = '2.1.x'
    elif current_version.vstring.startswith('2.2'):
        version_family = '2.2.x'
    elif current_version.vstring.startswith('3.0'):
        version_family = '3.0.x'
    elif '3.1' <= current_version < '4.0':
        version_family = '3.x'
    elif '4.0' <= current_version < '4.1':
        version_family = 'trunk'
    else:
        # when this occurs, it's time to update this manifest a bit!
        raise RuntimeError("4.1+ not yet supported on upgrade tests!")

    global VERSION_FAMILY
    VERSION_FAMILY = version_family
    logger.info("Setting version family to %s\n" % VERSION_FAMILY)


class VersionMeta(namedtuple('_VersionMeta', ('name', 'family', 'variant', 'version', 'min_proto_v', 'max_proto_v', 'java_versions'))):
    """
    VersionMeta's are namedtuples that capture data about version family, protocols supported, and current version identifiers
    they must have a 'variant' value of 'current' or 'indev', where 'current' means most recent released version,
    'indev' means where changing code is found.
    """
    @property
    def java_version(self):
        return max(self.java_versions)

    @property
    def matches_current_env_version_family(self):
        """
        Returns boolean indicating whether this meta matches the current version family of the environment.

        e.g. Returns true if the current env version family is 3.x and the meta's family attribute is a match.
        """
        return self.family == VERSION_FAMILY

    def clone_with_local_env_version(self):
        """
        Returns a new object cloned from this one, with the version replaced with the local env version.
        """
        cassandra_dir, cassandra_version = cassandra_dir_and_version(CONFIG)
        if cassandra_version:
            return self._replace(version=cassandra_version)
        return self._replace(version="clone:{}".format(cassandra_dir))


indev_2_1_x = VersionMeta(name='indev_2_1_x', family='2.1', variant='indev', version='github:apache/cassandra-2.1', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))
current_2_1_x = VersionMeta(name='current_2_1_x', family='2.1', variant='current', version='2.1.20', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))

indev_2_2_x = VersionMeta(name='indev_2_2_x', family='2.2', variant='indev', version='github:apache/cassandra-2.2', min_proto_v=1, max_proto_v=4, java_versions=(7, 8))
current_2_2_x = VersionMeta(name='current_2_2_x', family='2.2', variant='current', version='2.2.13', min_proto_v=1, max_proto_v=4, java_versions=(7, 8))

indev_3_0_x = VersionMeta(name='indev_3_0_x', family='3.0', variant='indev', version='github:apache/cassandra-3.0', min_proto_v=3, max_proto_v=4, java_versions=(8,))
current_3_0_x = VersionMeta(name='current_3_0_x', family='3.0', variant='current', version='3.0.23', min_proto_v=3, max_proto_v=4, java_versions=(8,))

indev_3_11_x = VersionMeta(name='indev_3_11_x', family='3.11', variant='indev', version='github:apache/cassandra-3.11', min_proto_v=3, max_proto_v=4, java_versions=(8,))
current_3_11_x = VersionMeta(name='current_3_11_x', family='3.11', variant='current', version='3.11.9', min_proto_v=3, max_proto_v=4, java_versions=(8,))

indev_trunk = VersionMeta(name='indev_trunk', family='trunk', variant='indev', version='github:apache/trunk', min_proto_v=4, max_proto_v=5, java_versions=(8,))


# MANIFEST maps a VersionMeta representing a line/variant to a list of other VersionMeta's representing supported upgrades
# Note on versions: 2.0 must upgrade to 2.1. Once at 2.1 or newer, upgrade is supported to any later version, including trunk (for now).
# "supported upgrade" means a few basic things, for an upgrade of version 'A' to higher version 'B':
#   1) The cluster will function in a mixed-version state, with some nodes on version A and some nodes on version B. Schema modifications are not supported on mixed-version clusters.
#   2) Features exclusive to version B may not work until all nodes are running version B.
#   3) Nodes upgraded to version B can read data stored by the predecessor version A, and from a data standpoint will function the same as if they always ran version B.
#   4) If a new sstable format is present in version B, writes will occur in that format after upgrade. Running sstableupgrade on version B will proactively convert version A sstables to version B.
MANIFEST = {
    indev_2_1_x: [indev_2_2_x, current_2_2_x, indev_3_0_x, current_3_0_x, indev_3_11_x, current_3_11_x],
    current_2_1_x: [indev_2_1_x, indev_2_2_x, current_2_2_x, indev_3_0_x, current_3_0_x, indev_3_11_x, current_3_11_x],

    indev_2_2_x: [indev_3_0_x, current_3_0_x, indev_3_11_x, current_3_11_x],
    current_2_2_x: [indev_2_2_x, indev_3_0_x, current_3_0_x, indev_3_11_x, current_3_11_x],

    indev_3_0_x: [indev_3_11_x, current_3_11_x, indev_trunk],
    current_3_0_x: [indev_3_0_x, indev_3_11_x, current_3_11_x, indev_trunk],

    current_3_11_x: [indev_3_11_x, indev_trunk],
    indev_3_11_x: [indev_trunk]
}

# Local env and custom path testing instructions. Use these steps to REPLACE the normal upgrade test cases with your own.
# 1) Add a VersionMeta for each version you wish to test (see examples below). Update the name, family, version, and protocol restrictions as needed. Use a unique name for each VersionMeta.
# 2) Update OVERRIDE_MANIFEST (see example below).
# 3) If you want to test using local code, set the version attribute using local slugs in the format 'local:/path/to/cassandra/:branch_name'
# 4) Run the tests!
#      To run all, use 'nosetests -v upgrade_tests/'. To run specific tests, use 'nosetests -vs --collect-only' to preview the test names, then run nosetests using the desired test name.
#      Note that nosetests outputs test names in a format that needs to be tweaked a bit before they will run from the command line.
custom_1 = VersionMeta(name='custom_branch_1', family='2.1.x', variant='indev', version='local:some_branch', min_proto_v=3, max_proto_v=4, java_versions=(7, 8))
custom_2 = VersionMeta(name='custom_branch_2', family='2.2.x', variant='indev', version='git:trunk', min_proto_v=3, max_proto_v=4, java_versions=(7, 8))
custom_3 = VersionMeta(name='custom_branch_3', family='3.0.x', variant='indev', version='git:cassandra-3.5', min_proto_v=3, max_proto_v=4, java_versions=(7, 8))
custom_4 = VersionMeta(name='custom_branch_4', family='3.x', variant='indev', version='git:cassandra-3.6', min_proto_v=3, max_proto_v=4, java_versions=(7, 8))
OVERRIDE_MANIFEST = {
    # EXAMPLE:
    # custom_1: [custom_2, custom_3],  # creates a test of custom_1 -> custom_2, and another test from custom_1 -> custom_3
    # custom_3: [custom_4]             # creates a test of custom_3 -> custom_4
}


def _have_common_proto(origin_meta, destination_meta):
    """
    Takes two VersionMeta objects, in order of test from start version to next version.
    Returns a boolean indicating if the given VersionMetas have a common protocol version.
    """
    return origin_meta.max_proto_v >= destination_meta.min_proto_v

def build_upgrade_pairs():
    """
    Using the manifest (above), builds a set of valid upgrades, according to current testing practices.

    Returns a list of UpgradePath's.
    """
    valid_upgrade_pairs = []
    manifest = OVERRIDE_MANIFEST or MANIFEST

    configured_strategy = CONFIG.getoption("--upgrade-version-selection").upper()
    version_select_strategy = VersionSelectionStrategies[configured_strategy].value[0]
    filter_for_current_family = CONFIG.getoption("--upgrade-target-version-only")

    for origin_meta, destination_metas in list(manifest.items()):
        for destination_meta in destination_metas:
            if not version_select_strategy(origin_meta, destination_meta):
                continue

            if not (origin_meta and destination_meta):  # None means we don't care about that version, which means we don't care about iterations involving it either
                logger.debug("skipping class creation as a version is undefined (this is normal), versions: {} and {}".format(origin_meta, destination_meta))
                continue

            if not _have_common_proto(origin_meta, destination_meta):
                logger.debug("skipping class creation, no compatible protocol version between {} and {}".format(origin_meta.name, destination_meta.name))
                continue

            # if either origin or destination match version, then do the test
            # the assumption is that a change in 3.0 could break upgrades to trunk, so include those tests as well
            if filter_for_current_family and not origin_meta.matches_current_env_version_family and not destination_meta.matches_current_env_version_family:
                logger.debug("skipping class creation, origin version {} and destination version {} do not match target version {}, and --upgrade-target-version-only was set".format(origin_meta.name, destination_meta.name, VERSION_FAMILY))
                continue

            path_name = 'Upgrade_' + origin_meta.name + '_To_' + destination_meta.name

            if not (RUN_STATIC_UPGRADE_MATRIX or OVERRIDE_MANIFEST):
                if destination_meta.matches_current_env_version_family:
                    # looks like this test should actually run in the current env, so let's set the final version to match the env exactly
                    oldmeta = destination_meta
                    newmeta = destination_meta.clone_with_local_env_version()
                    logger.debug("{} appears applicable to current env. Overriding final test version from {} to {}".format(path_name, oldmeta.version, newmeta.version))
                    destination_meta = newmeta

            valid_upgrade_pairs.append(
                UpgradePath(
                    name=path_name,
                    starting_version=origin_meta.version,
                    upgrade_version=destination_meta.version,
                    starting_meta=origin_meta,
                    upgrade_meta=destination_meta
                )
            )

    return valid_upgrade_pairs
