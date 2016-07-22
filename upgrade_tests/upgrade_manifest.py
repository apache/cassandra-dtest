from collections import namedtuple
from distutils.version import LooseVersion

from dtest import (CASSANDRA_GITREF, CASSANDRA_VERSION_FROM_BUILD,
                   RUN_STATIC_UPGRADE_MATRIX, debug)

# UpgradePath's contain data about upgrade paths we wish to test
# They also contain VersionMeta's for each version the path is testing
UpgradePath = namedtuple('UpgradePath', ('name', 'starting_version', 'upgrade_version', 'starting_meta', 'upgrade_meta'))


def _get_version_family():
    """
    Detects the version family (line) using dtest.py:CASSANDRA_VERSION_FROM_BUILD
    """
    current_version = LooseVersion(CASSANDRA_VERSION_FROM_BUILD)

    version_family = 'unknown'
    if current_version.vstring.startswith('2.0'):
        version_family = '2.0.x'
    elif current_version.vstring.startswith('2.1'):
        version_family = '2.1.x'
    elif current_version.vstring.startswith('2.2'):
        version_family = '2.2.x'
    elif current_version.vstring.startswith('3.0'):
        version_family = '3.0.x'
    elif current_version > '3.0':
        version_family = '3.x'
    elif current_version >= '4.0':
        # when this occurs, it's time to update this manifest a bit!
        raise RuntimeError("4.0 not yet supported on upgrade tests!")

    return version_family


VERSION_FAMILY = _get_version_family()


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
        return self._replace(version=CASSANDRA_GITREF or CASSANDRA_VERSION_FROM_BUILD)


indev_2_0_x = None  # None if release not likely
current_2_0_x = VersionMeta(name='current_2_0_x', family='2.0.x', variant='current', version='2.0.17', min_proto_v=1, max_proto_v=2, java_versions=(7,))

indev_2_1_x = VersionMeta(name='indev_2_1_x', family='2.1.x', variant='indev', version='git:cassandra-2.1', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))
current_2_1_x = VersionMeta(name='current_2_1_x', family='2.1.x', variant='current', version='2.1.15', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))

indev_2_2_x = VersionMeta(name='indev_2_2_x', family='2.2.x', variant='indev', version='git:cassandra-2.2', min_proto_v=1, max_proto_v=4, java_versions=(7, 8))
current_2_2_x = VersionMeta(name='current_2_2_x', family='2.2.x', variant='current', version='2.2.7', min_proto_v=1, max_proto_v=4, java_versions=(7, 8))

indev_3_0_x = VersionMeta(name='indev_3_0_x', family='3.0.x', variant='indev', version='git:cassandra-3.0', min_proto_v=3, max_proto_v=4, java_versions=(8,))
current_3_0_x = VersionMeta(name='current_3_0_x', family='3.0.x', variant='current', version='3.0.8', min_proto_v=3, max_proto_v=4, java_versions=(8,))

indev_3_x = VersionMeta(name='indev_3_x', family='3.x', variant='indev', version='git:trunk', min_proto_v=3, max_proto_v=4, java_versions=(8,))
current_3_x = VersionMeta(name='current_3_x', family='3.x', variant='current', version='3.7', min_proto_v=3, max_proto_v=4, java_versions=(8,))


# MANIFEST maps a VersionMeta representing a line/variant to a list of other VersionMeta's representing supported upgrades
# Note on versions: 2.0 must upgrade to 2.1. Once at 2.1 or newer, upgrade is supported to any later version, including trunk (for now).
# "supported upgrade" means a few basic things, for an upgrade of version 'A' to higher version 'B':
#   1) The cluster will function in a mixed-version state, with some nodes on version A and some nodes on version B. Schema modifications are not supported on mixed-version clusters.
#   2) Features exclusive to version B may not work until all nodes are running version B.
#   3) Nodes upgraded to version B can read data stored by the predecessor version A, and from a data standpoint will function the same as if they always ran version B.
#   4) If a new sstable format is present in version B, writes will occur in that format after upgrade. Running sstableupgrade on version B will proactively convert version A sstables to version B.
MANIFEST = {
    indev_2_0_x: [indev_2_1_x, current_2_1_x],
    current_2_0_x: [indev_2_0_x, indev_2_1_x, current_2_1_x],

    indev_2_1_x: [indev_2_2_x, current_2_2_x, indev_3_0_x, current_3_0_x, indev_3_x, current_3_x],
    current_2_1_x: [indev_2_1_x, indev_2_2_x, current_2_2_x, indev_3_0_x, current_3_0_x, indev_3_x, current_3_x],

    indev_2_2_x: [indev_3_0_x, current_3_0_x, indev_3_x, current_3_x],
    current_2_2_x: [indev_2_2_x, indev_3_0_x, current_3_0_x, indev_3_x, current_3_x],

    indev_3_0_x: [indev_3_x, current_3_x],
    current_3_0_x: [indev_3_0_x, indev_3_x, current_3_x],

    current_3_x: [indev_3_x],
}

# Local env and custom path testing instructions. Use these steps to REPLACE the normal upgrade test cases with your own.
# 1) Add a VersionMeta for each version you wish to test (see examples below). Update the name, family, version, and protocol restrictions as needed. Use a unique name for each VersionMeta.
# 2) Update OVERRIDE_MANIFEST (see example below).
# 3) If using ccm local: slugs, make sure you have LOCAL_GIT_REPO defined in your env. This is the path to your git repo.
# 4) Run the tests!
#      To run all, use 'nosetests -v upgrade_tests/'. To run specific tests, use 'nosetests -vs --collect-only' to preview the test names, then run nosetests using the desired test name.
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


def _is_targeted_variant_combo(origin_meta, destination_meta):
    """
    Takes two VersionMeta objects, in order of test from start version to next version.
    Returns a boolean indicating if this is a test pair we care about.

    for now we only test upgrades of these types:
      current -> in-dev (aka: released -> branch)
    """
    # if we're overriding the test manifest, we don't want to filter anything out
    if OVERRIDE_MANIFEST:
        return True

    # is this an upgrade variant combination we care about?
    return (origin_meta.variant == 'current' and destination_meta.variant == 'indev')


def build_upgrade_pairs():
    """
    Using the manifest (above), builds a set of valid upgrades, according to current testing practices.

    Returns a list of UpgradePath's.
    """
    valid_upgrade_pairs = []
    manifest = OVERRIDE_MANIFEST or MANIFEST

    for origin_meta, destination_metas in manifest.items():
        for destination_meta in destination_metas:
            if not (origin_meta and destination_meta):  # None means we don't care about that version, which means we don't care about iterations involving it either
                debug("skipping class creation as a version is undefined (this is normal), versions: {} and {}".format(origin_meta, destination_meta))
                continue

            if not _is_targeted_variant_combo(origin_meta, destination_meta):
                debug("skipping class creation, no testing of '{}' to '{}' (for {} upgrade to {})".format(origin_meta.variant, destination_meta.variant, origin_meta.name, destination_meta.name))
                continue

            if not _have_common_proto(origin_meta, destination_meta):
                debug("skipping class creation, no compatible protocol version between {} and {}".format(origin_meta.name, destination_meta.name))
                continue

            path_name = 'Upgrade_' + origin_meta.name + '_To_' + destination_meta.name

            if not (RUN_STATIC_UPGRADE_MATRIX or OVERRIDE_MANIFEST):
                if destination_meta.matches_current_env_version_family:
                    # looks like this test should actually run in the current env, so let's set the final version to match the env exactly
                    oldmeta = destination_meta
                    newmeta = destination_meta.clone_with_local_env_version()
                    debug("{} appears applicable to current env. Overriding final test version from {} to {}".format(path_name, oldmeta.version, newmeta.version))
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
