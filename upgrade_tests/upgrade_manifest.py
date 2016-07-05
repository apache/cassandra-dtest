from collections import namedtuple

from dtest import debug

# UpgradePath's contain data about upgrade paths we wish to test
# They also contain VersionMeta's for each version the path is testing
UpgradePath = namedtuple('UpgradePath', ('name', 'starting_version', 'upgrade_version', 'starting_meta', 'upgrade_meta'))


class VersionMeta(namedtuple('_VersionMeta', ('name', 'variant', 'version', 'min_proto_v', 'max_proto_v', 'java_versions'))):
    """
    VersionMeta's are namedtuples that capture data about version lines, protocols supported, and current version identifiers
    they must have a 'variant' value of 'current', 'indev', or 'next', where 'current' means most recent released version,
    'indev' means where changing code is found, 'next' means a tentative tag.
    """
    @property
    def java_version(self):
        return max(self.java_versions)


indev_2_0_x = None  # None if release not likely
current_2_0_x = VersionMeta(name='current_2_0_x', variant='current', version='2.0.17', min_proto_v=1, max_proto_v=2, java_versions=(7,))
next_2_0_x = None  # None if not yet tagged

indev_2_1_x = VersionMeta(name='indev_2_1_x', variant='indev', version='git:cassandra-2.1', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))
current_2_1_x = VersionMeta(name='current_2_1_x', variant='current', version='2.1.15', min_proto_v=1, max_proto_v=3, java_versions=(7, 8))
next_2_1_x = None  # None if not yet tagged

indev_2_2_x = VersionMeta(name='indev_2_2_x', variant='indev', version='git:cassandra-2.2', min_proto_v=1, max_proto_v=4, java_versions=(7, 8))
current_2_2_x = VersionMeta(name='current_2_2_x', variant='current', version='2.2.7', min_proto_v=1, max_proto_v=4, java_versions=(7, 8))
next_2_2_x = None  # None if not yet tagged

indev_3_0_x = VersionMeta(name='indev_3_0_x', variant='indev', version='git:cassandra-3.0', min_proto_v=3, max_proto_v=4, java_versions=(8,))
current_3_0_x = VersionMeta(name='current_3_0_x', variant='current', version='3.0.8', min_proto_v=3, max_proto_v=4, java_versions=(8,))
next_3_0_x = None  # None if not yet tagged

indev_3_x = VersionMeta(name='indev_3_x', variant='indev', version='git:cassandra-3.7', min_proto_v=3, max_proto_v=4, java_versions=(8,))
current_3_x = VersionMeta(name='current_3_x', variant='current', version='3.7', min_proto_v=3, max_proto_v=4, java_versions=(8,))
next_3_x = None  # None if not yet tagged

head_trunk = VersionMeta(name='head_trunk', variant='indev', version='git:trunk', min_proto_v=3, max_proto_v=4, java_versions=(8,))

# MANIFEST maps a VersionMeta representing a line/variant to a list of other VersionMeta's representing supported upgrades
# Note on versions: 2.0 must upgrade to 2.1. Once at 2.1 or newer, upgrade is supported to any later version, including trunk (for now).
# "supported upgrade" means a few basic things, for an upgrade of version 'A' to higher version 'B':
#   1) The cluster will function in a mixed-version state, with some nodes on version A and some nodes on version B. Schema modifications are not supported on mixed-version clusters.
#   2) Features exclusive to version B may not work until all nodes are running version B.
#   3) Nodes upgraded to version B can read data stored by the predecessor version A, and from a data standpoint will function the same as if they always ran version B.
#   4) If a new sstable format is present in version B, writes will occur in that format after upgrade. Running sstableupgrade on version B will proactively convert version A sstables to version B.
MANIFEST = {
    indev_2_0_x: [indev_2_1_x, current_2_1_x, next_2_1_x],
    current_2_0_x: [indev_2_0_x, indev_2_1_x, current_2_1_x, next_2_1_x],
    next_2_0_x: [indev_2_1_x, current_2_1_x, next_2_1_x],

    indev_2_1_x: [indev_2_2_x, current_2_2_x, next_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    current_2_1_x: [indev_2_1_x, indev_2_2_x, current_2_2_x, next_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    next_2_1_x: [indev_2_2_x, current_2_2_x, next_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],

    indev_2_2_x: [indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    current_2_2_x: [indev_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    next_2_2_x: [indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],

    indev_3_0_x: [indev_3_x, current_3_x, next_3_x, head_trunk],
    current_3_0_x: [indev_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    next_3_0_x: [indev_3_x, current_3_x, next_3_x, head_trunk],

    indev_3_x: [head_trunk],
    current_3_x: [indev_3_x, head_trunk],
    next_3_x: [head_trunk],
}

# Local env and custom path testing instructions. Use these steps to REPLACE the normal upgrade test cases with your own.
# 1) Add a VersionMeta for each version you wish to test (see examples below). Update the name, version, and protocol restrictions as needed. Use a unique name for each VersionMeta.
# 2) Update OVERRIDE_MANIFEST (see example below).
# 3) If using ccm local: slugs, make sure you have LOCAL_GIT_REPO defined in your env. This is the path to your git repo.
# 4) Run the tests!
#      export UPGRADE_TEST_RUN=true
#      To run all, use 'nosetests -v upgrade_tests/'. To run specific tests, use 'nosetests -vs --collect-only' to preview the test names, then run nosetests using the desired test name.
custom_1 = VersionMeta(name='custom_branch_1', variant='indev', version='local:some_branch', min_proto_v=3, max_proto_v=4, java_versions=(7, 8))
custom_2 = VersionMeta(name='custom_branch_2', variant='indev', version='git:trunk', min_proto_v=3, max_proto_v=4, java_versions=(7, 8))
custom_3 = VersionMeta(name='custom_branch_3', variant='indev', version='git:cassandra-3.5', min_proto_v=3, max_proto_v=4, java_versions=(7, 8))
custom_4 = VersionMeta(name='custom_branch_4', variant='indev', version='git:cassandra-3.6', min_proto_v=3, max_proto_v=4, java_versions=(7, 8))
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
      current -> next (aka: released -> proposed release point)
      next -> in-dev (aka: proposed release point -> branch)
    """
    return (bool(OVERRIDE_MANIFEST) or  # if we're overriding the test manifest, we don't want to filter anything out
            (origin_meta.variant == 'current' and destination_meta.variant == 'indev') or
            (origin_meta.variant == 'current' and destination_meta.variant == 'next') or
            (origin_meta.variant == 'next' and destination_meta.variant == 'indev'))


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

            valid_upgrade_pairs.append(
                UpgradePath(
                    name='Upgrade_' + origin_meta.name + '_To_' + destination_meta.name,
                    starting_version=origin_meta.version,
                    upgrade_version=destination_meta.version,
                    starting_meta=origin_meta,
                    upgrade_meta=destination_meta
                )
            )

    return valid_upgrade_pairs
