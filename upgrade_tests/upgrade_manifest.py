from collections import namedtuple

from dtest import debug

# UpgradePath's contain data about upgrade paths we wish to test
# They also contain VersionMeta's for each version the path is testing
UpgradePath = namedtuple('UpgradePath', ('name', 'starting_version', 'upgrade_version', 'starting_meta', 'upgrade_meta'))

# VersionMeta's capture data about version lines, protocols supported, and current version identifiers
# they should have a 'variant' value of 'current', 'indev', or 'next':
#    'current' means most recent released version, 'indev' means where changing code is found, 'next' means a tentative tag
VersionMeta = namedtuple('VersionMeta', ('name', 'variant', 'version', 'min_proto_v', 'max_proto_v'))

indev_2_0_x = None  # None if release not likely
current_2_0_x = VersionMeta(name='current_2_0_x', variant='current', version='2.0.17', min_proto_v=1, max_proto_v=2)
next_2_0_x = None  # None if not yet tagged

indev_2_1_x = VersionMeta(name='indev_2_1_x', variant='indev', version='git:cassandra-2.1', min_proto_v=1, max_proto_v=3)
current_2_1_x = VersionMeta(name='current_2_1_x', variant='current', version='2.1.14', min_proto_v=1, max_proto_v=3)
next_2_1_x = None  # None if not yet tagged

indev_2_2_x = VersionMeta(name='indev_2_2_x', variant='indev', version='git:cassandra-2.2', min_proto_v=1, max_proto_v=4)
current_2_2_x = VersionMeta(name='current_2_2_x', variant='current', version='2.2.6', min_proto_v=1, max_proto_v=4)
next_2_2_x = None  # None if not yet tagged

indev_3_0_x = VersionMeta(name='indev_3_0_x', variant='indev', version='git:cassandra-3.0', min_proto_v=3, max_proto_v=4)
current_3_0_x = VersionMeta(name='current_3_0_x', variant='current', version='3.0.5', min_proto_v=3, max_proto_v=4)
next_3_0_x = None  # None if not yet tagged

indev_3_x = VersionMeta(name='indev_3_x', variant='indev', version='git:cassandra-3.7', min_proto_v=3, max_proto_v=4)
current_3_x = VersionMeta(name='current_3_x', variant='current', version='3.5', min_proto_v=3, max_proto_v=4)
next_3_x = None  # None if not yet tagged

head_trunk = VersionMeta(name='head_trunk', variant='indev', version='git:trunk', min_proto_v=3, max_proto_v=4)


# maps an VersionMeta representing a line/variant to a list of other VersionMeta's representing supported upgrades
MANIFEST = {
    # commented out until we have a solution for specifying java versions in upgrade tests
    # indev_2_0_x:                [indev_2_1_x, current_2_1_x, next_2_1_x],
    # current_2_0_x: [indev_2_0_x, indev_2_1_x, current_2_1_x, next_2_1_x],
    # next_2_0_x:                 [indev_2_1_x, current_2_1_x, next_2_1_x],

    indev_2_1_x:                [indev_2_2_x, current_2_2_x, next_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    current_2_1_x: [indev_2_1_x, indev_2_2_x, current_2_2_x, next_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    next_2_1_x:                 [indev_2_2_x, current_2_2_x, next_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],

    indev_2_2_x:                [indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    current_2_2_x: [indev_2_2_x, indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    next_2_2_x:                 [indev_3_0_x, current_3_0_x, next_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],

    indev_3_0_x:                [indev_3_x, current_3_x, next_3_x, head_trunk],
    current_3_0_x: [indev_3_0_x, indev_3_x, current_3_x, next_3_x, head_trunk],
    next_3_0_x:                 [indev_3_x, current_3_x, next_3_x, head_trunk],

    indev_3_x:              [head_trunk],
    current_3_x: [indev_3_x, head_trunk],
    next_3_x:               [head_trunk],
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
    return (origin_meta.variant == 'current' and destination_meta.variant == 'indev')\
        or (origin_meta.variant == 'current' and destination_meta.variant == 'next')\
        or (origin_meta.variant == 'next' and destination_meta.variant == 'indev')


def build_upgrade_pairs():
    """
    Using the manifest (above), builds a set of valid upgrades, according to current testing practices.

    Returns a list of UpgradePath's.
    """
    valid_upgrade_pairs = []
    for origin_meta, destination_metas in MANIFEST.items():
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
