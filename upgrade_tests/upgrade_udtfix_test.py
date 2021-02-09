import pytest
import logging

from dtest import RUN_STATIC_UPGRADE_MATRIX, Tester
from distutils.version import LooseVersion
from tools.misc import add_skip
from .upgrade_manifest import build_upgrade_pairs, CASSANDRA_3_0

since = pytest.mark.since
logger = logging.getLogger(__name__)

@pytest.mark.upgrade_test
@since('3.11.6')
class UpgradeUDTFixTest(Tester):
    __test__ = False

    """
    @jira_ticket: CASSANDRA-TBD

    3.0 sstable cannot be read by 3.11 and newer

    OSS C* 3.0 only support frozen UDTs. A frozen type is marked using
    "org.apache.cassandra.db.marshal.FrozenType(...)" using the AbstractType class hierarchy.
    But 3.0 does not write the "FrozenType()" type into the serialization header in the sstable
    metadata. CASSANDRA-7423 added support for non-frozen UDTs) in C* 3.6. Since then, UDTs can
    be either frozen or non-frozen, so it becomes important that the "FrozenType bracket" is
    present in the serialization header, because the serialization order of columns depends on
    its type (fixed length type column first, then variable length type columns, then multi-cell
    (non-frozen) type columns). This means, that C* 3.6 and newer will interpret the originally
    frozen UDT as non-frozen and deserialization fails.
    """

    def test_udtfix_in_sstable(self):
        """
        Verify that the fix to modify the serialization-header in the sstable metadata works for a single node.
        """
        cluster = self.cluster
        cluster.set_install_dir(version=self.UPGRADE_PATH.starting_version)
        cluster.populate(1)
        node1, = cluster.nodelist()
        cluster.start()

        session = self.patient_cql_connection(node1)
        self._schema_and_data(node1, session, "ks", "{'class': 'SimpleStrategy', 'replication_factor': 1}")
        references = self._read_rows(session, "ks")
        session.cluster.shutdown()

        logger.debug("Upgrading from {} to {}".format(self.UPGRADE_PATH.starting_version, self.UPGRADE_PATH.upgrade_version))
        cluster.stop()
        cluster.set_install_dir(version=self.UPGRADE_PATH.upgrade_version)

        log_mark = node1.mark_log()
        cluster.start()
        self._verify_upgrade_log(log_mark, node1)

        session = self.patient_cql_connection(node1)
        check = self._read_rows(session, "ks")
        assert references == check

    def test_udtfix_in_messaging(self):
        """
        Paranoia test for CASSANDRA-TBD, but verify that internode reads are not affected - with the data
        both on the upgraded and non-upgraded node.
        """
        cluster = self.cluster
        cluster.set_install_dir(version=self.UPGRADE_PATH.starting_version)
        cluster.populate([1, 1])
        node1, node2 = cluster.nodelist()
        cluster.start()

        logger.debug("Started 2-DC cluster")

        session = self.patient_cql_connection(node1)
        self._schema_and_data(node1, session, "ks1", "{'class': 'NetworkTopologyStrategy', 'dc1': 1}")
        self._schema_and_data(node1, session, "ks2", "{'class': 'NetworkTopologyStrategy', 'dc2': 1}")
        references11 = self._read_rows(session, "ks1")
        references12 = self._read_rows(session, "ks2")
        session.cluster.shutdown()

        session = self.patient_cql_connection(node2)
        references21 = self._read_rows(session, "ks1")
        references22 = self._read_rows(session, "ks2")
        session.cluster.shutdown()

        assert references11 == references21
        assert references12 == references22

        logger.debug("Upgrading {} from {} to {}".format(node1.name, self.UPGRADE_PATH.starting_version, self.UPGRADE_PATH.upgrade_version))
        node1.stop()
        node1.set_install_dir(version=self.UPGRADE_PATH.upgrade_version)
        cluster.set_configuration_options()  # re-write cassandra-topology.properties (it's overridden by Node.set_install_dir)
        log_mark = node1.mark_log()
        node1.start(wait_for_binary_proto=True)
        self._verify_upgrade_log(log_mark, node1)

        session = self.exclusive_cql_connection(node1)
        check11 = self._read_rows(session, "ks1")
        check12 = self._read_rows(session, "ks2")
        assert references11 == check11
        assert references12 == check12
        session.cluster.shutdown()

        session = self.exclusive_cql_connection(node2)
        check21 = self._read_rows(session, "ks1")
        check22 = self._read_rows(session, "ks2")
        assert references11 == check21
        assert references12 == check22
        session.cluster.shutdown()

    def _schema_and_data(self, node, session, ks, replication):
        logger.debug("Creating schema and writing data (+flush)")
        session.execute("CREATE KEYSPACE {} "
                        "WITH replication = {}".format(ks, replication))
        session.execute("CREATE TYPE {}.user_type (some_field text, some_int int)".format(ks))
        session.execute("CREATE TABLE {}.some_table (id int PRIMARY KEY, "
                        "a_udt_column_name frozen<user_type>, "
                        "col_a int, "
                        "col_c int)".format(ks))
        # NOTE: the names (internal order) of the regular columns matters (for this test)!
        # Regular column order is:
        # 1. fixed length type columns
        # 2. variable length type columns
        # 3. complex/multi-cell type columns
        # The UDT field must not be the last field.
        session.execute("INSERT INTO {}.some_table (id, col_a, a_udt_column_name, col_c) "
                        "VALUES (1, 12, {{some_field:'some', some_int:42}}, 13)".format(ks))
        session.execute("INSERT INTO {}.some_table (id, col_a, a_udt_column_name, col_c) "
                        "VALUES (2, 14, {{some_field:'field', some_int:11}}, 15)".format(ks))
        session.execute("INSERT INTO {}.some_table (id, col_c) "
                        "VALUES (3, 16)".format(ks))
        session.execute("INSERT INTO {}.some_table (id, col_c) "
                        "VALUES (4, 17)".format(ks))
        node.flush()

    def _read_rows(self, session, ks):
        return [list(session.execute("SELECT id, col_a, a_udt_column_name, col_c "
                                     "FROM {}.some_table WHERE id = {}".format(ks, x + 1)))[0] for x in range(4)]

    def _verify_upgrade_log(self, log_mark, node):
        logger.debug("Verifying messages in log file")
        assert 1 == len(node.grep_log(
            'Detected upgrade from .* to .*, fixing UDT type references in sstable metadata serialization-headers',
            from_mark=log_mark))
        assert 1 == len(node.grep_log("Column 'a_udt_column_name' needs to be updated from type "
                                      "'user_type' to 'frozen<user_type>'", from_mark=log_mark))
        assert 1 == len(node.grep_log('Writing new metadata file ', from_mark=log_mark))
        assert 1 == len(node.grep_log('Finished scanning all data directories...', from_mark=log_mark))


for path in build_upgrade_pairs():
    gen_class_name = UpgradeUDTFixTest.__name__ + '_' + path.name
    assert gen_class_name not in globals()
    spec = {'UPGRADE_PATH': path,
            '__test__': True}

    start_family = spec['UPGRADE_PATH'].starting_meta.family
    upgrade_family = spec['UPGRADE_PATH'].upgrade_meta.family
    start_family_applies = start_family == CASSANDRA_3_0
    upgrade_applies_to_env = RUN_STATIC_UPGRADE_MATRIX or start_family_applies
    cls = type(gen_class_name, (UpgradeUDTFixTest,), spec)
    if not upgrade_applies_to_env:
        add_skip(cls, 'test not applicable to env.')
    globals()[gen_class_name] = cls
