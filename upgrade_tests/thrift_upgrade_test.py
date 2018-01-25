import itertools
import pytest
import logging

from cassandra.query import dict_factory

from dtest import RUN_STATIC_UPGRADE_MATRIX, Tester
from thrift_bindings.thrift010 import Cassandra
from thrift_bindings.thrift010.Cassandra import (Column, ColumnDef,
                                           ColumnParent, ConsistencyLevel,
                                           SlicePredicate, SliceRange)
from thrift_test import _i64, get_thrift_client
from tools.assertions import assert_length_equal
from .upgrade_base import UpgradeTester
from .upgrade_manifest import build_upgrade_pairs

since = pytest.mark.since
logger = logging.getLogger(__name__)


def _create_dense_super_cf(name):
    return Cassandra.CfDef('ks', name, column_type='Super',
                           key_validation_class='AsciiType',        # pk
                           comparator_type='AsciiType',             # ck
                           default_validation_class='AsciiType',    # SC value
                           subcomparator_type='LongType')           # SC key


def _create_sparse_super_cf(name):
    cd1 = ColumnDef('col1', 'LongType', None, None)
    cd2 = ColumnDef('col2', 'LongType', None, None)
    return Cassandra.CfDef('ks', name, column_type='Super',
                           column_metadata=[cd1, cd2],
                           key_validation_class='AsciiType',
                           comparator_type='AsciiType',
                           subcomparator_type='AsciiType')


def _validate_sparse_cql(cursor, cf='sparse_super_1', column1='column1', col1='col1', col2='col2', key='key'):
    cursor.execute('use ks')

    assert (list(cursor.execute("SELECT * FROM {}".format(cf))) ==
                 [{key: 'k1', column1: 'key1', col1: 200, col2: 300},
                  {key: 'k1', column1: 'key2', col1: 200, col2: 300},
                  {key: 'k2', column1: 'key1', col1: 200, col2: 300},
                  {key: 'k2', column1: 'key2', col1: 200, col2: 300}])

    assert (list(cursor.execute("SELECT * FROM {} WHERE {} = 'k1'".format(cf, key))) ==
                 [{key: 'k1', column1: 'key1', col1: 200, col2: 300},
                  {key: 'k1', column1: 'key2', col1: 200, col2: 300}])

    assert (list(cursor.execute("SELECT * FROM {} WHERE {} = 'k2' AND {} = 'key1'".format(cf, key, column1))) ==
                 [{key: 'k2', column1: 'key1', col1: 200, col2: 300}])


def _validate_sparse_thrift(client, cf='sparse_super_1'):
    client.transport.open()
    client.set_keyspace('ks')
    result = client.get_slice('k1', ColumnParent(cf), SlicePredicate(slice_range=SliceRange('', '', False, 5)), ConsistencyLevel.ONE)
    assert_length_equal(result, 2)
    assert result[0].super_column.name == 'key1'
    assert result[1].super_column.name == 'key2'

    for cosc in result:
        assert cosc.super_column.columns[0].name == 'col1'
        assert cosc.super_column.columns[0].value == _i64(200)
        assert cosc.super_column.columns[1].name == 'col2'
        assert cosc.super_column.columns[1].value == _i64(300)
        assert cosc.super_column.columns[2].name == 'value1'
        assert cosc.super_column.columns[2].value == _i64(100)


def _validate_dense_cql(cursor, cf='dense_super_1', key='key', column1='column1', column2='column2', value='value'):
    cursor.execute('use ks')

    assert (list(cursor.execute("SELECT * FROM {}".format(cf))) ==
                 [{key: 'k1', column1: 'key1', column2: 100, value: 'value1'},
                  {key: 'k1', column1: 'key2', column2: 100, value: 'value1'},
                  {key: 'k2', column1: 'key1', column2: 200, value: 'value2'},
                  {key: 'k2', column1: 'key2', column2: 200, value: 'value2'}])

    assert (list(cursor.execute("SELECT * FROM {} WHERE {} = 'k1'".format(cf, key))) ==
                 [{key: 'k1', column1: 'key1', column2: 100, value: 'value1'},
                  {key: 'k1', column1: 'key2', column2: 100, value: 'value1'}])

    assert (list(cursor.execute("SELECT * FROM {} WHERE {} = 'k1' AND {} = 'key1'".format(cf, key, column1))) ==
                 [{key: 'k1', column1: 'key1', column2: 100, value: 'value1'}])

    assert (list(cursor.execute("SELECT * FROM {} WHERE {} = 'k1' AND {} = 'key1' AND {} = 100".format(cf, key, column1, column2))) ==
                 [{key: 'k1', column1: 'key1', column2: 100, value: 'value1'}])


def _validate_dense_thrift(client, cf='dense_super_1'):
    client.transport.open()
    client.set_keyspace('ks')
    result = client.get_slice('k1', ColumnParent(cf), SlicePredicate(slice_range=SliceRange('', '', False, 5)), ConsistencyLevel.ONE)
    assert_length_equal(result, 2)
    assert result[0].super_column.name == 'key1'
    assert result[1].super_column.name == 'key2'

    print((result[0]))
    print((result[1]))
    for cosc in result:
        assert cosc.super_column.columns[0].name == _i64(100)
        assert cosc.super_column.columns[0].value == 'value1'


@pytest.mark.upgrade_test
class TestUpgradeSuperColumnsThrough(Tester):
    def upgrade_to_version(self, tag, nodes=None):
        logger.debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            logger.debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            node.set_configuration_options(values={'start_rpc': 'true'})
            logger.debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            logger.debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
            node.nodetool('upgradesstables -a')

    def prepare(self, num_nodes=1, cassandra_version="github:apache/cassandra-2.2"):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version=cassandra_version)

        cluster.populate(num_nodes)
        for node in self.cluster.nodelist():
            node.set_configuration_options(values={'start_rpc': 'true'})

        cluster.start()
        return cluster

    def test_dense_supercolumn_3_0_created(self):
        cluster = self.prepare(cassandra_version='github:apache/cassandra-3.0')
        node = self.cluster.nodelist()[0]
        cursor = self.patient_cql_connection(node, row_factory=dict_factory)

        cursor.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '1' };")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        client.system_add_column_family(_create_dense_super_cf('dense_super_1'))

        for i in range(1, 3):
            client.insert('k1', ColumnParent('dense_super_1', 'key{}'.format(i)), Column(_i64(100), 'value1', 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('dense_super_1', 'key{}'.format(i)), Column(_i64(200), 'value2', 0), ConsistencyLevel.ONE)

        _validate_dense_thrift(client, cf='dense_super_1')

        node.stop()
        self.set_node_to_current_version(node)
        node.set_configuration_options(values={'start_rpc': 'true'})
        node.start()

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)
        client = get_thrift_client(host, port)

        _validate_dense_thrift(client, cf='dense_super_1')
        _validate_dense_cql(cursor, cf='dense_super_1')

    def test_dense_supercolumn(self):
        cluster = self.prepare()
        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")
        cursor = self.patient_cql_connection(node, row_factory=dict_factory)

        cursor.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '1' };")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        client.system_add_column_family(_create_dense_super_cf('dense_super_1'))

        for i in range(1, 3):
            client.insert('k1', ColumnParent('dense_super_1', 'key{}'.format(i)), Column(_i64(100), 'value1', 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('dense_super_1', 'key{}'.format(i)), Column(_i64(200), 'value2', 0), ConsistencyLevel.ONE)

        _validate_dense_thrift(client, cf='dense_super_1')
        _validate_dense_cql(cursor, cf='dense_super_1')

        self.upgrade_to_version('github:apache/cassandra-3.0')

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)
        client = get_thrift_client(host, port)

        _validate_dense_thrift(client, cf='dense_super_1')

        node.stop()
        self.set_node_to_current_version(node)
        node.set_configuration_options(values={'start_rpc': 'true'})
        node.start()

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)
        client = get_thrift_client(host, port)

        _validate_dense_thrift(client, cf='dense_super_1')
        _validate_dense_cql(cursor, cf='dense_super_1')

    def test_sparse_supercolumn(self):
        cluster = self.prepare()
        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")
        cursor = self.patient_cql_connection(node, row_factory=dict_factory)

        cursor.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy','replication_factor': '1' };")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        cf = _create_sparse_super_cf('sparse_super_2')
        client.system_add_column_family(cf)

        for i in range(1, 3):
            client.insert('k1', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("value1", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

            client.insert('k2', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("value2", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

        _validate_sparse_thrift(client, cf='sparse_super_2')
        _validate_sparse_cql(cursor, cf='sparse_super_2')

        self.upgrade_to_version('github:apache/cassandra-3.0')

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)
        client = get_thrift_client(host, port)

        _validate_sparse_thrift(client, cf='sparse_super_2')

        node.stop()
        self.set_node_to_current_version(node)
        node.set_configuration_options(values={'start_rpc': 'true'})
        node.start()

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)
        client = get_thrift_client(host, port)

        _validate_sparse_thrift(client, cf='sparse_super_2')
        _validate_sparse_cql(cursor, cf='sparse_super_2')


@pytest.mark.upgrade_test
@since('2.1', max_version='4.0.0')
class TestThrift(UpgradeTester):
    """
    Verify dense and sparse supercolumn functionality with and without renamed columns
    in 3.X after upgrading from 2.x.

    @jira_ticket CASSANDRA-12373
    """

    def test_dense_supercolumn(self):
        cursor = self.prepare(nodes=2, rf=2, row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        client.system_add_column_family(_create_dense_super_cf('dense_super_1'))

        for i in range(1, 3):
            client.insert('k1', ColumnParent('dense_super_1', 'key{}'.format(i)), Column(_i64(100), 'value1', 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('dense_super_1', 'key{}'.format(i)), Column(_i64(200), 'value2', 0), ConsistencyLevel.ONE)

        _validate_dense_cql(cursor)
        _validate_dense_thrift(client)

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            client = get_thrift_client(host, port)
            _validate_dense_cql(cursor)
            _validate_dense_thrift(client)

    def test_dense_supercolumn_with_renames(self):
        cursor = self.prepare(row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        client.system_add_column_family(_create_dense_super_cf('dense_super_2'))

        for i in range(1, 3):
            client.insert('k1', ColumnParent('dense_super_2', 'key{}'.format(i)), Column(_i64(100), 'value1', 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('dense_super_2', 'key{}'.format(i)), Column(_i64(200), 'value2', 0), ConsistencyLevel.ONE)

        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME key TO renamed_key")
        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME column1 TO renamed_column1")
        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME column2 TO renamed_column2")
        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME value TO renamed_value")

        _validate_dense_cql(cursor, cf='dense_super_2', key='renamed_key', column1='renamed_column1', column2='renamed_column2', value='renamed_value')
        _validate_dense_thrift(client, cf='dense_super_2')

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            client = get_thrift_client(host, port)
            _validate_dense_cql(cursor, cf='dense_super_2', key='renamed_key', column1='renamed_column1', column2='renamed_column2', value='renamed_value')
            _validate_dense_thrift(client, cf='dense_super_2')

    def test_sparse_supercolumn_with_renames(self):
        cursor = self.prepare(row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        cf = _create_sparse_super_cf('sparse_super_1')
        client.system_add_column_family(cf)

        cursor.execute("ALTER TABLE ks.sparse_super_1 RENAME key TO renamed_key")
        cursor.execute("ALTER TABLE ks.sparse_super_1 RENAME column1 TO renamed_column1")

        for i in range(1, 3):
            client.insert('k1', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("value1", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

            client.insert('k2', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("value2", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_1', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

        _validate_sparse_thrift(client)
        _validate_sparse_cql(cursor, column1='renamed_column1', key='renamed_key')

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            client = get_thrift_client(host, port)
            _validate_sparse_cql(cursor, column1='renamed_column1', key='renamed_key')
            _validate_sparse_thrift(client)

    def test_sparse_supercolumn(self):
        cursor = self.prepare(row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        cf = _create_sparse_super_cf('sparse_super_2')
        client.system_add_column_family(cf)

        for i in range(1, 3):
            client.insert('k1', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("value1", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k1', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

            client.insert('k2', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("value2", _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col1", _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k2', ColumnParent('sparse_super_2', 'key{}'.format(i)), Column("col2", _i64(300), 0), ConsistencyLevel.ONE)

        _validate_sparse_thrift(client, cf='sparse_super_2')
        _validate_sparse_cql(cursor, cf='sparse_super_2')

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            client = get_thrift_client(host, port)
            _validate_sparse_thrift(client, cf='sparse_super_2')
            _validate_sparse_cql(cursor, cf='sparse_super_2')


topology_specs = [
    {'NODES': 3,
     'RF': 3,
     'CL': ConsistencyLevel.ALL},
    {'NODES': 2,
     'RF': 1},
]
specs = [dict(s, UPGRADE_PATH=p, __test__=True)
         for s, p in itertools.product(topology_specs, build_upgrade_pairs())]

for spec in specs:
    suffix = 'Nodes{num_nodes}RF{rf}_{pathname}'.format(num_nodes=spec['NODES'],
                                                        rf=spec['RF'],
                                                        pathname=spec['UPGRADE_PATH'].name)
    gen_class_name = TestThrift.__name__ + suffix
    assert gen_class_name not in globals()

    upgrade_applies_to_env = RUN_STATIC_UPGRADE_MATRIX or spec['UPGRADE_PATH'].upgrade_meta.matches_current_env_version_family
    if not upgrade_applies_to_env:
        pytest.mark.skip(reason='test not applicable to env.')
    globals()[gen_class_name] = type(gen_class_name, (TestThrift,), spec)
