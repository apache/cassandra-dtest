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
from tools.assertions import assert_length_equal, assert_lists_of_dicts_equal
from tools.misc import wait_for_agreement, add_skip
from .upgrade_base import UpgradeTester
from .upgrade_manifest import build_upgrade_pairs

since = pytest.mark.since
logger = logging.getLogger(__name__)


def _create_dense_super_cf(thrift, name):
    cfdef = Cassandra.CfDef('ks', name, column_type='Super',
                           key_validation_class='AsciiType',        # pk
                           comparator_type='AsciiType',             # ck
                           default_validation_class='AsciiType',    # SC value
                           subcomparator_type='LongType')           # SC key
    thrift.system_add_column_family(cfdef)
    wait_for_agreement(thrift)


def _create_sparse_super_cf(thrift, name):
    cd1 = ColumnDef('col1'.encode(), 'LongType', None, None)
    cd2 = ColumnDef('col2'.encode(), 'LongType', None, None)
    cfdef = Cassandra.CfDef('ks', name, column_type='Super',
                           column_metadata=[cd1, cd2],
                           key_validation_class='AsciiType',
                           comparator_type='AsciiType',
                           subcomparator_type='AsciiType')
    thrift.system_add_column_family(cfdef)
    wait_for_agreement(thrift)


def unpack(lst):
    result_list = []
    for item_dict in lst:
        normalized_dict = {}
        for key, value in item_dict.items():
            if hasattr(value, "items"):
                assert(key == '')
                for a, b in value.items():
                    normalized_dict[a] = b
            else:
                normalized_dict[key] = value
        result_list.append(normalized_dict)
    return result_list


def add_value(list):
    """Helper for _validate_sparse_cql to modify expected results based"""
    for item in list:
        key = item.get('key', None)
        if key is None:
            key = item.get('renamed_key')

        value_key = 'value1' if key == 'k1' else 'value2'
        item[value_key]=_i64(100)


def _validate_sparse_cql(cursor, cf='sparse_super_1', column1='column1', col1='col1', col2='col2', key='key', is_version_4_or_greater=False):
    cursor.execute('use ks')

    result = unpack(list(cursor.execute("SELECT * FROM {}".format(cf))))

    expected = [{key: 'k1', column1: 'key1', col1: 200, col2: 300},
     {key: 'k1', column1: 'key2', col1: 200, col2: 300},
     {key: 'k2', column1: 'key1', col1: 200, col2: 300},
     {key: 'k2', column1: 'key2', col1: 200, col2: 300}]
    if is_version_4_or_greater:
        add_value(expected)
    assert_lists_of_dicts_equal(result, expected)

    result = unpack(list(cursor.execute("SELECT * FROM {} WHERE {} = 'k1'".format(cf, key))))
    expected =  [{key: 'k1', column1: 'key1', col1: 200, col2: 300},
                  {key: 'k1', column1: 'key2', col1: 200, col2: 300}]
    if is_version_4_or_greater:
        add_value(expected)
    assert_lists_of_dicts_equal(result, expected)

    result = unpack(list(cursor.execute("SELECT * FROM {} WHERE {} = 'k2' AND {} = 'key1'".format(cf, key, column1))))
    expected = [{key: 'k2', column1: 'key1', col1: 200, col2: 300}]
    if is_version_4_or_greater:
        add_value(expected)
    assert_lists_of_dicts_equal(result, expected)


def _validate_sparse_thrift(client, cf='sparse_super_1'):
    try:
        client.transport.open()
    except:
        pass
    client.set_keyspace('ks')
    result = client.get_slice('k1'.encode(), ColumnParent(cf), SlicePredicate(slice_range=SliceRange(''.encode(), ''.encode(), False, 5)), ConsistencyLevel.ONE)
    assert_length_equal(result, 2)
    assert result[0].super_column.name == 'key1'.encode()
    assert result[1].super_column.name == 'key2'.encode()

    for cosc in result:
        assert cosc.super_column.columns[0].name == 'col1'.encode()
        assert cosc.super_column.columns[0].value == _i64(200)
        assert cosc.super_column.columns[1].name == 'col2'.encode()
        assert cosc.super_column.columns[1].value == _i64(300)
        assert cosc.super_column.columns[2].name == 'value1'.encode()
        assert cosc.super_column.columns[2].value == _i64(100)


def _validate_dense_cql(cursor, cf='dense_super_1', key='key', column1='column1', column2='column2', value='value', is_version_4_or_greater=False):
    cursor.execute('use ks')

    expected = [{key: 'k1', column1: 'key1', column2: 100, value: 'value1'},
                  {key: 'k1', column1: 'key2', column2: 100, value: 'value1'},
                  {key: 'k2', column1: 'key1', column2: 200, value: 'value2'},
                  {key: 'k2', column1: 'key2', column2: 200, value: 'value2'}]
    if is_version_4_or_greater:
        expected[0][100]='value1'
        expected[1][100]='value1'
        expected[2][200]='value2'
        expected[3][200]='value2'
        for dict in expected:
            del dict[value]
        for dict in expected:
            del dict[column2]
    result = unpack(list(cursor.execute("SELECT * FROM {}".format(cf))))
    assert_lists_of_dicts_equal(result, expected)

    result = unpack(list(cursor.execute("SELECT * FROM {} WHERE {} = 'k1'".format(cf, key))))
    expected = [{key: 'k1', column1: 'key1', column2: 100, value: 'value1'},
                  {key: 'k1', column1: 'key2', column2: 100, value: 'value1'}]
    if is_version_4_or_greater:
        expected[0][100]='value1'
        expected[1][100]='value1'
        for dict in expected:
            del dict[value]
        for dict in expected:
            del dict[column2]
    assert_lists_of_dicts_equal(result, expected)

    result = unpack(list(cursor.execute("SELECT * FROM {} WHERE {} = 'k1' AND {} = 'key1'".format(cf, key, column1))))
    expected = [{key: 'k1', column1: 'key1', column2: 100, value: 'value1'}]
    if is_version_4_or_greater:
        expected[0][100]='value1'
        for dict in expected:
            del dict[value]
        for dict in expected:
            del dict[column2]
    assert_lists_of_dicts_equal(result, expected)

    if is_version_4_or_greater:
        result = unpack(list(cursor.execute("SELECT * FROM {} WHERE {} = 'k1' AND {} = 'key1' AND \"\" CONTAINS KEY 100 ALLOW FILTERING".format(cf, key, column1, column2))))
    else:
        result = list(cursor.execute("SELECT * FROM {} WHERE {} = 'k1' AND {} = 'key1' AND {} = 100".format(cf, key, column1, column2)))

    expected = [{key: 'k1', column1: 'key1', column2: 100, value: 'value1'}]
    if is_version_4_or_greater:
        expected[0][100]='value1'
        for dict in expected:
            del dict[value]
        for dict in expected:
            del dict[column2]
    assert_lists_of_dicts_equal(result, expected)


def _validate_dense_thrift(client, cf='dense_super_1'):
    try:
        client.transport.open()
    except:
        pass
    client.set_keyspace('ks')
    result = client.get_slice('k1'.encode(), ColumnParent(cf), SlicePredicate(slice_range=SliceRange(''.encode(), ''.encode(), False, 5)), ConsistencyLevel.ONE)
    assert_length_equal(result, 2)
    assert result[0].super_column.name == 'key1'.encode()
    assert result[1].super_column.name == 'key2'.encode()

    print((result[0]))
    print((result[1]))
    for cosc in result:
        assert cosc.super_column.columns[0].name == _i64(100)
        assert cosc.super_column.columns[0].value == 'value1'.encode()


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
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()
        for node in nodes:
            node.set_configuration_options(values={'start_rpc': 'true'})

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
        self.fixture_dtest_setup.reinitialize_cluster_for_different_version()

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

        _create_dense_super_cf(client, 'dense_super_1')

        for i in range(1, 3):
            client.insert('k1'.encode(), ColumnParent('dense_super_1', 'key{}'.format(i).encode()), Column(_i64(100), 'value1'.encode(), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('dense_super_1', 'key{}'.format(i).encode()), Column(_i64(200), 'value2'.encode(), 0), ConsistencyLevel.ONE)

        _validate_dense_thrift(client, cf='dense_super_1')

        self.set_node_to_current_version(node)
        #4.0 doesn't support compact storage
        if node.get_cassandra_version() >= '4':
            cursor.execute("ALTER TABLE ks.dense_super_1 DROP COMPACT STORAGE;")

        node.stop()
        if node.get_cassandra_version() < '4':
           node.set_configuration_options(values={'start_rpc': 'true'})
        node.start()

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)

        if node.get_cassandra_version() < '4':
            client = get_thrift_client(host, port)
            _validate_dense_thrift(client, cf='dense_super_1')
        _validate_dense_cql(cursor, cf='dense_super_1', is_version_4_or_greater=node.get_cassandra_version() >= '4')

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

        _create_dense_super_cf(client, 'dense_super_1')

        for i in range(1, 3):
            client.insert('k1'.encode(), ColumnParent('dense_super_1', 'key{}'.format(i).encode()), Column(_i64(100), 'value1'.encode(), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('dense_super_1', 'key{}'.format(i).encode()), Column(_i64(200), 'value2'.encode(), 0), ConsistencyLevel.ONE)

        _validate_dense_thrift(client, cf='dense_super_1')
        _validate_dense_cql(cursor, cf='dense_super_1')

        self.upgrade_to_version('github:apache/cassandra-3.0')

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)
        client = get_thrift_client(host, port)

        _validate_dense_thrift(client, cf='dense_super_1')

        self.set_node_to_current_version(node)
        #4.0 doesn't support compact storage
        if node.get_cassandra_version() >= '4':
            cursor.execute("ALTER TABLE ks.dense_super_1 DROP COMPACT STORAGE;")

        node.stop()
        if node.get_cassandra_version() < '4':
            node.set_configuration_options(values={'start_rpc': 'true'})
        node.start()

        if node.get_cassandra_version() < '4':
            client = get_thrift_client(host, port)
            _validate_dense_thrift(client, cf='dense_super_1')

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)
        _validate_dense_cql(cursor, cf='dense_super_1', is_version_4_or_greater=node.get_cassandra_version() >= '4')

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

        _create_sparse_super_cf(client, 'sparse_super_2')

        for i in range(1, 3):
            client.insert('k1'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("value1".encode(), _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k1'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("col1".encode(), _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k1'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("col2".encode(), _i64(300), 0), ConsistencyLevel.ONE)

            client.insert('k2'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("value2".encode(), _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("col1".encode(), _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("col2".encode(), _i64(300), 0), ConsistencyLevel.ONE)

        _validate_sparse_thrift(client, cf='sparse_super_2')
        _validate_sparse_cql(cursor, cf='sparse_super_2')

        self.upgrade_to_version('github:apache/cassandra-3.0')

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)
        client = get_thrift_client(host, port)

        _validate_sparse_thrift(client, cf='sparse_super_2')

        self.set_node_to_current_version(node)
        is_version_4_or_greater = node.get_cassandra_version() >= '4'
        #4.0 doesn't support compact storage
        if is_version_4_or_greater:
            cursor.execute("ALTER TABLE ks.sparse_super_2 DROP COMPACT STORAGE;")

        node.stop()
        if not is_version_4_or_greater:
            node.set_configuration_options(values={'start_rpc': 'true'})
        node.start()

        if not is_version_4_or_greater:
            client = get_thrift_client(host, port)
            _validate_sparse_thrift(client, cf='sparse_super_2')

        cursor = self.patient_cql_connection(node, row_factory=dict_factory)
        _validate_sparse_cql(cursor, cf='sparse_super_2', is_version_4_or_greater=is_version_4_or_greater)


@pytest.mark.upgrade_test
@since('2.1', max_version='3.99')
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

        _create_dense_super_cf(client, 'dense_super_1')

        for i in range(1, 3):
            client.insert('k1'.encode(), ColumnParent('dense_super_1', 'key{}'.format(i).encode()), Column(_i64(100), 'value1'.encode(), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('dense_super_1', 'key{}'.format(i).encode()), Column(_i64(200), 'value2'.encode(), 0), ConsistencyLevel.ONE)

        _validate_dense_cql(cursor)
        _validate_dense_thrift(client)

        version_string = self.upgrade_version_string()
        is_version_4_or_greater = version_string == 'trunk' or version_string >= '4.0'
        #4.0 doesn't support compact storage
        if is_version_4_or_greater:
            cursor.execute("ALTER TABLE ks.dense_super_1 DROP COMPACT STORAGE;")

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            if not is_version_4_or_greater:
                client = get_thrift_client(host, port)
                _validate_dense_thrift(client)
            _validate_dense_cql(cursor, is_version_4_or_greater=is_version_4_or_greater)

    def test_dense_supercolumn_with_renames(self):
        cursor = self.prepare(row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        _create_dense_super_cf(client, 'dense_super_2')

        for i in range(1, 3):
            client.insert('k1'.encode(), ColumnParent('dense_super_2', 'key{}'.format(i).encode()), Column(_i64(100), 'value1'.encode(), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('dense_super_2', 'key{}'.format(i).encode()), Column(_i64(200), 'value2'.encode(), 0), ConsistencyLevel.ONE)

        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME key TO renamed_key")
        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME column1 TO renamed_column1")
        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME column2 TO renamed_column2")
        cursor.execute("ALTER TABLE ks.dense_super_2 RENAME value TO renamed_value")

        _validate_dense_cql(cursor, cf='dense_super_2', key='renamed_key', column1='renamed_column1', column2='renamed_column2', value='renamed_value')
        _validate_dense_thrift(client, cf='dense_super_2')

        version_string = self.upgrade_version_string()
        is_version_4_or_greater = version_string == 'trunk' or version_string >= '4.0'
        #4.0 doesn't support compact storage
        if is_version_4_or_greater:
            cursor.execute("ALTER TABLE ks.dense_super_2 DROP COMPACT STORAGE;")

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            if not is_version_4_or_greater:
                client = get_thrift_client(host, port)
                _validate_dense_thrift(client, cf='dense_super_2')
            _validate_dense_cql(cursor, cf='dense_super_2', key='renamed_key', column1='renamed_column1', column2='renamed_column2', value='renamed_value', is_version_4_or_greater=is_version_4_or_greater)

    def test_sparse_supercolumn_with_renames(self):
        cursor = self.prepare(row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        _create_sparse_super_cf(client, 'sparse_super_1')

        cursor.execute("ALTER TABLE ks.sparse_super_1 RENAME key TO renamed_key")
        cursor.execute("ALTER TABLE ks.sparse_super_1 RENAME column1 TO renamed_column1")

        for i in range(1, 3):
            client.insert('k1'.encode(), ColumnParent('sparse_super_1', 'key{}'.format(i).encode()), Column("value1".encode(), _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k1'.encode(), ColumnParent('sparse_super_1', 'key{}'.format(i).encode()), Column("col1".encode(), _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k1'.encode(), ColumnParent('sparse_super_1', 'key{}'.format(i).encode()), Column("col2".encode(), _i64(300), 0), ConsistencyLevel.ONE)

            client.insert('k2'.encode(), ColumnParent('sparse_super_1', 'key{}'.format(i).encode()), Column("value2".encode(), _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('sparse_super_1', 'key{}'.format(i).encode()), Column("col1".encode(), _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('sparse_super_1', 'key{}'.format(i).encode()), Column("col2".encode(), _i64(300), 0), ConsistencyLevel.ONE)

        _validate_sparse_thrift(client)
        _validate_sparse_cql(cursor, column1='renamed_column1', key='renamed_key')

        version_string = self.upgrade_version_string()
        is_version_4_or_greater = version_string == 'trunk' or version_string >= '4.0'
        #4.0 doesn't support compact storage
        if is_version_4_or_greater:
            cursor.execute("ALTER TABLE ks.sparse_super_1 DROP COMPACT STORAGE;")

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            if not is_version_4_or_greater:
                client = get_thrift_client(host, port)
                _validate_sparse_thrift(client)
            _validate_sparse_cql(cursor, column1='renamed_column1', key='renamed_key', is_version_4_or_greater=is_version_4_or_greater)

    def test_sparse_supercolumn(self):
        cursor = self.prepare(row_factory=dict_factory)
        cluster = self.cluster

        node = self.cluster.nodelist()[0]
        node.nodetool("enablethrift")

        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)

        client.transport.open()
        client.set_keyspace('ks')

        _create_sparse_super_cf(client, 'sparse_super_2')

        for i in range(1, 3):
            client.insert('k1'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("value1".encode(), _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k1'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("col1".encode(), _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k1'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("col2".encode(), _i64(300), 0), ConsistencyLevel.ONE)

            client.insert('k2'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("value2".encode(), _i64(100), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("col1".encode(), _i64(200), 0), ConsistencyLevel.ONE)
            client.insert('k2'.encode(), ColumnParent('sparse_super_2', 'key{}'.format(i).encode()), Column("col2".encode(), _i64(300), 0), ConsistencyLevel.ONE)

        _validate_sparse_thrift(client, cf='sparse_super_2')
        _validate_sparse_cql(cursor, cf='sparse_super_2')

        version_string = self.upgrade_version_string()
        is_version_4_or_greater = version_string == 'trunk' or version_string >= '4.0'
        #4.0 doesn't support compact storage
        if is_version_4_or_greater:
            cursor.execute("ALTER TABLE ks.sparse_super_2 DROP COMPACT STORAGE;")

        for is_upgraded, cursor in self.do_upgrade(cursor, row_factory=dict_factory, use_thrift=True):
            logger.debug("Querying {} node".format("upgraded" if is_upgraded else "old"))
            if not is_version_4_or_greater:
                client = get_thrift_client(host, port)
                _validate_sparse_thrift(client, cf='sparse_super_2')
            _validate_sparse_cql(cursor, cf='sparse_super_2', is_version_4_or_greater=is_version_4_or_greater)


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
    cls = type(gen_class_name, (TestThrift,), spec)
    if not upgrade_applies_to_env:
        add_skip(cls, 'test not applicable to env.')
    globals()[gen_class_name] = cls
