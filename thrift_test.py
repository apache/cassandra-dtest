import re
import struct
import time
import uuid
import pytest
import logging
import codecs

from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException
from thrift.transport import TSocket, TTransport

from tools.assertions import assert_length_equal
from tools.misc import ImmutableMapping

from dtest_setup_overrides import DTestSetupOverrides
from dtest import CASSANDRA_VERSION_FROM_BUILD, Tester

from thrift_bindings.thrift010 import Cassandra
from thrift_bindings.thrift010.Cassandra import (CfDef, Column, ColumnDef,
                                           ColumnOrSuperColumn, ColumnParent,
                                           ColumnPath, ColumnSlice,
                                           ConsistencyLevel, CounterColumn,
                                           Deletion, IndexExpression,
                                           IndexOperator, IndexType,
                                           InvalidRequestException, KeyRange,
                                           KeySlice, KsDef, MultiSliceRequest,
                                           Mutation, NotFoundException,
                                           SlicePredicate, SliceRange,
                                           SuperColumn)
from tools.assertions import (assert_all, assert_none, assert_one)

since = pytest.mark.since
logger = logging.getLogger(__name__)
utf8encoder = codecs.getencoder('utf-8')
def utf8encode(str):
    return utf8encoder(str)[0]

def get_thrift_client(host='127.0.0.1', port=9160):
    socket = TSocket.TSocket(host, port)
    transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Cassandra.Client(protocol)
    client.transport = transport
    return client


client = None

pid_fname = "system_test.pid"


def pid():
    return int(open(pid_fname).read())


@since('2.0', max_version='4')
class TestThrift(Tester):

    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self):
        dtest_setup_overrides = DTestSetupOverrides()
        """
        @jira_ticket CASSANDRA-7653
        """
        dtest_setup_overrides.cluster_options = ImmutableMapping(
            {'partitioner': 'org.apache.cassandra.dht.ByteOrderedPartitioner',
             'start_rpc': 'true'})
        return dtest_setup_overrides

    @pytest.fixture(scope='function', autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup, set_dtest_setup_on_function):
        fixture_dtest_setup.cluster.populate(1)
        node1, = fixture_dtest_setup.cluster.nodelist()

        # If vnodes are not used, we must set our own initial_token
        # Because ccm will not set a hex token for ByteOrderedPartitioner
        # automatically. It does not matter what token we set as we only
        # ever use one node.
        if not self.dtest_config.use_vnodes:
            node1.set_configuration_options(values={'initial_token': 'abcd'})

        fixture_dtest_setup.cluster.start(wait_for_binary_proto=True)
        fixture_dtest_setup.cluster.nodelist()[0].watch_log_for("Listening for thrift clients")  # Wait for the thrift port to open
        time.sleep(0.1)
        # this is ugly, but the whole test module is written against a global client
        global client
        client = get_thrift_client()
        client.transport.open()
        self.define_schema()

        yield client

        client.transport.close()

    def define_schema(self):
        keyspace1 = Cassandra.KsDef('Keyspace1', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor': '1'},
                                    cf_defs=[
            Cassandra.CfDef('Keyspace1', 'Standard1'),
            Cassandra.CfDef('Keyspace1', 'Standard2'),
            Cassandra.CfDef('Keyspace1', 'Standard3', column_metadata=[Cassandra.ColumnDef(utf8encode('c1'), 'AsciiType'), Cassandra.ColumnDef(utf8encode('c2'), 'AsciiType')]),
            Cassandra.CfDef('Keyspace1', 'Standard4', column_metadata=[Cassandra.ColumnDef(utf8encode('c1'), 'AsciiType')]),
            Cassandra.CfDef('Keyspace1', 'StandardLong1', comparator_type='LongType'),
            Cassandra.CfDef('Keyspace1', 'StandardInteger1', comparator_type='IntegerType'),
            Cassandra.CfDef('Keyspace1', 'StandardComposite', comparator_type='CompositeType(AsciiType, AsciiType)'),
            Cassandra.CfDef('Keyspace1', 'Super1', column_type='Super', subcomparator_type='LongType'),
            Cassandra.CfDef('Keyspace1', 'Super2', column_type='Super', subcomparator_type='LongType'),
            Cassandra.CfDef('Keyspace1', 'Super3', column_type='Super', comparator_type='LongType', subcomparator_type='UTF8Type'),
            Cassandra.CfDef('Keyspace1', 'Counter1', default_validation_class='CounterColumnType'),
            Cassandra.CfDef('Keyspace1', 'SuperCounter1', column_type='Super', default_validation_class='CounterColumnType'),
            Cassandra.CfDef('Keyspace1', 'Indexed1', column_metadata=[Cassandra.ColumnDef(utf8encode('birthdate'), 'LongType', Cassandra.IndexType.KEYS, 'birthdate_index')]),
            Cassandra.CfDef('Keyspace1', 'Indexed2', comparator_type='TimeUUIDType', column_metadata=[Cassandra.ColumnDef(uuid.UUID('00000000-0000-1000-0000-000000000000').bytes, 'LongType', Cassandra.IndexType.KEYS)]),
            Cassandra.CfDef('Keyspace1', 'Indexed3', comparator_type='TimeUUIDType', column_metadata=[Cassandra.ColumnDef(uuid.UUID('00000000-0000-1000-0000-000000000000').bytes, 'UTF8Type', Cassandra.IndexType.KEYS)]),
            Cassandra.CfDef('Keyspace1', 'Indexed4', column_metadata=[Cassandra.ColumnDef(utf8encode('a'), 'LongType', Cassandra.IndexType.KEYS, 'a_index'), Cassandra.ColumnDef(utf8encode('z'), 'UTF8Type')]),
            Cassandra.CfDef('Keyspace1', 'Expiring', default_time_to_live=2)
        ])

        keyspace2 = Cassandra.KsDef('Keyspace2', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor': '1'},
                                    cf_defs=[
                                        Cassandra.CfDef('Keyspace2', 'Standard1'),
                                        Cassandra.CfDef('Keyspace2', 'Standard3'),
                                        Cassandra.CfDef('Keyspace2', 'Super3', column_type='Super', subcomparator_type='BytesType'),
                                        Cassandra.CfDef('Keyspace2', 'Super4', column_type='Super', subcomparator_type='TimeUUIDType'), ])

        for ks in [keyspace1, keyspace2]:
            client.system_add_keyspace(ks)


def i64(n):
    return _i64(n)


def i32(n):
    return _i32(n)


def i16(n):
    return _i16(n)


def composite(item1, item2=None, eoc=b'\x00'):
    if isinstance(item1, str):
        item1 = utf8encode(item1)
    if isinstance(item2, str):
        item2 = utf8encode(item2)

    packed = _i16(len(item1)) + item1 + eoc
    if item2 is not None:
        packed += _i16(len(item2)) + item2
        packed += eoc
    return packed


def _i64(n):
    return struct.pack('>q', n)  # big endian = network order


def _i32(n):
    return struct.pack('>i', n)  # big endian = network order


def _i16(n):
    return struct.pack('>h', n)  # big endian = network order


_SIMPLE_COLUMNS = [Column(utf8encode('c1'), utf8encode('value1'), 0),
                   Column(utf8encode('c2'), utf8encode('value2'), 0)]
_SUPER_COLUMNS = [SuperColumn(name=utf8encode('sc1'), columns=[Column(_i64(4), utf8encode('value4'), 0)]),
                  SuperColumn(name=utf8encode('sc2'), columns=[Column(_i64(5), utf8encode('value5'), 0),
                                                   Column(_i64(6), utf8encode('value6'), 0)])]


def _assert_column(column_family, key, column, value, ts=0):
    if isinstance(key, str):
        key = utf8encode(key)
    if isinstance(value, str):
        value = utf8encode(value)

    try:
        assert client.get(key, ColumnPath(column_family, column=column), ConsistencyLevel.ONE).column == Column(column, value, ts)
    except NotFoundException:
        raise Exception('expected %s:%s:%s:%s, but was not present' % (column_family, key, column, value))


def _assert_columnpath_exists(key, column_path):
    if isinstance(key, str):
        key = utf8encode(key)
    try:
        assert client.get(key, column_path, ConsistencyLevel.ONE)
    except NotFoundException:
        raise Exception('expected %s with %s but was not present.' % (key, column_path))


def _assert_no_columnpath(key, column_path):
    if isinstance(key, str):
        key = utf8encode(key)
    try:
        client.get(key, column_path, ConsistencyLevel.ONE)
        assert False, ('columnpath %s existed in %s when it should not' % (column_path, key))
    except NotFoundException:
        assert True, 'column did not exist'


def _insert_simple():
    return _insert_multi([utf8encode('key1')])


def _insert_multi(keys):
    CL = ConsistencyLevel.ONE
    for key in keys:
        if isinstance(key, str):
            key = utf8encode(key)
        client.insert(key, ColumnParent('Standard1'), Column(utf8encode('c1'), utf8encode('value1'), 0), CL)
        client.insert(key, ColumnParent('Standard1'), Column(utf8encode('c2'), utf8encode('value2'), 0), CL)


def _insert_batch():
    cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS],
             'Standard2': [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]}
    client.batch_mutate({utf8encode('key1'): cfmap}, ConsistencyLevel.ONE)


def _big_slice(key, column_parent):
    if isinstance(key, str):
        key = utf8encode(key)
    p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1000))
    return client.get_slice(key, column_parent, p, ConsistencyLevel.ONE)


def _big_multislice(keys, column_parent):
    p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1000))
    return client.multiget_slice(keys, column_parent, p, ConsistencyLevel.ONE)


def _verify_batch():
    _verify_simple()
    L = [result.column
         for result in _big_slice(utf8encode('key1'), ColumnParent('Standard2'))]
    assert L == _SIMPLE_COLUMNS, L


def _verify_simple():
    assert client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('c1')), ConsistencyLevel.ONE).column == Column(utf8encode('c1'), utf8encode('value1'), 0)
    L = [result.column
         for result in _big_slice(utf8encode('key1'), ColumnParent('Standard1'))]
    assert L == _SIMPLE_COLUMNS, L


def _insert_super(key='key1'):
    if isinstance(key, str):
        key = utf8encode(key)
    client.insert(key, ColumnParent('Super1', utf8encode('sc1')), Column(_i64(4), utf8encode('value4'), 0), ConsistencyLevel.ONE)
    client.insert(key, ColumnParent('Super1', utf8encode('sc2')), Column(_i64(5), utf8encode('value5'), 0), ConsistencyLevel.ONE)
    client.insert(key, ColumnParent('Super1', utf8encode('sc2')), Column(_i64(6), utf8encode('value6'), 0), ConsistencyLevel.ONE)


def _insert_range():
    client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c1'), utf8encode('value1'), 0), ConsistencyLevel.ONE)
    client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c2'), utf8encode('value2'), 0), ConsistencyLevel.ONE)
    client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c3'), utf8encode('value3'), 0), ConsistencyLevel.ONE)


def _verify_range():
    p = SlicePredicate(slice_range=SliceRange(utf8encode('c1'), utf8encode('c2'), False, 1000))
    result = client.get_slice(utf8encode('key1'), ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == utf8encode('c1')
    assert result[1].column.name == utf8encode('c2')

    p = SlicePredicate(slice_range=SliceRange(utf8encode('c3'), utf8encode('c2'), True, 1000))
    result = client.get_slice(utf8encode('key1'), ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].column.name == utf8encode('c3')
    assert result[1].column.name == utf8encode('c2')

    p = SlicePredicate(slice_range=SliceRange(utf8encode('a'), utf8encode('z'), False, 1000))
    result = client.get_slice(utf8encode('key1'), ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 3, result

    p = SlicePredicate(slice_range=SliceRange(utf8encode('a'), utf8encode('z'), False, 2))
    result = client.get_slice(utf8encode('key1'), ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2, result


def _set_keyspace(keyspace):
    client.set_keyspace(keyspace)


def _insert_super_range():
    client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc1')), Column(_i64(4), utf8encode('value4'), 0), ConsistencyLevel.ONE)
    client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), Column(_i64(5), utf8encode('value5'), 0), ConsistencyLevel.ONE)
    client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), Column(_i64(6), utf8encode('value6'), 0), ConsistencyLevel.ONE)
    client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc3')), Column(_i64(7), utf8encode('value7'), 0), ConsistencyLevel.ONE)
    time.sleep(0.1)


def _verify_super_range():
    p = SlicePredicate(slice_range=SliceRange(utf8encode('sc2'), utf8encode('sc3'), False, 2))
    result = client.get_slice(utf8encode('key1'), ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == utf8encode('sc2')
    assert result[1].super_column.name == utf8encode('sc3')

    p = SlicePredicate(slice_range=SliceRange(utf8encode('sc3'), utf8encode('sc2'), True, 2))
    result = client.get_slice(utf8encode('key1'), ColumnParent('Super1'), p, ConsistencyLevel.ONE)
    assert len(result) == 2
    assert result[0].super_column.name == utf8encode('sc3')
    assert result[1].super_column.name == utf8encode('sc2')


def _verify_super(supercf='Super1', key='key1'):
    if isinstance(key, str):
        key = utf8encode(key)
    assert client.get(key, ColumnPath(supercf, utf8encode('sc1'), _i64(4)), ConsistencyLevel.ONE).column == Column(_i64(4), utf8encode('value4'), 0)
    slice = [result.super_column
             for result in _big_slice(key, ColumnParent('Super1'))]
    assert slice == _SUPER_COLUMNS, slice


def _expect_exception(fn, type_):
    try:
        r = fn()
    except type_ as t:
        return t
    else:
        raise Exception('expected %s; got %s' % (type_.__name__, r))


def _expect_missing(fn):
    _expect_exception(fn, NotFoundException)


def get_range_slice(client, parent, predicate, start, end, count, cl, row_filter=None):
    kr = KeyRange(start, end, count=count, row_filter=row_filter)
    return client.get_range_slices(parent, predicate, kr, cl)


def _insert_six_columns(key='abc'):
    if isinstance(key, str):
        key = utf8encode(key)
    CL = ConsistencyLevel.ONE
    client.insert(key, ColumnParent('Standard1'), Column(utf8encode('a'), utf8encode('1'), 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column(utf8encode('b'), utf8encode('2'), 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column(utf8encode('c'), utf8encode('3'), 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column(utf8encode('d'), utf8encode('4'), 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column(utf8encode('e'), utf8encode('5'), 0), CL)
    client.insert(key, ColumnParent('Standard1'), Column(utf8encode('f'), utf8encode('6'), 0), CL)


def _big_multi_slice(key='abc'):
    if isinstance(key, str):
        key = utf8encode(key)
    c1 = ColumnSlice()
    c1.start = utf8encode('a')
    c1.finish = utf8encode('c')
    c2 = ColumnSlice()
    c2.start = utf8encode('e')
    c2.finish = utf8encode('f')
    m = MultiSliceRequest()
    m.key = key
    m.column_parent = ColumnParent('Standard1')
    m.column_slices = [c1, c2]
    m.reversed = False
    m.count = 10
    m.consistency_level = ConsistencyLevel.ONE
    return client.get_multi_slice(m)


_MULTI_SLICE_COLUMNS = [Column(utf8encode('a'), utf8encode('1'), 0), Column(utf8encode('b'), utf8encode('2'), 0), Column(utf8encode('c'), utf8encode('3'), 0), Column(utf8encode('e'), utf8encode('5'), 0), Column(utf8encode('f'), utf8encode('6'), 0)]


@since('2.0', max_version='4')
class TestMutations(TestThrift):

    def truncate_all(self, *table_names):
        for table in table_names:
            client.truncate(table)

    def test_insert(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')
        _insert_simple()
        _verify_simple()

    def test_empty_slice(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard2', 'Super1')
        assert _big_slice(utf8encode('key1'), ColumnParent('Standard2')) == []
        assert _big_slice(utf8encode('key1'), ColumnParent('Super1')) == []

    def test_cas(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Standard3', 'Standard4')

        def cas(expected, updates, column_family):
            return client.cas(utf8encode('key1'), column_family, expected, updates, ConsistencyLevel.SERIAL, ConsistencyLevel.QUORUM)

        def test_cas_operations(first_columns, second_columns, column_family):
            # partition should be empty, so cas expecting any existing values should fail
            cas_result = cas(first_columns, first_columns, column_family)
            assert not cas_result.success
            assert len(cas_result.current_values) == 0, cas_result

            # cas of empty columns -> first_columns should succeed
            # and the reading back from the table should match first_columns
            assert cas([], first_columns, column_family).success
            result = [cosc.column for cosc in _big_slice(utf8encode('key1'), ColumnParent(column_family))]
            # CAS will use its own timestamp, so we can't just compare result == _SIMPLE_COLUMNS
            assert dict((c.name, c.value) for c in result) == dict((ex.name, ex.value) for ex in first_columns)

            # now that the partition has been updated, repeating the
            # operation which expects it to be empty should not succeed
            cas_result = cas([], first_columns, column_family)
            assert not cas_result.success
            # When we CAS for non-existence, current_values is the first live column of the row
            assert dict((c.name, c.value) for c in cas_result.current_values) == {first_columns[0].name: first_columns[0].value}, cas_result

            # CL.SERIAL for reads
            assert client.get(utf8encode('key1'), ColumnPath(column_family, column=first_columns[0].name), ConsistencyLevel.SERIAL).column.value == first_columns[0].value

            # cas first_columns -> second_columns should succeed
            assert cas(first_columns, second_columns, column_family).success

            # as before, an operation with an incorrect expectation should fail
            cas_result = cas(first_columns, second_columns, column_family)
            assert not cas_result.success

        updated_columns = [Column(utf8encode('c1'), utf8encode('value101'), 1),
                           Column(utf8encode('c2'), utf8encode('value102'), 1)]

        logger.debug("Testing CAS operations on dynamic cf")
        test_cas_operations(_SIMPLE_COLUMNS, updated_columns, 'Standard1')
        logger.debug("Testing CAS operations on static cf")
        test_cas_operations(_SIMPLE_COLUMNS, updated_columns, 'Standard3')
        logger.debug("Testing CAS on mixed static/dynamic cf")
        test_cas_operations(_SIMPLE_COLUMNS, updated_columns, 'Standard4')

    def test_missing_super(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Super1', utf8encode('sc1'), _i64(1)), ConsistencyLevel.ONE))
        _insert_super()
        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Super1', utf8encode('sc1'), _i64(1)), ConsistencyLevel.ONE))

    def test_count(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Standard2', 'Super1')

        _insert_simple()
        _insert_super()
        p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1000))
        assert client.get_count(utf8encode('key1'), ColumnParent('Standard2'), p, ConsistencyLevel.ONE) == 0
        assert client.get_count(utf8encode('key1'), ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 2
        assert client.get_count(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), p, ConsistencyLevel.ONE) == 2
        assert client.get_count(utf8encode('key1'), ColumnParent('Super1'), p, ConsistencyLevel.ONE) == 2

        # Let's make that a little more interesting
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c3'), utf8encode('value3'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c4'), utf8encode('value4'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c5'), utf8encode('value5'), 0), ConsistencyLevel.ONE)

        p = SlicePredicate(slice_range=SliceRange(utf8encode('c2'), utf8encode('c4'), False, 1000))
        assert client.get_count(utf8encode('key1'), ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 3

    def test_count_paging(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        _insert_simple()

        # Exercise paging
        column_parent = ColumnParent('Standard1')
        # Paging for small columns starts at 1024 columns
        columns_to_insert = [Column(utf8encode('c%d' % (i,)), utf8encode('value%d' % (i,)), 0) for i in range(3, 1026)]
        cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(c)) for c in columns_to_insert]}
        client.batch_mutate({utf8encode('key1') : cfmap}, ConsistencyLevel.ONE)

        p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 2000))
        assert client.get_count(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE) == 1025

        # Ensure that the count limit isn't clobbered
        p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 10))
        assert client.get_count(utf8encode('key1'), ColumnParent('Standard1'), p, ConsistencyLevel.ONE) == 10

    # test get_count() to work correctly with 'count' settings around page size (CASSANDRA-4833)
    def test_count_around_page_size(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        def slice_predicate(count):
            return SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, count))

        key = utf8encode('key1')
        parent = ColumnParent('Standard1')
        cl = ConsistencyLevel.ONE

        for i in range(0, 3050):
            client.insert(key, parent, Column(utf8encode(str(i)), utf8encode(''), 0), cl)

        # same as page size
        assert client.get_count(key, parent, slice_predicate(1024), cl) == 1024

        # 1 above page size
        assert client.get_count(key, parent, slice_predicate(1025), cl) == 1025

        # above number or columns
        assert client.get_count(key, parent, slice_predicate(4000), cl) == 3050

        # same as number of columns
        assert client.get_count(key, parent, slice_predicate(3050), cl) == 3050

        # 1 above number of columns
        assert client.get_count(key, parent, slice_predicate(3051), cl) == 3050

    def test_super_insert(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        _insert_super()
        _verify_super()

    def test_super_get(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        _insert_super()
        result = client.get(utf8encode('key1'), ColumnPath('Super1', utf8encode('sc2')), ConsistencyLevel.ONE).super_column
        assert result == _SUPER_COLUMNS[1], result

    def test_super_subcolumn_limit(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')
        _insert_super()
        p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1))
        column_parent = ColumnParent('Super1', utf8encode('sc2'))
        slice = [result.column
                 for result in client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(5), utf8encode('value5'), 0)], slice
        p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), True, 1))
        slice = [result.column
                 for result in client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(_i64(6), utf8encode('value6'), 0)], slice

    def test_long_order(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('StandardLong1')

        def long_xrange(start, stop, step):
            i = start
            while i < stop:
                yield i
                i += step
        L = []
        for i in long_xrange(0, 104294967296, 429496729):
            name = _i64(i)
            client.insert(utf8encode('key1'), ColumnParent('StandardLong1'), Column(name, utf8encode('v'), 0), ConsistencyLevel.ONE)
            L.append(name)
        slice = [result.column.name for result in _big_slice(utf8encode('key1'), ColumnParent('StandardLong1'))]
        assert slice == L, slice

    def test_integer_order(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('StandardInteger1')

        def long_xrange(start, stop, step):
            i = start
            while i >= stop:
                yield i
                i -= step
        L = []
        for i in long_xrange(104294967296, 0, 429496729):
            name = _i64(i)
            client.insert(utf8encode('key1'), ColumnParent('StandardInteger1'), Column(name, utf8encode('v'), 0), ConsistencyLevel.ONE)
            L.append(name)
        slice = [result.column.name for result in _big_slice(utf8encode('key1'), ColumnParent('StandardInteger1'))]
        L.sort()
        assert slice == L, slice

    def test_time_uuid(self):
        _set_keyspace('Keyspace2')
        self.truncate_all('Super4')

        import uuid
        L = []

        # 100 isn't enough to fail reliably if the comparator is borked
        for i in range(500):
            L.append(uuid.uuid1())
            client.insert(utf8encode('key1'), ColumnParent('Super4', utf8encode('sc1')), Column(L[-1].bytes, utf8encode('value%s' % i), i), ConsistencyLevel.ONE)
        slice = _big_slice(utf8encode('key1'), ColumnParent('Super4', utf8encode('sc1')))
        assert len(slice) == 500, len(slice)
        for i in range(500):
            u = slice[i].column
            assert u.value == utf8encode('value%s' % i)
            assert u.name == L[i].bytes

        p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), True, 1))
        column_parent = ColumnParent('Super4', utf8encode('sc1'))
        slice = [result.column
                 for result in client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[-1].bytes, utf8encode('value499'), 499)], slice

        p = SlicePredicate(slice_range=SliceRange(utf8encode(''), L[2].bytes, False, 1000))
        column_parent = ColumnParent('Super4', utf8encode('sc1'))
        slice = [result.column
                 for result in client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[0].bytes, utf8encode('value0'), 0),
                         Column(L[1].bytes, utf8encode('value1'), 1),
                         Column(L[2].bytes, utf8encode('value2'), 2)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, utf8encode(''), True, 1000))
        column_parent = ColumnParent('Super4', utf8encode('sc1'))
        slice = [result.column
                 for result in client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, utf8encode('value2'), 2),
                         Column(L[1].bytes, utf8encode('value1'), 1),
                         Column(L[0].bytes, utf8encode('value0'), 0)], slice

        p = SlicePredicate(slice_range=SliceRange(L[2].bytes, utf8encode(''), False, 1))
        column_parent = ColumnParent('Super4', utf8encode('sc1'))
        slice = [result.column
                 for result in client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE)]
        assert slice == [Column(L[2].bytes, utf8encode('value2'), 2)], slice

    def test_long_remove(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('StandardLong1')

        column_parent = ColumnParent('StandardLong1')
        sp = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1))
        for i in range(10):
            parent = ColumnParent('StandardLong1')

            client.insert(utf8encode('key1'), parent, Column(_i64(i), utf8encode('value1'), 10 * i), ConsistencyLevel.ONE)
            client.remove(utf8encode('key1'), ColumnPath('StandardLong1'), 10 * i + 1, ConsistencyLevel.ONE)
            slice = client.get_slice(utf8encode('key1'), column_parent, sp, ConsistencyLevel.ONE)
            assert slice == [], slice
            # resurrect
            client.insert(utf8encode('key1'), parent, Column(_i64(i), utf8encode('value2'), 10 * i + 2), ConsistencyLevel.ONE)
            slice = [result.column
                     for result in client.get_slice(utf8encode('key1'), column_parent, sp, ConsistencyLevel.ONE)]
            assert slice == [Column(_i64(i), utf8encode('value2'), 10 * i + 2)], (slice, i)

    def test_integer_remove(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('StandardInteger1')

        column_parent = ColumnParent('StandardInteger1')
        sp = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1))
        for i in range(10):
            parent = ColumnParent('StandardInteger1')

            client.insert(utf8encode('key1'), parent, Column(_i64(i), utf8encode('value1'), 10 * i), ConsistencyLevel.ONE)
            client.remove(utf8encode('key1'), ColumnPath('StandardInteger1'), 10 * i + 1, ConsistencyLevel.ONE)
            slice = client.get_slice(utf8encode('key1'), column_parent, sp, ConsistencyLevel.ONE)
            assert slice == [], slice
            # resurrect
            client.insert(utf8encode('key1'), parent, Column(_i64(i), utf8encode('value2'), 10 * i + 2), ConsistencyLevel.ONE)
            slice = [result.column
                     for result in client.get_slice(utf8encode('key1'), column_parent, sp, ConsistencyLevel.ONE)]
            assert slice == [Column(_i64(i), utf8encode('value2'), 10 * i + 2)], (slice, i)

    def test_batch_insert(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Standard2')
        _insert_batch()
        _verify_batch()

    def test_batch_mutate_standard_columns(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Standard2')

        column_families = ['Standard1', 'Standard2']
        keys = [utf8encode('key_%d' % i) for i in range(27, 32)]
        mutations = [Mutation(ColumnOrSuperColumn(c)) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)
        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for key in keys:
                _assert_column(column_family, key, utf8encode('c1'), utf8encode('value1'))

    def test_batch_mutate_remove_standard_columns(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Standard2')

        column_families = ['Standard1', 'Standard2']
        keys = [utf8encode('key_%d' % i) for i in range(11, 21)]
        _insert_multi(keys)

        mutations = [Mutation(deletion=Deletion(20, predicate=SlicePredicate(column_names=[c.name]))) for c in _SIMPLE_COLUMNS]
        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath(key, ColumnPath(column_family, column=c.name))

    def test_batch_mutate_remove_standard_row(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Standard2')

        column_families = ['Standard1', 'Standard2']
        keys = [utf8encode('key_%d' % i) for i in range(11, 21)]
        _insert_multi(keys)

        mutations = [Mutation(deletion=Deletion(20))]
        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for column_family in column_families:
            for c in _SIMPLE_COLUMNS:
                for key in keys:
                    _assert_no_columnpath(key, ColumnPath(column_family, column=c.name))

    def test_batch_mutate_remove_super_columns_with_standard_under(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1', 'Super2')

        column_families = ['Super1', 'Super2']
        keys = [utf8encode('key_%d' % i) for i in range(11, 21)]
        _insert_super()

        mutations = []
        for sc in _SUPER_COLUMNS:
            names = []
            for c in sc.columns:
                names.append(c.name)
            mutations.append(Mutation(deletion=Deletion(20, super_column=c.name, predicate=SlicePredicate(column_names=names))))

        mutation_map = dict((column_family, mutations) for column_family in column_families)

        keyed_mutations = dict((key, mutation_map) for key in keys)

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)
        for column_family in column_families:
            for sc in _SUPER_COLUMNS:
                for c in sc.columns:
                    for key in keys:
                        _assert_no_columnpath(key, ColumnPath(column_family, super_column=sc.name, column=c.name))

    def test_batch_mutate_remove_super_columns_with_none_given_underneath(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        keys = [utf8encode('key_%d' % i) for i in range(17, 21)]

        for key in keys:
            _insert_super(key)

        mutations = []

        for sc in _SUPER_COLUMNS:
            mutations.append(Mutation(deletion=Deletion(20,
                                                        super_column=sc.name)))

        mutation_map = {'Super1': mutations}

        keyed_mutations = dict((key, mutation_map) for key in keys)

        # Sanity check
        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_columnpath_exists(key, ColumnPath('Super1', super_column=sc.name))

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for sc in _SUPER_COLUMNS:
            for c in sc.columns:
                for key in keys:
                    _assert_no_columnpath(key, ColumnPath('Super1', super_column=sc.name))

    def test_batch_mutate_remove_super_columns_entire_row(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        keys = [utf8encode('key_%d' % i) for i in range(17, 21)]

        for key in keys:
            _insert_super(key)

        mutations = []

        mutations.append(Mutation(deletion=Deletion(20)))

        mutation_map = {'Super1': mutations}

        keyed_mutations = dict((key, mutation_map) for key in keys)

        # Sanity check
        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_columnpath_exists(key, ColumnPath('Super1', super_column=sc.name))

        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for sc in _SUPER_COLUMNS:
            for key in keys:
                _assert_no_columnpath(key, ColumnPath('Super1', super_column=sc.name))

    # known failure: see CASSANDRA-10046
    def test_batch_mutate_remove_slice_standard(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        columns = [Column(utf8encode('c1'), utf8encode('value1'), 0),
                   Column(utf8encode('c2'), utf8encode('value2'), 0),
                   Column(utf8encode('c3'), utf8encode('value3'), 0),
                   Column(utf8encode('c4'), utf8encode('value4'), 0),
                   Column(utf8encode('c5'), utf8encode('value5'), 0)]

        for column in columns:
            client.insert(utf8encode('key'), ColumnParent('Standard1'), column, ConsistencyLevel.ONE)

        d = Deletion(1, predicate=SlicePredicate(slice_range=SliceRange(start=utf8encode('c2'), finish=utf8encode('c4'))))
        client.batch_mutate({utf8encode('key'): {'Standard1': [Mutation(deletion=d)]}}, ConsistencyLevel.ONE)

        _assert_columnpath_exists(utf8encode('key'), ColumnPath('Standard1', column=utf8encode('c1')))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Standard1', column=utf8encode('c2')))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Standard1', column=utf8encode('c3')))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Standard1', column=utf8encode('c4')))
        _assert_columnpath_exists(utf8encode('key'), ColumnPath('Standard1', column=utf8encode('c5')))

    # known failure: see CASSANDRA-10046
    def test_batch_mutate_remove_slice_of_entire_supercolumns(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        columns = [SuperColumn(name=utf8encode('sc1'), columns=[Column(_i64(1), utf8encode('value1'), 0)]),
                   SuperColumn(name=utf8encode('sc2'),
                               columns=[Column(_i64(2), utf8encode('value2') , 0), Column(_i64(3), utf8encode('value3') , 0)]),
                   SuperColumn(name=utf8encode('sc3'), columns=[Column(_i64(4), utf8encode('value4'), 0)]),
                   SuperColumn(name=utf8encode('sc4'),
                               columns=[Column(_i64(5), utf8encode('value5') , 0), Column(_i64(6), utf8encode('value6') , 0)]),
                   SuperColumn(name=utf8encode('sc5'), columns=[Column(_i64(7), utf8encode('value7'), 0)])]

        for column in columns:
            for subcolumn in column.columns:
                client.insert(utf8encode('key'), ColumnParent('Super1', column.name), subcolumn, ConsistencyLevel.ONE)

        d = Deletion(1, predicate=SlicePredicate(slice_range=SliceRange(start=utf8encode('sc2') , finish=utf8encode('sc4') )))
        client.batch_mutate({utf8encode('key'): {'Super1': [Mutation(deletion=d)]}}, ConsistencyLevel.ONE)

        _assert_columnpath_exists(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc1'), column=_i64(1)))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc2'), column=_i64(2)))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc2'), column=_i64(3)))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc3'), column=_i64(4)))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc4'), column=_i64(5)))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc4'), column=_i64(6)))
        _assert_columnpath_exists(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc5'), column=_i64(7)))

    @since('1.0', '2.2')
    @pytest.mark.skip(reason="Runs but fails and looks like it actually should fail since 8099?")
    def test_batch_mutate_remove_slice_part_of_supercolumns(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        columns = [Column(_i64(1), utf8encode('value1'), 0),
                   Column(_i64(2), utf8encode('value2'), 0),
                   Column(_i64(3), utf8encode('value3'), 0),
                   Column(_i64(4), utf8encode('value4'), 0),
                   Column(_i64(5), utf8encode('value5'), 0)]

        for column in columns:
            client.insert(utf8encode('key'), ColumnParent('Super1', utf8encode('sc1')), column, ConsistencyLevel.ONE)

        r = SliceRange(start=_i64(2), finish=_i64(4))
        d = Deletion(1, super_column=utf8encode('sc1') , predicate=SlicePredicate(slice_range=r))
        client.batch_mutate({utf8encode('key'): {'Super1' : [Mutation(deletion=d)]}}, ConsistencyLevel.ONE)

        _assert_columnpath_exists(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc1'), column=_i64(1)))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc1'), column=_i64(2)))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc1'), column=_i64(3)))
        _assert_no_columnpath(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc1'), column=_i64(4)))
        _assert_columnpath_exists(utf8encode('key'), ColumnPath('Super1', super_column=utf8encode('sc1'), column=_i64(5)))

    def test_batch_mutate_insertions_and_deletions(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1', 'Super2')

        first_insert = SuperColumn(utf8encode("sc1"),
                                   columns=[Column(_i64(20), utf8encode('value20'), 3),
                                            Column(_i64(21), utf8encode('value21'), 3)])
        second_insert = SuperColumn(utf8encode("sc1"),
                                    columns=[Column(_i64(20), utf8encode('value20'), 3),
                                             Column(_i64(21), utf8encode('value21'), 3)])
        first_deletion = {'super_column': utf8encode("sc1"),
                          'predicate': SlicePredicate(column_names=[_i64(22), _i64(23)])}
        second_deletion = {'super_column': utf8encode("sc2"),
                           'predicate': SlicePredicate(column_names=[_i64(22), _i64(23)])}

        keys = [utf8encode('key_30'), utf8encode('key_31')]
        for key in keys:
            sc = SuperColumn(utf8encode('sc1'), [Column(_i64(22), utf8encode('value22'), 0),
                                     Column(_i64(23), utf8encode('value23'), 0)])
            cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=sc))]}
            client.batch_mutate({key: cfmap}, ConsistencyLevel.ONE)

            sc2 = SuperColumn(utf8encode('sc2'), [Column(_i64(22), utf8encode('value22'), 0),
                                      Column(_i64(23), utf8encode('value23'), 0)])
            cfmap2 = {'Super2': [Mutation(ColumnOrSuperColumn(super_column=sc2))]}
            client.batch_mutate({key: cfmap2}, ConsistencyLevel.ONE)

        cfmap3 = {
            'Super1': [Mutation(ColumnOrSuperColumn(super_column=first_insert)),
                       Mutation(deletion=Deletion(3, **first_deletion))],

            'Super2': [Mutation(deletion=Deletion(2, **second_deletion)),
                       Mutation(ColumnOrSuperColumn(super_column=second_insert))]
        }

        keyed_mutations = dict((key, cfmap3) for key in keys)
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        for key in keys:
            for c in [_i64(22), _i64(23)]:
                _assert_no_columnpath(key, ColumnPath('Super1', super_column=utf8encode('sc1'), column=c))
                _assert_no_columnpath(key, ColumnPath('Super2', super_column=utf8encode('sc2'), column=c))

            for c in [_i64(20), _i64(21)]:
                _assert_columnpath_exists(key, ColumnPath('Super1', super_column=utf8encode('sc1'), column=c))
                _assert_columnpath_exists(key, ColumnPath('Super2', super_column=utf8encode('sc1'), column=c))

    def test_bad_system_calls(self):
        def duplicate_index_names():
            _set_keyspace('Keyspace1')
            cd1 = ColumnDef(utf8encode('foo'), 'BytesType', IndexType.KEYS, 'i')
            cd2 = ColumnDef(utf8encode('bar'), 'BytesType', IndexType.KEYS, 'i')
            cf = CfDef('Keyspace1', 'BadCF', column_metadata=[cd1, cd2])
            client.system_add_column_family(cf)
        _expect_exception(duplicate_index_names, InvalidRequestException)

    def test_bad_batch_calls(self):
        # mutate_does_not_accept_cosc_and_deletion_in_same_mutation
        def too_full():
            _set_keyspace('Keyspace1')
            col = ColumnOrSuperColumn(column=Column(utf8encode("foo"), utf8encode('bar'), 0))
            dele = Deletion(2, predicate=SlicePredicate(column_names=[utf8encode('baz')]))
            client.batch_mutate({utf8encode('key_34'): {'Standard1': [Mutation(col, dele)]}},
                                ConsistencyLevel.ONE)
        _expect_exception(too_full, InvalidRequestException)

        # test_batch_mutate_does_not_accept_cosc_on_undefined_cf:
        def bad_cf():
            _set_keyspace('Keyspace1')
            col = ColumnOrSuperColumn(column=Column(utf8encode("foo"), utf8encode('bar'), 0))
            client.batch_mutate({utf8encode('key_36'): {'Undefined': [Mutation(col)]}},
                                ConsistencyLevel.ONE)
        _expect_exception(bad_cf, InvalidRequestException)

        # test_batch_mutate_does_not_accept_deletion_on_undefined_cf
        def bad_cf_2():
            _set_keyspace('Keyspace1')
            d = Deletion(2, predicate=SlicePredicate(column_names=[utf8encode('baz')]))
            client.batch_mutate({utf8encode('key_37'): {'Undefined': [Mutation(deletion=d)]}},
                                ConsistencyLevel.ONE)
        _expect_exception(bad_cf_2, InvalidRequestException)

        # a column value that does not match the declared validator
        def send_string_instead_of_long():
            _set_keyspace('Keyspace1')
            col = ColumnOrSuperColumn(column=Column(utf8encode('birthdate'), utf8encode('bar'), 0))
            client.batch_mutate({utf8encode('key_38'): {'Indexed1': [Mutation(col)]}},
                                ConsistencyLevel.ONE)
        _expect_exception(send_string_instead_of_long, InvalidRequestException)

    def test_column_name_lengths(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        _expect_exception(lambda: client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode(''), utf8encode('value'), 0), ConsistencyLevel.ONE), InvalidRequestException)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('x' * 1), utf8encode('value'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('x' * 127), utf8encode('value'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('x' * 128), utf8encode('value'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('x' * 129), utf8encode('value'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('x' * 255), utf8encode('value'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('x' * 256), utf8encode('value'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('x' * 257), utf8encode('value'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('x' * (2 ** 16 - 1)), utf8encode('value'), 0), ConsistencyLevel.ONE)
        _expect_exception(lambda: client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('x' * (2 ** 16)), utf8encode('value'), 0), ConsistencyLevel.ONE), InvalidRequestException)

    def test_bad_calls(self):
        _set_keyspace('Keyspace1')

        # missing arguments
        _expect_exception(lambda: client.insert(None, None, None, None), TApplicationException)
        # supercolumn in a non-super CF
        _expect_exception(lambda: client.insert(utf8encode('key1'), ColumnParent('Standard1', utf8encode('x')), Column(utf8encode('y'), utf8encode('value'), 0), ConsistencyLevel.ONE), InvalidRequestException)
        # no supercolumn in a super CF
        _expect_exception(lambda: client.insert(utf8encode('key1'), ColumnParent('Super1'), Column(utf8encode('y'), utf8encode('value'), 0), ConsistencyLevel.ONE), InvalidRequestException)
        # column but no supercolumn in remove
        _expect_exception(lambda: client.remove(utf8encode('key1'), ColumnPath('Super1', column=utf8encode('x')), 0, ConsistencyLevel.ONE), InvalidRequestException)
        # super column in non-super CF
        _expect_exception(lambda: client.remove(utf8encode('key1'), ColumnPath('Standard1', utf8encode('y'), utf8encode('x')), 0, ConsistencyLevel.ONE), InvalidRequestException)
        # key too long
        _expect_exception(lambda: client.get(utf8encode('x' * 2 ** 16), ColumnPath('Standard1', column=utf8encode('c1')), ConsistencyLevel.ONE), InvalidRequestException)
        # empty key
        _expect_exception(lambda: client.get(utf8encode(''), ColumnPath('Standard1', column=utf8encode('c1')), ConsistencyLevel.ONE), InvalidRequestException)
        cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c)) for c in _SUPER_COLUMNS],
                 'Super2': [Mutation(ColumnOrSuperColumn(super_column=c)) for c in _SUPER_COLUMNS]}
        _expect_exception(lambda: client.batch_mutate({utf8encode(''): cfmap}, ConsistencyLevel.ONE), InvalidRequestException)
        # empty column name
        _expect_exception(lambda: client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('')), ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify column name
        _expect_exception(lambda: client.get(utf8encode('key1'), ColumnPath('Standard1'), ConsistencyLevel.ONE), InvalidRequestException)
        # supercolumn in a non-super CF
        _expect_exception(lambda: client.get(utf8encode('key1'), ColumnPath('Standard1', utf8encode('x'), utf8encode('y')), ConsistencyLevel.ONE), InvalidRequestException)
        # get doesn't specify supercolumn name
        _expect_exception(lambda: client.get(utf8encode('key1'), ColumnPath('Super1'), ConsistencyLevel.ONE), InvalidRequestException)
        # invalid CF
        _expect_exception(lambda: get_range_slice(client, ColumnParent('S'), SlicePredicate(column_names=[utf8encode(''), utf8encode('')]), utf8encode(''), utf8encode(''), 5, ConsistencyLevel.ONE), InvalidRequestException)
        # 'x' is not a valid Long
        _expect_exception(lambda: client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc1')), Column(utf8encode('x'), utf8encode('value'), 0), ConsistencyLevel.ONE), InvalidRequestException)
        # start is not a valid Long
        p = SlicePredicate(slice_range=SliceRange(utf8encode('x'), utf8encode(''), False, 1))
        column_parent = ColumnParent('StandardLong1')
        _expect_exception(lambda: client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish
        p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        column_parent = ColumnParent('StandardLong1')
        _expect_exception(lambda: client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start is not a valid Long, supercolumn version
        p = SlicePredicate(slice_range=SliceRange(utf8encode('x'), utf8encode(''), False, 1))
        column_parent = ColumnParent('Super1', utf8encode('sc1'))
        _expect_exception(lambda: client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish, supercolumn version
        p = SlicePredicate(slice_range=SliceRange(_i64(10), _i64(0), False, 1))
        column_parent = ColumnParent('Super1', utf8encode('sc1'))
        _expect_exception(lambda: client.get_slice(utf8encode('key1'), column_parent, p, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # start > finish, key version
        _expect_exception(lambda: get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('')]), utf8encode('z'), utf8encode('a'), 1, ConsistencyLevel.ONE), InvalidRequestException)
        # ttl must be greater or equals to zero
        column = Column(utf8encode('cttl1'), utf8encode('value1'), 0, -1)
        _expect_exception(lambda: client.insert(utf8encode('key1'), ColumnParent('Standard1'), column, ConsistencyLevel.ONE),
                          InvalidRequestException)
        # don't allow super_column in Deletion for standard Columntest_expiration_with_default_ttl_and_zero_ttl
        deletion = Deletion(1, utf8encode('supercolumn'), None)
        mutation = Mutation(deletion=deletion)
        mutations = {utf8encode('key'): {'Standard1': [mutation]}}
        _expect_exception(lambda: client.batch_mutate(mutations, ConsistencyLevel.QUORUM),
                          InvalidRequestException)
        # 'x' is not a valid long
        deletion = Deletion(1, utf8encode('x'), None)
        mutation = Mutation(deletion=deletion)
        mutations = {utf8encode('key'): {'Super3': [mutation]}}
        _expect_exception(lambda: client.batch_mutate(mutations, ConsistencyLevel.QUORUM), InvalidRequestException)
        # counters don't support ANY
        _expect_exception(lambda: client.add(utf8encode('key1'), ColumnParent('Counter1', utf8encode('x')), CounterColumn(utf8encode('y'), 1), ConsistencyLevel.ANY), InvalidRequestException)

    def test_batch_insert_super(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1', 'Super2')

        cfmap = {'Super1': [Mutation(ColumnOrSuperColumn(super_column=c))
                            for c in _SUPER_COLUMNS],
                 'Super2': [Mutation(ColumnOrSuperColumn(super_column=c))
                            for c in _SUPER_COLUMNS]}
        client.batch_mutate({utf8encode('key1'): cfmap}, ConsistencyLevel.ONE)
        _verify_super('Super1')
        _verify_super('Super2')

    def test_cf_remove_column(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        _insert_simple()
        client.remove(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('c1')), 1, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('c1')), ConsistencyLevel.ONE))
        assert client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('c2')), ConsistencyLevel.ONE).column \
            == Column(utf8encode('c2'), utf8encode('value2'), 0)
        assert _big_slice(utf8encode('key1'), ColumnParent('Standard1')) \
            == [ColumnOrSuperColumn(column=Column(utf8encode('c2'), utf8encode('value2'), 0))]

        # New insert, make sure it shows up post-remove:
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c3'), utf8encode('value3'), 0), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice(utf8encode('key1'), ColumnParent('Standard1'))]
        assert columns == [Column(utf8encode('c2'), utf8encode('value2'), 0), Column(utf8encode('c3'), utf8encode('value3'), 0)], columns

        # Test resurrection.  First, re-insert the value w/ older timestamp,
        # and make sure it stays removed
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c1'), utf8encode('value1'), 0), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice(utf8encode('key1'), ColumnParent('Standard1'))]
        assert columns == [Column(utf8encode('c2'), utf8encode('value2'), 0), Column(utf8encode('c3'), utf8encode('value3'), 0)], columns
        # Next, w/ a newer timestamp; it should come back:
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c1'), utf8encode('value1'), 2), ConsistencyLevel.ONE)
        columns = [result.column
                   for result in _big_slice(utf8encode('key1'), ColumnParent('Standard1'))]
        assert columns == [Column(utf8encode('c1'), utf8encode('value1'), 2), Column(utf8encode('c2'), utf8encode('value2'), 0), Column(utf8encode('c3'), utf8encode('value3'), 0)], columns

    def test_cf_remove(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Super1')

        _insert_simple()
        _insert_super()

        # Remove the key1:Standard1 cf; verify super is unaffected
        client.remove(utf8encode('key1'), ColumnPath('Standard1'), 3, ConsistencyLevel.ONE)
        assert _big_slice(utf8encode('key1'), ColumnParent('Standard1')) == []
        _verify_super()

        # Test resurrection.  First, re-insert a value w/ older timestamp,
        # and make sure it stays removed:
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c1'), utf8encode('value1'), 0), ConsistencyLevel.ONE)
        assert _big_slice(utf8encode('key1'), ColumnParent('Standard1')) == []
        # Next, w/ a newer timestamp; it should come back:
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), Column(utf8encode('c1'), utf8encode('value1'), 4), ConsistencyLevel.ONE)
        result = _big_slice(utf8encode('key1'), ColumnParent('Standard1'))
        assert result == [ColumnOrSuperColumn(column=Column(utf8encode('c1'), utf8encode('value1'), 4))], result

        # check removing the entire super cf, too.
        client.remove(utf8encode('key1'), ColumnPath('Super1'), 3, ConsistencyLevel.ONE)
        assert _big_slice(utf8encode('key1'), ColumnParent('Super1')) == []
        assert _big_slice(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc1'))) == []

    def test_super_cf_remove_and_range_slice(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        client.insert(utf8encode('key3'), ColumnParent('Super1', utf8encode('sc1')), Column(_i64(1), utf8encode('v1'), 0), ConsistencyLevel.ONE)
        client.remove(utf8encode('key3'), ColumnPath('Super1', utf8encode('sc1')), 5, ConsistencyLevel.ONE)

        rows = {}
        for row in get_range_slice(client, ColumnParent('Super1'), SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1000)), utf8encode(''), utf8encode(''), 1000, ConsistencyLevel.ONE):
            scs = [cosc.super_column for cosc in row.columns]
            rows[row.key] = scs
        assert rows == {utf8encode('key3'): []}, rows

    def test_super_cf_remove_column(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Super1')

        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove(utf8encode('key1'), ColumnPath('Super1', utf8encode('sc2'), _i64(5)), 5, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Super1', utf8encode('sc2'), _i64(5)), ConsistencyLevel.ONE))
        super_columns = [result.super_column for result in _big_slice(utf8encode('key1'), ColumnParent('Super1'))]
        assert super_columns == [SuperColumn(name=utf8encode('sc1'), columns=[Column(_i64(4), utf8encode('value4'), 0)]),
                                 SuperColumn(name=utf8encode('sc2'), columns=[Column(_i64(6), utf8encode('value6'), 0)])]
        _verify_simple()

        # New insert, make sure it shows up post-remove:
        client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), Column(_i64(7), utf8encode('value7'), 0), ConsistencyLevel.ONE)
        super_columns_expected = [SuperColumn(name=utf8encode('sc1'),
                                              columns=[Column(_i64(4), utf8encode('value4'), 0)]),
                                  SuperColumn(name=utf8encode('sc2'),
                                              columns=[Column(_i64(6), utf8encode('value6'), 0), Column(_i64(7), utf8encode('value7'), 0)])]

        super_columns = [result.super_column for result in _big_slice(utf8encode('key1'), ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Test resurrection.  First, re-insert the value w/ older timestamp,
        # and make sure it stays removed:
        client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), Column(_i64(5), utf8encode('value5'), 0), ConsistencyLevel.ONE)

        super_columns = [result.super_column for result in _big_slice(utf8encode('key1'), ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), Column(_i64(5), utf8encode('value5'), 6), ConsistencyLevel.ONE)
        super_columns = [result.super_column for result in _big_slice(utf8encode('key1'), ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name=utf8encode('sc1'), columns=[Column(_i64(4), utf8encode('value4'), 0)]),
                                  SuperColumn(name=utf8encode('sc2'), columns=[Column(_i64(5), utf8encode('value5'), 6),
                                                                   Column(_i64(6), utf8encode('value6'), 0),
                                                                   Column(_i64(7), utf8encode('value7'), 0)])]
        assert super_columns == super_columns_expected, super_columns

        # shouldn't be able to specify a column w/o a super column for remove
        cp = ColumnPath(column_family='Super1', column=utf8encode('sc2'))
        e = _expect_exception(lambda: client.remove(utf8encode('key1'), cp, 5, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("column cannot be specified without") >= 0

    def test_super_cf_remove_supercolumn(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Super1')

        _insert_simple()
        _insert_super()

        # Make sure remove clears out what it's supposed to, and _only_ that:
        client.remove(utf8encode('key1'), ColumnPath('Super1', utf8encode('sc2')), 5, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Super1', utf8encode('sc2'), _i64(5)), ConsistencyLevel.ONE))
        super_columns = _big_slice(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')))
        assert super_columns == [], super_columns
        super_columns_expected = [SuperColumn(name=utf8encode('sc1'), columns=[Column(_i64(4), utf8encode('value4'), 0)])]
        super_columns = [result.super_column
                         for result in _big_slice(utf8encode('key1'), ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns
        _verify_simple()

        # Test resurrection.  First, re-insert the value w/ older timestamp,
        # and make sure it stays removed:
        client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), Column(_i64(5), utf8encode('value5'), 1), ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice(utf8encode('key1'), ColumnParent('Super1'))]
        assert super_columns == super_columns_expected, super_columns

        # Next, w/ a newer timestamp; it should come back
        client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), Column(_i64(5), utf8encode('value5'), 6), ConsistencyLevel.ONE)
        super_columns = [result.super_column
                         for result in _big_slice(utf8encode('key1'), ColumnParent('Super1'))]
        super_columns_expected = [SuperColumn(name=utf8encode('sc1'), columns=[Column(_i64(4), utf8encode('value4'), 0)]),
                                  SuperColumn(name=utf8encode('sc2'), columns=[Column(_i64(5), utf8encode('value5'), 6)])]
        assert super_columns == super_columns_expected, super_columns

        # check slicing at the subcolumn level too
        p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1000))
        columns = [result.column
                   for result in client.get_slice(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), p, ConsistencyLevel.ONE)]
        assert columns == [Column(_i64(5), utf8encode('value5'), 6)], columns

    def test_super_cf_resurrect_subcolumn(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        key = utf8encode('vijay')
        client.insert(key, ColumnParent('Super1', utf8encode('sc1')), Column(_i64(4), utf8encode('value4'), 0), ConsistencyLevel.ONE)

        client.remove(key, ColumnPath('Super1', utf8encode('sc1')), 1, ConsistencyLevel.ONE)

        client.insert(key, ColumnParent('Super1', utf8encode('sc1')), Column(_i64(4), utf8encode('value4'), 2), ConsistencyLevel.ONE)

        result = client.get(key, ColumnPath('Super1', utf8encode('sc1')), ConsistencyLevel.ONE)
        assert result.super_column.columns is not None, result.super_column

    def test_empty_range(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Super1')

        assert get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('c1'), utf8encode('c1')]), utf8encode(''), utf8encode(''), 1000, ConsistencyLevel.ONE) == []
        _insert_simple()
        assert get_range_slice(client, ColumnParent('Super1'), SlicePredicate(column_names=[utf8encode('c1'), utf8encode('c1')]), utf8encode(''), utf8encode(''), 1000, ConsistencyLevel.ONE) == []

    @since('2.1')
    def test_super_cql_read_compatibility(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        _insert_super(utf8encode("key1"))
        _insert_super(utf8encode("key2"))

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        session.execute('USE "Keyspace1"')

        assert_all(session, "SELECT * FROM \"Super1\"",
                   [[utf8encode("key1"), utf8encode("sc1"), 4, utf8encode("value4")],
                    [utf8encode("key1"), utf8encode("sc2"), 5, utf8encode("value5")],
                    [utf8encode("key1"), utf8encode("sc2"), 6, utf8encode("value6")],
                    [utf8encode("key2"), utf8encode("sc1"), 4, utf8encode("value4")],
                    [utf8encode("key2"), utf8encode("sc2"), 5, utf8encode("value5")],
                    [utf8encode("key2"), utf8encode("sc2"), 6, utf8encode("value6")]])

        assert_all(session, "SELECT * FROM \"Super1\" WHERE key=textAsBlob('key1')",
                   [[utf8encode("key1"), utf8encode("sc1"), 4, utf8encode("value4")],
                    [utf8encode("key1"), utf8encode("sc2"), 5, utf8encode("value5")],
                    [utf8encode("key1"), utf8encode("sc2"), 6, utf8encode("value6")]])

        assert_all(session, "SELECT * FROM \"Super1\" WHERE key=textAsBlob('key1') AND column1=textAsBlob('sc2')",
                   [[utf8encode("key1"), utf8encode("sc2"), 5, utf8encode("value5")],
                    [utf8encode("key1"), utf8encode("sc2"), 6, utf8encode("value6")]])

        assert_all(session, "SELECT * FROM \"Super1\" WHERE key=textAsBlob('key1') AND column1=textAsBlob('sc2') AND column2 = 5",
                   [[utf8encode("key1"), utf8encode("sc2"), 5, utf8encode("value5")]])

        assert_all(session, "SELECT * FROM \"Super1\" WHERE key = textAsBlob('key1') AND column1 = textAsBlob('sc2')",
                   [[utf8encode("key1"), utf8encode("sc2"), 5, utf8encode("value5")],
                    [utf8encode("key1"), utf8encode("sc2"), 6, utf8encode("value6")]])

        assert_all(session, "SELECT column2, value FROM \"Super1\" WHERE key = textAsBlob('key1') AND column1 = textAsBlob('sc2')",
                   [[5, utf8encode("value5")],
                    [6, utf8encode("value6")]])

    @since('2.1')
    def test_super_cql_write_compatibility(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        session.execute('USE "Keyspace1"')

        query = "INSERT INTO \"Super1\" (key, column1, column2, value) VALUES (textAsBlob(%s), textAsBlob(%s), %s, textAsBlob(%s)) USING TIMESTAMP 1234"
        session.execute(query, ("key1", "sc1", 4, "value4"))
        session.execute(query, ("key1", "sc2", 5, "value5"))
        session.execute(query, ("key1", "sc2", 6, "value6"))
        session.execute(query, ("key2", "sc1", 4, "value4"))
        session.execute(query, ("key2", "sc2", 5, "value5"))
        session.execute(query, ("key2", "sc2", 6, "value6"))

        p = SlicePredicate(slice_range=SliceRange(utf8encode('sc1'), utf8encode('sc2'), False, 2))
        result = client.get_slice(utf8encode('key1'), ColumnParent('Super1'), p, ConsistencyLevel.ONE)
        assert_length_equal(result, 2)
        assert result[0].super_column.name == utf8encode('sc1')
        assert result[0].super_column.columns[0], Column(_i64(4), utf8encode('value4') == 1234)
        assert result[1].super_column.name == utf8encode('sc2')
        assert result[1].super_column.columns, [Column(_i64(5), utf8encode('value5'), 1234), Column(_i64(6), utf8encode('value6') == 1234)]

    def test_range_with_remove(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        _insert_simple()
        assert get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('c1'), utf8encode('c1')]), utf8encode('key1'), utf8encode(''), 1000, ConsistencyLevel.ONE)[0].key == utf8encode('key1')

        client.remove(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('c1')), 1, ConsistencyLevel.ONE)
        client.remove(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('c2')), 1, ConsistencyLevel.ONE)
        actual = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('c1'), utf8encode('c2')]), utf8encode(''), utf8encode(''), 1000, ConsistencyLevel.ONE)
        assert actual == [KeySlice(columns=[], key=utf8encode('key1'))], actual

    def test_range_with_remove_cf(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        _insert_simple()
        assert get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('c1'), utf8encode('c1')]), utf8encode('key1'), utf8encode(''), 1000, ConsistencyLevel.ONE)[0].key == utf8encode('key1')

        client.remove(utf8encode('key1'), ColumnPath('Standard1'), 1, ConsistencyLevel.ONE)
        actual = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('c1'), utf8encode('c1')]), utf8encode(''), utf8encode(''), 1000, ConsistencyLevel.ONE)
        assert actual == [KeySlice(columns=[], key=utf8encode('key1'))], actual

    def test_range_collation(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in range(100)]:
            key = utf8encode(key)
            client.insert(key, ColumnParent('Standard1'), Column(key, utf8encode('v'), 0), ConsistencyLevel.ONE)

        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('-a'), utf8encode('-a')]), utf8encode(''), utf8encode(''), 1000, ConsistencyLevel.ONE)
        L = ['-a', '-b', '0', '1', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '2', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '3', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '4', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '5', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '6', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '7', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '8', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '9', '90', '91', '92', '93', '94', '95', '96', '97', '98', '99', 'a', 'b']
        assert len(slices) == len(L)
        for key, ks in zip(L, slices):
            key = utf8encode(key)
            assert key == ks.key

    def test_range_partial(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        for key in ['-a', '-b', 'a', 'b'] + [str(i) for i in range(100)]:
            key = utf8encode(key)
            client.insert(key, ColumnParent('Standard1'), Column(key, utf8encode('v'), 0), ConsistencyLevel.ONE)

        def check_slices_against_keys(keyList, sliceList):
            assert len(keyList) == len(sliceList), "%d vs %d" % (len(keyList), len(sliceList))
            for key, ks in zip(keyList, sliceList):
                key = utf8encode(key)
                assert key == ks.key

        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('-a'), utf8encode('-a')]), utf8encode('a'), utf8encode(''), 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['a', 'b'], slices)

        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('-a'), utf8encode('-a')]), utf8encode(''), utf8encode('15'), 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['-a', '-b', '0', '1', '10', '11', '12', '13', '14', '15'], slices)

        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('-a'), utf8encode('-a')]), utf8encode('50'), utf8encode('51'), 1000, ConsistencyLevel.ONE)
        check_slices_against_keys(['50', '51'], slices)

        slices = get_range_slice(client, ColumnParent('Standard1'), SlicePredicate(column_names=[utf8encode('-a'), utf8encode('-a')]), utf8encode('1'), utf8encode(''), 10, ConsistencyLevel.ONE)
        check_slices_against_keys(['1', '10', '11', '12', '13', '14', '15', '16', '17', '18'], slices)

    def test_get_slice_range(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        _insert_range()
        _verify_range()

    def test_get_slice_super_range(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        _insert_super_range()
        _verify_super_range()

    def test_get_range_slices_tokens(self):
        _set_keyspace('Keyspace2')
        self.truncate_all('Super3')

        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            key = utf8encode(key)
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                cnameutf = utf8encode(cname)
                client.insert(key, ColumnParent('Super3', utf8encode('sc1')), Column(cnameutf, utf8encode('v-' + cname), 0), ConsistencyLevel.ONE)

        cp = ColumnParent('Super3', utf8encode('sc1'))
        predicate = SlicePredicate(column_names=[utf8encode('col1'), utf8encode('col3')])
        range = KeyRange(start_token='55', end_token='55', count=100)
        result = client.get_range_slices(cp, predicate, range, ConsistencyLevel.ONE)
        assert len(result) == 5
        assert result[0].columns[0].column.name == utf8encode('col1')
        assert result[0].columns[1].column.name == utf8encode('col3')

    def test_get_range_slice_super(self):
        _set_keyspace('Keyspace2')
        self.truncate_all('Super3')

        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            key = utf8encode(key)
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                cnameutf = utf8encode(cname)
                client.insert(key, ColumnParent('Super3', utf8encode('sc1')), Column(cnameutf, utf8encode('v-' + cname), 0), ConsistencyLevel.ONE)

        cp = ColumnParent('Super3', utf8encode('sc1'))
        result = get_range_slice(client, cp, SlicePredicate(column_names=[utf8encode('col1'), utf8encode('col3')]), utf8encode('key2'), utf8encode('key4'), 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert result[0].columns[0].column.name == utf8encode('col1')
        assert result[0].columns[1].column.name == utf8encode('col3')

        cp = ColumnParent('Super3')
        result = get_range_slice(client, cp, SlicePredicate(column_names=[utf8encode('sc1')]), utf8encode('key2'), utf8encode('key4'), 5, ConsistencyLevel.ONE)
        assert len(result) == 3
        assert list(set(row.columns[0].super_column.name for row in result))[0] == utf8encode('sc1')

    def test_get_range_slice(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        for key in ['key1', 'key2', 'key3', 'key4', 'key5']:
            key = utf8encode(key)
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                cnameutf = utf8encode(cname)
                client.insert(key, ColumnParent('Standard1'), Column(cnameutf, utf8encode('v-' + cname), 0), ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        # test empty slice
        result = get_range_slice(client, cp, SlicePredicate(column_names=[utf8encode('col1'), utf8encode('col3')]), utf8encode('key6'), utf8encode(''), 1, ConsistencyLevel.ONE)
        assert len(result) == 0

        # test empty columns
        result = get_range_slice(client, cp, SlicePredicate(column_names=[utf8encode('a')]), utf8encode('key2'), utf8encode(''), 1, ConsistencyLevel.ONE)
        assert len(result) == 1
        assert len(result[0].columns) == 0

        # test column_names predicate
        result = get_range_slice(client, cp, SlicePredicate(column_names=[utf8encode('col1'), utf8encode('col3')]), utf8encode('key2'), utf8encode('key4'), 5, ConsistencyLevel.ONE)
        assert len(result) == 3, result
        assert result[0].columns[0].column.name == utf8encode('col1')
        assert result[0].columns[1].column.name == utf8encode('col3')

        # row limiting via count.
        result = get_range_slice(client, cp, SlicePredicate(column_names=[utf8encode('col1'), utf8encode('col3')]), utf8encode('key2'), utf8encode('key4'), 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # test column slice predicate
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start=utf8encode('col2'), finish=utf8encode('col4'), reversed=False, count=5)), utf8encode('key1'), utf8encode('key2'), 5, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].key == utf8encode('key1')
        assert result[1].key == utf8encode('key2')
        assert len(result[0].columns) == 3
        assert result[0].columns[0].column.name == utf8encode('col2')
        assert result[0].columns[2].column.name == utf8encode('col4')

        # col limiting via count
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start=utf8encode('col2'), finish=utf8encode('col4'), reversed=False, count=2)), utf8encode('key1'), utf8encode('key2'), 5, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 2

        # and reversed
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start=utf8encode('col4'), finish=utf8encode('col2'), reversed=True, count=5)), utf8encode('key1'), utf8encode('key2'), 5, ConsistencyLevel.ONE)
        assert result[0].columns[0].column.name == utf8encode('col4')
        assert result[0].columns[2].column.name == utf8encode('col2')

        # row limiting via count
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(start=utf8encode('col2'), finish=utf8encode('col4'), reversed=False, count=5)), utf8encode('key1'), utf8encode('key2'), 1, ConsistencyLevel.ONE)
        assert len(result) == 1

        # removed data
        client.remove(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('col1')), 1, ConsistencyLevel.ONE)
        result = get_range_slice(client, cp, SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''))), utf8encode('key1'), utf8encode('key2'), 5, ConsistencyLevel.ONE)
        assert len(result) == 2, result
        assert result[0].columns[0].column.name == utf8encode('col2'), result[0].columns[0].column.name
        assert result[1].columns[0].column.name == utf8encode('col1')

    def test_wrapped_range_slices(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        def copp_token(key):
            # I cheated and generated this from Java
            return {'a': '00530000000100000001',
                    'b': '00540000000100000001',
                    'c': '00550000000100000001',
                    'd': '00560000000100000001',
                    'e': '00580000000100000001'}[key]
        keylist = [utf8encode(key) for key in ['a', 'b', 'c', 'd', 'e']]
        for key in keylist:
            for cname in ['col1', 'col2', 'col3', 'col4', 'col5']:
                cnameutf = utf8encode(cname)
                client.insert(key, ColumnParent('Standard1'), Column(cnameutf, utf8encode('v-' + cname), 0), ConsistencyLevel.ONE)
        cp = ColumnParent('Standard1')

        result = client.get_range_slices(cp, SlicePredicate(column_names=[utf8encode('col1'), utf8encode('col3')]), KeyRange(start_token=copp_token('e'), end_token=copp_token('e')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == keylist, [row.key for row in result]

        result = client.get_range_slices(cp, SlicePredicate(column_names=[utf8encode('col1'), utf8encode('col3')]), KeyRange(start_token=copp_token('c'), end_token=copp_token('c')), ConsistencyLevel.ONE)
        assert [row.key for row in result] == keylist, [row.key for row in result]

    def test_get_slice_by_names(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1', 'Super1')

        _insert_range()
        p = SlicePredicate(column_names=[utf8encode('c1'), utf8encode('c2')])
        result = client.get_slice(utf8encode('key1'), ColumnParent('Standard1'), p, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].column.name == utf8encode('c1')
        assert result[1].column.name == utf8encode('c2')

        _insert_super()
        p = SlicePredicate(column_names=[_i64(4)])
        result = client.get_slice(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc1')), p, ConsistencyLevel.ONE)
        assert len(result) == 1
        assert result[0].column.name == _i64(4)

    def test_multiget_slice_with_compact_table(self):
        """Insert multiple keys in a compact table and retrieve them using the multiget_slice interface"""
        _set_keyspace('Keyspace1')

        # create
        cd = ColumnDef(utf8encode('v'), 'AsciiType', None, None)
        newcf = CfDef('Keyspace1', 'CompactColumnFamily', default_validation_class='AsciiType', column_metadata=[cd])
        client.system_add_column_family(newcf)

        CL = ConsistencyLevel.ONE
        for i in range(0, 5):
            client.insert(utf8encode('key' + str(i)), ColumnParent('CompactColumnFamily'), Column(utf8encode('v'), utf8encode('value' + str(i)), 0), CL)
        time.sleep(0.1)

        p = SlicePredicate(column_names=[utf8encode('v')])
        rows = client.multiget_slice([utf8encode('key' + str(i)) for i in range(0, 5)], ColumnParent('CompactColumnFamily'), p, ConsistencyLevel.ONE)

        for i in range(0, 5):
            key = utf8encode('key' + str(i))
            assert key in rows
            assert len(rows[key]) == 1
            assert rows[key][0].column.name == utf8encode('v')
            assert rows[key][0].column.value == utf8encode('value' + str(i))

    def test_multiget_slice(self):
        """Insert multiple keys and retrieve them using the multiget_slice interface"""
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        # Generate a list of 10 keys and insert them
        num_keys = 10
        keys = [utf8encode('key' + str(i)) for i in range(1, num_keys + 1)]
        _insert_multi(keys)

        # Retrieve all 10 key slices
        rows = _big_multislice(keys, ColumnParent('Standard1'))

        columns = [ColumnOrSuperColumn(c) for c in _SIMPLE_COLUMNS]
        # Validate if the returned rows have the keys requested and if the ColumnOrSuperColumn is what was inserted
        for key in keys:
            assert key in rows
            assert columns == rows[key]

    def test_multi_count(self):
        """Insert multiple keys and count them using the multiget interface"""
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        # Generate a list of 10 keys countaining 1 to 10 columns and insert them
        num_keys = 10
        for i in range(1, num_keys + 1):
            key = utf8encode('key' + str(i))
            for j in range(1, i + 1):
                client.insert(key, ColumnParent('Standard1'), Column(utf8encode('c' + str(j)), utf8encode('value' + str(j)), 0), ConsistencyLevel.ONE)

        # Count columns in all 10 keys
        keys = [utf8encode('key' + str(i)) for i in range(1, num_keys + 1)]
        p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1000))
        counts = client.multiget_count(keys, ColumnParent('Standard1'), p, ConsistencyLevel.ONE)

        # Check the returned counts
        for i in range(1, num_keys + 1):
            key = utf8encode('key' + str(i))
            assert counts[key] == i

    def test_batch_mutate_super_deletion(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        _insert_super('test')
        d = Deletion(1, predicate=SlicePredicate(column_names=[utf8encode('sc1')]))
        cfmap = {'Super1': [Mutation(deletion=d)]}
        client.batch_mutate({utf8encode('test'): cfmap}, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Super1', utf8encode('sc1')), ConsistencyLevel.ONE))

    def test_super_reinsert(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Super1')

        for x in range(3):
            client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), Column(_i64(x), utf8encode('value'), 1), ConsistencyLevel.ONE)

        client.remove(utf8encode('key1'), ColumnPath('Super1'), 2, ConsistencyLevel.ONE)

        for x in range(3):
            client.insert(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), Column(_i64(x + 3), utf8encode('value'), 3), ConsistencyLevel.ONE)

        for n in range(1, 4):
            p = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, n))
            slice = client.get_slice(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc2')), p, ConsistencyLevel.ONE)
            assert len(slice) == n, "expected %s results; found %s" % (n, slice)

    def test_describe_keyspace(self):
        try:
            client.system_drop_keyspace("ValidKsForUpdate")
        except InvalidRequestException:
            pass  # The keyspace doesn't exit, because this test was run in isolation.

        kspaces = client.describe_keyspaces()
        if self.cluster.version() >= '3.0':
            assert len(kspaces) == 7, [x.name for x in kspaces]  # ['Keyspace2', 'Keyspace1', 'system', 'system_traces', 'system_auth', 'system_distributed', 'system_schema']
        elif self.cluster.version() >= '2.2':
            assert len(kspaces) == 6, [x.name for x in kspaces]  # ['Keyspace2', 'Keyspace1', 'system', 'system_traces', 'system_auth', 'system_distributed']
        else:
            assert len(kspaces) == 4, [x.name for x in kspaces]  # ['Keyspace2', 'Keyspace1', 'system', 'system_traces']

        sysks = client.describe_keyspace("system")
        assert sysks in kspaces

        ks1 = client.describe_keyspace("Keyspace1")
        assert ks1.strategy_options['replication_factor'] == '1', ks1.strategy_options
        for cf in ks1.cf_defs:
            if cf.name == "Standard1":
                cf0 = cf
                break
        assert cf0.comparator_type == "org.apache.cassandra.db.marshal.BytesType"

    def test_describe(self):
        assert client.describe_cluster_name() == 'test'

    def test_describe_ring(self):
        assert list(client.describe_ring('Keyspace1'))[0].endpoints == ['127.0.0.1']

    def test_describe_token_map(self):
        # test/conf/cassandra.yaml specifies org.apache.cassandra.dht.ByteOrderedPartitioner
        # which uses BytesToken, so this just tests that the string representation of the token
        # matches a regex pattern for BytesToken.toString().
        ring = list(client.describe_token_map().items())
        if not self.dtest_config.use_vnodes:
            assert len(ring) == 1
        else:
            assert len(ring) == int(self.dtest_config.num_tokens)
        token, node = ring[0]
        if self.dtest_config.use_vnodes:
            assert re.match("[0-9A-Fa-f]{32}", token)
        assert node == '127.0.0.1'

    def test_describe_partitioner(self):
        # Make sure this just reads back the values from the config.
        assert client.describe_partitioner() == "org.apache.cassandra.dht.ByteOrderedPartitioner"

    def test_describe_snitch(self):
        assert client.describe_snitch() == "org.apache.cassandra.locator.SimpleSnitch"

    def test_invalid_ks_names(self):
        def invalid_keyspace():
            client.system_add_keyspace(KsDef('in-valid', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor': '1'}, cf_defs=[]))
        _expect_exception(invalid_keyspace, InvalidRequestException)

    def test_invalid_strategy_class(self):
        def add_invalid_keyspace():
            client.system_add_keyspace(KsDef('ValidKs', 'InvalidStrategyClass', {}, cf_defs=[]))
        exc = _expect_exception(add_invalid_keyspace, InvalidRequestException)
        s = str(exc)
        assert s.find("InvalidStrategyClass") > -1, s
        assert s.find("Unable to find replication strategy") > -1, s

        def update_invalid_keyspace():
            client.system_add_keyspace(KsDef('ValidKsForUpdate', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor': '1'}, cf_defs=[]))
            client.system_update_keyspace(KsDef('ValidKsForUpdate', 'InvalidStrategyClass', {}, cf_defs=[]))

        exc = _expect_exception(update_invalid_keyspace, InvalidRequestException)
        s = str(exc)
        assert s.find("InvalidStrategyClass") > -1, s
        assert s.find("Unable to find replication strategy") > -1, s

    def test_invalid_cf_names(self):
        def invalid_cf():
            _set_keyspace('Keyspace1')
            newcf = CfDef('Keyspace1', 'in-valid')
            client.system_add_column_family(newcf)
        _expect_exception(invalid_cf, InvalidRequestException)

        def invalid_cf_inside_new_ks():
            cf = CfDef('ValidKsName_invalid_cf', 'in-valid')
            _set_keyspace('system')
            client.system_add_keyspace(KsDef('ValidKsName_invalid_cf', 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor': '1'}, cf_defs=[cf]))
        _expect_exception(invalid_cf_inside_new_ks, InvalidRequestException)

    def test_system_cf_recreate(self):
        "ensures that keyspaces and column familes can be dropped and recreated in short order"
        for x in range(2):

            keyspace = 'test_cf_recreate'
            cf_name = 'recreate_cf'

            # create
            newcf = CfDef(keyspace, cf_name)
            newks = KsDef(keyspace, 'org.apache.cassandra.locator.SimpleStrategy', {'replication_factor': '1'}, cf_defs=[newcf])
            client.system_add_keyspace(newks)
            _set_keyspace(keyspace)

            # insert
            client.insert(utf8encode('key0'), ColumnParent(cf_name), Column(utf8encode('colA'), utf8encode('colA-value'), 0), ConsistencyLevel.ONE)
            col1 = client.get_slice(utf8encode('key0'), ColumnParent(cf_name), SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 100)), ConsistencyLevel.ONE)[0].column
            assert col1.name == utf8encode('colA') and col1.value == utf8encode('colA-value')

            # drop
            client.system_drop_column_family(cf_name)

            # recreate
            client.system_add_column_family(newcf)

            # query
            cosc_list = client.get_slice(utf8encode('key0'), ColumnParent(cf_name), SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 100)), ConsistencyLevel.ONE)
            # this was failing prior to CASSANDRA-1477.
            assert len(cosc_list) == 0, 'cosc length test failed'

            client.system_drop_keyspace(keyspace)

    def test_system_keyspace_operations(self):
        # create.  note large RF, this is OK
        keyspace = KsDef('CreateKeyspace',
                         'org.apache.cassandra.locator.SimpleStrategy',
                         {'replication_factor': '10'},
                         cf_defs=[CfDef('CreateKeyspace', 'CreateKsCf')])
        client.system_add_keyspace(keyspace)
        newks = client.describe_keyspace('CreateKeyspace')
        assert 'CreateKsCf' in [x.name for x in newks.cf_defs]

        _set_keyspace('CreateKeyspace')

        # modify valid
        modified_keyspace = KsDef('CreateKeyspace',
                                  'org.apache.cassandra.locator.OldNetworkTopologyStrategy',
                                  {'replication_factor': '1'},
                                  cf_defs=[])
        client.system_update_keyspace(modified_keyspace)
        modks = client.describe_keyspace('CreateKeyspace')
        assert modks.strategy_class == modified_keyspace.strategy_class
        assert modks.strategy_options == modified_keyspace.strategy_options

        # check strategy options are validated on modify
        def modify_invalid_ks():
            client.system_update_keyspace(KsDef('CreateKeyspace',
                                                'org.apache.cassandra.locator.SimpleStrategy',
                                                {},
                                                cf_defs=[]))
        _expect_exception(modify_invalid_ks, InvalidRequestException)

        # drop
        client.system_drop_keyspace('CreateKeyspace')

        def get_second_ks():
            client.describe_keyspace('CreateKeyspace')
        _expect_exception(get_second_ks, NotFoundException)

        # check strategy options are validated on creation
        def create_invalid_ks():
            client.system_add_keyspace(KsDef('InvalidKeyspace',
                                             'org.apache.cassandra.locator.SimpleStrategy',
                                             {},
                                             cf_defs=[]))
        _expect_exception(create_invalid_ks, InvalidRequestException)

    def test_create_then_drop_ks(self):
        keyspace = KsDef('AddThenDrop',
                         strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                         strategy_options={'replication_factor': '1'},
                         cf_defs=[])

        def test_existence():
            client.describe_keyspace(keyspace.name)
        _expect_exception(test_existence, NotFoundException)
        client.set_keyspace('system')
        client.system_add_keyspace(keyspace)
        test_existence()
        client.system_drop_keyspace(keyspace.name)

    def test_column_validators(self):
        # columndef validation for regular CF
        ks = 'Keyspace1'
        _set_keyspace(ks)
        cd = ColumnDef(utf8encode('col'), 'LongType', None, None)
        cf = CfDef('Keyspace1', 'ValidatorColumnFamily', column_metadata=[cd])
        client.system_add_column_family(cf)
        ks_def = client.describe_keyspace(ks)
        assert 'ValidatorColumnFamily' in [x.name for x in ks_def.cf_defs]

        cp = ColumnParent('ValidatorColumnFamily')
        col0 = Column(utf8encode('col'), _i64(42), 0)
        col1 = Column(utf8encode('col'), utf8encode("ceci n'est pas 64bit"), 0)
        client.insert(utf8encode('key0'), cp, col0, ConsistencyLevel.ONE)
        e = _expect_exception(lambda: client.insert(utf8encode('key1'), cp, col1, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("failed validation") >= 0

        # columndef validation for super CF
        scf = CfDef('Keyspace1', 'ValidatorSuperColumnFamily', column_type='Super', column_metadata=[cd])
        client.system_add_column_family(scf)
        ks_def = client.describe_keyspace(ks)
        assert 'ValidatorSuperColumnFamily' in [x.name for x in ks_def.cf_defs]

        scp = ColumnParent('ValidatorSuperColumnFamily', utf8encode('sc1'))
        client.insert(utf8encode('key0'), scp, col0, ConsistencyLevel.ONE)
        e = _expect_exception(lambda: client.insert(utf8encode('key1'), scp, col1, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("failed validation") >= 0

        # columndef and cfdef default validation
        cf = CfDef('Keyspace1', 'DefaultValidatorColumnFamily', column_metadata=[cd], default_validation_class='UTF8Type')
        client.system_add_column_family(cf)
        ks_def = client.describe_keyspace(ks)
        assert 'DefaultValidatorColumnFamily' in [x.name for x in ks_def.cf_defs]

        dcp = ColumnParent('DefaultValidatorColumnFamily')
        # inserting a longtype into column 'col' is valid at the columndef level
        client.insert(utf8encode('key0'), dcp, col0, ConsistencyLevel.ONE)
        # inserting a UTF8type into column 'col' fails at the columndef level
        e = _expect_exception(lambda: client.insert(utf8encode('key1'), dcp, col1, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("failed validation") >= 0

        # insert a longtype into column 'fcol' should fail at the cfdef level
        col2 = Column(utf8encode('fcol'), _i64(4224), 0)
        e = _expect_exception(lambda: client.insert(utf8encode('key1'), dcp, col2, ConsistencyLevel.ONE), InvalidRequestException)
        assert e.why.find("failed validation") >= 0
        # insert a UTF8type into column 'fcol' is valid at the cfdef level
        col3 = Column(utf8encode('fcol'), utf8encode("Stringin' it up in the Stringtel Stringifornia"), 0)
        client.insert(utf8encode('key0'), dcp, col3, ConsistencyLevel.ONE)

    def test_system_column_family_operations(self):
        _set_keyspace('Keyspace1')
        # create
        cd = ColumnDef(utf8encode('ValidationColumn'), 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'NewColumnFamily', column_metadata=[cd])
        client.system_add_column_family(newcf)
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewColumnFamily' in [x.name for x in ks1.cf_defs]
        cfid = [x.id for x in ks1.cf_defs if x.name == 'NewColumnFamily'][0]

        # modify invalid
        modified_cf = CfDef('Keyspace1', 'NewColumnFamily', column_metadata=[cd])
        modified_cf.id = cfid

        def fail_invalid_field():
            modified_cf.comparator_type = 'LongType'
            client.system_update_column_family(modified_cf)
        _expect_exception(fail_invalid_field, InvalidRequestException)

        # modify valid
        modified_cf.comparator_type = 'BytesType'  # revert back to old value.
        modified_cf.gc_grace_seconds = 1
        client.system_update_column_family(modified_cf)
        ks1 = client.describe_keyspace('Keyspace1')
        server_cf = [x for x in ks1.cf_defs if x.name == 'NewColumnFamily'][0]
        assert server_cf
        assert server_cf.gc_grace_seconds == 1

        # drop
        client.system_drop_column_family('NewColumnFamily')
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewColumnFamily' not in [x.name for x in ks1.cf_defs]
        assert 'Standard1' in [x.name for x in ks1.cf_defs]

        # Make a LongType CF and add a validator
        newcf = CfDef('Keyspace1', 'NewLongColumnFamily', comparator_type='LongType')
        client.system_add_column_family(newcf)

        three = _i64(3)
        cd = ColumnDef(three, 'LongType', None, None)
        ks1 = client.describe_keyspace('Keyspace1')
        modified_cf = [x for x in ks1.cf_defs if x.name == 'NewLongColumnFamily'][0]
        modified_cf.column_metadata = [cd]
        client.system_update_column_family(modified_cf)

        ks1 = client.describe_keyspace('Keyspace1')
        server_cf = [x for x in ks1.cf_defs if x.name == 'NewLongColumnFamily'][0]
        assert server_cf.column_metadata[0].name == _i64(3), server_cf.column_metadata

    def test_dynamic_indexes_creation_deletion(self):
        _set_keyspace('Keyspace1')
        cfdef = CfDef('Keyspace1', 'BlankCF')
        client.system_add_column_family(cfdef)

        ks1 = client.describe_keyspace('Keyspace1')
        cfid = [x.id for x in ks1.cf_defs if x.name == 'BlankCF'][0]
        modified_cd = ColumnDef(utf8encode('birthdate'), 'BytesType', IndexType.KEYS, None)
        modified_cf = CfDef('Keyspace1', 'BlankCF', column_metadata=[modified_cd])
        modified_cf.id = cfid
        client.system_update_column_family(modified_cf)

        # Add a second indexed CF ...
        birthdate_coldef = ColumnDef(utf8encode('birthdate'), 'BytesType', IndexType.KEYS, None)
        age_coldef = ColumnDef(utf8encode('age'), 'BytesType', IndexType.KEYS, 'age_index')
        cfdef = CfDef('Keyspace1', 'BlankCF2', column_metadata=[birthdate_coldef, age_coldef])
        client.system_add_column_family(cfdef)

        # ... and update it to have a third index
        ks1 = client.describe_keyspace('Keyspace1')
        cfdef = [x for x in ks1.cf_defs if x.name == 'BlankCF2'][0]
        name_coldef = ColumnDef(utf8encode('name'), 'BytesType', IndexType.KEYS, 'name_index')
        cfdef.column_metadata.append(name_coldef)
        client.system_update_column_family(cfdef)

        # Now drop the indexes
        ks1 = client.describe_keyspace('Keyspace1')
        cfdef = [x for x in ks1.cf_defs if x.name == 'BlankCF2'][0]
        birthdate_coldef = ColumnDef(utf8encode('birthdate'), 'BytesType', None, None)
        age_coldef = ColumnDef(utf8encode('age'), 'BytesType', None, None)
        name_coldef = ColumnDef(utf8encode('name'), 'BytesType', None, None)
        cfdef.column_metadata = [birthdate_coldef, age_coldef, name_coldef]
        client.system_update_column_family(cfdef)

        ks1 = client.describe_keyspace('Keyspace1')
        cfdef = [x for x in ks1.cf_defs if x.name == 'BlankCF'][0]
        birthdate_coldef = ColumnDef(utf8encode('birthdate'), 'BytesType', None, None)
        cfdef.column_metadata = [birthdate_coldef]
        client.system_update_column_family(cfdef)

        client.system_drop_column_family('BlankCF')
        client.system_drop_column_family('BlankCF2')

    def test_dynamic_indexes_with_system_update_cf(self):
        _set_keyspace('Keyspace1')
        cd = ColumnDef(utf8encode('birthdate'), 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'ToBeIndexed', default_validation_class='LongType', column_metadata=[cd])
        client.system_add_column_family(newcf)

        client.insert(utf8encode('key1'), ColumnParent('ToBeIndexed'), Column(utf8encode('birthdate'), _i64(1), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key2'), ColumnParent('ToBeIndexed'), Column(utf8encode('birthdate'), _i64(2), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key2'), ColumnParent('ToBeIndexed'), Column(utf8encode('b'), _i64(2), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key3'), ColumnParent('ToBeIndexed'), Column(utf8encode('birthdate'), _i64(3), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key3'), ColumnParent('ToBeIndexed'), Column(utf8encode('b'), _i64(3), 0), ConsistencyLevel.ONE)

        # First without index
        cp = ColumnParent('ToBeIndexed')
        sp = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode('')))
        key_range = KeyRange(utf8encode(''), utf8encode(''), None, None, [IndexExpression(utf8encode('birthdate'), IndexOperator.EQ, _i64(1))], 100)
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == utf8encode('key1')
        assert len(result[0].columns) == 1, result[0].columns

        # add an index on 'birthdate'
        ks1 = client.describe_keyspace('Keyspace1')
        cfid = [x.id for x in ks1.cf_defs if x.name == 'ToBeIndexed'][0]
        modified_cd = ColumnDef(utf8encode('birthdate'), 'BytesType', IndexType.KEYS, 'bd_index')
        modified_cf = CfDef('Keyspace1', 'ToBeIndexed', column_metadata=[modified_cd])
        modified_cf.id = cfid
        client.system_update_column_family(modified_cf)

        ks1 = client.describe_keyspace('Keyspace1')
        server_cf = [x for x in ks1.cf_defs if x.name == 'ToBeIndexed'][0]
        assert server_cf
        assert server_cf.column_metadata[0].index_type == modified_cd.index_type
        assert server_cf.column_metadata[0].index_name == modified_cd.index_name

        # sleep a bit to give time for the index to build.
        time.sleep(5)

        # repeat query on one index expression
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == utf8encode('key1')
        assert len(result[0].columns) == 1, result[0].columns

    def test_system_super_column_family_operations(self):
        _set_keyspace('Keyspace1')

        # create
        cd = ColumnDef(utf8encode('ValidationColumn'), 'BytesType', None, None)
        newcf = CfDef('Keyspace1', 'NewSuperColumnFamily', 'Super', column_metadata=[cd])
        client.system_add_column_family(newcf)
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewSuperColumnFamily' in [x.name for x in ks1.cf_defs]

        # drop
        client.system_drop_column_family('NewSuperColumnFamily')
        ks1 = client.describe_keyspace('Keyspace1')
        assert 'NewSuperColumnFamily' not in [x.name for x in ks1.cf_defs]
        assert 'Standard1' in [x.name for x in ks1.cf_defs]

    def test_insert_ttl(self):
        """ Test simple insertion of a column with ttl """
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        column = Column(utf8encode('cttl1'), utf8encode('value1'), 0, 5)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        assert client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('cttl1')), ConsistencyLevel.ONE).column == column

    def test_simple_expiration(self):
        """ Test that column ttled do expires """
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        column = Column(utf8encode('cttl3'), utf8encode('value1'), 0, 2)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        c = client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('cttl3')), ConsistencyLevel.ONE).column
        assert c == column
        time.sleep(3)
        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('cttl3')), ConsistencyLevel.ONE))

    def test_expiration_with_default_ttl(self):
        """ Test that column with default ttl do expires """
        _set_keyspace('Keyspace1')
        self.truncate_all('Expiring')

        column = Column(utf8encode('cttl3'), utf8encode('value1'), 0)
        client.insert(utf8encode('key1'), ColumnParent('Expiring'), column, ConsistencyLevel.ONE)
        client.get(utf8encode('key1'), ColumnPath('Expiring', column=utf8encode('cttl3')), ConsistencyLevel.ONE).column
        time.sleep(3)
        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Expiring', column=utf8encode('cttl3')), ConsistencyLevel.ONE))

    @since('3.6')
    def test_expiration_with_default_ttl_and_zero_ttl(self):
        """
        Test that we can remove the default ttl by setting the ttl explicitly to zero
        CASSANDRA-11207
        """
        _set_keyspace('Keyspace1')
        self.truncate_all('Expiring')

        column = Column(utf8encode('cttl3'), utf8encode('value1'), 0, 0)
        client.insert(utf8encode('key1'), ColumnParent('Expiring'), column, ConsistencyLevel.ONE)
        c = client.get(utf8encode('key1'), ColumnPath('Expiring', column=utf8encode('cttl3')), ConsistencyLevel.ONE).column
        assert Column(utf8encode('cttl3'), utf8encode('value1'), 0) == c

    def test_simple_expiration_batch_mutate(self):
        """ Test that column ttled do expires using batch_mutate """
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        column = Column(utf8encode('cttl4'), utf8encode('value1'), 0, 2)
        cfmap = {'Standard1': [Mutation(ColumnOrSuperColumn(column))]}
        client.batch_mutate({utf8encode('key1'): cfmap}, ConsistencyLevel.ONE)
        c = client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('cttl4')), ConsistencyLevel.ONE).column
        assert c == column
        time.sleep(3)
        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('cttl4')), ConsistencyLevel.ONE))

    def test_update_expiring(self):
        """ Test that updating a column with ttl override the ttl """
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        column1 = Column(utf8encode('cttl4'), utf8encode('value1'), 0, 1)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), column1, ConsistencyLevel.ONE)
        column2 = Column(utf8encode('cttl4'), utf8encode('value1'), 1)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), column2, ConsistencyLevel.ONE)
        time.sleep(1.5)
        assert client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('cttl4')), ConsistencyLevel.ONE).column == column2

    def test_remove_expiring(self):
        """ Test removing a column with ttl """
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        column = Column(utf8encode('cttl5'), utf8encode('value1'), 0, 10)
        client.insert(utf8encode('key1'), ColumnParent('Standard1'), column, ConsistencyLevel.ONE)
        client.remove(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('cttl5')), 1, ConsistencyLevel.ONE)
        _expect_missing(lambda: client.get(utf8encode('key1'), ColumnPath('Standard1', column=utf8encode('ctt5')), ConsistencyLevel.ONE))

    def test_describe_ring_on_invalid_keyspace(self):
        def req():
            client.describe_ring('system')
        _expect_exception(req, InvalidRequestException)

    def test_incr_decr_standard_add(self, request):
        _set_keyspace('Keyspace1')
        key = utf8encode(request.node.name)

        d1 = 12
        d2 = -21
        d3 = 35
        # insert positive and negative values and check the counts
        client.add(key, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv1 = client.get(key, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        client.add(key, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c1'), d2), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv2 = client.get(key, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == (d1 + d2)

        client.add(key, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c1'), d3), ConsistencyLevel.ONE)
        time.sleep(0.1)
        rv3 = client.get(key, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv3.counter_column.value == (d1 + d2 + d3)

    def test_incr_decr_super_add(self, request):
        _set_keyspace('Keyspace1')
        key = utf8encode(request.node.name)

        d1 = -234
        d2 = 52345
        d3 = 3123

        client.add(key, ColumnParent(column_family='SuperCounter1', super_column=utf8encode('sc1')), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        client.add(key, ColumnParent(column_family='SuperCounter1', super_column=utf8encode('sc1')), CounterColumn(utf8encode('c2'), d2), ConsistencyLevel.ONE)
        rv1 = client.get(key, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1')), ConsistencyLevel.ONE)
        assert rv1.counter_super_column.columns[0].value == d1
        assert rv1.counter_super_column.columns[1].value == d2

        client.add(key, ColumnParent(column_family='SuperCounter1', super_column=utf8encode('sc1')), CounterColumn(utf8encode('c1'), d2), ConsistencyLevel.ONE)
        rv2 = client.get(key, ColumnPath('SuperCounter1', utf8encode('sc1'), utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == (d1 + d2)

        client.add(key, ColumnParent(column_family='SuperCounter1', super_column=utf8encode('sc1')), CounterColumn(utf8encode('c1'), d3), ConsistencyLevel.ONE)
        rv3 = client.get(key, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv3.counter_column.value == (d1 + d2 + d3)

    def test_incr_standard_remove(self, request):
        _set_keyspace('Keyspace1')
        key1 = utf8encode(request.node.name + "_1")
        key2 = utf8encode(request.node.name + "_2")

        d1 = 124

        # insert value and check it exists
        client.add(key1, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        rv1 = client.get(key1, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        client.remove_counter(key1, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        _assert_no_columnpath(key1, ColumnPath(column_family='Counter1', column=utf8encode('c1')))

        # insert again and this time delete the whole row, check that it is gone
        client.add(key2, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        rv2 = client.get(key2, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        client.remove_counter(key2, ColumnPath(column_family='Counter1'), ConsistencyLevel.ONE)
        _assert_no_columnpath(key2, ColumnPath(column_family='Counter1', column=utf8encode('c1')))

    def test_incr_super_remove(self, request):
        _set_keyspace('Keyspace1')
        key1 = utf8encode(request.node.name + "_1")
        key2 = utf8encode(request.node.name + "_2")

        d1 = 52345

        # insert value and check it exists
        client.add(key1, ColumnParent(column_family='SuperCounter1', super_column=utf8encode('sc1')), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        rv1 = client.get(key1, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        client.remove_counter(key1, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')), ConsistencyLevel.ONE)
        _assert_no_columnpath(key1, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')))

        # insert again and this time delete the whole row, check that it is gone
        client.add(key2, ColumnParent(column_family='SuperCounter1', super_column=utf8encode('sc1')), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        rv2 = client.get(key2, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        client.remove_counter(key2, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1')), ConsistencyLevel.ONE)
        _assert_no_columnpath(key2, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')))

    def test_incr_decr_standard_remove(self, request):
        _set_keyspace('Keyspace1')
        key1 = utf8encode(request.node.name + "_1")
        key2 = utf8encode(request.node.name + "_2")

        d1 = 124

        # insert value and check it exists
        client.add(key1, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        rv1 = client.get(key1, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        client.remove_counter(key1, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        _assert_no_columnpath(key1, ColumnPath(column_family='Counter1', column=utf8encode('c1')))

        # insert again and this time delete the whole row, check that it is gone
        client.add(key2, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        rv2 = client.get(key2, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        client.remove_counter(key2, ColumnPath(column_family='Counter1'), ConsistencyLevel.ONE)
        _assert_no_columnpath(key2, ColumnPath(column_family='Counter1', column=utf8encode('c1')))

    def test_incr_decr_super_remove(self, request):
        _set_keyspace('Keyspace1')
        key1 = utf8encode(request.node.name + "_1")
        key2 = utf8encode(request.node.name + "_2")

        d1 = 52345

        # insert value and check it exists
        client.add(key1, ColumnParent(column_family='SuperCounter1', super_column=utf8encode('sc1')), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        rv1 = client.get(key1, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1

        # remove the previous column and check that it is gone
        client.remove_counter(key1, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')), ConsistencyLevel.ONE)
        _assert_no_columnpath(key1, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')))

        # insert again and this time delete the whole row, check that it is gone
        client.add(key2, ColumnParent(column_family='SuperCounter1', super_column=utf8encode('sc1')), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        rv2 = client.get(key2, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1
        client.remove_counter(key2, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1')), ConsistencyLevel.ONE)
        _assert_no_columnpath(key2, ColumnPath(column_family='SuperCounter1', super_column=utf8encode('sc1'), column=utf8encode('c1')))

    def test_incr_decr_standard_batch_add(self, request):
        _set_keyspace('Keyspace1')
        key = utf8encode(request.node.name)

        d1 = 12
        d2 = -21
        update_map = {key: {'Counter1': [
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn(utf8encode('c1'), d1))),
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn(utf8encode('c1'), d2))),
        ]}}

        # insert positive and negative values and check the counts
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        rv1 = client.get(key, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1 + d2

    def test_incr_decr_standard_batch_remove(self, request):
        _set_keyspace('Keyspace1')
        key1 = utf8encode(request.node.name + "_1")
        key2 = utf8encode(request.node.name + "_2")

        d1 = 12
        d2 = -21

        # insert positive and negative values and check the counts
        update_map = {key1: {'Counter1': [
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn(utf8encode('c1'), d1))),
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn(utf8encode('c1'), d2))),
        ]}}
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        rv1 = client.get(key1, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv1.counter_column.value == d1 + d2

        # remove the previous column and check that it is gone
        update_map = {key1: {'Counter1': [
            Mutation(deletion=Deletion(predicate=SlicePredicate(column_names=[utf8encode('c1')]))),
        ]}}
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        _assert_no_columnpath(key1, ColumnPath(column_family='Counter1', column=utf8encode('c1')))

        # insert again and this time delete the whole row, check that it is gone
        update_map = {key2: {'Counter1': [
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn(utf8encode('c1'), d1))),
            Mutation(column_or_supercolumn=ColumnOrSuperColumn(counter_column=CounterColumn(utf8encode('c1'), d2))),
        ]}}
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        rv2 = client.get(key2, ColumnPath(column_family='Counter1', column=utf8encode('c1')), ConsistencyLevel.ONE)
        assert rv2.counter_column.value == d1 + d2

        update_map = {key2: {'Counter1': [
            Mutation(deletion=Deletion()),
        ]}}
        client.batch_mutate(update_map, ConsistencyLevel.ONE)
        _assert_no_columnpath(key2, ColumnPath(column_family='Counter1', column=utf8encode('c1')))

    # known failure: see CASSANDRA-10046
    def test_range_deletion(self):
        """ Tests CASSANDRA-7990 """
        _set_keyspace('Keyspace1')
        self.truncate_all('StandardComposite')

        for i in range(10):
            column_name = composite(str(i), str(i))
            column = Column(column_name, utf8encode('value'), int(time.time() * 1000))
            client.insert(utf8encode('key1'), ColumnParent('StandardComposite'), column, ConsistencyLevel.ONE)

        delete_slice = SlicePredicate(slice_range=SliceRange(composite('3', eoc=b'\xff'), composite('6', b'\x01'), False, 100))
        mutations = [Mutation(deletion=Deletion(int(time.time() * 1000), predicate=delete_slice))]
        keyed_mutations = {utf8encode('key1'): {'StandardComposite': mutations}}
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        slice_predicate = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 100))
        results = client.get_slice(utf8encode('key1'), ColumnParent('StandardComposite'), slice_predicate, ConsistencyLevel.ONE)
        columns = [result.column.name for result in results]
        assert columns == [composite('0', '0'), composite('1', '1'), composite('2', '2'),
             composite('6', '6'), composite('7', '7'), composite('8', '8'), composite('9', '9')]

    @pytest.mark.skipif(CASSANDRA_VERSION_FROM_BUILD == '3.9', reason="Test doesn't run on 3.9")
    def test_range_deletion_eoc_0(self):
        """
        This test confirms that a range tombstone with a final EOC of 0
        results in a exclusive deletion except for cells that exactly match the tombstone bound.

        @jira_ticket CASSANDRA-12423

        """
        _set_keyspace('Keyspace1')
        self.truncate_all('StandardComposite')

        for i in range(10):
            column_name = composite(str(i), str(i))
            column = Column(column_name, utf8encode('value'), int(time.time() * 1000))
            client.insert(utf8encode('key1'), ColumnParent('StandardComposite'), column, ConsistencyLevel.ONE)

        # insert a partial cell name (just the first element of the composite)
        column_name = composite('6', None, eoc=b'\x00')
        column = Column(column_name, utf8encode('value'), int(time.time() * 1000))
        client.insert(utf8encode('key1'), ColumnParent('StandardComposite'), column, ConsistencyLevel.ONE)

        # sanity check the query
        slice_predicate = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 100))
        results = client.get_slice(utf8encode('key1'), ColumnParent('StandardComposite'), slice_predicate, ConsistencyLevel.ONE)
        columns = [result.column.name for result in results]
        assert columns == [composite('0', '0'), composite('1', '1'), composite('2', '2'), composite('3', '3'), composite('4', '4'), composite('5', '5'),
             composite('6'),
             composite('6', '6'),
             composite('7', '7'), composite('8', '8'), composite('9', '9')]

        # do a slice deletion with (6, ) as the end
        delete_slice = SlicePredicate(slice_range=SliceRange(composite('3', eoc=b'\xff'), composite('6', b'\x00'), False, 100))
        mutations = [Mutation(deletion=Deletion(int(time.time() * 1000), predicate=delete_slice))]
        keyed_mutations = {utf8encode('key1'): {'StandardComposite': mutations}}
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        # check the columns post-deletion, (utf8encode('6'), ) because it is an exact much but not (6, 6)
        results = client.get_slice(utf8encode('key1'), ColumnParent('StandardComposite'), slice_predicate, ConsistencyLevel.ONE)
        columns = [result.column.name for result in results]
        assert columns == [composite('0', '0'), composite('1', '1'), composite('2', '2'),
             composite('6', '6'),
             composite('7', '7'), composite('8', '8'), composite('9', '9')]

        # do another slice deletion, but make the end (6, 6) this time
        delete_slice = SlicePredicate(slice_range=SliceRange(composite('3', eoc=b'\xff'), composite('6', '6', b'\x00'), False, 100))
        mutations = [Mutation(deletion=Deletion(int(time.time() * 1000), predicate=delete_slice))]
        keyed_mutations = {utf8encode('key1'): {'StandardComposite': mutations}}
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        # check the columns post-deletion, now (6, 6) is also gone
        results = client.get_slice(utf8encode('key1'), ColumnParent('StandardComposite'), slice_predicate, ConsistencyLevel.ONE)
        columns = [result.column.name for result in results]
        assert columns == [composite('0', '0'), composite('1', '1'), composite('2', '2'),
             composite('7', '7'), composite('8', '8'), composite('9', '9')]

    def test_incr_decr_standard_slice(self, request):
        _set_keyspace('Keyspace1')
        key = utf8encode(request.node.name)

        d1 = 12
        d2 = -21
        client.add(key, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c1'), d1), ConsistencyLevel.ONE)
        client.add(key, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c2'), d1), ConsistencyLevel.ONE)
        client.add(key, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c3'), d1), ConsistencyLevel.ONE)
        client.add(key, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c3'), d2), ConsistencyLevel.ONE)
        client.add(key, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c4'), d1), ConsistencyLevel.ONE)
        client.add(key, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c5'), d1), ConsistencyLevel.ONE)

        # insert positive and negative values and check the counts
        counters = client.get_slice(key, ColumnParent('Counter1'), SlicePredicate([utf8encode('c3'), utf8encode('c4')]), ConsistencyLevel.ONE)

        assert counters[0].counter_column.value == d1 + d2
        assert counters[1].counter_column.value == d1

    def test_incr_decr_standard_multiget_slice(self, request):
        _set_keyspace('Keyspace1')
        key1 = utf8encode(request.node.name + "_1")
        key2 = utf8encode(request.node.name + "_2")

        d1 = 12
        d2 = -21
        client.add(key1, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c2'), d1), ConsistencyLevel.ONE)
        client.add(key1, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c3'), d1), ConsistencyLevel.ONE)
        client.add(key1, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c3'), d2), ConsistencyLevel.ONE)
        client.add(key1, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c4'), d1), ConsistencyLevel.ONE)
        client.add(key1, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c5'), d1), ConsistencyLevel.ONE)

        client.add(key2, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c2'), d1), ConsistencyLevel.ONE)
        client.add(key2, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c3'), d1), ConsistencyLevel.ONE)
        client.add(key2, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c3'), d2), ConsistencyLevel.ONE)
        client.add(key2, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c4'), d1), ConsistencyLevel.ONE)
        client.add(key2, ColumnParent(column_family='Counter1'), CounterColumn(utf8encode('c5'), d1), ConsistencyLevel.ONE)

        # insert positive and negative values and check the counts
        counters = client.multiget_slice([key1, key2], ColumnParent('Counter1'), SlicePredicate([utf8encode('c3'), utf8encode('c4')]), ConsistencyLevel.ONE)

        assert counters[key1][0].counter_column.value == d1 + d2
        assert counters[key1][1].counter_column.value == d1
        assert counters[key2][0].counter_column.value == d1 + d2
        assert counters[key2][1].counter_column.value == d1

    def test_counter_get_slice_range(self, request):
        _set_keyspace('Keyspace1')
        key = utf8encode(request.node.name)

        client.add(key, ColumnParent('Counter1'), CounterColumn(utf8encode('c1'), 1), ConsistencyLevel.ONE)
        client.add(key, ColumnParent('Counter1'), CounterColumn(utf8encode('c2'), 2), ConsistencyLevel.ONE)
        client.add(key, ColumnParent('Counter1'), CounterColumn(utf8encode('c3'), 3), ConsistencyLevel.ONE)

        p = SlicePredicate(slice_range=SliceRange(utf8encode('c1'), utf8encode('c2'), False, 1000))
        result = client.get_slice(key, ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].counter_column.name == utf8encode('c1')
        assert result[1].counter_column.name == utf8encode('c2')

        p = SlicePredicate(slice_range=SliceRange(utf8encode('c3'), utf8encode('c2'), True, 1000))
        result = client.get_slice(key, ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].counter_column.name == utf8encode('c3')
        assert result[1].counter_column.name == utf8encode('c2')

        p = SlicePredicate(slice_range=SliceRange(utf8encode('a'), utf8encode('z'), False, 1000))
        result = client.get_slice(key, ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
        assert len(result) == 3, result

        p = SlicePredicate(slice_range=SliceRange(utf8encode('a'), utf8encode('z'), False, 2))
        result = client.get_slice(key, ColumnParent('Counter1'), p, ConsistencyLevel.ONE)
        assert len(result) == 2, result

    def test_counter_get_slice_super_range(self, request):
        _set_keyspace('Keyspace1')
        key = utf8encode(request.node.name)

        client.add(key, ColumnParent('SuperCounter1', utf8encode('sc1')), CounterColumn(_i64(4), 4), ConsistencyLevel.ONE)
        client.add(key, ColumnParent('SuperCounter1', utf8encode('sc2')), CounterColumn(_i64(5), 5), ConsistencyLevel.ONE)
        client.add(key, ColumnParent('SuperCounter1', utf8encode('sc2')), CounterColumn(_i64(6), 6), ConsistencyLevel.ONE)
        client.add(key, ColumnParent('SuperCounter1', utf8encode('sc3')), CounterColumn(_i64(7), 7), ConsistencyLevel.ONE)

        p = SlicePredicate(slice_range=SliceRange(utf8encode('sc2'), utf8encode('sc3'), False, 2))
        result = client.get_slice(key, ColumnParent('SuperCounter1'), p, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].counter_super_column.name == utf8encode('sc2')
        assert result[1].counter_super_column.name == utf8encode('sc3')

        p = SlicePredicate(slice_range=SliceRange(utf8encode('sc3'), utf8encode('sc2'), True, 2))
        result = client.get_slice(key, ColumnParent('SuperCounter1'), p, ConsistencyLevel.ONE)
        assert len(result) == 2
        assert result[0].counter_super_column.name == utf8encode('sc3')
        assert result[1].counter_super_column.name == utf8encode('sc2')

    def test_index_scan(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Indexed1')

        client.insert(utf8encode('key1'), ColumnParent('Indexed1'), Column(utf8encode('birthdate'), _i64(1), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key2'), ColumnParent('Indexed1'), Column(utf8encode('birthdate'), _i64(2), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key2'), ColumnParent('Indexed1'), Column(utf8encode('b'), _i64(2), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key3'), ColumnParent('Indexed1'), Column(utf8encode('birthdate'), _i64(3), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key3'), ColumnParent('Indexed1'), Column(utf8encode('b'), _i64(3), 0), ConsistencyLevel.ONE)

        # simple query on one index expression
        cp = ColumnParent('Indexed1')
        sp = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode('')))
        key_range = KeyRange(utf8encode(''), utf8encode(''), None, None, [IndexExpression(utf8encode('birthdate'), IndexOperator.EQ, _i64(1))], 100)
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == utf8encode('key1')
        assert len(result[0].columns) == 1, result[0].columns

        # without index
        key_range = KeyRange(utf8encode(''), utf8encode(''), None, None, [IndexExpression(utf8encode('b'), IndexOperator.EQ, _i64(1))], 100)
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 0, result

        # but unindexed expression added to indexed one is ok
        key_range = KeyRange(utf8encode(''), utf8encode(''), None, None, [IndexExpression(utf8encode('b'), IndexOperator.EQ, _i64(3)), IndexExpression(utf8encode('birthdate'), IndexOperator.EQ, _i64(3))], 100)
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert result[0].key == utf8encode('key3')
        assert len(result[0].columns) == 2, result[0].columns

    def test_index_scan_uuid_names(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Indexed3')

        sp = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode('')))
        cp = ColumnParent('Indexed3')  # timeuuid name, utf8 values
        u = uuid.UUID('00000000-0000-1000-0000-000000000000').bytes
        u2 = uuid.UUID('00000000-0000-1000-0000-000000000001').bytes
        client.insert(utf8encode('key1'), ColumnParent('Indexed3'), Column(u, utf8encode('a'), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Indexed3'), Column(u2, utf8encode('b'), 0), ConsistencyLevel.ONE)
        # name comparator + data validator of incompatible types -- see CASSANDRA-2347
        key_range = KeyRange(utf8encode(''), utf8encode(''), None, None, [IndexExpression(u, IndexOperator.EQ, utf8encode('a')), IndexExpression(u2, IndexOperator.EQ, utf8encode('b'))], 100)
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result

        cp = ColumnParent('Indexed2')  # timeuuid name, long values

        # name must be valid (TimeUUID)
        key_range = KeyRange(utf8encode(''), utf8encode(''), None, None, [IndexExpression(utf8encode('foo'), IndexOperator.EQ, uuid.UUID('00000000-0000-1000-0000-000000000000').bytes)], 100)
        _expect_exception(lambda: client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE), InvalidRequestException)

        # value must be valid (TimeUUID)
        key_range = KeyRange(utf8encode(''), utf8encode(''), None, None, [IndexExpression(uuid.UUID('00000000-0000-1000-0000-000000000000').bytes, IndexOperator.EQ, utf8encode("foo"))], 100)
        _expect_exception(lambda: client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE), InvalidRequestException)

    def test_index_scan_expiring(self):
        """ Test that column ttled expires from KEYS index"""
        _set_keyspace('Keyspace1')
        self.truncate_all('Indexed1')

        client.insert(utf8encode('key1'), ColumnParent('Indexed1'), Column(utf8encode('birthdate'), _i64(1), 0, 2), ConsistencyLevel.ONE)
        cp = ColumnParent('Indexed1')
        sp = SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode('')))
        key_range = KeyRange(utf8encode(''), utf8encode(''), None, None, [IndexExpression(utf8encode('birthdate'), IndexOperator.EQ, _i64(1))], 100)
        # query before expiration
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        # wait for expiration and requery
        time.sleep(3)
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 0, result

    def test_index_scan_indexed_column_outside_slice_predicate(self):
        """
        Verify that performing an indexed read works when the indexed column
        is not included in the slice predicate. Checks both cases where the
        predicate contains a slice range or a set of column names, which
        translate to slice and names queries server-side.
        @jira_ticket CASSANDRA-11523
        """
        _set_keyspace('Keyspace1')
        self.truncate_all('Indexed4')

        client.insert(utf8encode('key1'), ColumnParent('Indexed4'), Column(utf8encode('a'), _i64(1), 0), ConsistencyLevel.ONE)
        client.insert(utf8encode('key1'), ColumnParent('Indexed4'), Column(utf8encode('z'), utf8encode('zzz'), 0), ConsistencyLevel.ONE)
        cp = ColumnParent('Indexed4')
        sp = SlicePredicate(slice_range=SliceRange(utf8encode('z'), utf8encode('z')))
        key_range = KeyRange(utf8encode(''), utf8encode(''), None, None, [IndexExpression(utf8encode('a'), IndexOperator.EQ, _i64(1))], 100)
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert len(result[0].columns) == 1, result[0].columns
        assert result[0].columns[0].column.name == utf8encode('z')

        sp = SlicePredicate(column_names=[utf8encode('z')])
        result = client.get_range_slices(cp, sp, key_range, ConsistencyLevel.ONE)
        assert len(result) == 1, result
        assert len(result[0].columns) == 1, result[0].columns
        assert result[0].columns[0].column.name == utf8encode('z')

    def test_column_not_found_quorum(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        key = utf8encode('doesntexist')
        column_path = ColumnPath(column_family="Standard1", column=utf8encode("idontexist"))
        try:
            client.get(key, column_path, ConsistencyLevel.QUORUM)
            assert False, ('columnpath %s existed in %s when it should not' % (column_path, key))
        except NotFoundException:
            assert True, 'column did not exist'

    def test_get_range_slice_after_deletion(self):
        _set_keyspace('Keyspace2')
        self.truncate_all('Super3')

        key = utf8encode('key1')
        # three supercoluns, each with "col1" subcolumn
        for i in range(1, 4):
            client.insert(key, ColumnParent('Super3', utf8encode('sc%d' % i)), Column(utf8encode('col1'), utf8encode('val1'), 0), ConsistencyLevel.ONE)

        cp = ColumnParent('Super3')
        predicate = SlicePredicate(slice_range=SliceRange(utf8encode('sc1'), utf8encode('sc3'), False, count=1))
        k_range = KeyRange(start_key=key, end_key=key, count=1)

        # validate count=1 restricts to 1 supercolumn
        result = client.get_range_slices(cp, predicate, k_range, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 1

        # remove sc1; add back subcolumn to override tombstone
        client.remove(key, ColumnPath('Super3', utf8encode('sc1')), 1, ConsistencyLevel.ONE)
        result = client.get_range_slices(cp, predicate, k_range, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 1
        client.insert(key, ColumnParent('Super3', utf8encode('sc1')), Column(utf8encode('col1'), utf8encode('val1'), 2), ConsistencyLevel.ONE)
        result = client.get_range_slices(cp, predicate, k_range, ConsistencyLevel.ONE)
        assert len(result[0].columns) == 1, result[0].columns
        assert result[0].columns[0].super_column.name == utf8encode('sc1')

    def test_multi_slice(self):
        _set_keyspace('Keyspace1')
        self.truncate_all('Standard1')

        _insert_six_columns('abc')
        L = [result.column
             for result in _big_multi_slice('abc')]
        assert L == _MULTI_SLICE_COLUMNS, L

    def test_truncate(self):
        _set_keyspace('Keyspace1')

        _insert_simple()
        _insert_super()

        # truncate Standard1
        self.truncate_all('Standard1')
        assert _big_slice(utf8encode('key1'), ColumnParent('Standard1')) == []

        # truncate Super1
        self.truncate_all('Super1')
        assert _big_slice(utf8encode('key1'), ColumnParent('Super1')) == []
        assert _big_slice(utf8encode('key1'), ColumnParent('Super1', utf8encode('sc1'))) == []

    @since('3.0')
    def test_cql_range_tombstone_and_static(self):
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        # Create a CQL table with a static column and insert a row
        session.execute('USE "Keyspace1"')
        session.execute("CREATE TABLE t (k text, s text static, t text, v text, PRIMARY KEY (k, t))")

        session.execute("INSERT INTO t (k, s, t, v) VALUES ('k', 's', 't', 'v') USING TIMESTAMP 0")
        assert_one(session, "SELECT * FROM t", ['k', 't', 's', 'v'])

        # Now submit a range deletion that should include both the row and the static value

        _set_keyspace('Keyspace1')

        mutations = [Mutation(deletion=Deletion(1, predicate=SlicePredicate(slice_range=SliceRange(utf8encode(''), utf8encode(''), False, 1000))))]
        mutation_map = dict((table, mutations) for table in ['t'])
        keyed_mutations = dict((key, mutation_map) for key in [utf8encode('k')])
        client.batch_mutate(keyed_mutations, ConsistencyLevel.ONE)

        # And check everything is gone
        assert_none(session, "SELECT * FROM t")

    def test_compact_storage_get(self):
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        # Create a CQL table with a static column and insert a row
        session.execute("USE \"Keyspace1\"")
        session.execute("CREATE TABLE IF NOT EXISTS cs1 (k int PRIMARY KEY,v int) WITH COMPACT STORAGE")

        _set_keyspace('Keyspace1')
        CL = ConsistencyLevel.ONE
        i = 1
        client.insert(_i32(i), ColumnParent('cs1'), Column(utf8encode('v'), _i32(i), 0), CL)
        _assert_column('cs1', _i32(i), utf8encode('v'), _i32(i), 0)

    @pytest.mark.skipif(CASSANDRA_VERSION_FROM_BUILD == '3.9', reason="Test doesn't run on 3.9")
    def test_range_tombstone_eoc_0(self):
        """
        Insert a range tombstone with EOC=0 for a compact storage table. Insert 2 rows that
        are just outside the range and check that they are present.

        @jira_ticket CASSANDRA-12423
        """
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)

        session.execute('USE "Keyspace1"')
        session.execute("CREATE TABLE test (id INT, c1 TEXT, c2 TEXT, v INT, PRIMARY KEY (id, c1, c2)) "
                        "with compact storage and compression = {'sstable_compression': ''};")

        _set_keyspace('Keyspace1')

        range_delete = {
            _i32(1): {
                'test': [Mutation(deletion=Deletion(2470761440040513,
                                                    predicate=SlicePredicate(slice_range=SliceRange(
                                                        start=composite('a'), finish=composite('asd')))))]
            }
        }

        client.batch_mutate(range_delete, ConsistencyLevel.ONE)

        session.execute("INSERT INTO test (id, c1, c2, v) VALUES (1, 'asd', '', 0) USING TIMESTAMP 1470761451368658")
        session.execute("INSERT INTO test (id, c1, c2, v) VALUES (1, 'asd', 'asd', 0) USING TIMESTAMP 1470761449416613")

        ret = list(session.execute('SELECT * FROM test'))
        assert 2 == len(ret)

        node1.nodetool('flush Keyspace1 test')
