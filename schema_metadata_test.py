import re

from nose.tools import assert_equal, assert_in

from cassandra import cqltypes
from dtest import Tester
from tools import since, require


def establish_indexes_table(version, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_indexes")
    cql = """
            create table {0} (
                        a uuid,
                        b uuid,
                        c uuid,
                        d uuid,
                        e uuid,
                        primary key((a,b),c))
              """

    session.execute(cql.format(table_name))
    index_name = _table_name_builder("idx_" + table_name_prefix, table_name)
    session.execute("CREATE INDEX {0} ON {1}( d )".format(index_name, table_name))


def verify_indexes_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_indexes")
    index_name = _table_name_builder("idx_" + table_name_prefix, table_name)
    meta = session.cluster.metadata.keyspaces[keyspace].indexes[index_name]
    assert_equal('d', meta.column.name)
    assert_equal(table_name, meta.column.table.name)

    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert_equal(1, len(meta.clustering_key))
    assert_equal('c', meta.clustering_key[0].name)

    assert_equal(1, len(meta.indexes))
    assert_equal('d', meta.indexes[index_name].column.name)
    assert_equal(3, len(meta.primary_key))
    assert_equal('a', meta.primary_key[0].name)
    assert_equal('b', meta.primary_key[1].name)
    assert_equal('c', meta.primary_key[2].name)


def establish_clustering_order_table(version, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_clustering_order")
    cql = """
                CREATE TABLE {0} (
                  event_type text,
                  insertion_time timestamp,
                  event blob,
                  PRIMARY KEY (event_type, insertion_time)
                )
                WITH CLUSTERING ORDER BY (insertion_time DESC);
              """

    session.execute(cql.format(table_name))


def verify_clustering_order_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_clustering_order")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert_equal(0, len(meta.indexes))
    assert_equal(2, len(meta.primary_key))
    assert_equal('event_type', meta.primary_key[0].name)
    assert_equal('insertion_time', meta.primary_key[1].name)
    assert_equal(1, len(meta.clustering_key))
    assert_equal('insertion_time', meta.clustering_key[0].name)
    assert_in('insertion_time DESC', meta.as_cql_query())


def establish_compact_storage_table(version, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_compact_storage")
    cql = """
                CREATE TABLE {0} (
                  block_id uuid,
                  sub_block_id int,
                  stuff blob,
                  PRIMARY KEY (block_id, sub_block_id)
                )
                WITH COMPACT STORAGE;
              """

    session.execute(cql.format(table_name))


def verify_compact_storage_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_compact_storage")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert_equal(3, len(meta.columns))
    assert_equal(2, len(meta.primary_key))
    assert_equal(1, len(meta.clustering_key))
    assert_equal('sub_block_id', meta.clustering_key[0].name)
    assert_equal('block_id', meta.primary_key[0].name)
    assert_equal(cqltypes.UUIDType, meta.primary_key[0].data_type)
    assert_equal('sub_block_id', meta.primary_key[1].name)
    assert_equal(cqltypes.Int32Type, meta.primary_key[1].data_type)
    assert_equal(1, len(meta.clustering_key))
    assert_equal('sub_block_id', meta.clustering_key[0].name)


def establish_compact_storage_composite_table(version, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_compact_storage_composite")
    cql = """
                CREATE TABLE {0} (
                    key text,
                    column1 int,
                    column2 int,
                    value text,
                    PRIMARY KEY(key, column1, column2)
                ) WITH COMPACT STORAGE;
              """

    session.execute(cql.format(table_name))


def verify_compact_storage_composite_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_compact_storage_composite")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert_equal(4, len(meta.columns))
    assert_equal(3, len(meta.primary_key))
    assert_equal('key', meta.primary_key[0].name)
    assert_equal(cqltypes.UTF8Type, meta.primary_key[0].data_type)
    assert_equal('column1', meta.primary_key[1].name)
    assert_equal(cqltypes.Int32Type, meta.primary_key[1].data_type)
    assert_equal('column2', meta.primary_key[2].name)
    assert_equal(cqltypes.Int32Type, meta.primary_key[2].data_type)
    assert_equal(2, len(meta.clustering_key))
    assert_equal('column1', meta.clustering_key[0].name)
    assert_equal(cqltypes.Int32Type, meta.clustering_key[0].data_type)
    assert_equal('column2', meta.clustering_key[1].name)
    assert_equal(cqltypes.Int32Type, meta.clustering_key[1].data_type)


def establish_nondefault_table_settings(version, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_nondefault_settings")
    cql = """
        create table {0} (
                    a uuid,
                    b uuid,
                    c uuid,
                    d uuid,
                    primary key((a,b),c) )
                WITH gc_grace_seconds = 9999
                AND bloom_filter_fp_chance = 0.5
                AND read_repair_chance = 0.99
                AND comment = 'insightful information'
                AND dclocal_read_repair_chance = 0.88
                AND compaction = {{'class': 'LeveledCompactionStrategy'}}
          """

    if version >= '3.0':
        cql += " AND compression = {{'class': 'DeflateCompressor', 'chunk_length_in_kb' : 128}}"
    else:
        cql += " AND compression = {{'sstable_compression': 'DeflateCompressor', 'chunk_length_kb' : 128}}"

    if version >= '3.0':
        cql += """ AND memtable_flush_period_in_ms = 2121
                   AND speculative_retry = '55PERCENTILE'"""

    if version >= '2.1':
        cql += """ AND max_index_interval = 20
                   AND min_index_interval = 1
                   AND default_time_to_live = 86400
                   AND caching = {{'keys': 'NONE', 'rows_per_partition': 'ALL'}}"""

    if version >= '2.0' and version <= '3.0':
        cql += " AND index_interval = 1"

    if version < '2.1':
        cql += " AND caching = 'ROWS_ONLY'"

    session.execute(cql.format(table_name))


def verify_nondefault_table_settings(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_nondefault_settings")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]

    assert_equal('insightful information', meta.options['comment'])
    assert_equal(0.88, meta.options['dclocal_read_repair_chance'])
    assert_equal(9999, meta.options['gc_grace_seconds'])
    assert_equal(0.99, meta.options['read_repair_chance'])
    assert_equal(0.5, meta.options['bloom_filter_fp_chance'])

    if created_on_version >= '2.1':
        assert_equal(86400, meta.options['default_time_to_live'])
        assert_equal(1, meta.options['min_index_interval'])
        assert_equal(20, meta.options['max_index_interval'])

    if created_on_version >= '3.0':
        assert_equal('55PERCENTILE', meta.options['speculative_retry'])
        assert_equal(2121, meta.options['memtable_flush_period_in_ms'])

    if current_version >= '3.0':
        assert_equal('org.apache.cassandra.io.compress.DeflateCompressor', meta.options['compression']['class'])
        assert_equal('128', meta.options['compression']['chunk_length_in_kb'])
        assert_equal('org.apache.cassandra.db.compaction.LeveledCompactionStrategy', meta.options['compaction']['class'])

    if '2.1' <= current_version < '3.0':
        assert_equal('{"keys":"NONE", "rows_per_partition":"ALL"}', meta.options['caching'])
        assert_in('"chunk_length_kb":"128"', meta.options['compression_parameters'])
        assert_in('"sstable_compression":"org.apache.cassandra.io.compress.DeflateCompressor"', meta.options['compression_parameters'])
    elif current_version >= '3.0':
        assert_equal('NONE', meta.options['caching']['keys'])
        assert_equal('ALL', meta.options['caching']['rows_per_partition'])
        assert_equal('org.apache.cassandra.io.compress.DeflateCompressor', meta.options['compression']['class'])
        assert_equal('128', meta.options['compression']['chunk_length_in_kb'])
        assert_equal('org.apache.cassandra.db.compaction.LeveledCompactionStrategy', meta.options['compaction']['class'])
    else:
        assert_equal('ROWS_ONLY', meta.options['caching'])

    assert_equal(2, len(meta.partition_key))
    assert_equal(meta.partition_key[0].name, 'a')
    assert_equal(meta.partition_key[1].name, 'b')

    assert_equal(1, len(meta.clustering_key))
    assert_equal(meta.clustering_key[0].name, 'c')


@require(6717)
# this method has been renamed to _* so it's removed from upgrade tests as it will fail on 2.2 -> 3.0
def _establish_uda(version, session, table_name_prefix=""):
    if version < '2.2':
        return
    function_name = _table_name_builder(table_name_prefix, "test_uda_function")
    aggregate_name = _table_name_builder(table_name_prefix, "test_uda_aggregate")

    session.execute('''
            CREATE FUNCTION {0}(current int, candidate int)
            CALLED ON NULL INPUT
            RETURNS int LANGUAGE java AS
            'if (current == null) return candidate; else return Math.max(current, candidate);';
        '''.format(function_name))

    session.execute('''
            CREATE AGGREGATE {0}(int)
            SFUNC {1}
            STYPE int
            INITCOND null;
        '''.format(aggregate_name, function_name))


@require(6717)
# this method has been renamed to _* so it's removed from upgrade tests as it will fail on 2.2 -> 3.0
def _verify_uda(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    if created_on_version < '2.2':
        return
    function_name = _table_name_builder(table_name_prefix, "test_uda_function")
    aggregate_name = _table_name_builder(table_name_prefix, "test_uda_aggregate")

    assert_in(function_name+"(int,int)", session.cluster.metadata.keyspaces[keyspace].functions.keys())
    assert_in(aggregate_name+"(int)", session.cluster.metadata.keyspaces[keyspace].aggregates.keys())

    aggr_meta = session.cluster.metadata.keyspaces[keyspace].aggregates[aggregate_name+"(int)"]
    assert_equal(function_name, aggr_meta.state_func)
    assert_equal(cqltypes.Int32Type, aggr_meta.state_type)
    assert_equal(cqltypes.Int32Type, aggr_meta.return_type)


def establish_udf(version, session, table_name_prefix=""):
    if version < '2.2':
        return
    function_name = _table_name_builder(table_name_prefix, "test_udf")
    session.execute('''
        CREATE OR REPLACE FUNCTION {0} (input double) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.log(input.doubleValue()));';
        '''.format(function_name))


def verify_udf(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    if created_on_version < '2.2':
        return
    function_name = _table_name_builder(table_name_prefix, "test_udf")
    assert_in(function_name+"(double)", session.cluster.metadata.keyspaces[keyspace].functions.keys())
    meta = session.cluster.metadata.keyspaces[keyspace].functions[function_name+"(double)"]
    assert_equal('java', meta.language)
    assert_equal(cqltypes.DoubleType, meta.return_type)
    assert_equal(['double'], meta.type_signature)
    assert_equal(['input'], meta.argument_names)
    assert_equal('return Double.valueOf(Math.log(input.doubleValue()));', meta.body)


def establish_udt_table(version, session, table_name_prefix=""):
    if version < '2.1':
        return
    table_name = _table_name_builder(table_name_prefix, "test_udt")
    session.execute('''
          CREATE TYPE {0} (
              street text,
              city text,
              zip int
          )'''.format(table_name))


def verify_udt_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    if created_on_version < '2.1':
        return
    table_name = _table_name_builder(table_name_prefix, "test_udt")
    meta = session.cluster.metadata.keyspaces[keyspace].user_types[table_name]

    assert_equal(meta.field_names, ['street', 'city', 'zip'])
    assert_equal('street', meta.field_names[0])
    assert_equal(cqltypes.UTF8Type, meta.field_types[0])
    assert_equal('city', meta.field_names[1])
    assert_equal(cqltypes.UTF8Type, meta.field_types[1])
    assert_equal('zip', meta.field_names[2])
    assert_equal(cqltypes.Int32Type, meta.field_types[2])


def establish_static_column_table(version, session, table_name_prefix=""):
    if version < '2.0':
        return
    table_name = _table_name_builder(table_name_prefix, "test_static_column")
    session.execute('''
          CREATE TABLE {0} (
             user text,
             balance int static,
             expense_id int,
             amount int,
             PRIMARY KEY (user, expense_id)
          )'''.format(table_name))


def verify_static_column_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    if created_on_version < '2.0':
        return
    table_name = _table_name_builder(table_name_prefix, "test_static_column")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert_equal(4, len(meta.columns))
    assert_equal(cqltypes.UTF8Type, meta.columns['user'].data_type)
    assert_equal(False, meta.columns['user'].is_static)
    assert_equal(cqltypes.Int32Type, meta.columns['balance'].data_type)
    assert_equal(True, meta.columns['balance'].is_static)
    assert_equal(cqltypes.Int32Type, meta.columns['expense_id'].data_type)
    assert_equal(False, meta.columns['expense_id'].is_static)
    assert_equal(cqltypes.Int32Type, meta.columns['amount'].data_type)
    assert_equal(False, meta.columns['amount'].is_static)


def establish_collection_datatype_table(version, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_collection_datatypes")
    cql = '''
        CREATE TABLE {0} (
            id uuid PRIMARY KEY,
            a list<int>,
            b list<text>,
            c set<int>,
            d set<text>,
            e map<text, text>,
            f map<text, int>,'''

    if version > '2.1':
        cql += '''
            g frozen<list<int>>,
            h frozen<list<text>>,
            i frozen<set<int>>,
            j frozen<set<text>>,
            k frozen<map<text, text>>,
            l frozen<map<text, int>>'''

    cql += ')'

    session.execute(cql.format(table_name))


def _validate_collection_column(collection_type, param_type, column, frozen=False):
    meta_type = collection_type.apply_parameters(param_type)
    if frozen:
        meta_type = cqltypes.FrozenType.apply_parameters([meta_type])

    assert_equal(str(meta_type), str(column.data_type), "column {0} invalid".format(column.name))


def verify_collection_datatype_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_collection_datatypes")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    if created_on_version > '2.1':
        assert_equal(13, len(meta.columns))
    else:
        assert_equal(7, len(meta.columns))

    validations = [
        (cqltypes.ListType, [cqltypes.Int32Type], meta.columns['a']),
        (cqltypes.ListType, [cqltypes.UTF8Type], meta.columns['b']),
        (cqltypes.SetType, [cqltypes.Int32Type], meta.columns['c']),
        (cqltypes.SetType, [cqltypes.UTF8Type], meta.columns['d']),
        (cqltypes.MapType, [cqltypes.UTF8Type, cqltypes.UTF8Type], meta.columns['e']),
        (cqltypes.MapType, [cqltypes.UTF8Type, cqltypes.Int32Type], meta.columns['f'])
    ]
    if created_on_version > '2.1':
        validations.append((cqltypes.ListType, [cqltypes.Int32Type], meta.columns['g'], True))
        validations.append((cqltypes.ListType, [cqltypes.UTF8Type], meta.columns['h'], True))
        validations.append((cqltypes.SetType, [cqltypes.Int32Type], meta.columns['i'], True))
        validations.append((cqltypes.SetType, [cqltypes.UTF8Type], meta.columns['j'], True))
        validations.append((cqltypes.MapType, [cqltypes.UTF8Type, cqltypes.UTF8Type], meta.columns['k'], True))
        validations.append((cqltypes.MapType, [cqltypes.UTF8Type, cqltypes.Int32Type], meta.columns['l'], True))

    for validation in validations:
        _validate_collection_column(*validation)


def establish_basic_datatype_table(version, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_basic_datatypes")
    cql = '''
        CREATE TABLE {0} (
            a ascii,
            b bigint PRIMARY KEY,
            c blob,
            d boolean,
            e decimal,
            f double,
            g float,
            h inet,
            i int,
            j text,
            k timestamp,
            l timeuuid,
            m uuid,
            n varchar,
            o varint,'''
    if version > '2.2':
        cql += ''' p date,
                   q smallint,
                   r time,
                   s tinyint'''

    cql += ')'

    session.execute(cql.format(table_name))


def verify_basic_datatype_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    table_name = _table_name_builder(table_name_prefix, "test_basic_datatypes")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    if created_on_version > '2.2':
        assert_equal(19, len(meta.columns))
    else:
        assert_equal(15, len(meta.columns))

    assert_equal(1, len(meta.primary_key))
    assert_equal('b', meta.primary_key[0].name)

    assert_equal(cqltypes.AsciiType, meta.columns['a'].data_type)
    assert_equal(cqltypes.LongType, meta.columns['b'].data_type)
    assert_equal(cqltypes.BytesType, meta.columns['c'].data_type)
    assert_equal(cqltypes.BooleanType, meta.columns['d'].data_type)
    assert_equal(cqltypes.DecimalType, meta.columns['e'].data_type)
    assert_equal(cqltypes.DoubleType, meta.columns['f'].data_type)
    assert_equal(cqltypes.FloatType, meta.columns['g'].data_type)
    assert_equal(cqltypes.InetAddressType, meta.columns['h'].data_type)
    assert_equal(cqltypes.Int32Type, meta.columns['i'].data_type)
    assert_equal(cqltypes.UTF8Type, meta.columns['j'].data_type)
    if created_on_version < '2.0':
        assert_equal(cqltypes.DateType, meta.columns['k'].data_type)
    else:
        assert_equal(cqltypes.TimestampType, meta.columns['k'].data_type)
    assert_equal(cqltypes.TimeUUIDType, meta.columns['l'].data_type)
    assert_equal(cqltypes.UUIDType, meta.columns['m'].data_type)
    assert_equal(cqltypes.UTF8Type, meta.columns['n'].data_type)
    assert_equal(cqltypes.IntegerType, meta.columns['o'].data_type)
    if created_on_version > '2.2':
        assert_equal(cqltypes.SimpleDateType, meta.columns['p'].data_type)
        assert_equal(cqltypes.ShortType, meta.columns['q'].data_type)
        assert_equal(cqltypes.TimeType, meta.columns['r'].data_type)
        assert_equal(cqltypes.ByteType, meta.columns['s'].data_type)


def _table_name_builder(prefix, table_name):
    if prefix == "":
        return table_name
    return "{0}_{1}".format(re.sub(r"[^A-Za-z0-9]", "_", prefix), table_name)[-47:]


class TestSchemaMetadata(Tester):
    def setUp(self):
        Tester.setUp(self)
        cluster = self.cluster
        cluster.schema_event_refresh_window = 0

        if cluster.version() >= '3.0':
            cluster.set_configuration_options({'enable_user_defined_functions': 'true',
                                               'enable_scripted_user_defined_functions': 'true'})
        elif cluster.version() >= '2.2':
            cluster.set_configuration_options({'enable_user_defined_functions': 'true'})
        cluster.populate(1).start()

        self.session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(self.session, 'ks', 1)

    def _keyspace_meta(self, keyspace_name="ks"):
        self.session.cluster.refresh_schema_metadata()
        return self.session.cluster.metadata.keyspaces[keyspace_name]

    def creating_and_dropping_keyspace_test(self):
        starting_keyspace_count = len(self.session.cluster.metadata.keyspaces)
        self.assertEqual(True, self._keyspace_meta().durable_writes)
        self.session.execute("""
                CREATE KEYSPACE so_long
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                    AND durable_writes = false
            """)
        self.assertEqual(False, self._keyspace_meta('so_long').durable_writes)
        self.session.execute("DROP KEYSPACE so_long")
        self.assertEqual(starting_keyspace_count, len(self.session.cluster.metadata.keyspaces))

    def creating_and_dropping_table_test(self):
        self.session.execute("create table born_to_die (id uuid primary key, name varchar)")
        meta = self._keyspace_meta().tables['born_to_die']
        self.assertEqual('ks', meta.keyspace.name)
        self.assertEqual('born_to_die', meta.name)
        self.assertEqual(1, len(meta.partition_key))
        self.assertEqual('id', meta.partition_key[0].name)
        self.assertEqual(2, len(meta.columns))
        self.assertIsNotNone(meta.columns.get('id'))
        self.assertEqual(cqltypes.UUIDType, meta.columns['id'].data_type)
        self.assertIsNotNone(meta.columns.get('name'))
        self.assertEqual(cqltypes.UTF8Type, meta.columns['name'].data_type)
        self.assertEqual(0, len(meta.clustering_key))
        self.assertEqual(0, len(meta.triggers))
        self.assertEqual(0, len(meta.indexes))
        self.session.execute("drop table born_to_die")
        self.assertIsNone(self._keyspace_meta().tables.get('born_to_die'))

    def creating_and_dropping_table_with_2ary_indexes_test(self):
        self.assertEqual(0, len(self._keyspace_meta().indexes))
        self.session.execute("create table born_to_die (id uuid primary key, name varchar)")
        self.session.execute("create index ix_born_to_die_name on born_to_die(name)")

        self.assertEqual(1, len(self._keyspace_meta().indexes))
        ix_meta = self._keyspace_meta().indexes['ix_born_to_die_name']
        self.assertEqual('COMPOSITES', ix_meta.index_type)
        self.assertEqual('name', ix_meta.column.name)
        self.assertEqual('ix_born_to_die_name', ix_meta.name)
        if self.cluster.version() < '2.0':
            self.assertEqual({'prefix_size': '0'}, ix_meta.index_options)
        else:
            self.assertEqual({}, ix_meta.index_options)

        self.session.execute("drop table born_to_die")
        self.assertIsNone(self._keyspace_meta().tables.get('born_to_die'))
        self.assertIsNone(self._keyspace_meta().indexes.get('ix_born_to_die_name'))
        self.assertEqual(0, len(self._keyspace_meta().indexes))

    @since('2.1')
    def creating_and_dropping_user_types_test(self):
        self.assertEqual(0, len(self._keyspace_meta().user_types))
        self.session.execute("CREATE TYPE soon_to_die (foo text, bar int)")
        self.assertEqual(1, len(self._keyspace_meta().user_types))

        ut_meta = self._keyspace_meta().user_types['soon_to_die']
        self.assertEqual('ks', ut_meta.keyspace)
        self.assertEqual('soon_to_die', ut_meta.name)
        self.assertEqual(['foo', 'bar'], ut_meta.field_names)
        self.assertEqual([cqltypes.UTF8Type, cqltypes.Int32Type], ut_meta.field_types)

        self.session.execute("DROP TYPE soon_to_die")
        self.assertEqual(0, len(self._keyspace_meta().user_types))

    @since('2.2')
    def creating_and_dropping_udf_test(self):
        self.assertEqual(0, len(self._keyspace_meta().functions), "expected to start with no indexes")
        self.session.execute("""
                CREATE OR REPLACE FUNCTION ks.wasteful_function (input double)
                    CALLED ON NULL INPUT
                    RETURNS double
                    LANGUAGE java AS 'return Double.valueOf(Math.log(input.doubleValue()));';
            """)
        self.assertEqual(1, len(self._keyspace_meta().functions), "udf count should be 1")
        udf_meta = self._keyspace_meta().functions['wasteful_function(double)']
        self.assertEqual('ks', udf_meta.keyspace)
        self.assertEqual('wasteful_function', udf_meta.name)
        self.assertEqual(['double'], udf_meta.type_signature)
        self.assertEqual(['input'], udf_meta.argument_names)
        self.assertEqual(cqltypes.DoubleType, udf_meta.return_type)
        self.assertEqual('java', udf_meta.language)
        self.assertEqual('return Double.valueOf(Math.log(input.doubleValue()));', udf_meta.body)
        self.assertTrue(udf_meta.called_on_null_input)
        self.session.execute("DROP FUNCTION ks.wasteful_function")
        self.assertEqual(0, len(self._keyspace_meta().functions), "expected udf list to be back to zero")

    @since('2.2')
    def creating_and_dropping_uda_test(self):
        self.assertEqual(0, len(self._keyspace_meta().functions), "expected to start with no indexes")
        self.assertEqual(0, len(self._keyspace_meta().aggregates), "expected to start with no aggregates")
        self.session.execute('''
                CREATE FUNCTION ks.max_val(current int, candidate int)
                CALLED ON NULL INPUT
                RETURNS int LANGUAGE java AS
                'if (current == null) return candidate; else return Math.max(current, candidate);'
            ''')
        self.session.execute('''
                CREATE AGGREGATE ks.kind_of_max_agg(int)
                SFUNC max_val
                STYPE int
                INITCOND -1
            ''')
        self.assertEqual(1, len(self._keyspace_meta().functions), "udf count should be 1")
        self.assertEqual(1, len(self._keyspace_meta().aggregates), "uda count should be 1")
        udf_meta = self._keyspace_meta().functions['max_val(int,int)']
        uda_meta = self._keyspace_meta().aggregates['kind_of_max_agg(int)']

        self.assertEqual('ks', udf_meta.keyspace)
        self.assertEqual('max_val', udf_meta.name)
        self.assertEqual(['int', 'int'], udf_meta.type_signature)
        self.assertEqual(['current', 'candidate'], udf_meta.argument_names)
        self.assertEqual(cqltypes.Int32Type, udf_meta.return_type)
        self.assertEqual('java', udf_meta.language)
        self.assertEqual('if (current == null) return candidate; else return Math.max(current, candidate);', udf_meta.body)
        self.assertTrue(udf_meta.called_on_null_input)

        self.assertEqual('ks', uda_meta.keyspace)
        self.assertEqual('kind_of_max_agg', uda_meta.name)
        self.assertEqual(['int'], uda_meta.type_signature)
        self.assertEqual('max_val', uda_meta.state_func)
        self.assertEqual(cqltypes.Int32Type, uda_meta.state_type)
        self.assertEqual(None, uda_meta.final_func)
        self.assertEqual(-1, uda_meta.initial_condition)
        self.assertEqual(cqltypes.Int32Type, uda_meta.return_type)

        self.session.execute("DROP AGGREGATE ks.kind_of_max_agg")
        self.assertEqual(0, len(self._keyspace_meta().aggregates), "expected uda list to be back to zero")
        self.session.execute("DROP FUNCTION ks.max_val")
        self.assertEqual(0, len(self._keyspace_meta().functions), "expected udf list to be back to zero")

    def basic_table_datatype_test(self):
        establish_basic_datatype_table(self.cluster.version(), self.session)
        verify_basic_datatype_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def collection_table_datatype_test(self):
        establish_collection_datatype_table(self.cluster.version(), self.session)
        verify_collection_datatype_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def clustering_order_test(self):
        establish_clustering_order_table(self.cluster.version(), self.session)
        verify_clustering_order_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def compact_storage_test(self):
        establish_compact_storage_table(self.cluster.version(), self.session)
        verify_compact_storage_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def compact_storage_composite_test(self):
        establish_compact_storage_composite_table(self.cluster.version(), self.session)
        verify_compact_storage_composite_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def nondefault_table_settings_test(self):
        establish_nondefault_table_settings(self.cluster.version(), self.session)
        verify_nondefault_table_settings(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def indexes_test(self):
        establish_indexes_table(self.cluster.version(), self.session)
        verify_indexes_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since('2.0')
    def static_column_test(self):
        establish_static_column_table(self.cluster.version(), self.session)
        verify_static_column_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since('2.1')
    def udt_table_test(self):
        establish_udt_table(self.cluster.version(), self.session)
        verify_udt_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since('2.2')
    def udf_test(self):
        establish_udf(self.cluster.version(), self.session)
        self.session.cluster.refresh_schema_metadata()
        verify_udf(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since('2.2')
    def uda_test(self):
        _establish_uda(self.cluster.version(), self.session)
        self.session.cluster.refresh_schema_metadata()
        _verify_uda(self.cluster.version(), self.cluster.version(), 'ks', self.session)
