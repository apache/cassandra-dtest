from cassandra import cqltypes
from dtest import Tester
from nose.tools import assert_equal, assert_in
from tools import since
import re


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

    if version >= '2.0':
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
    return "{0}_{1}".format(re.sub(r"[^A-Za-z0-9]", "_", prefix), table_name)


class TestSchemaMetadata(Tester):
    def basic_table_datatype_test(self):
        cluster = self.cluster
        cluster.populate(1).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 1)
        establish_basic_datatype_table(self.cluster.version(), session)
        verify_basic_datatype_table(self.cluster.version(), self.cluster.version(), 'ks', session)

    @since('2.0')
    def static_column_test(self):
        cluster = self.cluster
        cluster.populate(1).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 1)
        establish_static_column_table(self.cluster.version(), session)
        verify_static_column_table(self.cluster.version(), self.cluster.version(), 'ks', session)

    def collection_table_datatype_test(self):
        cluster = self.cluster
        cluster.populate(1).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 1)
        establish_collection_datatype_table(self.cluster.version(), session)
        verify_collection_datatype_table(self.cluster.version(), self.cluster.version(), 'ks', session)

    def clustering_order_test(self):
        cluster = self.cluster
        cluster.populate(1).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 1)
        establish_clustering_order_table(self.cluster.version(), session)
        verify_clustering_order_table(self.cluster.version(), self.cluster.version(), 'ks', session)

    def compact_storage_test(self):
        cluster = self.cluster
        cluster.populate(1).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 1)
        establish_compact_storage_table(self.cluster.version(), session)
        verify_compact_storage_table(self.cluster.version(), self.cluster.version(), 'ks', session)

    def compact_storage_composite_test(self):
        cluster = self.cluster
        cluster.populate(1).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 1)
        establish_compact_storage_composite_table(self.cluster.version(), session)
        verify_compact_storage_composite_table(self.cluster.version(), self.cluster.version(), 'ks', session)

    def nondefault_table_settings_test(self):
        cluster = self.cluster
        cluster.populate(1).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 1)
        establish_nondefault_table_settings(self.cluster.version(), session)
        verify_nondefault_table_settings(self.cluster.version(), self.cluster.version(), 'ks', session)

    def indexes_test(self):
        cluster = self.cluster
        cluster.populate(1).start()

        session = self.patient_cql_connection(cluster.nodelist()[0])
        self.create_ks(session, 'ks', 1)
        establish_indexes_table(self.cluster.version(), session)
        verify_indexes_table(self.cluster.version(), self.cluster.version(), 'ks', session)
