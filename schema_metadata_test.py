import pytest
import logging

from collections import defaultdict
from uuid import uuid4

from dtest import Tester, create_ks

since = pytest.mark.since
logger = logging.getLogger(__name__)


def establish_durable_writes_keyspace(version, session, table_name_prefix=""):
    session.execute("""
        CREATE KEYSPACE {}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """.format(_cql_name_builder(table_name_prefix, "durable_writes_default")))

    session.execute("""
        CREATE KEYSPACE {}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            AND durable_writes = 'false';
    """.format(_cql_name_builder(table_name_prefix, "durable_writes_false")))

    session.execute("""
        CREATE KEYSPACE {}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            AND durable_writes = 'true';
    """.format(_cql_name_builder(table_name_prefix, "durable_writes_true")))


def verify_durable_writes_keyspace(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    expected = {
        "durable_writes_default": True,
        "durable_writes_true": True,
        "durable_writes_false": False
    }
    for keyspace, is_durable in expected.items():
        keyspace_name = _cql_name_builder(table_name_prefix, keyspace)
        meta = session.cluster.metadata.keyspaces[keyspace_name]
        assert is_durable == meta.durable_writes, "keyspace [{}] had durable_writes of [{}] should be [{}]".format(keyspace_name, meta.durable_writes, is_durable)


def establish_indexes_table(version, session, table_name_prefix=""):
    table_name = _cql_name_builder(table_name_prefix, "test_indexes")
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
    index_name = _cql_name_builder("idx_" + table_name_prefix, table_name)
    logger.debug("table name: [{}], index name: [{}], prefix: [{}]".format(table_name, index_name, table_name_prefix))
    session.execute("CREATE INDEX {0} ON {1}( d )".format(index_name, table_name))


def verify_indexes_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    table_name = _cql_name_builder(table_name_prefix, "test_indexes")
    index_name = _cql_name_builder("idx_" + table_name_prefix, table_name)
    logger.debug("table name: [{}], index name: [{}], prefix: [{}]".format(table_name, index_name, table_name_prefix))
    meta = session.cluster.metadata.keyspaces[keyspace].indexes[index_name]

    assert 'd' == meta.index_options['target']

    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert 1 == len(meta.clustering_key)
    assert 'c' == meta.clustering_key[0].name

    assert 1 == len(meta.indexes)

    assert {'target': 'd'} == meta.indexes[index_name].index_options
    assert 3 == len(meta.primary_key)
    assert 'a' == meta.primary_key[0].name
    assert 'b' == meta.primary_key[1].name
    assert 'c' == meta.primary_key[2].name


def establish_clustering_order_table(version, session, table_name_prefix=""):
    table_name = _cql_name_builder(table_name_prefix, "test_clustering_order")
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
    table_name = _cql_name_builder(table_name_prefix, "test_clustering_order")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert 0 == len(meta.indexes)
    assert 2 == len(meta.primary_key)
    assert 'event_type' == meta.primary_key[0].name
    assert 'insertion_time' == meta.primary_key[1].name
    assert 1 == len(meta.clustering_key)
    assert 'insertion_time' == meta.clustering_key[0].name
    assert 'insertion_time DESC' in meta.as_cql_query()


def establish_compact_storage_table(version, session, table_name_prefix=""):
    table_name = _cql_name_builder(table_name_prefix, "test_compact_storage")
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
    table_name = _cql_name_builder(table_name_prefix, "test_compact_storage")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert 3 == len(meta.columns)
    assert 2 == len(meta.primary_key)
    assert 1 == len(meta.clustering_key)
    assert 'sub_block_id' == meta.clustering_key[0].name
    assert 'block_id' == meta.primary_key[0].name
    assert 'uuid' == meta.primary_key[0].cql_type
    assert 'sub_block_id' == meta.primary_key[1].name
    assert 'int' == meta.primary_key[1].cql_type
    assert 1 == len(meta.clustering_key)
    assert 'sub_block_id' == meta.clustering_key[0].name


def establish_compact_storage_composite_table(version, session, table_name_prefix=""):
    table_name = _cql_name_builder(table_name_prefix, "test_compact_storage_composite")
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
    table_name = _cql_name_builder(table_name_prefix, "test_compact_storage_composite")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert 4 == len(meta.columns)
    assert 3 == len(meta.primary_key)
    assert 'key' == meta.primary_key[0].name
    assert 'text' == meta.primary_key[0].cql_type
    assert 'column1' == meta.primary_key[1].name
    assert 'int' == meta.primary_key[1].cql_type
    assert 'column2' == meta.primary_key[2].name
    assert 'int' == meta.primary_key[2].cql_type
    assert 2 == len(meta.clustering_key)
    assert 'column1' == meta.clustering_key[0].name
    assert 'int' == meta.clustering_key[0].cql_type
    assert 'column2' == meta.clustering_key[1].name
    assert 'int' == meta.clustering_key[1].cql_type


def establish_nondefault_table_settings(version, session, table_name_prefix=""):
    table_name = _cql_name_builder(table_name_prefix, "test_nondefault_settings")
    cql = """
        create table {0} (
                    a uuid,
                    b uuid,
                    c uuid,
                    d uuid,
                    primary key((a,b),c) )
                WITH gc_grace_seconds = 9999
                AND bloom_filter_fp_chance = 0.5
                AND comment = 'insightful information'
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
    table_name = _cql_name_builder(table_name_prefix, "test_nondefault_settings")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]

    assert 'insightful information' == meta.options['comment']
    assert 9999 == meta.options['gc_grace_seconds']
    assert 0.5 == meta.options['bloom_filter_fp_chance']

    if created_on_version >= '2.1':
        assert 86400 == meta.options['default_time_to_live']
        assert 1 == meta.options['min_index_interval']
        assert 20 == meta.options['max_index_interval']

    if created_on_version >= '3.0':
        assert 2121 == meta.options['memtable_flush_period_in_ms']

    if created_on_version >= '3.0':
        if created_on_version >= '4.0':
            assert '55p' == meta.options['speculative_retry']
        else:
            assert '55PERCENTILE' == meta.options['speculative_retry']

    if current_version >= '3.0':
        assert 'org.apache.cassandra.io.compress.DeflateCompressor' == meta.options['compression']['class']
        assert '128' == meta.options['compression']['chunk_length_in_kb']
        assert 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy' == meta.options['compaction']['class']

    if '2.1' <= current_version < '3.0':
        assert '{"keys":"NONE", "rows_per_partition":"ALL"}' == meta.options['caching']
        assert '"chunk_length_kb":"128"' in meta.options['compression_parameters']
        assert '"sstable_compression":"org.apache.cassandra.io.compress.DeflateCompressor"' in meta.options['compression_parameters']
    elif current_version >= '3.0':
        assert 'NONE' == meta.options['caching']['keys']
        assert 'ALL' == meta.options['caching']['rows_per_partition']
        assert 'org.apache.cassandra.io.compress.DeflateCompressor' == meta.options['compression']['class']
        assert '128' == meta.options['compression']['chunk_length_in_kb']
        assert 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy' == meta.options['compaction']['class']
    else:
        assert 'ROWS_ONLY' == meta.options['caching']

    assert 2 == len(meta.partition_key)
    assert meta.partition_key[0].name == 'a'
    assert meta.partition_key[1].name == 'b'

    assert 1 == len(meta.clustering_key)
    assert meta.clustering_key[0].name == 'c'


def establish_uda(version, session, table_name_prefix=""):
    if version < '2.2':
        return
    function_name = _cql_name_builder(table_name_prefix, "test_uda_function")
    aggregate_name = _cql_name_builder(table_name_prefix, "test_uda_aggregate")

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


def verify_uda(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    if created_on_version < '2.2':
        return
    function_name = _cql_name_builder(table_name_prefix, "test_uda_function")
    aggregate_name = _cql_name_builder(table_name_prefix, "test_uda_aggregate")

    assert function_name + "(int,int)" in list(session.cluster.metadata.keyspaces[keyspace].functions.keys())
    assert aggregate_name + "(int)" in list(session.cluster.metadata.keyspaces[keyspace].aggregates.keys())

    aggr_meta = session.cluster.metadata.keyspaces[keyspace].aggregates[aggregate_name + "(int)"]
    assert function_name == aggr_meta.state_func
    assert 'int' == aggr_meta.state_type
    assert 'int' == aggr_meta.return_type


def establish_udf(version, session, table_name_prefix=""):
    if version < '2.2':
        return
    function_name = _cql_name_builder(table_name_prefix, "test_udf")
    session.execute('''
        CREATE OR REPLACE FUNCTION {0} (input double) CALLED ON NULL INPUT RETURNS double LANGUAGE java AS 'return Double.valueOf(Math.log(input.doubleValue()));';
        '''.format(function_name))


def verify_udf(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    if created_on_version < '2.2':
        return
    function_name = _cql_name_builder(table_name_prefix, "test_udf")
    assert function_name + "(double)" in list(session.cluster.metadata.keyspaces[keyspace].functions.keys())
    meta = session.cluster.metadata.keyspaces[keyspace].functions[function_name + "(double)"]
    assert 'java' == meta.language
    assert 'double' == meta.return_type
    assert ['double'] == meta.argument_types
    assert ['input'] == meta.argument_names
    assert 'return Double.valueOf(Math.log(input.doubleValue()));' == meta.body


def establish_udt_table(version, session, table_name_prefix=""):
    if version < '2.1':
        return
    table_name = _cql_name_builder(table_name_prefix, "test_udt")
    session.execute('''
          CREATE TYPE {0} (
              street text,
              city text,
              zip int
          )'''.format(table_name))


def verify_udt_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    if created_on_version < '2.1':
        return
    table_name = _cql_name_builder(table_name_prefix, "test_udt")
    meta = session.cluster.metadata.keyspaces[keyspace].user_types[table_name]

    assert meta.field_names == ['street', 'city', 'zip']
    assert 'street' == meta.field_names[0]
    assert 'text' == meta.field_types[0]
    assert 'city' == meta.field_names[1]
    assert 'text' == meta.field_types[1]
    assert 'zip' == meta.field_names[2]
    assert 'int' == meta.field_types[2]


def establish_static_column_table(version, session, table_name_prefix=""):
    if version < '2.0':
        return
    table_name = _cql_name_builder(table_name_prefix, "test_static_column")
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
    table_name = _cql_name_builder(table_name_prefix, "test_static_column")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    assert 4 == len(meta.columns)
    assert 'text' == meta.columns['user'].cql_type
    assert False == meta.columns['user'].is_static
    assert 'int' == meta.columns['balance'].cql_type
    assert True == meta.columns['balance'].is_static
    assert 'int' == meta.columns['expense_id'].cql_type
    assert False == meta.columns['expense_id'].is_static
    assert 'int' == meta.columns['amount'].cql_type
    assert False == meta.columns['amount'].is_static


def establish_collection_datatype_table(version, session, table_name_prefix=""):
    table_name = _cql_name_builder(table_name_prefix, "test_collection_datatypes")
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


def verify_collection_datatype_table(created_on_version, current_version, keyspace, session, table_name_prefix=""):
    table_name = _cql_name_builder(table_name_prefix, "test_collection_datatypes")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    if created_on_version > '2.1':
        assert 13 == len(meta.columns)
    else:
        assert 7 == len(meta.columns)

    assert 'list<int>' == meta.columns['a'].cql_type
    assert 'list<text>' == meta.columns['b'].cql_type
    assert 'set<int>' == meta.columns['c'].cql_type
    assert 'set<text>' == meta.columns['d'].cql_type
    assert 'map<text, text>' == meta.columns['e'].cql_type
    assert 'map<text, int>' == meta.columns['f'].cql_type

    if created_on_version > '2.1':
        assert 'frozen<list<int>>' == meta.columns['g'].cql_type
        assert 'frozen<list<text>>' == meta.columns['h'].cql_type
        assert 'frozen<set<int>>' == meta.columns['i'].cql_type
        assert 'frozen<set<text>>' == meta.columns['j'].cql_type
        assert 'frozen<map<text, text>>' == meta.columns['k'].cql_type
        assert 'frozen<map<text, int>>' == meta.columns['l'].cql_type


def establish_basic_datatype_table(version, session, table_name_prefix=""):
    table_name = _cql_name_builder(table_name_prefix, "test_basic_datatypes")
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
    table_name = _cql_name_builder(table_name_prefix, "test_basic_datatypes")
    meta = session.cluster.metadata.keyspaces[keyspace].tables[table_name]
    if created_on_version > '2.2':
        assert 19 == len(meta.columns)
    else:
        assert 15 == len(meta.columns)

    assert 1 == len(meta.primary_key)
    assert 'b' == meta.primary_key[0].name

    assert 'ascii' == meta.columns['a'].cql_type
    assert 'bigint' == meta.columns['b'].cql_type
    assert 'blob' == meta.columns['c'].cql_type
    assert 'boolean' == meta.columns['d'].cql_type
    assert 'decimal' == meta.columns['e'].cql_type
    assert 'double' == meta.columns['f'].cql_type
    assert 'float' == meta.columns['g'].cql_type
    assert 'inet' == meta.columns['h'].cql_type
    assert 'int' == meta.columns['i'].cql_type
    assert 'text' == meta.columns['j'].cql_type
    assert 'timestamp' == meta.columns['k'].cql_type
    assert 'timeuuid' == meta.columns['l'].cql_type
    assert 'uuid' == meta.columns['m'].cql_type
    assert 'text' == meta.columns['n'].cql_type
    assert 'varint' == meta.columns['o'].cql_type
    if created_on_version > '2.2':
        assert 'date' == meta.columns['p'].cql_type
        assert 'smallint' == meta.columns['q'].cql_type
        assert 'time' == meta.columns['r'].cql_type
        assert 'tinyint' == meta.columns['s'].cql_type


def _cql_name_builder(prefix, table_name):
    def get_uuid_str():
        return str(uuid4()).replace('-', '')
    try:
        consistent_hasher = _cql_name_builder.consistent_hasher
    except AttributeError:
        consistent_hasher = defaultdict(get_uuid_str)
        _cql_name_builder.consistent_hasher = consistent_hasher

    return "gen_{}".format(consistent_hasher[prefix + table_name])  # 'gen' as in 'generated'


class TestSchemaMetadata(Tester):
    @pytest.fixture(scope='function', autouse=True)
    def fixture_set_cluster_settings(self, fixture_dtest_setup):
        cluster = fixture_dtest_setup.cluster
        cluster.schema_event_refresh_window = 0

        if cluster.version() >= '3.0':
            cluster.set_configuration_options({'enable_user_defined_functions': 'true',
                                               'enable_scripted_user_defined_functions': 'true'})
        elif cluster.version() >= '2.2':
            cluster.set_configuration_options({'enable_user_defined_functions': 'true'})
        cluster.populate(1).start()

        self.session = fixture_dtest_setup.patient_cql_connection(cluster.nodelist()[0])
        create_ks(self.session, 'ks', 1)

    def _keyspace_meta(self, keyspace_name="ks"):
        self.session.cluster.refresh_schema_metadata()
        return self.session.cluster.metadata.keyspaces[keyspace_name]

    def test_creating_and_dropping_keyspace(self):
        starting_keyspace_count = len(self.session.cluster.metadata.keyspaces)
        assert True == self._keyspace_meta().durable_writes
        self.session.execute("""
                CREATE KEYSPACE so_long
                    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                    AND durable_writes = false
            """)
        assert False == self._keyspace_meta('so_long').durable_writes
        self.session.execute("DROP KEYSPACE so_long")
        assert starting_keyspace_count == len(self.session.cluster.metadata.keyspaces)

    def test_creating_and_dropping_table(self):
        self.session.execute("create table born_to_die (id uuid primary key, name varchar)")
        meta = self._keyspace_meta().tables['born_to_die']
        assert 'ks' == meta.keyspace_name
        assert 'born_to_die' == meta.name
        assert 1 == len(meta.partition_key)
        assert 'id' == meta.partition_key[0].name
        assert 2 == len(meta.columns)
        assert meta.columns.get('id') is not None
        assert 'uuid' == meta.columns['id'].cql_type
        assert meta.columns.get('name') is not None
        assert 'text' == meta.columns['name'].cql_type
        assert 0 == len(meta.clustering_key)
        assert 0 == len(meta.triggers)
        assert 0 == len(meta.indexes)
        self.session.execute("drop table born_to_die")
        assert self._keyspace_meta().tables.get('born_to_die') is None

    def test_creating_and_dropping_table_with_2ary_indexes(self):
        assert 0 == len(self._keyspace_meta().indexes)
        self.session.execute("create table born_to_die (id uuid primary key, name varchar)")
        self.session.execute("create index ix_born_to_die_name on born_to_die(name)")

        assert 1 == len(self._keyspace_meta().indexes)
        ix_meta = self._keyspace_meta().indexes['ix_born_to_die_name']
        assert 'ix_born_to_die_name' == ix_meta.name

        assert {'target': 'name'} == ix_meta.index_options
        assert 'COMPOSITES' == ix_meta.kind

        self.session.execute("drop table born_to_die")
        assert self._keyspace_meta().tables.get('born_to_die') is None
        assert self._keyspace_meta().indexes.get('ix_born_to_die_name') is None
        assert 0 == len(self._keyspace_meta().indexes)

    @since('2.1')
    def test_creating_and_dropping_user_types(self):
        assert 0 == len(self._keyspace_meta().user_types)
        self.session.execute("CREATE TYPE soon_to_die (foo text, bar int)")
        assert 1 == len(self._keyspace_meta().user_types)

        ut_meta = self._keyspace_meta().user_types['soon_to_die']
        assert 'ks' == ut_meta.keyspace
        assert 'soon_to_die' == ut_meta.name
        assert ['foo', 'bar'] == ut_meta.field_names
        assert ['text', 'int'] == ut_meta.field_types

        self.session.execute("DROP TYPE soon_to_die")
        assert 0 == len(self._keyspace_meta().user_types)

    @since('2.2')
    def test_creating_and_dropping_udf(self):
        assert 0 == len(self._keyspace_meta().functions), "expected to start with no indexes"
        self.session.execute("""
                CREATE OR REPLACE FUNCTION ks.wasteful_function (input double)
                    CALLED ON NULL INPUT
                    RETURNS double
                    LANGUAGE java AS 'return Double.valueOf(Math.log(input.doubleValue()));';
            """)
        assert 1 == len(self._keyspace_meta().functions), "udf count should be 1"
        udf_meta = self._keyspace_meta().functions['wasteful_function(double)']
        assert 'ks' == udf_meta.keyspace
        assert 'wasteful_function' == udf_meta.name
        assert ['double'] == udf_meta.argument_types
        assert ['input'] == udf_meta.argument_names
        assert 'double' == udf_meta.return_type
        assert 'java' == udf_meta.language
        assert 'return Double.valueOf(Math.log(input.doubleValue()));' == udf_meta.body
        assert udf_meta.called_on_null_input
        self.session.execute("DROP FUNCTION ks.wasteful_function")
        assert 0 == len(self._keyspace_meta().functions), "expected udf list to be back to zero"

    @since('2.2')
    def test_creating_and_dropping_uda(self):
        assert 0 == len(self._keyspace_meta().functions), "expected to start with no indexes"
        assert 0 == len(self._keyspace_meta().aggregates), "expected to start with no aggregates"
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
        assert 1 == len(self._keyspace_meta().functions), "udf count should be 1"
        assert 1 == len(self._keyspace_meta().aggregates), "uda count should be 1"
        udf_meta = self._keyspace_meta().functions['max_val(int,int)']
        uda_meta = self._keyspace_meta().aggregates['kind_of_max_agg(int)']

        assert 'ks' == udf_meta.keyspace
        assert 'max_val' == udf_meta.name
        assert ['int', 'int'] == udf_meta.argument_types
        assert ['current', 'candidate'] == udf_meta.argument_names
        assert 'int' == udf_meta.return_type
        assert 'java' == udf_meta.language
        assert 'if (current == null) return candidate; else return Math.max(current, candidate);' == udf_meta.body
        assert udf_meta.called_on_null_input

        assert 'ks' == uda_meta.keyspace
        assert 'kind_of_max_agg' == uda_meta.name
        assert ['int'] == uda_meta.argument_types
        assert 'max_val' == uda_meta.state_func
        assert 'int' == uda_meta.state_type
        assert None == uda_meta.final_func
        assert '-1' == uda_meta.initial_condition
        assert 'int' == uda_meta.return_type

        self.session.execute("DROP AGGREGATE ks.kind_of_max_agg")
        assert 0 == len(self._keyspace_meta().aggregates), "expected uda list to be back to zero"
        self.session.execute("DROP FUNCTION ks.max_val")
        assert 0 == len(self._keyspace_meta().functions), "expected udf list to be back to zero"

    def test_basic_table_datatype(self):
        establish_basic_datatype_table(self.cluster.version(), self.session)
        verify_basic_datatype_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def test_collection_table_datatype(self):
        establish_collection_datatype_table(self.cluster.version(), self.session)
        verify_collection_datatype_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def test_clustering_order(self):
        establish_clustering_order_table(self.cluster.version(), self.session)
        verify_clustering_order_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since("2.0", max_version="3.X")  # Compact Storage
    def test_compact_storage(self):
        establish_compact_storage_table(self.cluster.version(), self.session)
        verify_compact_storage_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since("2.0", max_version="3.X")  # Compact Storage
    def test_compact_storage_composite(self):
        establish_compact_storage_composite_table(self.cluster.version(), self.session)
        verify_compact_storage_composite_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def test_nondefault_table_settings(self):
        establish_nondefault_table_settings(self.cluster.version(), self.session)
        verify_nondefault_table_settings(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def test_indexes(self):
        establish_indexes_table(self.cluster.version(), self.session)
        verify_indexes_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    def test_durable_writes(self):
        establish_durable_writes_keyspace(self.cluster.version(), self.session)
        verify_durable_writes_keyspace(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since('2.0')
    def test_static_column(self):
        establish_static_column_table(self.cluster.version(), self.session)
        verify_static_column_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since('2.1')
    def test_udt_table(self):
        establish_udt_table(self.cluster.version(), self.session)
        verify_udt_table(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since('2.2')
    def test_udf(self):
        establish_udf(self.cluster.version(), self.session)
        self.session.cluster.refresh_schema_metadata()
        verify_udf(self.cluster.version(), self.cluster.version(), 'ks', self.session)

    @since('2.2')
    def test_uda(self):
        establish_uda(self.cluster.version(), self.session)
        self.session.cluster.refresh_schema_metadata()
        verify_uda(self.cluster.version(), self.cluster.version(), 'ks', self.session)
