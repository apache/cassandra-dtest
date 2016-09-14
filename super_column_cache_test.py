from dtest import Tester
from thrift_bindings.v22.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.v22.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                        ColumnParent, KsDef, Mutation,
                                        SlicePredicate, SliceRange,
                                        SuperColumn)
from thrift_tests import get_thrift_client
from tools.misc import ImmutableMapping


class TestSCCache(Tester):
    cluster_options = ImmutableMapping({'start_rpc': 'true'})

    def sc_with_row_cache_test(self):
        """ Test for bug reported in #4190 """
        cluster = self.cluster

        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        self.patient_cql_connection(node1)

        node = self.cluster.nodelist()[0]
        host, port = node.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()

        ksdef = KsDef()
        ksdef.name = 'ks'
        ksdef.strategy_class = 'SimpleStrategy'
        ksdef.strategy_options = {'replication_factor': '1'}
        ksdef.cf_defs = []

        client.system_add_keyspace(ksdef)
        client.set_keyspace('ks')

        # create a super column family with UTF8 for all types
        cfdef = CfDef()
        cfdef.keyspace = 'ks'
        cfdef.name = 'Users'
        cfdef.column_type = 'Super'
        cfdef.comparator_type = 'UTF8Type'
        cfdef.subcomparator_type = 'UTF8Type'
        cfdef.key_validation_class = 'UTF8Type'
        cfdef.default_validation_class = 'UTF8Type'
        cfdef.caching = 'rows_only'

        client.system_add_column_family(cfdef)

        column = Column(name='name', value='Mina', timestamp=100)
        client.batch_mutate(
            {'mina': {'Users': [Mutation(ColumnOrSuperColumn(super_column=SuperColumn('attrs', [column])))]}},
            ThriftConsistencyLevel.ONE)

        column_parent = ColumnParent(column_family='Users')
        predicate = SlicePredicate(slice_range=SliceRange("", "", False, 100))
        super_columns = client.get_slice('mina', column_parent, predicate, ThriftConsistencyLevel.ONE)
        self.assertEqual(1, len(super_columns))
        super_column = super_columns[0].super_column
        self.assertEqual('attrs', super_column.name)
        self.assertEqual(1, len(super_column.columns))
        self.assertEqual('name', super_column.columns[0].name)
        self.assertEqual('Mina', super_column.columns[0].value)

        # add a 'country' subcolumn
        column = Column(name='country', value='Canada', timestamp=100)
        client.batch_mutate(
            {'mina': {'Users': [Mutation(ColumnOrSuperColumn(super_column=SuperColumn('attrs', [column])))]}},
            ThriftConsistencyLevel.ONE)

        super_columns = client.get_slice('mina', column_parent, predicate, ThriftConsistencyLevel.ONE)
        self.assertEqual(1, len(super_columns))
        super_column = super_columns[0].super_column
        self.assertEqual('attrs', super_column.name)
        self.assertEqual(2, len(super_column.columns))

        self.assertEqual('country', super_column.columns[0].name)
        self.assertEqual('Canada', super_column.columns[0].value)

        self.assertEqual('name', super_column.columns[1].name)
        self.assertEqual('Mina', super_column.columns[1].value)

        # add a 'region' subcolumn
        column = Column(name='region', value='Quebec', timestamp=100)
        client.batch_mutate(
            {'mina': {'Users': [Mutation(ColumnOrSuperColumn(super_column=SuperColumn('attrs', [column])))]}},
            ThriftConsistencyLevel.ONE)

        super_columns = client.get_slice('mina', column_parent, predicate, ThriftConsistencyLevel.ONE)
        self.assertEqual(1, len(super_columns))
        super_column = super_columns[0].super_column
        self.assertEqual('attrs', super_column.name)
        self.assertEqual(3, len(super_column.columns))

        self.assertEqual('country', super_column.columns[0].name)
        self.assertEqual('Canada', super_column.columns[0].value)

        self.assertEqual('name', super_column.columns[1].name)
        self.assertEqual('Mina', super_column.columns[1].value)

        self.assertEqual('region', super_column.columns[2].name)
        self.assertEqual('Quebec', super_column.columns[2].value)
