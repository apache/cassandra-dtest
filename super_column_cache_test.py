import pytest
import logging

from dtest_setup_overrides import DTestSetupOverrides

from dtest import Tester
from thrift_bindings.thrift010.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.thrift010.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                        ColumnParent, KsDef, Mutation,
                                        SlicePredicate, SliceRange,
                                        SuperColumn)
from thrift_test import get_thrift_client
from tools.misc import ImmutableMapping

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('2.0', max_version='4')
class TestSCCache(Tester):

    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    def test_sc_with_row_cache(self):
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

        column = Column(name='name'.encode(), value='Mina'.encode(), timestamp=100)
        client.batch_mutate(
            {'mina'.encode(): {'Users': [Mutation(ColumnOrSuperColumn(super_column=SuperColumn('attrs'.encode(), [column])))]}},
            ThriftConsistencyLevel.ONE)

        column_parent = ColumnParent(column_family='Users')
        predicate = SlicePredicate(slice_range=SliceRange("".encode(), "".encode(), False, 100))
        super_columns = client.get_slice('mina'.encode(), column_parent, predicate, ThriftConsistencyLevel.ONE)
        assert 1 == len(super_columns)
        super_column = super_columns[0].super_column
        assert 'attrs'.encode() == super_column.name
        assert 1 == len(super_column.columns)
        assert 'name'.encode() == super_column.columns[0].name
        assert 'Mina'.encode() == super_column.columns[0].value

        # add a 'country' subcolumn
        column = Column(name='country'.encode(), value='Canada'.encode(), timestamp=100)
        client.batch_mutate(
            {'mina'.encode(): {'Users': [Mutation(ColumnOrSuperColumn(super_column=SuperColumn('attrs'.encode(), [column])))]}},
            ThriftConsistencyLevel.ONE)

        super_columns = client.get_slice('mina'.encode(), column_parent, predicate, ThriftConsistencyLevel.ONE)
        assert 1 == len(super_columns)
        super_column = super_columns[0].super_column
        assert 'attrs'.encode() == super_column.name
        assert 2 == len(super_column.columns)

        assert 'country'.encode() == super_column.columns[0].name
        assert 'Canada'.encode() == super_column.columns[0].value

        assert 'name'.encode() == super_column.columns[1].name
        assert 'Mina'.encode() == super_column.columns[1].value

        # add a 'region' subcolumn
        column = Column(name='region'.encode(), value='Quebec'.encode(), timestamp=100)
        client.batch_mutate(
            {'mina'.encode(): {'Users': [Mutation(ColumnOrSuperColumn(super_column=SuperColumn('attrs'.encode(), [column])))]}},
            ThriftConsistencyLevel.ONE)

        super_columns = client.get_slice('mina'.encode(), column_parent, predicate, ThriftConsistencyLevel.ONE)
        assert 1 == len(super_columns)
        super_column = super_columns[0].super_column
        assert 'attrs'.encode() == super_column.name
        assert 3 == len(super_column.columns)

        assert 'country'.encode() == super_column.columns[0].name
        assert 'Canada'.encode() == super_column.columns[0].value

        assert 'name'.encode() == super_column.columns[1].name
        assert 'Mina'.encode() == super_column.columns[1].value

        assert 'region'.encode() == super_column.columns[2].name
        assert 'Quebec'.encode() == super_column.columns[2].value
