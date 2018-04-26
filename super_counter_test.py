import time
import pytest
import logging

from dtest_setup_overrides import DTestSetupOverrides
from dtest import Tester, create_ks
from thrift_test import get_thrift_client
from tools.misc import ImmutableMapping

from thrift_bindings.thrift010.Cassandra import (CfDef, ColumnParent, ColumnPath,
                                                 ConsistencyLevel, CounterColumn)

since = pytest.mark.since
logger = logging.getLogger(__name__)


@since('2.0', max_version='4')
class TestSuperCounterClusterRestart(Tester):
    """
    This test is part of this issue:
    https://issues.apache.org/jira/browse/CASSANDRA-3821
    """
    @pytest.fixture(scope='function', autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({'start_rpc': 'true'})
        return dtest_setup_overrides

    def test_functional(self):
        NUM_SUBCOLS = 100
        NUM_ADDS = 100

        cluster = self.cluster
        cluster.populate(3).start()
        node1 = cluster.nodelist()[0]

        time.sleep(.5)
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)
        time.sleep(1)  # wait for propagation

        # create the columnfamily using thrift
        host, port = node1.network_interfaces['thrift']
        thrift_conn = get_thrift_client(host, port)
        thrift_conn.transport.open()
        thrift_conn.set_keyspace('ks')
        cf_def = CfDef(keyspace='ks', name='cf', column_type='Super',
                       default_validation_class='CounterColumnType')
        thrift_conn.system_add_column_family(cf_def)

        # let the sediment settle to to the bottom before drinking...
        time.sleep(2)

        for subcol in range(NUM_SUBCOLS):
            for add in range(NUM_ADDS):
                column_parent = ColumnParent(column_family='cf',
                                             super_column=('subcol_%d' % subcol).encode())
                counter_column = CounterColumn('col_0'.encode(), 1)
                thrift_conn.add('row_0'.encode(), column_parent, counter_column,
                                ConsistencyLevel.QUORUM)
        time.sleep(1)
        cluster.flush()

        logger.debug("Stopping cluster")
        cluster.stop()
        time.sleep(5)
        logger.debug("Starting cluster")
        cluster.start()
        time.sleep(5)

        thrift_conn = get_thrift_client(host, port)
        thrift_conn.transport.open()
        thrift_conn.set_keyspace('ks')

        from_db = []

        for i in range(NUM_SUBCOLS):
            column_path = ColumnPath(column_family='cf', column='col_0'.encode(),
                                     super_column=(('subcol_%d' % i).encode()))
            column_or_super_column = thrift_conn.get('row_0'.encode(), column_path,
                                                     ConsistencyLevel.QUORUM)
            val = column_or_super_column.counter_column.value
            logger.debug(str(val)),
            from_db.append(val)
        logger.debug("")

        expected = [NUM_ADDS for i in range(NUM_SUBCOLS)]

        if from_db != expected:
            raise Exception("Expected a bunch of the same values out of the db. Got this: " + str(from_db))
