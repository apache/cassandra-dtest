import time

from flaky import flaky

from dtest import CASSANDRA_VERSION_FROM_BUILD, Tester, debug
from thrift_bindings.v22.ttypes import \
    ConsistencyLevel as ThriftConsistencyLevel
from thrift_bindings.v22.ttypes import (CfDef, Column, ColumnOrSuperColumn,
                                        ColumnParent, CounterColumn, KsDef,
                                        Mutation, SlicePredicate, SliceRange,
                                        SuperColumn, TimedOutException)
from thrift_tests import get_thrift_client
from tools.decorators import known_failure, since
from tools.flaky import RerunTestException, requires_rerun


@since('2.0', max_version='2.1.x')
class TestSCUpgrade(Tester):
    """
    Tests upgrade between 1.2->2.0 for super columns (since that's where we
    removed then internally).
    """

    def __init__(self, *args, **kwargs):
        self.ignore_log_patterns = [
            # This one occurs if we do a non-rolling upgrade, the node
            # it's trying to send the migration to hasn't started yet,
            # and when it does, it gets replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
        ]
        if CASSANDRA_VERSION_FROM_BUILD < '2.2':
            _known_teardown_race_error = (
                'ScheduledThreadPoolExecutor$ScheduledFutureTask@[0-9a-f]+ '
                'rejected from org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor'
            )
            # don't alter ignore_log_patterns on the class, just the obj for this test
            self.ignore_log_patterns += [_known_teardown_race_error]

        Tester.__init__(self, *args, **kwargs)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12616',
                   flaky=True)
    def upgrade_with_index_creation_test(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="1.2.16")
        if "memtable_allocation_type" in cluster._config_options:
            cluster._config_options.__delitem__("memtable_allocation_type")
        cluster.populate(2).start()

        node1, node2 = cluster.nodelist()

        # wait for the rpc server to start
        session = self.patient_exclusive_cql_connection(node1)

        host, port = node1.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()

        ksdef = KsDef()
        ksdef.name = 'test'
        ksdef.strategy_class = 'SimpleStrategy'
        ksdef.strategy_options = {'replication_factor': '2'}
        ksdef.durable_writes = True
        ksdef.cf_defs = []

        client.system_add_keyspace(ksdef)

        session.cluster.control_connection.wait_for_schema_agreement()

        client.set_keyspace('test')

        # create a super column family with UTF8 for all types
        cfdef = CfDef()
        cfdef.keyspace = 'test'
        cfdef.name = 'sc_test'
        cfdef.column_type = 'Super'
        cfdef.comparator_type = 'UTF8Type'
        cfdef.subcomparator_type = 'UTF8Type'
        cfdef.key_validation_class = 'UTF8Type'
        cfdef.default_validation_class = 'UTF8Type'
        cfdef.caching = 'rows_only'

        client.system_add_column_family(cfdef)

        session.cluster.control_connection.wait_for_schema_agreement()

        for i in range(2):
            supercol_name = 'sc%d' % i
            for j in range(2):
                col_name = 'c%d' % j
                column = Column(name=col_name, value='v', timestamp=100)
                client.batch_mutate(
                    {'k0': {'sc_test': [Mutation(ColumnOrSuperColumn(super_column=SuperColumn(supercol_name, [column])))]}},
                    ThriftConsistencyLevel.ONE)

        session.cluster.shutdown()
        client.transport.close()

        # Upgrade nodes to 2.0 for intermediate sstable conversion
        # See CASSANDRA-7008
        self.upgrade_to_version("binary:2.0.17")
        time.sleep(.5)

        # Upgrade node 1
        node1.flush()
        time.sleep(.5)
        node1.stop(wait_other_notice=True)
        self.set_node_to_current_version(node1)
        node1.start(wait_other_notice=True, wait_for_binary_proto=True)
        time.sleep(.5)

        # wait for the RPC server to start
        session = self.patient_exclusive_cql_connection(node1)

        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('test')

        # fetch all supercolumns
        column_parent = ColumnParent(column_family='sc_test')
        predicate = SlicePredicate(slice_range=SliceRange("", "", False, 100))
        super_columns = client.get_slice('k0', column_parent, predicate, ThriftConsistencyLevel.QUORUM)
        self.assertEqual(2, len(super_columns))
        for i in range(2):
            super_column = super_columns[i].super_column
            self.assertEqual('sc%d' % i, super_column.name)
            self.assertEqual(2, len(super_column.columns))
            for j in range(2):
                column = super_column.columns[j]
                self.assertEqual('c%d' % j, column.name)
                self.assertEqual('v', column.value)

        # fetch a single supercolumn
        column_parent = ColumnParent(column_family='sc_test', super_column='sc1')
        columns = client.get_slice('k0', column_parent, predicate, ThriftConsistencyLevel.QUORUM)
        self.assertEqual(2, len(columns))
        for j in range(2):
            column = columns[j].column
            self.assertEqual('c%d' % j, column.name)
            self.assertEqual('v', column.value)

        # fetch a single subcolumn
        predicate = SlicePredicate(column_names=['c1'])
        columns = client.get_slice('k0', column_parent, predicate, ThriftConsistencyLevel.QUORUM)
        self.assertEqual(1, len(columns))
        column = columns[0].column
        self.assertEqual('c%d' % j, column.name)
        self.assertEqual('v', column.value)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12340',
                   flaky=True)
    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-12616',
                   flaky=True)
    @flaky(max_runs=3, rerun_filter=requires_rerun)
    def upgrade_with_counters_test(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="1.2.19")
        if "memtable_allocation_type" in cluster._config_options:
            cluster._config_options.__delitem__("memtable_allocation_type")
        cluster.populate(3).start()

        node1, node2, node3 = cluster.nodelist()

        # wait for the rpc server to start
        session = self.patient_exclusive_cql_connection(node1)

        host, port = node1.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()

        ksdef = KsDef()
        ksdef.name = 'test'
        ksdef.strategy_class = 'SimpleStrategy'
        ksdef.strategy_options = {'replication_factor': '2'}
        ksdef.durable_writes = True
        ksdef.cf_defs = []

        client.system_add_keyspace(ksdef)
        client.set_keyspace('test')

        # create a super column family with UTF8 for all types except for the
        # values, which are counters
        cfdef = CfDef()
        cfdef.keyspace = 'test'
        cfdef.name = 'sc_test'
        cfdef.column_type = 'Super'
        cfdef.comparator_type = 'UTF8Type'
        cfdef.subcomparator_type = 'UTF8Type'
        cfdef.key_validation_class = 'UTF8Type'
        cfdef.default_validation_class = 'CounterColumnType'

        client.system_add_column_family(cfdef)

        session.cluster.control_connection.wait_for_schema_agreement()

        for i in range(2):
            supercol_name = 'sc%d' % i
            column_parent = ColumnParent(column_family='sc_test', super_column=supercol_name)
            for j in range(2):
                col_name = 'c%d' % j
                column = CounterColumn(name=col_name, value=1)
                for k in range(20):
                    try:
                        client.add('Counter1', column_parent, column, ThriftConsistencyLevel.ONE)
                    except TimedOutException:
                        raise RerunTestException("re-run test to verify")

        # If we are on 2.1 or any higher version upgrade to 2.0.latest.
        # Otherwise, we must be on a 2.0.x, so we should be upgrading to that version.
        # This will let us test upgrading from 1.2.19 to each of the 2.0 minor releases.
        # Upgrade nodes to 2.0.
        # See CASSANDRA-7008
        self.upgrade_to_version("binary:2.0.17", [node1])
        time.sleep(.5)

        # wait for the RPC server to start
        session = self.patient_exclusive_cql_connection(node1)

        for node in (node1, node2, node3):
            host, port = node.network_interfaces['thrift']
            client = get_thrift_client(host, port)
            client.transport.open()
            client.set_keyspace('test')
            for i in range(2):
                supercol_name = 'sc%d' % i
                column_parent = ColumnParent(column_family='sc_test', super_column=supercol_name)
                for j in range(2):
                    col_name = 'c%d' % j
                    column = CounterColumn(name=col_name, value=1)
                    for k in range(50):
                        try:
                            client.add('Counter1', column_parent, column, ThriftConsistencyLevel.ONE)
                        except TimedOutException:
                            raise RerunTestException("re-run test to verify")

            client.transport.close()

        # Upgrade nodes to 2.0.
        # See CASSANDRA-7008
        self.upgrade_to_version("binary:2.0.17", [node2, node3])
        time.sleep(.5)

        host, port = node1.network_interfaces['thrift']
        client = get_thrift_client(host, port)
        client.transport.open()
        client.set_keyspace('test')

        column_parent = ColumnParent(column_family='sc_test')
        predicate = SlicePredicate(slice_range=SliceRange("", "", False, 100))
        super_columns = client.get_slice('Counter1', column_parent, predicate, ThriftConsistencyLevel.QUORUM)
        self.assertEqual(2, len(super_columns))
        for i in range(2):
            super_column = super_columns[i].counter_super_column
            self.assertEqual('sc%d' % i, super_column.name)
            self.assertEqual(2, len(super_column.columns))
            for j in range(2):
                column = super_column.columns[j]
                self.assertEqual('c%d' % j, column.name)
                self.assertEqual(170, column.value)

        # fetch a single supercolumn
        column_parent = ColumnParent(column_family='sc_test', super_column='sc1')
        columns = client.get_slice('Counter1', column_parent, predicate, ThriftConsistencyLevel.QUORUM)
        self.assertEqual(2, len(columns))
        for j in range(2):
            column = columns[j].counter_column
            self.assertEqual('c%d' % j, column.name)
            self.assertEqual(170, column.value)

        # fetch a single subcolumn
        predicate = SlicePredicate(column_names=['c1'])
        columns = client.get_slice('Counter1', column_parent, predicate, ThriftConsistencyLevel.QUORUM)
        self.assertEqual(1, len(columns))
        column = columns[0].counter_column
        self.assertEqual('c%d' % j, column.name)
        self.assertEqual(170, column.value)

    def upgrade_to_version(self, tag, nodes=None):
        debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            if tag < "2.1":
                if "memtable_allocation_type" in node.config_options:
                    node.config_options.__delitem__("memtable_allocation_type")
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
            node.nodetool('upgradesstables -a')
