import re
import logging
import types
from struct import pack
from uuid import UUID

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.query import SimpleStatement
from cassandra.protocol import ConfigurationException
from ccmlib.node import Node

from dtest import Tester, mk_bman_path
from tools.jmxutils import JolokiaAgent, make_mbean
from tools.data import rows_to_list
from tools.assertions import (assert_all)

from cassandra.metadata import Murmur3Token, OrderedDict
import pytest

since = pytest.mark.since

logging.getLogger('cassandra').setLevel(logging.CRITICAL)

NODELOCAL = 11
class SSTable(object):

    def __init__(self, name, repaired, pending_id):
        self.name = name
        self.repaired = repaired
        self.pending_id = pending_id


class TableMetrics(object):

    def __init__(self, node, keyspace, table):
        assert isinstance(node, Node)
        self.jmx = JolokiaAgent(node)
        self.write_latency_mbean = make_mbean("metrics", type="Table", name="WriteLatency", keyspace=keyspace, scope=table)
        self.speculative_reads_mbean = make_mbean("metrics", type="Table", name="SpeculativeRetries", keyspace=keyspace, scope=table)
        self.transient_writes_mbean = make_mbean("metrics", type="Table", name="TransientWrites", keyspace=keyspace, scope=table)

    @property
    def write_count(self):
        return self.jmx.read_attribute(self.write_latency_mbean, "Count")

    @property
    def speculative_reads(self):
        return self.jmx.read_attribute(self.speculative_reads_mbean, "Count")

    @property
    def transient_writes(self):
        return self.jmx.read_attribute(self.transient_writes_mbean, "Count")

    def start(self):
        self.jmx.start()

    def stop(self):
        self.jmx.stop()

    def __enter__(self):
        """ For contextmanager-style usage. """
        self.start()
        return self

    def __exit__(self, exc_type, value, traceback):
        """ For contextmanager-style usage. """
        self.stop()


class StorageProxy(object):

    def __init__(self, node):
        assert isinstance(node, Node)
        self.node = node
        self.jmx = JolokiaAgent(node)
        self.mbean = make_mbean("db", type="StorageProxy")

    def start(self):
        self.jmx.start()

    def stop(self):
        self.jmx.stop()

    @property
    def blocking_read_repair(self):
        return self.jmx.read_attribute(self.mbean, "ReadRepairRepairedBlocking")

    @property
    def speculated_data_request(self):
        return self.jmx.read_attribute(self.mbean, "ReadRepairSpeculatedRequest")

    @property
    def speculated_data_repair(self):
        return self.jmx.read_attribute(self.mbean, "ReadRepairSpeculatedRepair")

    def __enter__(self):
        """ For contextmanager-style usage. """
        self.start()
        return self

    def __exit__(self, exc_type, value, traceback):
        """ For contextmanager-style usage. """
        self.stop()

class StorageService(object):

    def __init__(self, node):
        assert isinstance(node, Node)
        self.node = node
        self.jmx = JolokiaAgent(node)
        self.mbean = make_mbean("db", type="StorageService")

    def start(self):
        self.jmx.start()

    def stop(self):
        self.jmx.stop()

    def get_replicas(self, ks, cf, key):
        return self.jmx.execute_method(self.mbean, "getNaturalEndpointsWithPort(java.lang.String,java.lang.String,java.lang.String,boolean)", [ks, cf, key, True])

    def __enter__(self):
        """ For contextmanager-style usage. """
        self.start()
        return self

    def __exit__(self, exc_type, value, traceback):
        """ For contextmanager-style usage. """
        self.stop()

def patch_start(startable):
    old_start = startable.start

    def new_start(self, *args, **kwargs):
        kwargs['jvm_args'] = kwargs.get('jvm_args', []) + ['-XX:-PerfDisableSharedMem',
                                                           '-Dcassandra.enable_nodelocal_queries=true']
        return old_start(*args, **kwargs)

    startable.start = types.MethodType(new_start, startable)
    return startable

def get_sstable_data(cls, node, keyspace):
    _sstable_name = re.compile(r'SSTable: (.+)')
    _repaired_at = re.compile(r'Repaired at: (\d+)')
    _pending_repair = re.compile(r'Pending repair: (\-\-|null|[a-f0-9\-]+)')

    out = node.run_sstablemetadata(keyspace=keyspace).stdout

    def matches(pattern):
        return filter(None, [pattern.match(l) for l in out.decode("utf-8").split('\n')])
    names = [m.group(1) for m in matches(_sstable_name)]
    repaired_times = [int(m.group(1)) for m in matches(_repaired_at)]

    def uuid_or_none(s):
        return None if s == 'null' or s == '--' else UUID(s)
    pending_repairs = [uuid_or_none(m.group(1)) for m in matches(_pending_repair)]
    assert names
    assert repaired_times
    assert pending_repairs
    assert len(names) == len(repaired_times) == len(pending_repairs)
    return [SSTable(*a) for a in zip(names, repaired_times, pending_repairs)]

@since('4.0')
class TransientReplicationBase(Tester):

    keyspace = "ks"
    table = "tbl"

    @pytest.fixture
    def cheap_quorums(self):
        return False

    def populate(self):
        self.cluster.populate(3, tokens=self.tokens, debug=True, install_byteman=True)

    def set_nodes(self):
        self.node1, self.node2, self.node3 = self.nodes

        # Make sure digest is not attempted against the transient node
        self.node3.byteman_submit([mk_bman_path('throw_on_digest.btm')])

    def stream_entire_sstables(self):
        return True

    def replication_factor(self):
        return '3/1'

    def tokens(self):
        return [0, 1, 2]

    def use_lcs(self):
        session = self.exclusive_cql_connection(self.node1)
        session.execute("ALTER TABLE %s.%s with compaction={'class': 'LeveledCompactionStrategy'};" % (self.keyspace, self.table))

    def setup_schema(self):
        session = self.exclusive_cql_connection(self.node1)
        replication_params = OrderedDict()
        replication_params['class'] = 'NetworkTopologyStrategy'
        replication_params['datacenter1'] = self.replication_factor()
        replication_params = ', '.join("'%s': '%s'" % (k, v) for k, v in replication_params.items())
        session.execute("CREATE KEYSPACE %s WITH REPLICATION={%s}" % (self.keyspace, replication_params))
        session.execute("CREATE TABLE %s.%s (pk int, ck int, value int, PRIMARY KEY (pk, ck)) WITH speculative_retry = 'NEVER' AND read_repair = 'NONE'" % (self.keyspace, self.table))

    @pytest.fixture(scope='function', autouse=True)
    def setup_cluster(self, fixture_dtest_setup):
        self.tokens = self.tokens()

        patch_start(self.cluster)
        self.cluster.set_configuration_options(values={'hinted_handoff_enabled': False,
                                                       'num_tokens': 1,
                                                       'stream_entire_sstables': self.stream_entire_sstables(),
                                                       'commitlog_sync_period_in_ms': 500,
                                                       'enable_transient_replication': True,
                                                       'dynamic_snitch': False})
        self.populate()
        self.cluster.start()

        self.nodes = [patch_start(node) for node in self.cluster.nodelist()]
        self.set_nodes()

        session = self.exclusive_cql_connection(self.node3)
        self.setup_schema()

    def assert_has_sstables(self, node, flush=False, compact=False):
        if flush:
            node.flush()
        if compact:
            node.nodetool(' '.join(['compact', self.keyspace, self.table]))

        sstables = node.get_sstables(self.keyspace, self.table)
        assert sstables

    def assert_has_no_sstables(self, node, flush=False, compact=False):
        if flush:
            node.flush()
        if compact:
            node.nodetool(' '.join(['compact', self.keyspace, self.table]))

        sstables = node.get_sstables(self.keyspace, self.table)
        assert not sstables

    def quorum(self, session, stmt_str):
        return session.execute(SimpleStatement(stmt_str, consistency_level=ConsistencyLevel.QUORUM))

    def nodelocal(self, session, stmt_str):
        return session.execute(SimpleStatement(stmt_str, consistency_level=NODELOCAL))

    def assert_local_rows(self, node, rows, ignore_order=False):
        assert_all(self.exclusive_cql_connection(node),
                   "SELECT * FROM %s.%s" % (self.keyspace, self.table),
                   rows,
                   cl=NODELOCAL,
                   ignore_order=ignore_order)

    def insert_row(self, pk, ck, value, session=None, node=None):
        session = session or self.exclusive_cql_connection(node or self.node1)
        token = Murmur3Token.from_key(pack('>i', pk)).value
        assert token < self.tokens[0] or self.tokens[-1] < token   # primary replica should be node1
        self.quorum(session, "INSERT INTO %s.%s (pk, ck, value) VALUES (%s, %s, %s)" % (self.keyspace, self.table, pk, ck, value))

    def delete_row(self, pk, ck, session=None, node=None):
        session = session or self.exclusive_cql_connection(node or self.node1)
        token = Murmur3Token.from_key(pack('>i', pk)).value
        assert token < self.tokens[0] or self.tokens[-1] < token   # primary replica should be node1
        self.quorum(session, "DELETE FROM %s.%s WHERE pk = %s AND ck = %s" % (self.keyspace, self.table, pk, ck))

    def read_as_list(self, query, session=None, node=None):
        session = session or self.exclusive_cql_connection(node or self.node1)
        return rows_to_list(self.quorum(session, query))

    def table_metrics(self, node):
        return TableMetrics(node, self.keyspace, self.table)

    def split(self, arr):
        arr1 = []
        arr2 = []
        for idx, item in enumerate(arr):
            if idx % 2 == 0:
                arr1.append(item)
            else:
                arr2.append(item)
        return (arr1, arr2)

    def generate_rows(self, partitions, rows):
        return [[pk, ck, pk+ck] for ck in range(rows) for pk in range(partitions)]

@since('4.0')
class TestTransientReplication(TransientReplicationBase):

    @pytest.mark.no_vnodes
    def test_transient_noop_write(self):
        """ If both full replicas are available, nothing should be written to the transient replica """
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        tm = lambda n: self.table_metrics(n)
        with tm(self.node1) as tm1, tm(self.node2) as tm2, tm(self.node3) as tm3:
            assert tm1.write_count == 0
            assert tm2.write_count == 0
            assert tm3.write_count == 0
            self.insert_row(1, 1, 1)
            assert tm1.write_count == 1
            assert tm2.write_count == 1
            assert tm3.write_count == 0

        self.assert_has_sstables(self.node1, flush=True)
        self.assert_has_sstables(self.node2, flush=True)
        self.assert_has_no_sstables(self.node3, flush=True)

    @pytest.mark.no_vnodes
    def test_transient_write(self):
        """ If write can't succeed on full replica, it's written to the transient node instead """
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        tm = lambda n: self.table_metrics(n)
        with tm(self.node1) as tm1, tm(self.node2) as tm2, tm(self.node3) as tm3:
            self.insert_row(1, 1, 1)
            # Stop writes to the other full node
            self.node2.byteman_submit([mk_bman_path('stop_writes.btm')])
            self.insert_row(1, 2, 2)

        # node1 should contain both rows
        self.assert_local_rows(self.node1,
                               [[1, 1, 1],
                                [1, 2, 2]])

        # write couldn't succeed on node2, so it has only the first row
        self.assert_local_rows(self.node2,
                               [[1, 1, 1]])

        # transient replica should hold only the second row
        self.assert_local_rows(self.node3,
                               [[1, 2, 2]])

    @pytest.mark.no_vnodes
    def test_transient_full_merge_read(self):
        """ When reading, transient replica should serve a missing read """
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        tm = lambda n: self.table_metrics(n)
        self.insert_row(1, 1, 1)
        # Stop writes to the other full node
        self.node2.byteman_submit([mk_bman_path('stop_writes.btm')])
        self.insert_row(1, 2, 2)

        # Stop reads from the node that will hold the second row
        self.node1.stop()

        # Whether we're reading from the full node or from the transient node, we should get consistent results
        for node in [self.node2, self.node3]:
            assert_all(self.exclusive_cql_connection(node),
                       "SELECT * FROM %s.%s" % (self.keyspace, self.table),
                       [[1, 1, 1],
                        [1, 2, 2]],
                       cl=ConsistencyLevel.QUORUM)

    @pytest.mark.no_vnodes
    def test_srp(self):
        """ When reading, transient replica should serve a missing read """
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        tm = lambda n: self.table_metrics(n)
        self.insert_row(1, 1, 1)
        self.insert_row(1, 2, 2)

        # Stop writes to the other full node
        self.node2.byteman_submit([mk_bman_path('stop_writes.btm')])
        self.delete_row(1, 1, node = self.node1)

        # Stop reads from the node that will hold the second row
        self.node1.stop()

        # Whether we're reading from the full node or from the transient node, we should get consistent results
        assert_all(self.exclusive_cql_connection(self.node3),
                   "SELECT * FROM %s.%s LIMIT 1" % (self.keyspace, self.table),
                   [[1, 2, 2]],
                   cl=ConsistencyLevel.QUORUM)

    @pytest.mark.no_vnodes
    def test_transient_full_merge_read_with_delete_transient_coordinator(self):
        self._test_transient_full_merge_read_with_delete(self.node3)

    @pytest.mark.no_vnodes
    def test_transient_full_merge_read_with_delete_full_coordinator(self):
        self._test_transient_full_merge_read_with_delete(self.node2)

    @pytest.mark.no_vnodes
    def _test_transient_full_merge_read_with_delete(self, coordinator):
        """ When reading, transient replica should serve a missing read """
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        tm = lambda n: self.table_metrics(n)
        self.insert_row(1, 1, 1)
        self.insert_row(1, 2, 2)
        # Stop writes to the other full node
        self.node2.byteman_submit([mk_bman_path('stop_writes.btm')])
        self.delete_row(1, 2)

        self.assert_local_rows(self.node3,
                               [])
        # Stop reads from the node that will hold the second row
        self.node1.stop()

        assert_all(self.exclusive_cql_connection(coordinator),
                   "SELECT * FROM %s.%s" % (self.keyspace, self.table),
                   [[1, 1, 1]],
                   cl=ConsistencyLevel.QUORUM)

    @pytest.mark.no_vnodes
    def test_cheap_quorums(self):
        """ writes shouldn't make it to transient nodes """
        session = self.exclusive_cql_connection(self.node1)
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        tm = lambda n: self.table_metrics(n)

        with tm(self.node1) as tm1, tm(self.node2) as tm2, tm(self.node3) as tm3:
            assert tm1.write_count == 0
            assert tm2.write_count == 0
            assert tm3.write_count == 0
            self.insert_row(1, 1, 1, session=session)
            assert tm1.write_count == 1
            assert tm2.write_count == 1
            assert tm3.write_count == 0

    @pytest.mark.no_vnodes
    def test_speculative_write(self):
        """ if a full replica isn't responding, we should send the write to the transient replica """
        session = self.exclusive_cql_connection(self.node1)
        self.node2.byteman_submit([mk_bman_path('slow_writes.btm')])

        self.insert_row(1, 1, 1, session=session)
        self.assert_local_rows(self.node1, [[1,1,1]])
        self.assert_local_rows(self.node2, [])
        self.assert_local_rows(self.node3, [[1,1,1]])

    @pytest.mark.skip(reason="Doesn't test quite the right combination of forbidden RF changes right now")
    def test_keyspace_rf_changes(self):
        """ they should throw an exception """
        session = self.exclusive_cql_connection(self.node1)
        replication_params = OrderedDict()
        replication_params['class'] = 'NetworkTopologyStrategy'
        assert self.replication_factor() == '3/1'
        replication_params['datacenter1'] = '5/2'
        replication_params = ', '.join("'%s': '%s'" % (k, v) for k, v in replication_params.items())
        with pytest.raises(ConfigurationException):
            session.execute("ALTER KEYSPACE %s WITH REPLICATION={%s}" % (self.keyspace, replication_params))

@since('4.0')
class TestTransientReplicationRepairStreamEntireSSTable(TransientReplicationBase):

    def stream_entire_sstables(self):
        return True

    def _test_speculative_write_repair_cycle(self, primary_range, optimized_repair, repair_coordinator, expect_node3_data, use_lcs=False):
        """
        if one of the full replicas is not available, data should be written to the transient replica, but removed after incremental repair
        """
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        if use_lcs:
            self.use_lcs()

        self.node2.byteman_submit([mk_bman_path('stop_writes.btm')])
        # self.insert_row(1)
        tm = lambda n: self.table_metrics(n)
        with tm(self.node1) as tm1, tm(self.node2) as tm2, tm(self.node3) as tm3:
            assert tm1.write_count == 0
            assert tm2.write_count == 0
            assert tm3.write_count == 0
            self.insert_row(1, 1, 1)
            assert tm1.write_count == 1
            assert tm2.write_count == 0
            assert tm3.write_count == 1

        self.assert_has_sstables(self.node1, flush=True)
        self.assert_has_no_sstables(self.node2, flush=True)
        self.assert_has_sstables(self.node3, flush=True)

        repair_opts = ['repair', self.keyspace]
        if primary_range: repair_opts.append('-pr')
        if optimized_repair: repair_opts.append('-os')
        self.node1.nodetool(' '.join(repair_opts))

        self.assert_has_sstables(self.node1, compact=True)
        self.assert_has_sstables(self.node2, compact=True)
        if expect_node3_data:
            self.assert_has_sstables(self.node3, compact=True)
        else:
            self.assert_has_no_sstables(self.node3, compact=True)

        entire_sstable = "true" if self.stream_entire_sstables() else "false"
        assert self.node2.grep_log('Incoming stream entireSSTable={}'.format(entire_sstable), filename='debug.log')

    @pytest.mark.no_vnodes
    def test_speculative_write_repair_cycle(self):
        """ incremental repair from full replica should remove data on node3 """
        self._test_speculative_write_repair_cycle(primary_range=False,
                                                  optimized_repair=False,
                                                  repair_coordinator=self.node1,
                                                  expect_node3_data=False)

    @pytest.mark.no_vnodes
    def test_primary_range_repair(self):
        """ primary range incremental repair from full replica should remove data on node3 """
        self._test_speculative_write_repair_cycle(primary_range=True,
                                                  optimized_repair=False,
                                                  repair_coordinator=self.node1,
                                                  expect_node3_data=False)

    @pytest.mark.no_vnodes
    def test_optimized_primary_range_repair(self):
        """ optimized primary range incremental repair from full replica should remove data on node3 """
        self._test_speculative_write_repair_cycle(primary_range=True,
                                                  optimized_repair=True,
                                                  repair_coordinator=self.node1,
                                                  expect_node3_data=False)

    @pytest.mark.no_vnodes
    def test_optimized_primary_range_repair_with_lcs(self):
        """ optimized primary range incremental repair from full replica should remove data on node3 using LCS """
        self._test_speculative_write_repair_cycle(primary_range=True,
                                                  optimized_repair=True,
                                                  repair_coordinator=self.node1,
                                                  expect_node3_data=False,
                                                  use_lcs=True)

    @pytest.mark.no_vnodes
    def test_transient_incremental_repair(self):
        """ transiently replicated ranges should be skipped when coordinating repairs """
        self._test_speculative_write_repair_cycle(primary_range=True,
                                                  optimized_repair=False,
                                                  repair_coordinator=self.node1,
                                                  expect_node3_data=False)

    @pytest.mark.no_vnodes
    def test_full_repair_from_full_replica(self):
        """ full repairs shouldn't replicate data to transient replicas """
        session = self.exclusive_cql_connection(self.node1)
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        self.insert_row(1, 1, 1, session=session)

        self.assert_has_sstables(self.node1, flush=True)
        self.assert_has_sstables(self.node2, flush=True)
        self.assert_has_no_sstables(self.node3, flush=True)

        self.node1.nodetool(' '.join(['repair', self.keyspace, '-full']))

        self.assert_has_sstables(self.node1, flush=True)
        self.assert_has_sstables(self.node2, flush=True)
        self.assert_has_no_sstables(self.node3, flush=True)

    @pytest.mark.no_vnodes
    def test_full_repair_from_transient_replica(self):
        """ full repairs shouldn't replicate data to transient replicas """
        session = self.exclusive_cql_connection(self.node1)
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        self.insert_row(1, 1, 1, session=session)

        self.assert_has_sstables(self.node1, flush=True)
        self.assert_has_sstables(self.node2, flush=True)
        self.assert_has_no_sstables(self.node3, flush=True)

        self.node3.nodetool(' '.join(['repair', self.keyspace, '-full']))

        self.assert_has_sstables(self.node1, flush=True)
        self.assert_has_sstables(self.node2, flush=True)
        self.assert_has_no_sstables(self.node3, flush=True)

@since('4.0')
class TestTransientReplicationRepairLegacyStreaming(TestTransientReplicationRepairStreamEntireSSTable):

    def stream_entire_sstables(self):
        return False

@since('4.0')
class TestTransientReplicationSpeculativeQueries(TransientReplicationBase):
    def setup_schema(self):
        session = self.exclusive_cql_connection(self.node1)
        replication_params = OrderedDict()
        replication_params['class'] = 'NetworkTopologyStrategy'
        replication_params['datacenter1'] = self.replication_factor()
        replication_params = ', '.join("'%s': '%s'" % (k, v) for k, v in replication_params.items())
        session.execute("CREATE KEYSPACE %s WITH REPLICATION={%s}" % (self.keyspace, replication_params))
        session.execute("CREATE TABLE %s.%s (pk int, ck int, value int, PRIMARY KEY (pk, ck)) WITH speculative_retry = 'NEVER' AND read_repair = 'NONE';" % (self.keyspace, self.table))

    @pytest.mark.no_vnodes
    def test_always_speculate(self):
        """ If write can't succeed on full replica, it's written to the transient node instead """
        session = self.exclusive_cql_connection(self.node1)
        session.execute("ALTER TABLE %s.%s WITH speculative_retry = 'ALWAYS';" % (self.keyspace, self.table))
        self.insert_row(1, 1, 1)
        # Stop writes to the other full node
        self.node2.byteman_submit([mk_bman_path('stop_writes.btm')])
        self.insert_row(1, 2, 2)

        for node in self.nodes:
            assert_all(self.exclusive_cql_connection(node),
                       "SELECT * FROM %s.%s WHERE pk = 1" % (self.keyspace, self.table),
                       [[1, 1, 1],
                        [1, 2, 2]],
                       cl=ConsistencyLevel.QUORUM)

    @pytest.mark.no_vnodes
    def test_custom_speculate(self):
        """ If write can't succeed on full replica, it's written to the transient node instead """
        session = self.exclusive_cql_connection(self.node1)
        session.execute("ALTER TABLE %s.%s WITH speculative_retry = '99.99PERCENTILE';" % (self.keyspace, self.table))
        self.insert_row(1, 1, 1)
        # Stop writes to the other full node
        self.node2.byteman_submit([mk_bman_path('stop_writes.btm')])
        self.insert_row(1, 2, 2)

        for node in self.nodes:
            assert_all(self.exclusive_cql_connection(node),
                       "SELECT * FROM %s.%s WHERE pk = 1" % (self.keyspace, self.table),
                       [[1, 1, 1],
                        [1, 2, 2]],
                       cl=ConsistencyLevel.QUORUM)

@since('4.0')
class TestMultipleTransientNodes(TransientReplicationBase):
    def populate(self):
        self.cluster.populate(5, tokens=self.tokens, debug=True, install_byteman=True)

    def set_nodes(self):
        self.node1, self.node2, self.node3, self.node4, self.node5 = self.nodes

    def replication_factor(self):
        return '5/2'

    def tokens(self):
        return [0, 1, 2, 3, 4]

    @pytest.mark.resource_intensive
    @pytest.mark.no_vnodes
    def test_transient_full_merge_read(self):
        """ When reading, transient replica should serve a missing read """
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        tm = lambda n: self.table_metrics(n)
        self.insert_row(1, 1, 1)
        # Stop writes to the other full node
        self.node2.byteman_submit([mk_bman_path('stop_writes.btm')])
        self.insert_row(1, 2, 2)

        self.assert_local_rows(self.node1,
                               [[1, 1, 1],
                                [1, 2, 2]])
        self.assert_local_rows(self.node2,
                               [[1, 1, 1]])
        self.assert_local_rows(self.node3,
                               [[1, 1, 1],
                                [1, 2, 2]])
        self.assert_local_rows(self.node4,
                               [[1, 2, 2]])
        self.assert_local_rows(self.node5,
                               [[1, 2, 2]])
        # Stop reads from the node that will hold the second row
        self.node1.stop()

        # Whether we're reading from the full node or from the transient node, we should get consistent results
        for node in [self.node2, self.node3, self.node4, self.node5]:
            assert_all(self.exclusive_cql_connection(node),
                       "SELECT * FROM %s.%s" % (self.keyspace, self.table),
                       [[1, 1, 1],
                        [1, 2, 2]],
                       cl=ConsistencyLevel.QUORUM)
