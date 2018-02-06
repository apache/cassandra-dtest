import re
import logging
import types
from struct import pack
from uuid import UUID

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from ccmlib.node import Node

from dtest import Tester
from tools.misc import ImmutableMapping
from tools.jmxutils import JolokiaAgent, make_mbean


from cassandra.metadata import Murmur3Token, OrderedDict
import pytest


logging.getLogger('cassandra').setLevel(logging.CRITICAL)


class SSTable(object):

    def __init__(self, name, repaired, pending_id):
        self.name = name
        self.repaired = repaired
        self.pending_id = pending_id


def jmx_start(to_start, **kwargs):
    kwargs['jvm_args'] = kwargs.get('jvm_args', []) + ['-XX:-PerfDisableSharedMem']
    to_start.start(**kwargs)


class TableMetrics(object):

    def __init__(self, node, keyspace, table):
        assert isinstance(node, Node)
        self.jmx = JolokiaAgent(node)
        self.write_latency = make_mbean("metrics", type="Table", name="WriteLatency", keyspace=keyspace, scope=table)

    @property
    def write_count(self):
        return self.jmx.read_attribute(self.write_latency, "Count")

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


def patch_start(startable):
    old_start = startable.start

    def new_start(self, *args, **kwargs):
        kwargs['jvm_args'] = kwargs.get('jvm_args', []) + ['-XX:-PerfDisableSharedMem']
        return old_start(*args, **kwargs)

    startable.start = types.MethodType(new_start, startable)

def get_sstable_data(cls, node, keyspace):
    _sstable_name = re.compile('SSTable: (.+)')
    _repaired_at = re.compile('Repaired at: (\d+)')
    _pending_repair = re.compile('Pending repair: (\-\-|null|[a-f0-9\-]+)')

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



class TestTransientReplication(Tester):

    keyspace = "ks"
    table = "tbl"

    @pytest.fixture
    def cheap_quorums(self):
        return False

    use_cheap_quorums = pytest.mark.parametrize('cheap_quorums', [True])

    @pytest.fixture(scope='function', autouse=True)
    def setup_cluster(self, fixture_dtest_setup, cheap_quorums):
        fixture_dtest_setup.setup_overrides.cluster_options = ImmutableMapping({'hinted_handoff_enabled': 'false',
                                                                                'num_tokens': 1,
                                                                                'commitlog_sync_period_in_ms': 500})
        self.tokens = [0, 1, 2]

        patch_start(self.cluster)
        self.cluster.populate(3, tokens=self.tokens, debug=True, install_byteman=True)
        self.cluster.start()

        # enable shared memory
        for node in self.cluster.nodelist():
            patch_start(node)

        self.nodes = self.cluster.nodelist()
        self.node1, self.node2, self.node3 = self.nodes
        session = self.exclusive_cql_connection(self.node3)
        # import pdb; pdb.set_trace()
        replication_params = OrderedDict()
        replication_params['class'] = 'NetworkTopologyStrategy'
        replication_params['datacenter1'] = '3/1'
        if cheap_quorums:
            replication_params['cheap_quorums'] = 'true'
        replication_params = ', '.join("'%s': '%s'" % (k, v) for k, v in replication_params.items())

        session.execute("CREATE KEYSPACE %s WITH REPLICATION={%s}" % (self.keyspace, replication_params))
        session.execute("CREATE TABLE %s.%s (k INT PRIMARY KEY, v INT)" % (self.keyspace, self.table))

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

    def insert_row(self, pk, session=None, node=None):
        session = session or self.exclusive_cql_connection(node or self.node1)
        token = Murmur3Token.from_key(pack('>i', pk)).value
        assert token < self.tokens[0] or self.tokens[-1] < token   # primary replica should be node1
        self.quorum(session, "INSERT INTO %s.%s (k, v) VALUES (%s, %s)" % (self.keyspace, self.table, pk, pk))

    def test_insert_repair_cycle(self):
        """
        data should be written to the transient replica, but removed after incremental repair
        """
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        self.insert_row(1)

        for node in self.cluster.nodelist():
            self.assert_has_sstables(node, flush=True)

        self.node1.nodetool(' '.join(['repair', self.keyspace, '-pr']))

        self.assert_has_sstables(self.node1, compact=True)
        self.assert_has_sstables(self.node2, compact=True)
        self.assert_has_no_sstables(self.node3, compact=True)

    def test_full_to_trans_read_repair(self):
        """
        Data on a full replica shouldn't be rr'd to transient replicas
        """
        session = self.exclusive_cql_connection(self.node1)
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        self.node3.stop(wait_other_notice=True)
        self.insert_row(1, session=session)

        self.assert_has_sstables(self.node1, flush=True)

        self.node3.start(wait_other_notice=True)
        self.assert_has_no_sstables(self.node3, flush=True)

        self.node2.stop(wait_other_notice=True)

        result = list(self.quorum(session, "SELECT * FROM %s.%s WHERE k=1" % (self.keyspace, self.table)))
        assert len(result) == 1
        assert result[0].k == 1
        self.assert_has_sstables(self.node1, flush=True)
        self.assert_has_no_sstables(self.node3, flush=True)

        with StorageProxy(self.node1) as sp:
            assert sp.blocking_read_repair == 0

    def test_trans_to_full_read_repair(self):
        """
        data on transient replicas should be rr'd to full replicas
        """
        session = self.exclusive_cql_connection(self.node1)
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        self.node2.stop(wait_other_notice=True)
        self.insert_row(1, session=session)

        self.assert_has_sstables(self.node1, flush=True)
        self.assert_has_sstables(self.node3, flush=True)

        self.node1.stop(wait_other_notice=True)
        self.node2.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.assert_has_no_sstables(self.node2, flush=True)

        session = self.exclusive_cql_connection(self.node2)
        result = list(self.quorum(session, "SELECT * FROM %s.%s WHERE k=1" % (self.keyspace, self.table)))

        with StorageProxy(self.node2) as sp:
            assert sp.blocking_read_repair == 1

        assert len(result) == 1
        assert result[0].k == 1
        self.assert_has_sstables(self.node2, flush=True)
        self.assert_has_sstables(self.node3, flush=True)

        # TODO: check the inverse; that we're not repairing every read

    @use_cheap_quorums
    def test_cheap_quorums(self):
        """ writes shouldn't make it to transient nodes """
        session = self.exclusive_cql_connection(self.node1)
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        tm = lambda n: TableMetrics(n, self.keyspace, self.table)

        with tm(self.node1) as tm1, tm(self.node2) as tm2, tm(self.node3) as tm3:
            assert tm1.write_count == 0
            assert tm2.write_count == 0
            assert tm3.write_count == 0
            self.insert_row(1, session=session)
            assert tm1.write_count == 1
            assert tm2.write_count == 1
            assert tm3.write_count == 0

    @use_cheap_quorums
    def test_speculative_write(self):
        """ if a full replica isn't responding, we should send the write to the transient replica """
        session = self.exclusive_cql_connection(self.node1)
        for node in self.nodes:
            self.assert_has_no_sstables(node)

        tm = lambda n: TableMetrics(n, self.keyspace, self.table)
        self.node2.byteman_submit(['./byteman/stop_writes.btm'])

        with tm(self.node1) as tm1, tm(self.node2) as tm2, tm(self.node3) as tm3:
            assert tm1.write_count == 0
            assert tm2.write_count == 0
            assert tm3.write_count == 0
            self.insert_row(1, session=session)
            assert tm1.write_count == 1
            assert tm2.write_count == 0
            assert tm3.write_count == 1

