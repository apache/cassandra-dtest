from abc import ABC, abstractmethod
from collections import defaultdict
from functools import partial
from itertools import repeat
import pytest
import time

from dtest import Tester, create_ks
from tools.jmxutils import (JolokiaAgent, make_mbean)

from cassandra import (ReadFailure, ReadTimeout, Unavailable,
                       WriteFailure, WriteTimeout,
                       ConsistencyLevel as CL)
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.marshal import int32_pack
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

since = pytest.mark.since

jmx = None

KEYSPACE = 'ks'
FAIL_WRITE_KEYSPACE = 'fail_keyspace'
VIEW_KEYSPACE = 'view_keyspace'
TABLE = 't'
VIEW = 'mv'
TOMBSTONE_FAILURE_THRESHOLD = 20
TOMBSTONE_FAIL_KEY = 10000001
JVM_ARGS = [f"-Dcassandra.test.fail_writes_ks={FAIL_WRITE_KEYSPACE}"]


class NoException(BaseException):
    pass


@since('4.0')
class TestClientRequestMetrics(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            'Testing write failures',  # The error to simulate a write failure
            'ERROR WRITE_FAILURE',  # Logged in DEBUG mode for write failures
            f"Scanned over {TOMBSTONE_FAILURE_THRESHOLD + 1} tombstones during query"  # Caused by the read failure tests
        )

    def setup_once(self):
        cluster = self.cluster
        cluster.set_configuration_options({'read_request_timeout_in_ms': 3000,
                                           'write_request_timeout_in_ms': 3000,
                                           'phi_convict_threshold': 12,
                                           'tombstone_failure_threshold': TOMBSTONE_FAILURE_THRESHOLD,
                                           'enable_materialized_views': 'true'})
        cluster.populate(2, debug=True)
        cluster.start(jvm_args=JVM_ARGS)
        node1 = cluster.nodelist()[0]

        global jmx
        jmx = JolokiaAgent(node1)
        jmx.start()

        s = self.session = self.patient_exclusive_cql_connection(node1, retry_policy=FallthroughRetryPolicy(), request_timeout=30)
        for k in [KEYSPACE, FAIL_WRITE_KEYSPACE]:
            create_ks(s, k, 2)
            s.execute(f"CREATE TABLE {k}.{TABLE} (k int, c int, v int, PRIMARY KEY (k,c))")

        create_ks(s, VIEW_KEYSPACE, 1)
        s.execute(f"CREATE TABLE {VIEW_KEYSPACE}.{TABLE} (k int, c int, v int, PRIMARY KEY (k,c))")
        s.execute(f"CREATE MATERIALIZED VIEW {VIEW_KEYSPACE}.{VIEW} AS SELECT * FROM {TABLE} WHERE c IS NOT NULL AND k IS NOT NULL PRIMARY KEY (c,k);")

        # Here we're doing a series of deletions in order to create enough tombstones to exceed the configured fail threshold.
        # This partition will be used to test read failures.
        for c in range(TOMBSTONE_FAILURE_THRESHOLD + 1):
            self.session.execute(f"DELETE FROM {KEYSPACE}.{TABLE} WHERE k={TOMBSTONE_FAIL_KEY} AND c={c}")

        node1.watch_log_for("Created default superuser role 'cassandra'")  # don't race with async default role creation, which creates a write
        node1.watch_log_for('Completed submission of build tasks for any materialized views defined at startup', filename='debug.log')  # view builds cause background reads

    def test_client_request_metrics(self):
        # this is written as a single test method in order to reuse the same cluster for all tests
        # setup_once configures and starts the cluster with all schema and preconditions required by all tests.
        self.setup_once()

        self.write_nominal()
        self.read_nominal()

        self.write_failures()
        self.write_unavailables()
        self.write_timeouts()

        self.read_failures()
        self.read_unavailables()
        self.read_timeouts()

        self.range_slice_failures()
        self.range_slice_unavailables()
        self.range_slice_timeouts()

        self.view_writes()

        self.cas_read()
        self.cas_read_contention()
        self.cas_read_failures()
        self.cas_read_unavailables()
        self.cas_read_timeouts()

        self.cas_write()
        self.cas_write_contention()
        self.cas_write_unavailables()
        self.cas_write_timeouts()
        self.cas_write_condition_not_met()

    def write_nominal(self):
        query_count = 5
        global_diff, cl_diff = self.validate_nominal('Write',
                                                     WriteMetrics,
                                                     SimpleStatement(
                                                         f"INSERT INTO {KEYSPACE}.{TABLE} (k,c) VALUES (0,0)",
                                                         consistency_level=CL.ONE),
                                                     query_count)

        assert global_diff['MutationSizeHistogram.Count'] == query_count

    def read_nominal(self):
        query_count = 5
        self.validate_nominal('Read',
                              ClientRequestMetrics,
                              SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k=0 AND c=0",
                                              consistency_level=CL.LOCAL_ONE),
                              query_count)

    def validate_nominal(self, global_scope, metric_class, statement, query_count):
        query_cl = statement.consistency_level
        cl_scope = scope_for_cl(global_scope, query_cl)
        # Validate other CLs metrics are not changing. We're not messing with Quorum because of the async write coming from
        # role creation at startup.
        # If the test takes long enough, it's possible to see some of the time-based metrics shift.
        other_cls = [c for c in CL.value_to_name if c not in (query_cl, CL.QUORUM)]
        global_baseline = metric_class(global_scope)
        cl_baseline = metric_class(cl_scope)
        other_baselines = [metric_class(scope_for_cl(global_scope, c)) for c in other_cls]
        cassandra_version = self.dtest_config.cassandra_version_from_build

        global_baseline.validate(cassandra_version)
        cl_baseline.validate(cassandra_version)
        for b in other_baselines:
            b.validate(cassandra_version)

        for _ in range(query_count):
            self.session.execute(statement)

        global_updated = metric_class(global_scope)
        cl_updated = metric_class(cl_scope)
        global_updated.validate(cassandra_version)
        cl_updated.validate(cassandra_version)

        global_diff = global_updated.diff(global_baseline)
        cl_diff = cl_updated.diff(cl_baseline)
        for diff in [global_diff, cl_diff]:
            assert diff['TotalLatency.Count'] > 0, diff.scope
            assert diff['Latency.Count'] == query_count, diff.scope

        other_updated = [metric_class(scope_for_cl(global_scope, c)) for c in other_cls]
        for updated, baseline in zip(other_updated, other_baselines):
            assert 'Latency.Count' not in updated.diff(baseline), updated.scope

        return global_diff, cl_diff

    def write_failures(self):
        query_count = 2
        diff = self.write_failures_variant('Write', 'WHERE k=0 AND c=0',
                                           query_count,
                                           self.validate_exception_metric_with_cl,
                                           WriteMetrics)
        assert diff['MutationSizeHistogram.Count'] == query_count

    def write_failures_variant(self, scope, constraint, query_count, validator, metric_class):
        query_cl = CL.ONE
        diff = validator(scope,
                         metric_class,
                         SimpleStatement(
                             f"UPDATE {FAIL_WRITE_KEYSPACE}.{TABLE} SET v=0 {constraint}",
                             consistency_level=query_cl),
                         query_count,
                         'Failures',
                         WriteFailure
                         )
        return diff

    def write_unavailables(self):
        # THREE will be unavailable since RF=2
        query_cl = CL.THREE
        query_count = 5
        diff = self.validate_exception_metric_with_cl('Write',
                                                      WriteMetrics,
                                                      SimpleStatement(
                                                          f"UPDATE {KEYSPACE}.{TABLE} SET v=0 WHERE k=0 AND c=0",
                                                          consistency_level=query_cl),
                                                      query_count,
                                                      'Unavailables',
                                                      Unavailable
                                                      )
        assert diff['MutationSizeHistogram.Count'] == query_count

    def write_timeouts(self):
        query_count = 2
        diff = self.write_timeouts_variant('Write', 'WHERE k=0 AND c=0',
                                           query_count,
                                           self.validate_exception_metric_with_cl,
                                           WriteMetrics)
        assert diff['MutationSizeHistogram.Count'] == query_count  # only done in this variant because CAS times out the request before mutation size is known

    def write_timeouts_variant(self, scope, constraint, query_count, validator, metric_class):
        query_cl = CL.TWO
        node2 = self.cluster.nodelist()[1]
        node2.pause()
        diff = validator(scope,
                         metric_class,
                         SimpleStatement(
                             f"UPDATE {KEYSPACE}.{TABLE} SET v=0 {constraint}",
                             consistency_level=query_cl),
                         query_count,
                         'Timeouts',
                         WriteTimeout
                         )
        node2.resume()
        return diff

    def read_failures(self):
        self.read_failures_variant('Read', f"WHERE k={TOMBSTONE_FAIL_KEY}",
                                   CL.TWO,
                                   self.validate_exception_metric_with_cl,
                                   ClientRequestMetrics)

    def read_unavailables(self):
        self.read_unavailables_variant('Read', 'WHERE k=0 AND c=0',
                                       self.validate_exception_metric_with_cl)

    def read_unavailables_variant(self, scope, constraint, validator):
        query_cl = CL.THREE
        validator(scope,
                  ClientRequestMetrics,
                  SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} {constraint}",
                                  consistency_level=query_cl),
                  5,
                  'Unavailables',
                  Unavailable
                  )

    def read_timeouts(self):
        self.read_timeouts_variant('Read', 'WHERE k=0',
                                   CL.TWO,
                                   self.validate_exception_metric_with_cl,
                                   ClientRequestMetrics)

    def read_timeouts_variant(self, scope, constraint, query_cl, validator, metric_class):
        node2 = self.cluster.nodelist()[1]
        node2.pause()
        validator(scope,
                  metric_class,
                  SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} {constraint}",
                                  consistency_level=query_cl),
                  1,
                  'Timeouts',
                  ReadTimeout
                  )
        node2.resume()

    def range_slice_failures(self):
        self.read_failures_variant('RangeSlice', '',
                                   CL.ONE,
                                   self.validate_metric,
                                   ClientRequestMetrics)

    def range_slice_unavailables(self):
        self.read_unavailables_variant('RangeSlice', '',
                                       self.validate_metric)

    def range_slice_timeouts(self):
        self.read_timeouts_variant('RangeSlice', f" WHERE TOKEN(k) < TOKEN({TOMBSTONE_FAIL_KEY})",
                                   CL.TWO,
                                   self.validate_metric,
                                   ClientRequestMetrics)

    def view_writes(self):
        # we need to know where the base table and MV replicas are going to have predictable metrics
        def ip_of_key(ks, key):
            hosts = self.session.cluster.metadata.get_replicas(ks, int32_pack(key))
            host = hosts[0]
            return host.address

        def key_for(ks, ip):
            return next(x for x in range(100000) if ip_of_key(ks, x) == ip)

        # base partition and mv partition on this node
        ks = VIEW_KEYSPACE
        query_count = 5
        key = key_for(ks, '127.0.0.1')
        diff = self.run_collect_view_write_metrics(SimpleStatement(f"INSERT INTO {ks}.{TABLE} (k,c) VALUES ({key},{key})", consistency_level=CL.ONE),
                                                   query_count)
        assert diff['Latency.Count'] == query_count
        assert diff['TotalLatency.Count'] > 0

        # base partition this node, mv partition remote
        key = key_for(ks, '127.0.0.1')
        key2 = key_for(ks, '127.0.0.2')
        diff = self.run_collect_view_write_metrics(SimpleStatement(f"INSERT INTO {ks}.{TABLE} (k,c) VALUES ({key},{key2})", consistency_level=CL.ONE),
                                                   query_count)
        assert diff['Latency.Count'] == query_count
        assert diff['TotalLatency.Count'] > 0
        assert diff['ViewReplicasAttempted.Count'] == query_count
        assert diff['ViewReplicasSuccess.Count'] == query_count
        assert diff['ViewWriteLatency.Count'] == query_count

        # base partition and mv both remote
        key = key_for(ks, '127.0.0.2')
        key2 = key_for(ks, '127.0.0.2')
        diff = self.run_collect_view_write_metrics(SimpleStatement(f"INSERT INTO {ks}.{TABLE} (k,c) VALUES ({key},{key2})", consistency_level=CL.ONE),
                                                   query_count)
        assert 'Latency.Count' not in diff

    def run_collect_view_write_metrics(self, statement, query_count):
        scope = 'ViewWrite'
        baseline = ViewWriteMetrics(scope)
        cassandra_version = self.dtest_config.cassandra_version_from_build
        baseline.validate(cassandra_version)
        for _ in range(query_count):
            self.session.execute(statement)

        # These metrics are not updated synchronously with the request, so we have to use the deterministic 'Count'
        # to watch for them to settle.
        sample = ViewWriteMetrics(scope)
        diff = sample.diff(baseline)
        while diff and 'Latency.Count' in diff:
            time.sleep(0.5)
            last = sample
            sample = ViewWriteMetrics(scope)
            diff = sample.diff(last)

        sample.validate(cassandra_version)

        return sample.diff(baseline)

    def cas_read(self):
        self.validate_metric('CASRead',
                             CASClientRequestMetrics,
                             SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k=0",
                                             consistency_level=CL.SERIAL),
                             2)

    def cas_read_contention(self):
        self.cas_contention(partial(CASClientRequestMetrics, 'CASRead'),
                            SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k=0",
                                            consistency_level=CL.SERIAL))

    def cas_contention(self, metric_factory, statement):

        query_count = 20
        cassandra_version = self.dtest_config.cassandra_version_from_build

        def sample():
            baseline = metric_factory()
            baseline.validate(cassandra_version)

            execute_concurrent_with_args(self.session,
                                         statement,
                                         repeat([], query_count), raise_on_first_error=False)

            updated = metric_factory()
            updated.validate(cassandra_version)

            return updated.diff(baseline)

        for _ in range(10):
            diff = sample()
            if 'ContentionHistogram.Count' in diff:
                break

        assert diff['Latency.Count'] == query_count
        assert diff['TotalLatency.Count'] > 0
        assert 0 < diff['ContentionHistogram.Count'] <= query_count

    def cas_read_failures(self):
        self.read_failures_variant('CASRead', f"WHERE k={TOMBSTONE_FAIL_KEY}",
                                   CL.SERIAL,
                                   self.validate_metric,
                                   CASClientRequestMetrics)

    def read_failures_variant(self, scope, constraint, query_cl, validator, metric_class):
        validator(scope,
                  metric_class,
                  SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} {constraint}",
                                  consistency_level=query_cl),
                  5,
                  'Failures',
                  ReadFailure
                  )

    def cas_read_unavailables(self):
        ks = KEYSPACE
        self.cas_unavailables_variant('CASRead',
                                      CASClientRequestMetrics,
                                      SimpleStatement(f"SELECT k FROM {ks}.{TABLE} WHERE k=0 AND c=0",
                                                      consistency_level=CL.SERIAL)
                                      )

    def cas_unavailables_variant(self, scope, metric_class, statement):
        # can't use the other variant because we actually need to set a sane CL and stop a node for unavailable.
        query_count = 5
        node2 = self.cluster.nodelist()[1]
        node2.stop()
        self.validate_metric(scope,
                             metric_class,
                             statement,
                             query_count,
                             'Unavailables',
                             Unavailable
                             )
        node2.start(jvm_args=JVM_ARGS)

    def cas_read_timeouts(self):
        self.read_timeouts_variant('CASRead', 'WHERE k=0',
                                   CL.SERIAL,
                                   self.validate_metric,
                                   CASClientRequestMetrics)

    def cas_write(self):
        self.validate_metric('CASWrite',
                             CASClientWriteRequestMetrics,
                             SimpleStatement(f"INSERT INTO {KEYSPACE}.{TABLE} (k,c) VALUES (0,0) IF NOT EXISTS",
                                             consistency_level=CL.ONE),
                             2)

    def cas_write_contention(self):
        self.cas_contention(partial(CASClientWriteRequestMetrics, 'CASWrite'),
                            SimpleStatement(
                                f"INSERT INTO {KEYSPACE}.{TABLE} (k,c) VALUES ({new_key()},0) IF NOT EXISTS",
                                consistency_level=CL.TWO))

    def cas_write_failures(self):
        query_count = 2
        diff = self.write_failures_variant('CASWrite', f"WHERE k={new_key()} AND c=0 IF v!=0",
                                           query_count,
                                           self.validate_metric,
                                           CASClientWriteRequestMetrics)
        # The way we're failing writes causes a StorageProxy::cas to throw before the metric is incremented on each
        # request after the first one.  We find the previous ballot in-progress and fail trying to commit it.
        assert diff['MutationSizeHistogram.Count'] == 1
        assert diff['UnfinishedCommit.Count'] == query_count - 1

    def cas_write_unavailables(self):
        ks = KEYSPACE
        self.cas_unavailables_variant('CASWrite',
                                      CASClientWriteRequestMetrics,
                                      SimpleStatement(f"UPDATE {ks}.{TABLE} SET v=2 WHERE k=0 AND c=0 IF v!=0",
                                                      consistency_level=CL.TWO)
                                      )

    def cas_write_timeouts(self):
        self.write_timeouts_variant('CASWrite', 'WHERE k=0 AND c=0 IF v!=0',
                                    2,
                                    self.validate_metric,
                                    CASClientWriteRequestMetrics)

    def cas_write_condition_not_met(self):
        scope = 'CASWrite'
        baseline = CASClientWriteRequestMetrics(scope)
        key = new_key()
        query_count = 5
        for _ in range(query_count):
            self.session.execute(f"UPDATE {KEYSPACE}.{TABLE} SET v=0 WHERE k={key} AND c=0 IF v!=0")

        updated = CASClientWriteRequestMetrics(scope)
        diff = updated.diff(baseline)
        assert diff['ConditionNotMet.Count'] == query_count - 1

    def validate_exception_metric_with_cl(self, global_scope, metric_class, statement, query_count, secondary_meter, expected_exception):
        query_cl = statement.consistency_level
        cl_scope = scope_for_cl(global_scope, query_cl)

        cl_baseline = metric_class(cl_scope)
        cl_baseline.validate(self.dtest_config.cassandra_version_from_build)

        core_diff = self.validate_metric(global_scope, metric_class, statement, query_count, secondary_meter, expected_exception)

        cl_updated = metric_class(cl_scope)
        cl_diff = cl_updated.diff(cl_baseline)
        assert cl_diff[f"{secondary_meter}.Count"] == query_count
        assert cl_diff[f"{secondary_meter}.MeanRate"] > 0

        return core_diff

    def validate_metric(self, scope, metric_class, statement, query_count, secondary_meter=None, expected_exception=NoException):
        baseline = metric_class(scope)

        for _ in range(query_count):
            try:
                self.session.execute(statement)
                if expected_exception != NoException:
                    assert False, f"Request did not raise expected exception: {expected_exception}"
            except expected_exception:
                pass

        updated = metric_class(scope)
        diff = updated.diff(baseline)
        assert diff['Latency.Count'] == query_count
        if secondary_meter:
            assert diff[f"{secondary_meter}.Count"] == query_count
            assert diff[f"{secondary_meter}.MeanRate"] > 0

        return diff


##############
# Utilities

class AbstractPropertyValues(ABC):
    def __init__(self, scope, name):
        self.scope = scope
        self.name = name
        self.mbean = make_mbean('metrics', type='ClientRequest', scope=scope, name=name)
        self.values = {}
        self.init()

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def validate(self, cassandra_version):
        pass

    def load(self, attr):
        self.values[attr] = jmx.read_attribute(self.mbean, attr)

    def diff(self, other):
        d = {}
        for k, v in self.values.items():
            if 'RecentValues' in k:
                continue
            v2 = other.values[k]
            if v != v2:
                key = f"{self.name}.{k}"
                d[key] = diff_value(v, v2)
        return d


class Counter(AbstractPropertyValues):
    def init(self):
        self.load("Count")

    def validate(self, cassandra_version):
        v = self.values['Count']
        assert isinstance(v, int), self.mbean
        assert v >= 0, self.mbean


class Meter(AbstractPropertyValues):
    def init(self):
        for a in ["Count",
                  "MeanRate",
                  "OneMinuteRate",
                  "FiveMinuteRate",
                  "FifteenMinuteRate",
                  "RateUnit"]:
            self.load(a)

    def validate(self, cassandra_version):
        assert self.values['RateUnit'] == 'events/second'
        for k, v in self.values.items():
            if k == 'RateUnit':
                continue
            is_non_negative(k, v)


stat_words = [
    'Min',
    'Max',
    'Mean',
    'StdDev',
    '50thPercentile',
    '75thPercentile',
    '95thPercentile',
    '98thPercentile',
    '99thPercentile',
    '999thPercentile',
    'RecentValues']


def validate_stat_values(prefix, values, cassandra_version):
    sample_count = values['Count']
    if sample_count:
        validate_sane_histogram_values(prefix, values, cassandra_version)
    else:
        validate_zero_histogram_values(prefix, values)


def validate_sane_histogram_values(prefix, values, cassandra_version):
    validators = defaultdict(lambda: is_positive)
    if 'DurationUnit' in values and cassandra_version >= '4.1':
        # Timer values (since 4.1) are in micros resolution. The default number of buckets should be 128.
        # See CASSANDRA-16760, CASSANDRA-17155
        validators['RecentValues'] = partial(is_histo_list, expected_len=128)
    else:
        validators['RecentValues'] = partial(is_histo_list, expected_len=165)
    validators['StdDev'] = is_non_negative
    validators['Min'] = is_non_negative

    for k, v in ((k, v) for k, v in values.items() if k in stat_words):
        validators[k](f"{prefix}.{k}", v)
    assert values['Min'] <= values['Max'], prefix
    assert values['Min'] <= values['Mean'], prefix
    assert values['Mean'] <= values['Max'], prefix

    last_pct = values['50thPercentile']
    for s in ['75th', '95th', '98th', '99th', '999th']:
        this_pct = values[f"{s}Percentile"]
        assert this_pct >= last_pct, prefix + ' ' + s
        last_pct = this_pct


def validate_zero_histogram_values(prefix, values):
    validators = defaultdict(lambda: is_zero)
    validators['RecentValues'] = is_zero_list
    validators['Mean'] = is_none
    validators['DurationUnit'] = is_microseconds
    for k, v in values.items():
        validators[k](f"{prefix}{k}", v)


class Histogram(AbstractPropertyValues):
    def init(self):
        for a in stat_words:
            self.load(a)
        self.load('Count')

    def validate(self, cassandra_version):
        validate_stat_values(self.mbean, self.values, cassandra_version)
        is_non_negative(self.mbean, self.values['Count'])


class LatencyMetricsTimer(Counter):

    def init(self):
        super(LatencyMetricsTimer, self).init()
        for a in stat_words:
            self.load(a)
        self.load('DurationUnit')

    def validate(self, cassandra_version):
        validate_stat_values(self.mbean, self.values, cassandra_version)
        is_microseconds(self.mbean, self.values['DurationUnit'])


class LatencyMetrics(object):
    def __init__(self, scope):
        self.scope = scope
        self.values = {'TotalLatency': Counter(scope, 'TotalLatency'),
                       'Latency': LatencyMetricsTimer(scope, 'Latency')}

    def diff(self, other):
        d = {}
        for k, v in self.values.items():
            d.update(other.values[k].diff(v))
        return d

    def validate(self, cassandra_version):
        for v in self.values.values():
            v.validate(cassandra_version)


class ClientRequestMetrics(LatencyMetrics):
    error_words = ['Failures', 'Timeouts', 'Unavailables']

    def __init__(self, scope):
        super(ClientRequestMetrics, self).__init__(scope)
        for mb in self.error_words:
            self.values[mb] = Meter(scope, mb)


class WriteMetrics(ClientRequestMetrics):
    def __init__(self, scope):
        super(WriteMetrics, self).__init__(scope)
        self.values['MutationSizeHistogram'] = Histogram(scope, 'MutationSizeHistogram')


class ViewWriteMetrics(ClientRequestMetrics):
    def __init__(self, scope):
        super(ViewWriteMetrics, self).__init__(scope)
        self.values['ViewReplicasAttempted'] = Counter(scope, 'ViewReplicasAttempted')
        self.values['ViewReplicasSuccess'] = Counter(scope, 'ViewReplicasSuccess')
        self.values['ViewWriteLatency'] = LatencyMetricsTimer(scope, 'ViewWriteLatency')

    def validate(self, cassandra_version):
        super(ViewWriteMetrics, self).validate(cassandra_version)
        self.values['ViewReplicasAttempted'].validate(cassandra_version)
        self.values['ViewReplicasSuccess'].validate(cassandra_version)
        self.values['ViewWriteLatency'].validate(cassandra_version)


class CASClientRequestMetrics(ClientRequestMetrics):
    def __init__(self, scope):
        super(CASClientRequestMetrics, self).__init__(scope)
        self.values['ContentionHistogram'] = Histogram(scope, 'ContentionHistogram')
        self.values['UnfinishedCommit'] = Counter(scope, 'UnfinishedCommit')
        self.values['UnknownResult'] = Meter(scope, 'UnknownResult')

    def validate(self, cassandra_version):
        super(CASClientRequestMetrics, self).validate(cassandra_version)
        self.values['ContentionHistogram'].validate(cassandra_version)
        self.values['UnfinishedCommit'].validate(cassandra_version)
        self.values['UnknownResult'].validate(cassandra_version)


class CASClientWriteRequestMetrics(CASClientRequestMetrics):
    def __init__(self, scope):
        super(CASClientWriteRequestMetrics, self).__init__(scope)
        self.values['MutationSizeHistogram'] = Histogram(scope, 'MutationSizeHistogram')
        self.values['ConditionNotMet'] = Counter(scope, 'ConditionNotMet')

    def validate(self, cassandra_version):
        super(CASClientWriteRequestMetrics, self).validate(cassandra_version)
        self.values['MutationSizeHistogram'].validate(cassandra_version)
        self.values['ConditionNotMet'].validate(cassandra_version)


def diff_num(v1, v2):
    return v2 - v1


def diff_str(v1, v2):
    return f"'{v1}' --> '{v2}'"


diff_function = {
    int: diff_num,
    float: diff_num,
    list: diff_str,
    str: diff_str,
}


def diff_value(v1, v2):
    if v1 is None or v2 is None:  # Before it's set, Mean "null" is returned as None
        return diff_str(v1, v2)
    return diff_function[type(v1)](v1, v2)


def scope_for_cl(scope, cl):
    return scope + '-' + CL.value_to_name[cl]


def is_zero(k, v):
    assert v == 0, k


def is_positive(k, v):
    assert v > 0, k


def is_non_negative(k, v):
    assert v >= 0, k


def is_none(k, v):
    assert v is None, k


def is_microseconds(k, v):
    assert v == 'microseconds', k


def is_zero_list(k, values):
    assert not any(values), k


def is_nonzero_list(k, values):
    assert any(values), k


def is_histo_list(k, values, expected_len):
    # since these values change on sampling, we can only generally verify it takes the proper form
    # There are in-tree unit tests around ClearableHistogram and DecayingEstimatedHistogramReservoir
    assert len(values) == expected_len, k
    assert all(isinstance(i, int) for i in values), k


last_key = 0


def new_key():
    global last_key
    last_key += 1
    return last_key
