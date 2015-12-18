from dtest import Tester
from tools import debug, known_failure
from jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem


# We currently only have attributes that are incrementing.
# MBEAN_VALUES are expressed in tuple with the first value being the class, then the type,
# a dictionary of arguments for make_mbean(), the attribute, and then the value.
# MBEAN_VALUES_PRE are values for up to release 2.2, MBEAN_VALUES_POST are for 3.0 and later.
# In 3.0 "ColumnFamily" has been renamed to "Table" and "Row" to "Partition".
# However, the old names are also supported for backward compatibility and we test them via
# the mbean aliases, see begin_test().
def MBEAN_VALUES_PRE(ks, table, node):
    return [('db', 'Caches', {}, 'CounterCacheKeysToSave', 2147483647),
            ('db', 'Caches', {}, 'CounterCacheSavePeriodInSeconds', 7200),
            ('db', 'BatchlogManager', {}, 'TotalBatchesReplayed', 0),
            ('db', 'Caches', {}, 'RowCacheSavePeriodInSeconds', 0),
            ('db', 'IndexSummaries', {}, 'MemoryPoolSizeInMB', 'MBeanIncrement'),
            ('db', 'IndexSummaries', {}, 'IndexIntervals', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'AllMemtablesLiveDataSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'AllMemtablesHeapSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'AllMemtablesOffHeapSize'}, 'Value', offheap_memtable_val(node)),
            ('metrics', 'ColumnFamily', {'name': 'BloomFilterDiskSpaceUsed'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'BloomFilterFalsePositives'}, 'Value', 'MBeanEqual'),
            ('metrics', 'ColumnFamily', {'name': 'IndexSummaryOffHeapMemoryUsed'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'LiveDiskSpaceUsed'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'LiveSSTableCount'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'MemtableColumnsCount'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'MemtableLiveDataSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'MemtableOnHeapSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'MemtableSwitchCount'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'ColumnFamily', {'name': 'MemtableOffHeapSize'}, 'Value', offheap_memtable_val(node)),
            ('metrics', 'ColumnFamily', {'name': 'PendingCompactions'}, 'Value', 'MBeanEqual'),
            ('metrics', 'ColumnFamily', {'name': 'MaxRowSize'}, 'Value', 'MBeanEqual'),
            ('metrics', 'ColumnFamily', {'name': 'MinRowSize'}, 'Value', 'MBeanEqual'),
            ('metrics', 'ColumnFamily', {'name': 'MeanRowSize'}, 'Value', 'MBeanDecrement'),
            ('metrics', 'ColumnFamily', {'name': 'RowCacheHit'}, 'Value', 'MBeanEqual'),
            ('metrics', 'ColumnFamily', {'name': 'RowCacheHitOutOfRange'}, 'Value', 'MBeanEqual'),
            ('metrics', 'ColumnFamily', {'name': 'RowCacheMiss'}, 'Value', 'MBeanEqual'),
            ('metrics', 'ColumnFamily', {'name': 'EstimatedRowSizeHistogram', 'keyspace': ks, 'scope': table}, 'Value', 'MBeanEqual'),
            ('metrics', 'ColumnFamily', {'name': 'EstimatedRowCount', 'keyspace': ks, 'scope': table}, 'Value', 'MBeanEqual')]


def MBEAN_VALUES_POST(ks, table, node):
    return [('db', 'Caches', {}, 'CounterCacheKeysToSave', 2147483647),
            ('db', 'Caches', {}, 'CounterCacheSavePeriodInSeconds', 7200),
            ('db', 'BatchlogManager', {}, 'TotalBatchesReplayed', 0),
            ('db', 'Caches', {}, 'RowCacheSavePeriodInSeconds', 0),
            ('db', 'IndexSummaries', {}, 'MemoryPoolSizeInMB', 'MBeanIncrement'),
            ('db', 'IndexSummaries', {}, 'IndexIntervals', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'AllMemtablesLiveDataSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'AllMemtablesHeapSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'AllMemtablesOffHeapSize'}, 'Value', offheap_memtable_val(node)),
            ('metrics', 'Table', {'name': 'BloomFilterDiskSpaceUsed'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'BloomFilterFalsePositives'}, 'Value', 'MBeanEqual'),
            ('metrics', 'Table', {'name': 'IndexSummaryOffHeapMemoryUsed'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'LiveDiskSpaceUsed'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'LiveSSTableCount'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'MemtableColumnsCount'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'MemtableLiveDataSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'MemtableOnHeapSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'MemtableSwitchCount'}, 'Value', 'MBeanIncrement'),
            ('metrics', 'Table', {'name': 'MemtableOffHeapSize'}, 'Value', offheap_memtable_val(node)),
            ('metrics', 'Table', {'name': 'PendingCompactions'}, 'Value', 'MBeanEqual'),
            ('metrics', 'Table', {'name': 'MaxPartitionSize'}, 'Value', 'MBeanEqual'),
            ('metrics', 'Table', {'name': 'MinPartitionSize'}, 'Value', 'MBeanEqual'),
            ('metrics', 'Table', {'name': 'MeanPartitionSize'}, 'Value', 'MBeanDecrement'),
            ('metrics', 'Table', {'name': 'RowCacheHit'}, 'Value', 'MBeanEqual'),
            ('metrics', 'Table', {'name': 'RowCacheHitOutOfRange'}, 'Value', 'MBeanEqual'),
            ('metrics', 'Table', {'name': 'RowCacheMiss'}, 'Value', 'MBeanEqual'),
            ('metrics', 'Table', {'name': 'EstimatedPartitionSizeHistogram', 'keyspace': ks, 'scope': table}, 'Value', 'MBeanEqual'),
            ('metrics', 'Table', {'name': 'EstimatedPartitionCount', 'keyspace': ks, 'scope': table}, 'Value', 'MBeanEqual')]


def offheap_memtable_val(node):
    memtable_allocation_type = node.get_conf_option('memtable_allocation_type')
    offheap_memtable = memtable_allocation_type is not None and memtable_allocation_type.startswith('offheap')
    return 'MBeanIncrement' if offheap_memtable else 'MBeanEqual'

class TestJMXMetrics(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    @known_failure(failure_source='test',
                   jira_url='https://issues.apache.org/jira/browse/CASSANDRA-10845')
    def begin_test(self):
        """
        @jira_ticket CASSANDRA-7436
        This test measures the values of MBeans before and after running a load. We expect
        the values to change a certain way, and thus deem them as 'MBeanEqual','MBeanDecrement',
        'MBeanIncrement', or a constant to express this expected change. If the value does not reflect
        this expected change, then it raises an AssertionError.

        @jira_ticket CASSANDRA-9448
        This test also makes sure to cover all metrics that were renamed by CASSANDRA-9448, in post 3.0
        we also check that the old alias names are the same as the new names.
        """
        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        remove_perf_disable_shared_mem(node)
        cluster.start(wait_for_binary_proto=True)
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'keyspace1', 1)
        session.execute("""
                        CREATE TABLE keyspace1.counter1 (
                            key blob,
                            column1 ascii,
                            value counter,
                            PRIMARY KEY (key, column1)
                        ) WITH COMPACT STORAGE
                            AND CLUSTERING ORDER BY (column1 ASC)
                            AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                            AND comment = ''
                            AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32'}
                            AND compression = {}
                            AND dclocal_read_repair_chance = 0.1
                            AND default_time_to_live = 0
                            AND gc_grace_seconds = 864000
                            AND max_index_interval = 2048
                            AND memtable_flush_period_in_ms = 0
                            AND min_index_interval = 128
                            AND read_repair_chance = 0.0
                            AND speculative_retry = 'NONE';
                        """)

        with JolokiaAgent(node) as jmx:
            debug("Cluster version {}".format(cluster.version()))
            if cluster.version() <= '2.2.X':
                mbean_values = MBEAN_VALUES_PRE('keyspace1', 'counter1', node)
                mbean_aliases = None
            else:
                mbean_values = MBEAN_VALUES_POST('keyspace1', 'counter1', node)
                mbean_aliases = MBEAN_VALUES_PRE('keyspace1', 'counter1', node)

            before = []
            for package, bean, bean_args, attribute, expected in mbean_values:
                mbean = make_mbean(package, type=bean, **bean_args)
                debug(mbean)
                before.append(jmx.read_attribute(mbean, attribute))

            if mbean_aliases:
                alias_counter = 0
                for package, bean, bean_args, attribute, expected in mbean_aliases:
                    mbean = make_mbean(package, type=bean, **bean_args)
                    debug(mbean)
                    self.assertEqual(before[alias_counter], jmx.read_attribute(mbean, attribute))
                    alias_counter += 1

            node.stress(['write', 'n=100K'])

            errors = []
            after = []
            attr_counter = 0
            for package, bean, bean_args, attribute, expected in mbean_values:
                mbean = make_mbean(package, type=bean, **bean_args)
                a_value = jmx.read_attribute(mbean, attribute)
                after.append(a_value)
                b_value = before[attr_counter]
                if expected == 'MBeanIncrement':
                    if b_value >= a_value:
                        errors.append(mbean + " has a before value of " + str(b_value) + " and after value of " + str(a_value) + " and did not increment" + "\n")
                elif expected == 'MBeanDecrement':
                    if b_value <= a_value:
                        errors.append(mbean + " has a before value of " + str(b_value) + " and after value of " + str(a_value) + " and did not decrement" + "\n")
                elif expected == 'MBeanEqual':
                    if b_value != a_value:
                        errors.append(mbean + " has a before value of " + str(b_value) + " and after value of " + str(a_value) + ", which are not equal" + "\n")
                elif expected == 'MBeanZero':
                    if not (b_value == 0 and a_value == 0):
                        errors.append(mbean + " has a before value of " + str(b_value) + " and after value of " + str(a_value) + " and they do not equal zero" + "\n")
                # If expected is none of the above, then expected should be a number.
                else:
                    if a_value != expected:
                        errors.append(mbean + " has an after value of " + str(a_value) + " which does not equal " + str(expected) + "\n")
                attr_counter += 1

            self.assertEqual(len(errors), 0, "\n" + "\n".join(errors))

            if mbean_aliases:
                alias_counter = 0
                for package, bean, bean_args, attribute, expected in mbean_aliases:
                    mbean = make_mbean(package, type=bean, **bean_args)
                    self.assertEqual(after[alias_counter], jmx.read_attribute(mbean, attribute))
                    alias_counter += 1
