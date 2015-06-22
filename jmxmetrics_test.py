from dtest import Tester
from jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem

# Currently only have attributes that are incrementing.
# MBEAN_VALUES are expressed in tuple with the first value being the class, 
# the package (may be tuple), the attribute, and then the value. 
def MBEAN_VALUES_PRE(version, ks, table):
    typeName = 'ColumnFamily' if version <= '2.2.X' else 'Table'

    ret = [ ('metrics', typeName, {'name' : 'AllMemtablesLiveDataSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', typeName, {'name' : 'AllMemtablesHeapSize'} , 'Value', 'MBeanIncrement'),
            ('metrics', typeName, {'name' : 'AllMemtablesOffHeapSize'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'BloomFilterDiskSpaceUsed'}, 'Value', 'MBeanIncrement'),
            ('metrics', typeName, {'name' : 'BloomFilterFalsePositives'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'IndexSummaryOffHeapMemoryUsed'}, 'Value', 'MBeanIncrement'),
            ('metrics', typeName, {'name' : 'LiveDiskSpaceUsed'}, 'Value', 'MBeanIncrement'),
            ('metrics', typeName, {'name' : 'LiveSSTableCount'}, 'Value', 'MBeanIncrement'),
            ('metrics', typeName, {'name' : 'MemtableColumnsCount'}, 'Value', 'MBeanIncrement'),
            ('metrics', typeName, {'name' : 'MemtableLiveDataSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', typeName, {'name' : 'MemtableOnHeapSize'}, 'Value', 'MBeanIncrement'),
            ('metrics', typeName, {'name' : 'MemtableSwitchCount'}, 'Value', 'MBeanIncrement'),
            ('db', 'IndexSummaries', {}, 'MemoryPoolSizeInMB', 'MBeanIncrement'),
            ('db', 'IndexSummaries', {}, 'IndexIntervals', 'MBeanIncrement'),
            ('db', 'Caches', {}, 'CounterCacheKeysToSave', 2147483647),
            ('db', 'Caches', {}, 'CounterCacheSavePeriodInSeconds', 7200),
            ('metrics', typeName, {'name' : 'MemtableOffHeapSize'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'PendingCompactions'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'CompressionRatio'}, 'Value', 'MBeanDecrement'),
            ('db', 'BatchlogManager', {}, 'TotalBatchesReplayed', 0),
            ('db', 'Caches', {}, 'RowCacheSavePeriodInSeconds', 0)]

    if version <= '2.2.X':
        ret.extend([
            ('metrics', typeName, {'name' : 'MaxRowSize'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'MinRowSize'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'MeanRowSize'}, 'Value', 'MBeanDecrement'),
            ('metrics', typeName, {'name' : 'RowCacheHit'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'RowCacheHitOutOfRange'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'RowCacheMiss'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'EstimatedRowSizeHistogram', 'keyspace' : ks, 'scope' : table}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'EstimatedRowCount', 'keyspace' : ks, 'scope' : table}, 'Value', 'MBeanEqual')])
    else:
        ret.extend([
            ('metrics', typeName, {'name' : 'MaxPartitionSize'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'MinPartitionSize'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'MeanPartitionSize'}, 'Value', 'MBeanDecrement'),
            ('metrics', typeName, {'name' : 'PartitionCacheHit'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'PartitionCacheHitOutOfRange'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'PartitionCacheMiss'}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'EstimatedPartitionSizeHistogram',  'keyspace' : ks, 'scope' : table}, 'Value', 'MBeanEqual'),
            ('metrics', typeName, {'name' : 'EstimatedPartitionCount',  'keyspace' : ks, 'scope' : table}, 'Value', 'MBeanEqual')])

    return ret

# MBEAN_VALUES_POST_3.0 = 
class TestJMXMetrics(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)

    def begin_test(self):
        """
        @jira_ticket CASSANDRA-7436
        This test measures the values of MBeans before and after running a load. We expect 
        the values to change a certain way, and thus deem them as 'MBeanEqual','MBeanDecrement', 
        'MBeanIncrement', or a constant to experss this expected change. If the value does not reflect
        this expected change, then it raises an AssertionError. 
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
                            AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
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
            mbean_values = MBEAN_VALUES_PRE(cluster.version(), 'keyspace1', 'counter1')
            before = []
            mbeans = []
            errors = []
            for package, bean, attribute, expected in MBEAN_VALUES:
                mbean = make_mbean(package, type=bean, **bean_args)
                mbeans.append(mbean)
                before.append(jmx.read_attribute(mbean, attribute))

            if cluster.version() < "2.1":
                node.stress(['-o', 'insert', '-n', '100000', '-p', '7100'])
            else: 
                node.stress(['write', 'n=100K', '-port jmx=7100'])

            attr_counter = 0
            for package, bean, bean_args, attribute, expected in mbean_values:
                a_value = jmx.read_attribute(mbeans[attr_counter], attribute)
                b_value = before[attr_counter]
                if expected == 'MBeanIncrement':
                    if b_value >= a_value:
                        errors.append(mbeans[attr_counter] + " has a before value of " + str(b_value) + " and after value of " + str(a_value) + " and did not increment" + "\n")
                elif expected == 'MBeanDecrement':
                    if b_value <= a_value:
                        errors.append(mbeans[attr_counter] + " has a before value of " + str(b_value) + " and after value of " + str(a_value) + " and did not decrement" + "\n")
                elif expected == 'MBeanEqual':
                    if b_value != a_value:
                        errors.append(mbeans[attr_counter] + " has a before value of " + str(b_value) + " and after value of " + str(a_value) + ", which are not equal" + "\n")
                elif expected == 'MBeanZero':
                    if not (b_value == 0 and a_value == 0):
                        errors.append(mbeans[attr_counter] + " has a before value of " + str(b_value) + " and after value of " + str(a_value) + " and they do not equal zero" + "\n")
                # If expected is none of the above, then expected should be a number.
                else:
                    if a_value != expected:
                        errors.append(mbeans[attr_counter] + " has an after value of " + str(a_value) + " which does not equal " + str(expected) + "\n")
                attr_counter += 1

            self.assertEqual(len(errors), 0, "\n" + "\n".join(errors))



