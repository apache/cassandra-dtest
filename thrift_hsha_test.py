from dtest import Tester, debug
import unittest
import time
import os
import subprocess
import shlex 
import pycassa

JNA_PATH = '/usr/share/java/jna.jar'
ATTACK_JAR = 'cassandra-attack.jar'

class ThriftHSHATest(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)


    @unittest.skip('Disable for now until CASSANDRA-6546 has more feedback')
    def test_closing_connections(self):
        """Test CASSANDRA-6546 - do connections get closed when disabling / renabling thrift service?"""
        cluster = self.cluster
        cluster.set_configuration_options(values={
            'rpc_server_type' : 'hsha',
            'rpc_max_threads' : 20
        })
        
        cluster.populate(1)
        cluster.start()
        (node1,) = cluster.nodelist()

        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'test', 1)
        cursor.execute("CREATE TABLE \"CF\" (key text PRIMARY KEY, val text) WITH COMPACT STORAGE;")
        def make_connection():
            pool = pycassa.ConnectionPool('test', timeout=None)
            cf = pycassa.ColumnFamily(pool, 'CF')
            return pool

        pools = []
        for i in xrange(100):
            debug("Creating connection pools..")
            for x in xrange(3):
                pools.append(make_connection())
            debug("Disabling/Enabling thrift iteration #{i}".format(i=i))
            node1.nodetool('disablethrift')
            node1.nodetool('enablethrift')

        raw_input("wait")

    @unittest.skipIf(not os.path.exists(ATTACK_JAR), "No attack jar found")
    @unittest.skipIf(not os.path.exists(JNA_PATH), "No JNA jar found")
    def test_6285(self):
        """Test CASSANDRA-6285 with Viktor Kuzmin's  attack jar.

        This jar file is not a part of this repository, you can
        compile it yourself from sources found on CASSANDRA-6285. This
        test will be skipped if the jar file is not found.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={ 'rpc_server_type' : 'hsha'})

        # Enable JNA:
        with open(os.path.join(self.test_path, 'test', 'cassandra.in.sh'),'w') as f:
            f.write('CLASSPATH={jna_path}:$CLASSPATH\n'.format(
                jna_path=JNA_PATH))

        cluster.populate(2)
        cluster.start(use_jna=True)
        debug("Cluster started.")
        (node1, node2) = cluster.nodelist()

        cursor = self.patient_cql_connection(node1).cursor()
        self.create_ks(cursor, 'tmp', 2)

        cursor.execute("""CREATE TABLE "CF" (
  key blob,
  column1 timeuuid,
  value blob,
  PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=7200 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='NONE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 2} AND
  compression={'chunk_length_kb': '64', 'sstable_compression': 'DeflateCompressor'};
""")



        debug("running attack jar...")
        p = subprocess.Popen(shlex.split("java -jar {attack_jar}".format(attack_jar=ATTACK_JAR)))
        p.communicate()

        debug("Stopping cluster..")
        cluster.stop()
        debug("Starting cluster..")
        cluster.start(no_wait=True)
        debug("Waiting 10 seconds before we're done..")
        time.sleep(10)
        
