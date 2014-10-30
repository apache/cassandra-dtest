from dtest import Tester, debug, DEFAULT_DIR
import unittest, time, os, subprocess, shlex, pycassa, glob, sys

JNA_PATH = '/usr/share/java/jna.jar'
ATTACK_JAR = 'cassandra-attack.jar'

# Use jna.jar in {CASSANDRA_DIR,DEFAULT_DIR}/lib/, since >=2.1 needs correct version
try:
    if glob.glob('%s/lib/jna-*.jar' % os.environ['CASSANDRA_DIR']):
        debug('Using jna.jar in CASSANDRA_DIR/lib..')
        JNA_IN_LIB = glob.glob('%s/lib/jna-*.jar' % os.environ['CASSANDRA_DIR'])
        JNA_PATH = JNA_IN_LIB[0]
except KeyError:
    if glob.glob('%s/lib/jna-*.jar' % DEFAULT_DIR):
        print ('Using jna.jar in DEFAULT_DIR/lib/..')
        JNA_IN_LIB = glob.glob('%s/lib/jna-*.jar' % DEFAULT_DIR)
        JNA_PATH = JNA_IN_LIB[0]

class ThriftHSHATest(Tester):

    def __init__(self, *args, **kwargs):
        Tester.__init__(self, *args, **kwargs)


    @unittest.skipIf(sys.platform == "win32", 'Could not be executed on Windows')
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

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'test', 1)
        cursor.execute("CREATE TABLE \"CF\" (key text PRIMARY KEY, val text) WITH COMPACT STORAGE;")
        def make_connection():
            pool = pycassa.ConnectionPool('test', timeout=None)
            cf = pycassa.ColumnFamily(pool, 'CF')
            return pool

        pools = []
        for i in xrange(10):
            debug("Creating connection pools..")
            for x in xrange(3):
                pools.append(make_connection())
            debug("Disabling/Enabling thrift iteration #{i}".format(i=i))
            node1.nodetool('disablethrift')
            node1.nodetool('enablethrift')
            debug("Closing connections from the client side..")
            for pool in pools:
                pool.dispose()
            stdout = subprocess.Popen(["lsof -a -p %s -iTCP -sTCP:CLOSE_WAIT" % node1.pid], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True).communicate()[0]
            lines = stdout.splitlines()
            self.assertEqual(len(lines), 0, "There are non-closed connections: %s" % stdout)

    @unittest.skipIf(not os.path.exists(ATTACK_JAR), "No attack jar found")
    @unittest.skipIf(not os.path.exists(JNA_PATH), "No JNA jar found")
    def test_6285(self):
        """Test CASSANDRA-6285 with Viktor Kuzmin's  attack jar.

        This jar file is not a part of this repository, you can
        compile it yourself from sources found on CASSANDRA-6285. This
        test will be skipped if the jar file is not found.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            'rpc_server_type' : 'hsha',
            'rpc_max_threads' : 20
        })

        # Enable JNA:
        with open(os.path.join(self.test_path, 'test', 'cassandra.in.sh'),'w') as f:
            f.write('CLASSPATH={jna_path}:$CLASSPATH\n'.format(
                jna_path=JNA_PATH))

        cluster.populate(2)
        nodes = (node1, node2) = cluster.nodelist()
        [n.start(use_jna=True) for n in nodes]
        debug("Cluster started.")

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'tmp', 2)

        cursor.execute("""CREATE TABLE "CF" (
  key blob,
  column1 timeuuid,
  value blob,
  PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE;
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

