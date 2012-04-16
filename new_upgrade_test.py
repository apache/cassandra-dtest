from dtest import Tester
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib.node import TimeoutError
import random
import os

from tools import ThriftConnection

CASSANDRA_VERSION = os.environ.get('CASSANDRA_VERSION', '1.0.0')

versions = (
    '0.8.10', '1.0.9',
)

class TestUpgrade(Tester):
    """
    upgrades a 3-node cluster through each of the above versions.
    """

    def upgrade_test(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_cassandra_dir(cassandra_version=versions[0])

        # Create a ring
        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
        [node1, node2, node3] = cluster.nodelist()

#        import ipdb; ipdb.set_trace()

        conn = ThriftConnection(node1)
        conn.create_ks('ks')
        conn.create_cf('ks', 'cf')
        conn.insert_row('ks', 'cf', 1)

        for version in versions[1:]:
            self.upgrade_to_version(version)


    def upgrade_to_version(self, version):
        def upgrade_node(node):
            node.flush()
            time.sleep(.5)
            node.stop()
            node.set_cassandra_dir(cassandra_version=version)
            try:
                node.start(wait_other_notice=False)
            except TimeoutError, e:
                pass 
            time.sleep(.5)

        for node in self.cluster.nodelist():
            upgrade_node(node)
            
        

    def upgrade08(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_cassandra_dir(cassandra_version="0.7.10")

        # Create a ring
        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
        [node1, node2, node3] = cluster.nodelist()

        cursor = self.cql_connection(node1).cursor()
        self.create_ks(cursor, 'ks', 3)
        self.create_cf(cursor, 'cf')

        for n in xrange(0, 10000):
            insert_c1c2(cursor, n, "QUORUM")

        # CQL driver can't talk to 0.8 so let's use the cli
        #for n in xrange(0, 10000):
        #    query_c1c2(cursor, n, "QUORUM")
        cli = node1.cli().do("use ks").do("consistencylevel as QUORUM")
        for n in xrange(0, 20):
            cli.do("get cf[k%d][c1]" % n)
            time.sleep(.1)
            assert re.search('=> \(column=c1', cli.last_output()), cli.last_output()
        cli.close()

        cursor.close()

        # Upgrade node1
        node1.flush()
        time.sleep(.5)
        node1.stop(wait_other_notice=True)
        node1.set_cassandra_dir(cassandra_version=CASSANDRA_VERSION)
        node1.start(wait_other_notice=True)

        time.sleep(.5)
        cursor = self.cql_connection(node1, 'ks').cursor()

        # Check we can still read and write
        for n in xrange(10000, 20000):
            insert_c1c2(cursor, n, "ALL")

        for n in xrange(0, 20000):
            query_c1c2(cursor, n, "ALL")

        # CQL driver can't talk to 0.8 so let's use the cli
        #cursor1 = self.cql_connection(node2, 'ks').cursor()
        #for n in xrange(0, 10000):
        #    query_c1c2(cursor1, n, "QUORUM")

        # Check from an old node (again cli is necessary)
        cli = node1.cli().do("use ks").do("consistencylevel as QUORUM")
        for n in xrange(9990, 10010):
            cli.do("get cf[k%d][c1]" % n)
            time.sleep(.1)
            assert re.search('=> \(column=c1', cli.last_output()), cli.last_output()
        cli.close()

        # Upgrade node2
        node2.flush()
        time.sleep(.5)
        node2.stop(wait_other_notice=True)
        node2.set_cassandra_dir(cassandra_version=CASSANDRA_VERSION)
        node2.start(wait_other_notice=True)

        # Check we can still read and write
        for n in xrange(20000, 30000):
            insert_c1c2(cursor, n, "ALL")

        for n in xrange(0, 30000):
            query_c1c2(cursor, n, "ALL")

        # Upgrade node3
        node3.flush()
        time.sleep(.5)
        node3.stop(wait_other_notice=True)
        node3.set_cassandra_dir(cassandra_version=CASSANDRA_VERSION)
        node3.start(wait_other_notice=True)

        # Check we can still read and write
        for n in xrange(30000, 40000):
            insert_c1c2(cursor, n, "ALL")

        for n in xrange(0, 40000):
            query_c1c2(cursor, n, "ALL")

        cluster.flush()

        # Check we can bootstrap a new 1.0 node
        cluster.set_cassandra_dir(cassandra_version=CASSANDRA_VERSION)
        initial_size = node1.data_size()
        assert_almost_equal(*[node.data_size() for node in cluster.nodelist()])
        node4 = new_node(cluster, token=3*(2**125))
        node4.start()
        node4.watch_log_for("Bootstrap/Replace/Move completed")
        time.sleep(2)

        cluster.cleanup()
        time.sleep(.5)

        tokens =  [node.initial_token for node in cluster.nodelist()]
        assert_almost_equal(*[node.data_size() for node in cluster.nodelist()])
        assert_almost_equal(node1.data_size(), (3 * initial_size) / 4)



