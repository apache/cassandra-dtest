import time
import os

from dtest import Tester, debug
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib import common as ccmcommon

import loadmaker

try:
    CASSANDRA_VERSION = os.environ['CASSANDRA_VERSION']
except KeyError:
    CASSANDRA_VERSION = 'git:trunk'

class TestRollingUpgrade(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestRollingUpgrade, self).__init__(*argv, **kwargs)
        # When a node goes down under load it prints an error in it's log. 
        # If we don't allow log errors, then the test will fail.
        self.allow_log_errors = True

    def rolling_upgrade_node(self, node, stress_node, create_ks_and_cf):
        """
        node is the node to upgrade. stress_ip is the node to run stress on.
        """
        debug("Called rolling_upgrade_node for: %s %s" % (node.name, node.address()))
        debug("Stress node log file: " + stress_node.logfilename())
        debug("Stress node address: " + stress_node.address() + ':' + stress_node.jmx_port)

        keyspace = 'rolling_ks'

        host, port = stress_node.network_interfaces['thrift']

        lm_standard = loadmaker.LoadMaker(
                host, port, 
                create_ks=create_ks_and_cf, create_cf=create_ks_and_cf,
                column_family_name='rolling_cf_standard',
                consistency_level='QUORUM', keyspace_name=keyspace)
        lm_counter_standard = loadmaker.LoadMaker(
                host, port,
                create_ks=create_ks_and_cf, create_cf=create_ks_and_cf,
                column_family_name='rolling_cf_counter_standard', 
                is_counter=True, consistency_level='QUORUM', num_cols=3, 
                keyspace_name=keyspace)

        loader_counter = loadmaker.ContinuousLoader( 
                load_makers=[lm_counter_standard],
                sleep_between=1,
                )
        loader_standard = loadmaker.ContinuousLoader(
                load_makers=[lm_standard],
                sleep_between=1,
                )
        loader_counter.read_and_validate(step=10)

        debug("Sleeping to get some data into the cluster")
        time.sleep(5)

        debug("pausing counter-add load. This is because a "
                  "dead-but-not-detected-dead node will cause errors on "
                  "counter-add.")
        loader_counter.pause()
        time.sleep(2)

        debug("draining...")
        node.nodetool('drain')
        debug("stopping...")
        node.stop(wait_other_notice=False)
        debug("Node stopped. Sleeping to let gossip detect it as down...")
        time.sleep(30)

        debug("Resuming counter loader")
        loader_counter.unpause()

        debug("Letting the counter loader generate some load")
        time.sleep(10)

        debug("Upgrading node")
        debug("setting dir...")
        node.set_cassandra_dir(cassandra_version=CASSANDRA_VERSION)

        debug("starting...")
        node.start(wait_other_notice=True)
        debug("upgradsstables...")
        node.nodetool('upgradesstables')

#        import ipdb; ipdb.set_trace()
        debug("validating standard data...")
        loader_standard.read_and_validate(step=10)
        loader_standard.exit()
        debug("validating counter data...")
        loader_counter.read_and_validate(step=10)
        loader_counter.exit()

        debug("Done upgrading node %s.\n" % node.name)


    def upgrade108_to_repo_test(self):
        """ Upgrade from 1.0.8 """

        cluster = self.cluster
        cluster.set_cassandra_dir(cassandra_version="1.0.8")

        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
        [node1, node2, node3] = cluster.nodelist()
        time.sleep(1)

        self.rolling_upgrade_node(node1, stress_node=node2, create_ks_and_cf=True)
        self.rolling_upgrade_node(node2, stress_node=node3, create_ks_and_cf=False)
        self.rolling_upgrade_node(node3, stress_node=node1, create_ks_and_cf=False)


