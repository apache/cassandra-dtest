from dtest import Tester
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib import common as ccmcommon
import time
import logging

import loadmaker


# NOTE: with nosetests, use the --nologcapture flag to let logging get through.
# then set the logging level here.
# A logging-level of DEBUG will show the load being created.
logging.basicConfig(level=logging.INFO)
logging.info("Starting...")


class TestGlobalRowKeyCache(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestGlobalRowKeyCache, self).__init__(*argv, **kwargs)
        # When a node goes down under load it prints an error in it's log. 
        # If we don't allow log errors, then the test will fail.
#        self.allow_log_errors = True


    def general_correctness_test(self):
        """
        Tests insert/update/delete/read in various configurations to validate
        that data written comes back correct.
        """
        for key_cache_size_in_mb in [0, 2]:
            for row_cache_size_in_mb in [0, 2]:
                self.setUp()
                self.tearDown()
                
                
        
        

    def general_correctness_unused(self):
        logging.info("Called rolling_upgrade_node for: %s %s" % 
                (node.name, node.address()))
        logging.info("Stress node log file: " + stress_node.logfilename())
        logging.info("Stress node address: " + stress_node.address() + ':' + 
                stress_node.jmx_port)

        keyspace = 'rolling_ks'
    
        lm_standard = loadmaker.LoadMaker(column_family_name='rolling_cf_standard',
                consistency_level='TWO', keyspace_name=keyspace)
        lm_super = loadmaker.LoadMaker(column_family_name='rolling_cf_super',
                column_family_type='super', num_cols=2, consistency_level='TWO', 
                keyspace_name=keyspace)
        lm_counter_standard = loadmaker.LoadMaker(
                column_family_name='rolling_cf_counter_standard', 
                is_counter=True, consistency_level='TWO', num_cols=3, 
                keyspace_name=keyspace)
        lm_counter_super = loadmaker.LoadMaker(
                column_family_name='rolling_cf_counter_super', 
                is_counter=True, consistency_level='TWO',
                column_family_type='super', num_cols=2,
                num_counter_rows=20, num_subcols=2, keyspace_name=keyspace)

        loader_standard = loadmaker.ContinuousLoader([stress_node.address()], 
                sleep_between=1,
                load_makers=
                [
                    lm_standard, 
                    lm_super, 
                ])
        loader_counter = loadmaker.ContinuousLoader([stress_node.address()], 
                sleep_between=1,
                load_makers=
                [
                    lm_counter_standard, 
                    lm_counter_super, 
                ])
    
        logging.debug("Sleeping to get some data into the cluster")
        time.sleep(5)

        logging.info("pausing counter-add load. This is because a "
                "dead-but-not-detected-dead node will cause errors on "
                "counter-add.")
        loader_counter.pause()
        time.sleep(2)

        logging.info("draining...")
        node.nodetool('drain')
        logging.info("stopping...")
        node.stop(wait_other_notice=False)
        logging.info("Node stopped. Sleeping to let gossip detect it as down...")
        time.sleep(30)

        logging.info("Resuming counter loader")
        loader_counter.unpause()

        logging.info("Letting the counter loader generate some load")
        time.sleep(10)

        logging.info("Upgrading node")
        logging.info("setting dir...")
        node.set_cassandra_dir(git_branch="trunk")
            
        logging.info("starting...")
        node.start(wait_other_notice=True)
        logging.info("scrubbing...")
        node.nodetool('scrub')

        logging.info("validating standard data...")
        loader_standard.read_and_validate(step=10)
        loader_standard.exit()
        logging.info("validating counter data...")
        loader_counter.read_and_validate(step=10)
        loader_counter.exit()

        logging.info("Done upgrading node %s.\n" % node.name)


    def upgrade089_to_repo(self):
        logging.info("*** Starting on upgrade089_to_repo_test ***")
        cluster = self.cluster

        cluster.set_cassandra_dir(cassandra_version="0.8.9")

        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
        [node1, node2, node3] = cluster.nodelist()

        self.rolling_upgrade_node(node1, stress_node=node2)
        self.rolling_upgrade_node(node2, stress_node=node3)
        self.rolling_upgrade_node(node3, stress_node=node1)

        cluster.flush()
        cluster.cleanup()



