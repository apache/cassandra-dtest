import sys
sys.path = ['/home/tahooie/ccm', '/home/tahooie/automaton'] + sys.path

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
logging.basicConfig(level=logging.INFO)
logging.info("Starting...")


class TestUpgrade(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestUpgrade, self).__init__(*argv, **kwargs)
        self.allow_log_errors = True

    def rolling_upgrade_node(self, node, stress_node, loader):
        """
        node is the node to upgrade. stress_ip is the node to run stress on.
        """
        print "Starting on upgrade procedure for node %s..." % node.name

        loader.update_server_list([stress_node.address()])

        print "Sleeping 5 seconds"
        time.sleep(5)
    
        logging.info("Upgrading node: %s %s" % (node.name, node.address()))
        logging.info("draining...")
        node.nodetool('drain')
        logging.info("sleeping 20")
        time.sleep(20)
        logging.info("stopping...")
        node.stop(wait_other_notice=True)
        logging.info("sleeping 1")
        time.sleep(1)
        logging.info("setting dir...")
        try:
            node.set_cassandra_dir(cassandra_version="cassandra")
        except Exception, e:
            new_exception = Exception("Did you clone and compile cassandra in $HOME/.ccm/repository/ ? original exception: " + str(e))
            raise new_exception
        logging.info("starting...")
        node.start(wait_other_notice=True)
        logging.info("sleeping 1")
        time.sleep(1)
        logging.info("scrubbing...")
        node.nodetool('scrub')


        logging.info("validating data...")
        loader.read_and_validate(step=10)
        logging.info("done validating")
        loader.check_exc()

        print "Done upgrading node %s." % node.name


    def upgrade089_to_repo_test(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_cassandra_dir(cassandra_version="1.0.6")

        # Create a ring
        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
        [node1, node2, node3] = cluster.nodelist()
        cluster.start()

        time.sleep(.5)

        print "Creating LoadMaker.."
        lm_standard = loadmaker.LoadMaker(column_family_name='rolling_cf_standard',
                consistency_level='QUORUM')
        lm_super = loadmaker.LoadMaker(column_family_name='rolling_cf_super',
                column_family_type='super', num_cols=2, consistency_level='TWO')
        lm_counter_standard = loadmaker.LoadMaker(
                column_family_name='rolling_cf_counter_standard', 
                is_counter=True, consistency_level='TWO', num_cols=3)
        lm_counter_super = loadmaker.LoadMaker(
                column_family_name='rolling_cf_counter_super', 
                is_counter=True, consistency_level='TWO',
                column_family_type='super', num_cols=2,
                num_counter_rows=20, num_subcols=2)
#        loader = loadmaker.ContinuousLoader([node2.address()], load_makers=
#                [
#                    lm_standard, 
#                    lm_super, 
#                    lm_counter_standard, 
#                    lm_counter_super,
#                ])

        print "sleeping 1"
        time.sleep(1)
        print "Generating some load"
        lm_standard.set_server_list([node2.address()])
        lm_standard.generate()
        print "Done generating load"
##########        lm_counter_super.validate(server_list=[node2.address()])
        print "draining..."
        node1.nodetool('drain')
        print "Done draining. stopping..."
        node1.stop()
        print "Done stopping."
#        node1.set_cassandra_dir(cassandra_version="cassandra")

#        import pdb; pdb.set_trace()
        print "sleeping 10"
        time.sleep(10)
        print "done sleeping"
        lm_standard.validate()

        print "c* dir set. Starting..."
        node1.start()

#        self.rolling_upgrade_node(node1, stress_node=node2, loader=loader)
#        self.rolling_upgrade_node(node2, stress_node=node3)
#        self.rolling_upgrade_node(node3, stress_node=node1)

#        loader.pause()
        cluster.flush()

        cluster.cleanup()


    def static_upgrade_node(self, node, stress_node):
        """
        a less intense upgrade. Doesn't leave stress running while upgrading.
        Intended for upgrading from version .7.10
        """
        # put some load on the cluster. 
        args = [
                    [   '--operation=INSERT', '--family-type=Standard', 
                        '--num-keys=100', '--consistency-level=TWO', 
                        '--average-size-values', '--create-index=KEYS', 
                        '--replication-factor=3', '--keep-trying=2',
                        '--threads=1', '--nodes='+stress_node.address()],
                    [   '--operation=INSERT', '--family-type=Super', 
                        '--num-keys=100', '--consistency-level=TWO', 
                        '--average-size-values', '--create-index=KEYS', 
                        '--replication-factor=3', '--keep-trying=2',
                        '--threads=1', '--nodes='+stress_node.address()],
                ]
        sm = StressMaker(stress_node, args)
        sm.join(120)
    
        node.nodetool('drain')
        time.sleep(1)
        node.stop(wait_other_notice=True)
        time.sleep(1)
        try:
            node.set_cassandra_dir(cassandra_version="cassandra")
        except Exception, e:
            new_exception = Exception("Did you clone and compile cassandra in $HOME/.ccm/repository/ ?"\
                    " original exception: " + str(e))
            raise new_exception
        node.start(wait_other_notice=True)
        time.sleep(1)
        node.nodetool('scrub')

        args = [
                    [   '--operation=READ', '--family-type=Standard', 
                        '--num-keys=100', '--consistency-level=ALL', 
                        '--threads=10', '--keep-trying=2',
                        '--nodes='+stress_node.address()],
                    [   '--operation=READ', '--family-type=Super', 
                        '--num-keys=100', '--consistency-level=ALL', 
                        '--threads=10', '--keep-trying=2',
                        '--nodes='+stress_node.address()],
        ]
        sm = StressMaker(stress_node, args)
        sm.join(60)

#    def upgrade106_to_repo_test(self):
#        cluster = self.cluster

####         Forcing cluster version on purpose
#        cluster.set_cassandra_dir(cassandra_version="1.0.6")

####         Create a ring
#        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
#        [node1, node2, node3] = cluster.nodelist()

#        time.sleep(.5)
#        self.rolling_upgrade_node(node1, stress_node=node2)
#        self.rolling_upgrade_node(node2, stress_node=node3)
#        self.rolling_upgrade_node(node3, stress_node=node1)

#        cluster.flush()

#        cluster.cleanup()

#    def upgrade0710_to_repo_test(self):
#        cluster = self.cluster

####         Forcing cluster version on purpose
#        cluster.set_cassandra_dir(cassandra_version="0.7.10")

####         Create a ring
#        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
#        [node1, node2, node3] = cluster.nodelist()

#        time.sleep(.5)
#        self.rolling_upgrade_node(node1, stress_node=node2)
#        self.rolling_upgrade_node(node2, stress_node=node3)
#        self.rolling_upgrade_node(node3, stress_node=node1)

#        cluster.flush()

#        cluster.cleanup()

