import sys
sys.path = ['/home/tahooie/ccm'] + sys.path

from dtest import Tester
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib import common as ccmcommon
import random
import multiprocessing
import time

class StressMaker(object):
    """
    calls Stress in separate processes in order to be able to kill them if needed.

    note that the stress functions run concurrently.
    """
    class Stressor(multiprocessing.Process):
        def __init__(self, node, stress_arg_list=[]):
            self._node = node
            self.stress_arg_list = stress_arg_list
            super(StressMaker.Stressor, self).__init__()
            self.daemon = True

        def run(self):
            print "Starting stress on %s with args %s" % (
                    self._node.name, str(self.stress_arg_list))
            self._node.stress(self.stress_arg_list)


    def __init__(self, node, stress_arg_lists):
        # keep track of each stressor and it's args in a list
        # of 2-tuples.
        self._node = node
        self._stressors = []
        for sal in stress_arg_lists:
            assert type(sal) == list, 'the type is ' + str(type(sal))
            stressor = StressMaker.Stressor(self._node, sal)
            stressor.start()
            self._stressors.append((stressor, sal))

    def is_alive(self):
        """
        Tells if any of the stressors are still running
        Remember that the first arg of each entry in self._stressors is a 
        Stressor object.
        """
        return reduce(lambda x, y: x or y[0].is_alive(), self._stressors, False)

    def join(self, time_limit_seconds=60):
        """
        Override the join() method to have a time limit - in case
        stress died and took the thread down with it.
        """
        print "joining..."
        start = time.time()
        while time.time() < start + time_limit_seconds:
            if not self.is_alive():
                break
            time.sleep(.1)
        for s in self._stressors:
            if s[0].is_alive():
                print "stress was still runing (or died) for node %s"\
                        " and args %s" % (self._node.name, s[1])
                s[0].terminate()
                print "Killed process."

class TestUpgrade(Tester):

    def __init__(self, *argv, **kwargs):
        super(TestUpgrade, self).__init__(*argv, **kwargs)
        self.allow_log_errors = True

    def rolling_upgrade_node(self, node, stress_node):
        """
        node is the node to upgrade. stress_ip is the node to run stress on.
        """
        print "Starting on upgrade procedure for node %s..." % node.name
        # put some load on the cluster. 
        args = [
                    [   '--operation=INSERT', '--family-type=Standard', 
                        '--num-keys=40000', '--consistency-level=TWO', 
                        '--average-size-values', '--create-index=KEYS', 
                        '--replication-factor=3', '--keep-trying=2',
                        '--threads=1', '--nodes='+stress_node.address()],
#                    [   '--operation=INSERT', '--family-type=Super', 
#                        '--num-keys=20000', '--consistency-level=TWO', 
#                        '--average-size-values', '--create-index=KEYS', 
#                        '--replication-factor=3', '--keep-trying=2',
#                        '--threads=1', '--nodes='+stress_node.address()],
#                    [   '--operation=COUNTER_ADD', '--family-type=Standard', 
#                        '--num-keys=10000', '--consistency-level=TWO', 
#                        '--replication-factor=3', '--keep-trying=2',
#                        '--threads=1', '--nodes='+stress_node.address()],
#                    [   '--operation=COUNTER_ADD', '--family-type=Super', 
#                        '--num-keys=1', '--consistency-level=TWO', 
#                        '--replication-factor=3', '--keep-trying=2',
#                        '--threads=1', '--nodes='+stress_node.address()],
                ]
        print "Starting stress."
        sm = StressMaker(stress_node, args)
        print "sleeping 10 seconds..."
        time.sleep(10) # make sure some data gets in before we shut down a node.
        print "Done sleeping"
    
        # Upgrade the node
        print "Upgrading!"
        print "draining..."
        node.nodetool('drain')
        print "sleeping 1"
        time.sleep(1)
        print "stopping..."
        node.stop(wait_other_notice=True)
        print "sleeping 1"
        time.sleep(1)
        print "setting dir..."
        try:
            node.set_cassandra_dir(cassandra_version="cassandra")
        except Exception, e:
            new_exception = Exception("Did you clone and compile cassandra in $HOME/.ccm/repository/ ?"\
                    " original exception: " + str(e))
            raise new_exception
        print "starting..."
        node.start(wait_other_notice=True)
        print "sleeping 1"
        time.sleep(1)
        print "scrubbing..."
        node.nodetool('scrub')

        if sm.is_alive():
            print "The stress continued through the entire upgrade!"
        else:
            print "Need to send more keys to stress"

        # wait for stress to finish.
        sm.join(120)


        print "Reading back with stress"
        args = [
                    [   '--operation=READ', '--family-type=Standard', 
                        '--num-keys=20000', '--consistency-level=ALL', 
                        '--threads=10', '--keep-trying=2',
                        '--nodes='+stress_node.address()],
                    [   '--operation=READ', '--family-type=Super', 
                        '--num-keys=20000', '--consistency-level=ALL', 
                        '--threads=10', '--keep-trying=2',
                        '--nodes='+stress_node.address()],
                    [   '--operation=COUNTER_GET', '--family-type=Standard', 
                        '--num-keys=10000', '--consistency-level=ALL', 
                        '--threads=10', '--keep-trying=2',
                        '--nodes='+stress_node.address()],
                    [   '--operation=COUNTER_GET', '--family-type=Super', 
                        '--num-keys=1', '--consistency-level=ALL', 
                        '--threads=10', '--keep-trying=2',
                        '--nodes='+stress_node.address()],
        ]
        sm = StressMaker(stress_node, args)
        print "Waiting up to 60 seconds for the stress process to quit"
        sm.join(60)

        print "Done upgrading node %s." % node.name

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

    def upgrade089_to_repo_test(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_cassandra_dir(cassandra_version="0.8.9")

        # Create a ring
        cluster.populate(3, tokens=[0, 2**125, 2**126]).start()
        [node1, node2, node3] = cluster.nodelist()
        cluster.start()

        time.sleep(.5)
        self.rolling_upgrade_node(node1, stress_node=node3)
#        self.rolling_upgrade_node(node2, stress_node=node3)
#        self.rolling_upgrade_node(node3, stress_node=node1)

        cluster.flush()

        cluster.cleanup()

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

