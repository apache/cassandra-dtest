from dtest import Tester, debug
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
from ccmlib.node import TimeoutError
import random
import os

from tools import ThriftConnection

class TestUpgradeTo1_1(Tester):
    """
    upgrades a 3-node cluster through each of the above versions.
    """

    def upgrade_test(self):
        self.num_rows = 0
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_cassandra_dir(cassandra_version='1.0.9')

        # Create a ring
        cluster.populate(2).start()
        time.sleep(1)

        for node in self.cluster.nodelist():
            debug('upgrading node ' + node.name)
            node.flush()
            time.sleep(.5)
            node.stop(wait_other_notice=True)
            node.set_cassandra_dir(cassandra_version='1.1.0')
            node.start(wait_other_notice=True)
            time.sleep(.5)

