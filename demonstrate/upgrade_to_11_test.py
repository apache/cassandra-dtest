from dtest import Tester, debug
from tools import *

class TestUpgradeTo1_1(Tester):
    """
    Demonstrates https://issues.apache.org/jira/browse/CASSANDRA-4195
    """

    def upgrade_test(self):
        self.num_rows = 0
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_cassandra_dir(cassandra_version='1.0.9')

        cluster.populate(3).start()
        time.sleep(1)

        for node in cluster.nodelist():
            node.flush()
            time.sleep(.5)
            node.stop(wait_other_notice=True)
            node.set_cassandra_dir(cassandra_version='1.1.0')
            node.start(wait_other_notice=True)
            time.sleep(.5)

