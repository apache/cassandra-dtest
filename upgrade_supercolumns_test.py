from dtest import Tester
from tools import *
from assertions import *
from ccmlib.cluster import Cluster
import random

# Tests upgrade between 1.2->2.0 for super columns (since that's where
# we removed then internally)
class TestSCUpgrade(Tester):

    def upgrade_with_index_creation_test(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_cassandra_dir(cassandra_version="1.2.16")
        cluster.populate(2).start()

        [node1, node2] = cluster.nodelist()

        cli = node1.cli()
        cli.do("create keyspace test with placement_strategy = 'SimpleStrategy' and strategy_options = {replication_factor : 2} and durable_writes = true")
        cli.do("use test")
        cli.do("create column family sc_test with column_type = 'Super' and comparator = 'UTF8Type' and subcomparator = 'UTF8Type' and default_validation_class = 'UTF8Type' and key_validation_class = 'UTF8Type'")

        for i in range(0, 2):
            for j in range(0, 2):
                cli.do("set sc_test['k0']['sc%d']['c%d'] = 'v'" % (i, j))

        assert not cli.has_errors(), cli.errors()
        cli.close()

        # Upgrade node 1
        node1.flush()
        time.sleep(.5)
        node1.stop(wait_other_notice=True)
        self.set_node_to_current_version(node1)
        node1.start(wait_other_notice=True)
        time.sleep(.5)

        cli = node1.cli()
        cli.do("use test")
        cli.do("consistencylevel as quorum")

        # Check we can still get data properly
        cli.do("get sc_test['k0']")
        assert_scs(cli, ['sc0', 'sc1'])
        assert_columns(cli, ['c0', 'c1'])

        cli.do("get sc_test['k0']['sc1']")
        assert_columns(cli, ['c0', 'c1'])

        cli.do("get sc_test['k0']['sc1']['c1']")
        assert_columns(cli, ['c1'])


def assert_scs(cli, names):
    assert not cli.has_errors(), cli.errors()
    output = cli.last_output()

    for name in names:
        assert re.search('super_column=%s' % name, output) is not None, 'Cannot find super column %s in %s' % (name, output)

def assert_columns(cli, names):
    assert not cli.has_errors(), cli.errors()
    output = cli.last_output()

    for name in names:
        assert re.search('name=%s' % name, output) is not None, 'Cannot find column %s in %s' % (name, output)
