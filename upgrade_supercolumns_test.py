from dtest import Tester, debug
from ccmlib.cluster import Cluster
from ccmlib.common import get_version_from_build
import random, os, time, re

# Tests upgrade between 1.2->2.0 for super columns (since that's where
# we removed then internally)
class TestSCUpgrade(Tester):

    def upgrade_with_index_creation_test(self):
        cluster = self.cluster

        # Forcing cluster version on purpose
        cluster.set_install_dir(version="1.2.16")
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

        CASSANDRA_DIR = os.environ.get('CASSANDRA_DIR')
        if get_version_from_build(CASSANDRA_DIR) >= '2.1':
            #Upgrade nodes to 2.0.
            #See CASSANDRA-7008
            self.upgrade_to_version("git:cassandra-2.0")
            time.sleep(.5)

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

    def upgrade_to_version(self, tag):
            """Upgrade Nodes - if *mixed_version* is True, only upgrade those nodes
            that are specified by *nodes*, otherwise ignore *nodes* specified
            and upgrade all nodes.
            """
            debug('Upgrading to ' + tag)
            nodes = self.cluster.nodelist()

            for node in nodes:
                debug('Shutting down node: ' + node.name)
                node.drain()
                node.watch_log_for("DRAINED")
                node.stop(wait_other_notice=False)

            # Update Cassandra Directory
            for node in nodes:
                node.set_install_dir(version=tag)
                debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
            self.cluster.set_install_dir(version=tag)

            # Restart nodes on new version
            for node in nodes:
                debug('Starting %s on new version (%s)' % (node.name, tag))
                # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
                node.set_log_level("INFO")
                node.start(wait_other_notice=True)
                node.nodetool('upgradesstables -a')

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
