import os

from ccmlib.node import ToolError

from dtest import Tester, debug
from tools import since


class TestNodetool(Tester):

    def test_decommission_after_drain_is_invalid(self):
        """
        @jira_ticket CASSANDRA-8741

        Running a decommission after a drain should generate
        an unsupported operation message and exit with an error
        code (which we receive as a ToolError exception).
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]
        node.drain(block_on_log=True)

        try:
            node.decommission()
            self.assertFalse("Expected nodetool error")
        except ToolError as e:
            self.assertEqual('', e.stderr)
            self.assertTrue('Unsupported operation' in e.stdout)

    def test_correct_dc_rack_in_nodetool_info(self):
        """
        @jira_ticket CASSANDRA-10382

        Test that nodetool info returns the correct rack and dc
        """

        cluster = self.cluster
        cluster.populate([2, 2])
        cluster.set_configuration_options(values={'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch'})

        for i, node in enumerate(cluster.nodelist()):
            with open(os.path.join(node.get_conf_dir(), 'cassandra-rackdc.properties'), 'w') as snitch_file:
                for line in ["dc={}".format(node.data_center), "rack=rack{}".format(i % 2)]:
                    snitch_file.write(line + os.linesep)

        cluster.start(wait_for_binary_proto=True)

        for i, node in enumerate(cluster.nodelist()):
            out, err, _ = node.nodetool('info')
            self.assertEqual(0, len(err), err)
            debug(out)
            for line in out.split(os.linesep):
                if line.startswith('Data Center'):
                    self.assertTrue(line.endswith(node.data_center),
                                    "Expected dc {} for {} but got {}".format(node.data_center, node.address(), line.rsplit(None, 1)[-1]))
                elif line.startswith('Rack'):
                    rack = "rack{}".format(i % 2)
                    self.assertTrue(line.endswith(rack),
                                    "Expected rack {} for {} but got {}".format(rack, node.address(), line.rsplit(None, 1)[-1]))

    @since('3.4')
    def test_nodetool_timeout_commands(self):
        """
        @jira_ticket CASSANDRA-10953

        Test that nodetool gettimeout and settimeout work at a basic level
        """
        cluster = self.cluster
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]

        types = ('read', 'range', 'write', 'counterwrite', 'cascontention',
                 'truncate', 'streamingsocket', 'misc')

        # read all of the timeouts, make sure we get a sane response
        for timeout_type in types:
            out, err, _ = node.nodetool('gettimeout {}'.format(timeout_type))
            self.assertEqual(0, len(err), err)
            debug(out)
            self.assertRegexpMatches(out, r'.* \d+ ms')

        # set all of the timeouts to 123
        for timeout_type in types:
            _, err, _ = node.nodetool('settimeout {} 123'.format(timeout_type))
            self.assertEqual(0, len(err), err)

        # verify that they're all reported as 123
        for timeout_type in types:
            out, err, _ = node.nodetool('gettimeout {}'.format(timeout_type))
            self.assertEqual(0, len(err), err)
            debug(out)
            self.assertRegexpMatches(out, r'.* 123 ms')

    def test_meaningless_notice_in_status(self):
        """
        @jira_ticket CASSANDRA-10176

        nodetool status don't return ownership when there is more than one user keyspace
        define (since they likely have different replication infos making ownership
        meaningless in general) and shows a helpful notice as to why it does that.
        This test checks that said notice is only printed is there is indeed more than
        one user keyspace.
        """
        cluster = self.cluster
        cluster.populate([3]).start()

        node = cluster.nodelist()[0]

        notice_message = r'effective ownership information is meaningless'

        # Do a first try without any keypace, we shouldn't have the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session = self.patient_cql_connection(node)
        session.execute("CREATE KEYSPACE ks1 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 1 keyspace, we should still not get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session.execute("CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1}")

        # With 2 keyspaces with the same settings, we should not get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertNotRegexpMatches(out, notice_message)

        session.execute("CREATE KEYSPACE ks3 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':3}")

        # With a keyspace without the same replication factor, we should get the notice
        out, err, _ = node.nodetool('status')
        self.assertEqual(0, len(err), err)
        self.assertRegexpMatches(out, notice_message)
