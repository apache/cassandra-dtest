from ccmlib.node import NodetoolError
from dtest import Tester, debug

import os


class TestNodetool(Tester):

    def test_decommission_after_drain_is_invalid(self):
        """
        @jira_ticket CASSANDRA-8741

        Running a decommission after a drain should generate
        an unsupported operation message and exit with an error
        code (which we receive as a NodetoolError exception).
        """
        cluster = self.cluster
        cluster.populate([3]).start()
        version = cluster.version()

        node = cluster.nodelist()[0]
        node.drain(block_on_log=True)

        try:
            node.decommission()
            self.assertFalse("Expected nodetool error")
        except NodetoolError as e:
            if version >= "2.1":
                self.assertEqual('', e.stderr)
                self.assertTrue('Unsupported operation' in e.stdout)
            else:
                self.assertEqual('', e.stdout)
                self.assertTrue('Unsupported operation' in e.stderr)

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
            out, err = node.nodetool('info')
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
