from ccmlib.node import NodetoolError
from dtest import Tester
from tools import require


class TestNodetool(Tester):

    @require("8741")
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
