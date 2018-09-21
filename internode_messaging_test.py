import pytest
import logging
import time

from dtest import Tester

since = pytest.mark.since
logger = logging.getLogger(__name__)

_LOG_ERR_ILLEGAL_CAPACITY = "Caused by: java.lang.IllegalArgumentException: Illegal Capacity: -1"


@since('4.0')
class TestInternodeMessaging(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            r'Illegal Capacity: -1',
            r'reported message size'
        )

    def test_message_corruption(self):
        """
        @jira_ticket CASSANDRA-14574

        Use byteman to corrupt an outgoing gossip ACK message, check that the recipient fails *once* on the message
        but does not spin out of control trying to process the rest of the bytes in the buffer.
        Then make sure normal messaging can occur after a reconnect (on a different socket, of course).
        """
        cluster = self.cluster
        cluster.populate(2, install_byteman=True)
        cluster.start(wait_other_notice=True)

        node1, node2 = cluster.nodelist()
        node1_log_mark = node1.mark_log()
        node2_log_mark = node2.mark_log()

        node2.byteman_submit(['./byteman/corrupt_internode_messages_gossip.btm'])

        # wait for the deserialization error to happen on node1
        time.sleep(10)
        assert len(node1.grep_log(_LOG_ERR_ILLEGAL_CAPACITY, from_mark=node1_log_mark)) == 1

        # now, make sure node2 reconnects (and continues gossiping).
        # node.watch_log_for() will time out if it cannot find the log entry
        assert node2.grep_log(r'successfully connected to 127.0.0.1:7000 \(GOSSIP\)',
                              from_mark=node2_log_mark, filename='debug.log')
