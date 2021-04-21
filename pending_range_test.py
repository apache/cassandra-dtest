import logging
import pytest
import re
import threading

from cassandra.query import SimpleStatement

from dtest import Tester, create_ks

logger = logging.getLogger(__name__)


@pytest.mark.no_vnodes
class TestPendingRangeMovements(Tester):

    @pytest.mark.resource_intensive
    def test_pending_range(self):
        """
        @jira_ticket CASSANDRA-10887
        """
        cluster = self.cluster
        # If we are on 2.1, we need to set the log level to debug or higher, as debug.log does not exist.
        if cluster.version() < '2.2':
            cluster.set_log_level('DEBUG')

        # Create 5 node cluster
        ring_delay_ms = 3_600_000  # 1 hour
        cluster.populate(5).start(jvm_args=['-Dcassandra.ring_delay_ms={}'.format(ring_delay_ms)])
        node1, node2 = cluster.nodelist()[0:2]

        # Set up RF=3 keyspace
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 3)

        session.execute("CREATE TABLE users (login text PRIMARY KEY, email text, name text, login_count int)")

        # We use the partition key 'jdoe3' because it belongs to node1.
        # The key MUST belong to node1 to repro the bug.
        session.execute("INSERT INTO users (login, email, name, login_count) VALUES ('jdoe3', 'jdoe@abc.com', 'Jane Doe', 1) IF NOT EXISTS;")

        lwt_query = SimpleStatement("UPDATE users SET email = 'janedoe@abc.com' WHERE login = 'jdoe3' IF email = 'jdoe@abc.com'")

        # Show we can execute LWT no problem
        for i in range(1000):
            session.execute(lwt_query)

        token = '-634023222112864484'

        mark = node1.mark_log()

        # Move a node without waiting for the response of nodetool, so we don't have to wait for ring_delay
        threading.Thread(target=(lambda: node1.nodetool('move {}'.format(token)))).start()

        # Watch the log so we know when the node is moving
        node1.watch_log_for('Moving .* to {}'.format(token), timeout=10, from_mark=mark)
        node1.watch_log_for('Sleeping {} ms before start streaming/fetching ranges'.format(ring_delay_ms),
                            timeout=10, from_mark=mark)

        # Watch the logs so we know when all the nodes see the status update to MOVING
        for node in cluster.nodelist():
            if cluster.version() >= '2.2':
                if cluster.version() >= '4.0':
                    node.watch_log_for('127.0.0.1:7000 state MOVING', timeout=10, filename='debug.log')
                else:
                    node.watch_log_for('127.0.0.1 state moving', timeout=10, filename='debug.log')
            else:
                # 2.1 doesn't have debug.log, so we are logging at trace, and look
                # in the system.log file
                node.watch_log_for('127.0.0.1 state moving', timeout=10, filename='system.log')

        # Once the node is MOVING, kill it immediately, let the other nodes notice
        node1.stop(gently=False, wait_other_notice=True)

        # Verify other nodes believe that the killed node is Down/Moving
        out, _, _ = node2.nodetool('ring')
        logger.debug("Nodetool Ring output: {}".format(out))
        assert re.search('127\.0\.0\.1.*?Down.*?Moving', out) is not None

        # Check we can still execute LWT
        for i in range(1000):
            session.execute(lwt_query)
