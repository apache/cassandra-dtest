from cassandra.query import SimpleStatement

from dtest import TRACE, Tester, debug
from tools import no_vnodes


@no_vnodes()
class TestPendingRangeMovements(Tester):

    def pending_range_test(self):
        """
        @jira_ticket CASSANDRA-10887
        """
        cluster = self.cluster
        # If we are on 2.1, we need to set the log level to debug or higher, as debug.log does not exist.
        if cluster.version() < '2.2' and not TRACE:
            cluster.set_log_level('DEBUG')

        # Create 5 node cluster
        cluster.populate(5).start(wait_for_binary_proto=True)
        node1, node2 = cluster.nodelist()[0:2]

        # Set up RF=3 keyspace
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 3)

        session.execute("CREATE TABLE users (login text PRIMARY KEY, email text, name text, login_count int)")

        # We use the partition key 'jdoe3' because it belongs to node1.
        # The key MUST belong to node1 to repro the bug.
        session.execute("INSERT INTO users (login, email, name, login_count) VALUES ('jdoe3', 'jdoe@abc.com', 'Jane Doe', 1) IF NOT EXISTS;")

        lwt_query = SimpleStatement("UPDATE users SET email = 'janedoe@abc.com' WHERE login = 'jdoe3' IF email = 'jdoe@abc.com'")

        # Show we can execute LWT no problem
        for i in xrange(1000):
            session.execute(lwt_query)

        token = '-634023222112864484'

        mark = node1.mark_log()

        # Move a node
        node1.nodetool('move {}'.format(token))

        # Watch the log so we know when the node is moving
        node1.watch_log_for('Moving .* to {}'.format(token), timeout=10, from_mark=mark)
        node1.watch_log_for('Sleeping 30000 ms before start streaming/fetching ranges', timeout=10, from_mark=mark)

        if cluster.version() >= '2.2':
            node2.watch_log_for('127.0.0.1 state moving', timeout=10, filename='debug.log')
        else:
            # 2.1 doesn't have debug.log, so we are logging at trace, and look
            # in the system.log file
            node2.watch_log_for('127.0.0.1 state moving', timeout=10, filename='system.log')

        # Once the node is MOVING, kill it immediately, let the other nodes notice
        node1.stop(gently=False, wait_other_notice=True)

        # Verify other nodes believe this is Down/Moving
        out, _, _ = node2.nodetool('ring')
        debug("Nodetool Ring output: {}".format(out))
        self.assertRegexpMatches(out, '127\.0\.0\.1.*?Down.*?Moving')

        # Check we can still execute LWT
        for i in xrange(1000):
            session.execute(lwt_query)
