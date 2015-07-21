import time
from nose.tools import timed
from cassandra import ReadTimeout
from cassandra import ConsistencyLevel as CL
from cassandra.query import SimpleStatement
from dtest import Tester, debug
from tools import no_vnodes, since
from threading import Event
from assertions import assert_invalid


class NotificationWaiter(object):
    """
    A helper class for waiting for pushed notifications from
    Cassandra over the native protocol.
    """

    def __init__(self, tester, node, notification_type):
        """
        `address` should be a ccmlib.node.Node instance
        `notification_type` should either be "TOPOLOGY_CHANGE", "STATUS_CHANGE",
        or "SCHEMA_CHANGE".
        """
        self.address = node.network_interfaces['binary'][0]
        self.notification_type = notification_type

        # get a single, new connection
        session = tester.patient_cql_connection(node)
        connection = session.cluster.connection_factory(self.address, is_control_connection=True)

        # coordinate with an Event
        self.event = Event()

        # the pushed notification
        self.notifications = []

        # register a callback for the notification type
        connection.register_watcher(
            notification_type, self.handle_notification, register_timeout=5.0)

    def handle_notification(self, notification):
        """
        Called when a notification is pushed from Cassandra.
        """

        debug("Source %s sent %s for %s" % (self.address, notification["change_type"], notification["address"][0],))

        self.notifications.append(notification)
        self.event.set()

    def wait_for_notifications(self, timeout, num_notifications=1):
        """
        Waits up to `timeout` seconds for notifications from Cassandra. If
        passed `num_notifications`, stop waiting when that many notifications
        are observed.
        """

        deadline = time.time() + timeout
        while time.time() < deadline:
            self.event.wait(deadline - time.time())
            self.event.clear()
            if len(self.notifications) >= num_notifications:
                break

        return self.notifications

    def clear_notifications(self):
        self.notifications = []
        self.event.clear()


class TestPushedNotifications(Tester):
    """
    Tests for pushed native protocol notification from Cassandra.
    """

    @no_vnodes()
    def move_single_node_test(self):
        """
        CASSANDRA-8516
        Moving a token should result in NODE_MOVED notifications.
        """
        self.cluster.populate(3).start(wait_for_binary_proto=True, wait_other_notice=True)

        # Despite waiting for each node to see the other nodes as UP, there is apparently
        # still a race condition that can result in NEW_NODE events being sent.  We don't
        # want to accidentally collect those, so for now we will just sleep a few seconds.
        time.sleep(3)

        waiters = [NotificationWaiter(self, node, "TOPOLOGY_CHANGE")
                   for node in self.cluster.nodes.values()]

        node1 = self.cluster.nodes.values()[0]
        node1.move("123")

        for waiter in waiters:
            debug("Waiting for notification from %s" % (waiter.address,))
            notifications = waiter.wait_for_notifications(60.0)
            self.assertEquals(1, len(notifications))
            notification = notifications[0]
            change_type = notification["change_type"]
            address, port = notification["address"]
            self.assertEquals("MOVED_NODE", change_type)
            self.assertEquals(self.get_ip_from_node(node1), address)

    def restart_node_test(self):
        """
        CASSANDRA-7816
        Restarting a node should generate exactly one DOWN and one UP notification
        """

        self.cluster.populate(2).start()
        node1, node2 = self.cluster.nodelist()

        waiter = NotificationWaiter(self, node1, "STATUS_CHANGE")

        for i in range(5):
            debug("Restarting second node...")
            node2.stop()
            node2.start()
            debug("Waiting for notifications from %s" % (waiter.address,))
            notifications = waiter.wait_for_notifications(timeout=60.0, num_notifications=2)
            self.assertEquals(2, len(notifications))
            self.assertEquals(self.get_ip_from_node(node2), notifications[0]["address"][0])
            self.assertEquals("DOWN", notifications[0]["change_type"])
            self.assertEquals(self.get_ip_from_node(node2), notifications[1]["address"][0])
            self.assertEquals("UP", notifications[1]["change_type"])
            waiter.clear_notifications()


class TestVariousNotifications(Tester):
    """
    Tests for various notifications/messages from Cassandra.
    """

    @since('2.2')
    def tombstone_failure_threshold_message_test(self):
        """
        Ensure nodes return an error message in case of TombstoneOverwhelmingExceptions rather
        than dropping the request. A drop makes the coordinator waits for the specified
        read_request_timeout_in_ms.
        @jira_ticket CASSANDRA-7886
        """

        self.allow_log_errors = True
        self.cluster.set_configuration_options(
            values={
                'tombstone_failure_threshold': 500,
                'read_request_timeout_in_ms': 30000,  # 30 seconds
                'range_request_timeout_in_ms': 40000
            }
        )
        self.cluster.populate(3).start()
        node1, node2, node3 = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)

        self.create_ks(session, 'test', 3)
        session.execute(
            "CREATE TABLE test ( "
            "id int, mytext text, col1 int, col2 int, col3 int, "
            "PRIMARY KEY (id, mytext) )"
        )

        # Add data with tombstones
        values = map(lambda i: str(i), range(1000))
        for value in values:
            session.execute(SimpleStatement(
                "insert into test (id, mytext, col1) values (1, '{}', null) ".format(
                    value
                ),
                consistency_level=CL.ALL
            ))

        failure_msg = ("Scanned over.* tombstones.* query aborted")

        @timed(25)
        def read_request_timeout_query():
            assert_invalid(
                session, SimpleStatement("select * from test where id in (1,2,3,4,5)", consistency_level=CL.ALL),
                expected=ReadTimeout,
            )

        read_request_timeout_query()

        failure = (node1.grep_log(failure_msg) or
                   node2.grep_log(failure_msg) or
                   node3.grep_log(failure_msg))

        self.assertTrue(failure, ("Cannot find tombstone failure threshold error in log "
                                  "after read_request_timeout_query"))
        mark1 = node1.mark_log()
        mark2 = node2.mark_log()
        mark3 = node3.mark_log()

        @timed(35)
        def range_request_timeout_query():
            assert_invalid(
                session, SimpleStatement("select * from test", consistency_level=CL.ALL),
                expected=ReadTimeout,
            )

        range_request_timeout_query()

        failure = (node1.watch_log_for(failure_msg, from_mark=mark1, timeout=5) or
                   node2.watch_log_for(failure_msg, from_mark=mark2, timeout=5) or
                   node3.watch_log_for(failure_msg, from_mark=mark3, timeout=5))

        self.assertTrue(failure, ("Cannot find tombstone failure threshold error in log "
                                  "after range_request_timeout_query"))
