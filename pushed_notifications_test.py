from dtest import Tester, debug
from tools import no_vnodes
from threading import Event


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
        connection = session.cluster.connection_factory(self.address)

        # coordinate with an Event
        self.event = Event()

        # the pushed notification
        self.notification = None

        # register a callback for the notification type
        connection.register_watcher(
            notification_type, self.handle_notification, register_timeout=5.0)

    def handle_notification(self, notification):
        """
        Called when a notification is pushed from Cassandra.
        """

        self.notification = notification
        self.event.set()

    def wait_for_notification(self, timeout):
        """
        Waits up to `timeout` seconds for a notification from Cassandra.
        """

        # Python 2.6 doesn't return None for timeouts only, so we need
        # to perform a different check
        self.event.wait(timeout)
        if self.notification is None:
            raise Exception("Timed out waiting for a %s notification from %s"
                            % (self.notification_type, self.address))
        else:
            return self.notification


class TestPushedNotifications(Tester):
    """
    Tests for pushed native protocol notification from Cassandra.
    """

    @no_vnodes()
    def move_single_node_test(self):
        """
        Moving a token should result in NODE_MOVED notifications.
        """
        self.cluster.populate(3).start()

        waiters = [NotificationWaiter(self, node, "TOPOLOGY_CHANGE")
                   for node in self.cluster.nodes.values()]

        node1 = self.cluster.nodes.values()[0]
        node1.move("123")

        for waiter in waiters:
            debug("Waiting for notification from %s" % (waiter.address,))
            notification = waiter.wait_for_notification(60.0)
            change_type = notification["change_type"]
            address, port = notification["address"]
            self.assertEquals("MOVED_NODE", change_type)
            self.assertEquals(self.get_ip_from_node(node1), address)
