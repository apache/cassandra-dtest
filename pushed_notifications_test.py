import time
from dtest import Tester, debug
from tools import no_vnodes
from threading import Event


class NotificationWaiter(object):
    """
    A helper class for waiting for pushed notifications from
    Cassandra over the native protocol.
    """

    def __init__(self, tester, node, notification_type, num_notifications=1):
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
        self.num_notifications = num_notifications

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

    def wait_for_notifications(self, timeout):
        """
        Waits up to `timeout` seconds for a notifications from Cassandra.
        """

        deadline = time.time() + timeout
        while time.time() < deadline:
            self.event.wait(deadline - time.time())
            self.event.clear()
            if len(self.notifications) >= self.num_notifications:
                break

        if len(self.notifications) < self.num_notifications:
            raise Exception("Timed out waiting for %s notifications from %s"
                            % (self.notification_type, self.address))
        else:
            return self.notifications


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
            notifications = waiter.wait_for_notifications(60.0)
            self.assertEquals(1, len(notifications))
            notification = notifications[0]
            change_type = notification["change_type"]
            address, port = notification["address"]
            self.assertEquals("MOVED_NODE", change_type)
            self.assertEquals(self.get_ip_from_node(node1), address)

    def restart_node_test(self):
        """
        Restarting a node should generate exactly one DOWN and one UP notification
        """        

        self.cluster.populate(2).start()

        node1, node2 = self.cluster.nodelist()

        waiter = NotificationWaiter(self, node1, "STATUS_CHANGE", num_notifications=2)

        for i in range(3):
            debug("Restarting second node...")
            node2.stop()
            node2.start()
            debug("Waiting for notifications from %s" % (waiter.address,))
            notifications = waiter.wait_for_notifications(60.0)
            self.assertEquals(2, len(notifications))
            self.assertEquals(self.get_ip_from_node(node2), notifications[0]["address"][0])
            self.assertEquals("DOWN", notifications[0]["change_type"])
            self.assertEquals(self.get_ip_from_node(node2), notifications[1]["address"][0])
            self.assertEquals("UP", notifications[1]["change_type"])



